// pg_txn on Kubernetes (k3s in Docker, see k3s.ts): an application
// Deployment whose pods each run their share of transactions (the PgTxn default),
// from the npm packages. A pod is SIGKILLed, all are rolled (SIGTERM) and scaled
// out while they work on shared actors; nothing is lost or applied twice.
// Needs the images pgtxn-containers-app:alpine and :bun (built by
// tests/containers) and a stock PostgreSQL (PG_IMAGE, default postgres:18).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { makePool, waitFor } from "../helpers.ts";
import { docker, ensureCluster, K3S, kubectl, loadImage, PORTS } from "./k3s.ts";

const DIR = path.dirname(fileURLToPath(import.meta.url));
const PG_IMAGE = process.env.PG_IMAGE ?? "postgres:18";
const APP_IMAGE = "pgtxn-containers-app:alpine";
const BUN_IMAGE = "pgtxn-containers-app:bun";
const NS = `pgtxn-${process.pid}`;
const ACTORS = 20;
const pool = makePool(5, `postgres://app:app@localhost:${PORTS.postgres[0]}/app`);
const one = async (sql: string) => Number(Object.values((await pool.query(sql)).rows[0])[0]);
const indent = (s: string, n: number) => s.split("\n").map((l) => " ".repeat(n) + l).join("\n");

function manifests(replicas: number): string {
  const receiver = readFileSync(path.join(DIR, "../containers/receiver.ts"), "utf8");
  const initdb = readFileSync(path.join(DIR, "../../docker/test-initdb/10-app-role.sh"), "utf8");
  return `
apiVersion: v1
kind: ConfigMap
metadata: { name: receiver }
data:
  receiver.ts: |
${indent(receiver, 4)}
---
apiVersion: v1
kind: ConfigMap
metadata: { name: initdb }
data:
  10-app-role.sh: |
${indent(initdb, 4)}
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: postgres }
spec:
  replicas: 1
  selector: { matchLabels: { app: postgres } }
  template:
    metadata: { labels: { app: postgres } }
    spec:
      terminationGracePeriodSeconds: 3
      containers:
        - name: postgres
          image: ${PG_IMAGE}
          imagePullPolicy: Never
          args: ["postgres", "-c", "max_connections=300"]
          env:
            - { name: POSTGRES_PASSWORD, value: postgres }
          readinessProbe:
            exec: { command: ["pg_isready", "-U", "app", "-d", "app"] }
            periodSeconds: 1
          volumeMounts: [{ name: initdb, mountPath: /docker-entrypoint-initdb.d }]
      volumes: [{ name: initdb, configMap: { name: initdb } }]
---
apiVersion: v1
kind: Service
metadata: { name: postgres }
spec:
  type: NodePort
  selector: { app: postgres }
  ports: [{ port: 5432, nodePort: ${PORTS.postgres[1]} }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: receiver }
spec:
  replicas: 1
  selector: { matchLabels: { app: receiver } }
  template:
    metadata: { labels: { app: receiver } }
    spec:
      # node as PID 1 without a SIGTERM handler would sit out the whole grace period
      terminationGracePeriodSeconds: 0
      containers:
        - name: receiver
          image: ${APP_IMAGE}
          imagePullPolicy: Never
          command: ["node", "/receiver/receiver.ts"]
          volumeMounts: [{ name: code, mountPath: /receiver }]
      volumes: [{ name: code, configMap: { name: receiver } }]
---
apiVersion: v1
kind: Service
metadata: { name: receiver }
spec:
  type: NodePort
  selector: { app: receiver }
  ports: [{ port: 8080, nodePort: ${PORTS.receiver[1]} }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: app }
spec:
  replicas: ${replicas}
  selector: { matchLabels: { app: app } }
  template:
    metadata: { labels: { app: app } }
    spec:
      terminationGracePeriodSeconds: 20
      containers:
        - name: app
          image: ${APP_IMAGE}
          imagePullPolicy: Never
          env:
            - { name: DATABASE_URL, value: "postgres://app:app@postgres:5432/app" }
            - { name: RECEIVER_URL, value: "http://receiver:8080" }
            - { name: JOBS, value: "100" }
            - { name: ACTORS, value: "${ACTORS}" }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: app-bun }
spec:
  replicas: ${replicas === 0 ? 0 : 1}
  selector: { matchLabels: { app: app-bun } }
  template:
    metadata: { labels: { app: app-bun } }
    spec:
      terminationGracePeriodSeconds: 20
      containers:
        - name: app
          image: ${BUN_IMAGE}
          imagePullPolicy: Never
          env:
            - { name: DATABASE_URL, value: "postgres://app:app@postgres:5432/app" }
            - { name: RECEIVER_URL, value: "http://receiver:8080" }
            - { name: JOBS, value: "100" }
            - { name: ACTORS, value: "${ACTORS}" }
`;
}

const deployment = (name: string) => kubectl(["-n", NS, "get", `deploy/${name}`, "-o", "json"]);
const appPods = () => kubectl(["-n", NS, "get", "pods", "-l", "app=app", "-o", "jsonpath={.items[*].metadata.name}"]).trim().split(/\s+/).filter(Boolean);

before(async () => {
  await ensureCluster();
  loadImage(PG_IMAGE);
  loadImage(APP_IMAGE);
  loadImage(BUN_IMAGE);
  // an earlier run's namespace (deleted in the background) still holds the NodePorts
  kubectl(["delete", "namespace", "-l", "pgtxn-test", "--wait=true", "--timeout=180s"]);
  kubectl(["create", "namespace", NS]);
  kubectl(["label", "namespace", NS, "pgtxn-test=true"]);
  // the application Deployment starts at 0 replicas: the schema comes first
  kubectl(["-n", NS, "apply", "-f", "-"], manifests(0));
  try {
    kubectl(["-n", NS, "wait", "--for=condition=available", "deploy/postgres", "deploy/receiver", "--timeout=180s"]);
  } catch (e) {
    const state = [["get", "pods", "-o", "wide"], ["describe", "pods", "-l", "app=postgres"], ["logs", "deploy/postgres", "--tail=30"]]
      .map((a) => { try { return kubectl(["-n", NS, ...a]); } catch (x) { return String(x); } }).join("\n");
    throw new Error(`${(e as Error).message}\n${state}`);
  }
  await waitFor(async () => pool.query("SELECT 1").then(() => true, () => false), "postgres", 60_000);
  await pool.query("CREATE TABLE counters (id bigint PRIMARY KEY, n integer NOT NULL DEFAULT 0)");
  await pool.query("INSERT INTO counters (id) SELECT generate_series(1, $1)", [ACTORS]);
});

after(async () => {
  await pool.end();
  kubectl(["delete", "namespace", NS, "--wait=false"]);
});

test("a Deployment survives a hard kill, a rollout and a scale-out; every transaction commits once", async (t) => {
  kubectl(["-n", NS, "scale", "deploy/app", "--replicas=3"]);
  kubectl(["-n", NS, "scale", "deploy/app-bun", "--replicas=1"]);
  await waitFor(async () => appPods().length === 3
    && (await one("SELECT count(*) FROM txn.transactions WHERE status = 'committed'").catch(() => 0)) >= 10, "work under way", 120_000);

  // a hard kill of one pod's application process (as an OOM kill would): no
  // SIGTERM; its transactions are resumed by the other pods once their leases
  // expire; kubelet restarts the container
  const victim = appPods()[0];
  let owner = "";
  const logs = (pod: string) => { try { return kubectl(["-n", NS, "logs", pod]); } catch { return ""; } };
  await waitFor(async () => (owner = /worker (\S+) started/.exec(logs(victim))?.[1] ?? "") !== "",
    "victim worker started", 60_000);
  await waitFor(async () => (await one(`SELECT count(*) FROM txn.effects WHERE status = 'running' AND lease_owner = '${owner}'`)) > 0,
    "victim calling effects", 60_000);
  const container = docker(["exec", K3S, "crictl", "ps", "-q", "--name", "^app$", "--label", `io.kubernetes.pod.name=${victim}`]).trim();
  const pid = container && String(JSON.parse(docker(["exec", K3S, "crictl", "inspect", container])).info.pid);
  assert.ok(pid, "found the victim's process");
  docker(["exec", K3S, "kill", "-9", pid]);
  // calls the killed process was in the middle of: the other pods must re-run them
  const inFlight = await one(`SELECT count(*) FROM txn.effects WHERE status = 'running' AND lease_owner = '${owner}'`);
  // a rolling restart: every pod gets SIGTERM and drains; new pods enqueue more work
  kubectl(["-n", NS, "rollout", "restart", "deploy/app"]);
  kubectl(["-n", NS, "rollout", "status", "deploy/app", "--timeout=180s"]);
  kubectl(["-n", NS, "scale", "deploy/app", "--replicas=5"]);
  kubectl(["-n", NS, "rollout", "status", "deploy/app", "--timeout=180s"]);

  let last = "";
  await waitFor(async () => {
    const running = await one("SELECT count(*) FROM txn.transactions WHERE status = 'running'");
    const pending = await one("SELECT count(*) FROM txn.effects WHERE status NOT IN ('succeeded')");
    const workers = await one("SELECT count(*) FROM txn.workers WHERE seen_at > now() - interval '20 seconds'");
    last = `running transactions ${running}, pending effects ${pending}, live workers ${workers}`;
    return running === 0 && pending === 0 && workers >= 6;
  }, "all work committed", 240_000).catch((e) => { throw new Error(`${e.message}: ${last}`); });

  const committed = await one("SELECT count(*) FROM txn.transactions WHERE status = 'committed'");
  assert.equal(await one("SELECT count(*) FROM txn.transactions WHERE status <> 'committed'"), 0);
  assert.equal(await one("SELECT sum(n) FROM counters"), committed, "every bump applied exactly once");
  const effects = (await pool.query("SELECT id::text AS id FROM txn.effects WHERE kind = 'call'")).rows.map((r) => r.id);
  assert.equal(effects.length, committed);
  const stats = await (await fetch(`http://localhost:${PORTS.receiver[0]}/stats`)).json() as { requests: number; keys: string[] };
  const seen = new Set(stats.keys);
  assert.deepEqual(effects.filter((id) => !seen.has(id)), [], "effects never delivered");
  const expired = await one("SELECT count(*) FROM txn.effect_attempts WHERE outcome = 'lease_expired'");
  assert.ok(expired >= inFlight, `the killed pod's ${inFlight} unfinished call(s) were re-run by other pods (${expired})`);
  const doctor = (await pool.query("SELECT status FROM txn.doctor() WHERE check_name = 'workers'")).rows[0];
  assert.equal(doctor.status, "ok");
  t.diagnostic(`${committed} transactions run by ${await one("SELECT count(DISTINCT lease_owner) FROM txn.effects")} pods `
    + `(3 Node pods + 1 Bun pod; 1 SIGKILLed while calling ${inFlight} effect(s), rolled, scaled to 5); `
    + `${stats.requests} HTTP requests for ${effects.length} effects; ${expired} call(s) re-run after the kill`);
});
