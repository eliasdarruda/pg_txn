// Horizontal scaling in containers with the published npm package: replicas
// of an application (Node on node:24-slim and node:24-alpine, Bun on
// oven/bun:1-alpine) share the transactions on a stock PostgreSQL. One
// replica is SIGKILLed mid-flight and others are stopped gracefully; every
// transaction commits exactly once and every effect reaches the receiver.
// Needs dist/npm (scripts/pack-npm.sh).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpSync, existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { makePool, waitFor, sleep } from "../helpers.ts";

const DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(DIR, "../..");
const PKGS = path.join(ROOT, "dist/npm");
const ACTORS = 20;
const JOBS = 60;
const pool = makePool(5, "postgres://app:app@localhost:55440/app");

const docker = (args: string[]) => execFileSync("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
const compose = (args: string[]) => docker(["compose", "-f", path.join(DIR, "compose.yml"), ...args]);
const one = async (sql: string) => Number(Object.values((await pool.query(sql)).rows[0])[0]);

before(async () => {
  const client = existsSync(PKGS) && readdirSync(PKGS).find((f) => f.startsWith("pg-txn-client-") && f.endsWith(".tgz"));
  if (!client) throw new Error("dist/npm is missing the packages: run scripts/pack-npm.sh");
  const ctx = mkdtempSync(path.join(os.tmpdir(), "pgtxn-app-"));
  try {
    cpSync(path.join(PKGS, client), path.join(ctx, "pkgs", client));
    cpSync(path.join(DIR, "app.ts"), path.join(ctx, "app.ts"));
    cpSync(path.join(DIR, "Dockerfile.app"), path.join(ctx, "Dockerfile"));
    for (const [tag, base] of [["slim", "node:24-slim"], ["alpine", "node:24-alpine"]]) {
      docker(["build", "-q", "-t", `pgtxn-containers-app:${tag}`, "--build-arg", `BASE=${base}`, ctx]);
    }
    cpSync(path.join(DIR, "Dockerfile.bun"), path.join(ctx, "Dockerfile"));
    docker(["build", "-q", "-t", "pgtxn-containers-app:bun", ctx]);
  } finally {
    rmSync(ctx, { recursive: true, force: true });
  }
  compose(["--profile", "app", "down", "-v", "--remove-orphans"]);
  compose(["up", "-d", "postgres", "receiver"]);
  await waitFor(async () => pool.query("SELECT 1").then(() => true, () => false), "postgres", 60_000);
  await pool.query("CREATE TABLE counters (id bigint PRIMARY KEY, n integer NOT NULL DEFAULT 0)");
  await pool.query("INSERT INTO counters (id) SELECT generate_series(1, $1)", [ACTORS]);
});

after(async () => {
  await pool.end();
  compose(["--profile", "app", "down", "-v", "--remove-orphans"]);
});

test("replicas share the transactions; a killed replica loses nothing; stopped ones drain", async (t) => {
  compose(["--profile", "app", "up", "-d", "--scale", "app=3", "app", "app-alpine", "app-bun"]);
  const replicas = compose(["ps", "-q", "app"]).trim().split("\n");
  const bun = compose(["ps", "-q", "app-bun"]).trim();
  assert.equal(replicas.length, 3);

  // every replica's worker is running transactions
  await waitFor(async () => (await one("SELECT count(*) FROM txn.transactions WHERE status = 'committed'").catch(() => 0)) >= 40,
    "work under way", 120_000);
  docker(["kill", "-s", "KILL", replicas[0]]);
  docker(["stop", "-t", "20", replicas[1]]);
  assert.equal(JSON.parse(docker(["inspect", "-f", "{{json .State}}", replicas[1]])).ExitCode, 0, "drained and exited");

  // everything that was enqueued finishes on the surviving replicas, including
  // the killed one's transactions (it may have died before enqueueing all of its own)
  await waitFor(async () => (await one("SELECT count(*) FROM txn.transactions WHERE status <> 'committed'")) === 0
    && (await one("SELECT count(*) FROM txn.transactions")) >= 4 * JOBS, "all transactions committed", 180_000);
  const total = await one("SELECT count(*) FROM txn.transactions WHERE status = 'committed'");
  assert.equal(await one("SELECT sum(n) FROM counters"), total, "every bump applied exactly once");

  // every effect reached the receiver, under its own idempotency key
  const effects = (await pool.query("SELECT id::text AS id FROM txn.effects WHERE kind = 'call' AND status = 'succeeded'")).rows.map((r) => r.id);
  assert.equal(effects.length, total);
  const stats = await (await fetch("http://localhost:55441/stats")).json() as { requests: number; keys: string[] };
  const seen = new Set(stats.keys);
  assert.deepEqual(effects.filter((id) => !seen.has(id)), [], "effects never delivered");
  const drivers = await one("SELECT count(DISTINCT lease_owner) FROM txn.effects WHERE kind = 'call'");
  assert.ok(drivers >= 3, `work spread over ${drivers} replicas`);

  const doctor = (await pool.query("SELECT status FROM txn.doctor() WHERE check_name = 'workers'")).rows[0];
  assert.equal(doctor.status, "ok");
  const resumed = await one("SELECT count(*) FROM txn.effect_attempts WHERE outcome = 'lease_expired'");
  t.diagnostic(`${total} transactions (of up to ${5 * JOBS} enqueued) from 5 replicas (3 Node slim, 1 Node alpine, 1 Bun) run by ${drivers} of them; `
    + `${stats.requests} HTTP requests for ${effects.length} effects; ${resumed} effect(s) re-run after the kill`);

  // Bun as PID 1 drains and exits cleanly too
  docker(["stop", "-t", "20", bun]);
  assert.equal(JSON.parse(docker(["inspect", "-f", "{{json .State}}", bun])).ExitCode, 0);
  await sleep(1);
});
