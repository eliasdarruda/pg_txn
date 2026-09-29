// Round 3 soak: a mixed workload for 90 s on two PgTxn instances in this
// process plus one child process that is SIGKILLed every 8-14 s and
// restarted, then a settle period (lease + grace + a minute for EffectLost)
// and the global invariants. RUNTIME: about 3 minutes (90 s workload, ~15 s
// drain, ~70 s settle). Run it on its own:
//   node --test tests/adversarial/r3-soak.test.ts
//
// Workload (all replicas define "r3s-job"; the child also runs inline keyed
// transactions and enqueues jobs):
//   - inline keyed transactions per tenant with a flaky idempotent provider
//     effect (retry) + compensation + a spawn; 10% throw after the effect;
//   - enqueued jobs (named, keyed per tenant) with the same effect;
//   - idempotent-id transactions delivered twice;
//   - sagas of 3 effects failing at step 3 half of the time.
// The "external provider" is a table written outside the transaction
// (autocommit), idempotent by key, so the money can be audited across
// processes: every provider call is either used by a committed transaction,
// released by exactly one compensation, lost with its process (documented
// EffectLost, counted) or ambiguous (killed mid-call, counted).
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RetryableError, type Tx } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, sleep, PG_URL } from "../helpers.ts";
import type pg from "pg";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const WORKLOAD_MS = 90_000;
const LEASE = 3_000;
const TENANTS = 12;
const pool = makePool(20);
const A = newPgTxn(pool, { leaseMs: LEASE, pollMs: 100 });
const B = newPgTxn(pool, { leaseMs: LEASE, pollMs: 100 });
type T = Tx<pg.PoolClient>;

before(async () => {
  await A.ready();
  await B.ready();
  await pool.query(`DROP TABLE IF EXISTS r3s_provider, r3s_ledger, r3s_releases, r3s_spans, r3s_spawns;
    CREATE TABLE r3s_provider (idem_key text PRIMARY KEY, tenant text NOT NULL, calls int NOT NULL DEFAULT 1);
    CREATE TABLE r3s_ledger (tx_id uuid PRIMARY KEY, tenant text NOT NULL, kind text NOT NULL, idem_key text UNIQUE);
    CREATE TABLE r3s_releases (idem_key text PRIMARY KEY, n int NOT NULL DEFAULT 1);
    CREATE TABLE r3s_spans (tx_id uuid PRIMARY KEY, tenant text NOT NULL, started_at timestamptz NOT NULL, ended_at timestamptz);
    CREATE TABLE r3s_spawns (effect_id uuid PRIMARY KEY)`);
});
after(async () => {
  await closeAll();
  await pool.end();
});

// ---------------------------------------------------------------- the application code (parent side)
const tenantOf = (i: number) => `t${i % TENANTS}`;

async function providerEffect(tx: T, tenant: string) {
  return tx.effect(async (ctx) => {
    await pool.query("INSERT INTO r3s_spans (tx_id, tenant, started_at) VALUES ($1, $2, clock_timestamp()) ON CONFLICT DO NOTHING", [ctx.txId, tenant]);
    if (Math.random() < 0.2) throw new RetryableError("flaky provider", { retryAfterMs: 5 });
    await pool.query("INSERT INTO r3s_provider (idem_key, tenant) VALUES ($1, $2) ON CONFLICT (idem_key) DO UPDATE SET calls = r3s_provider.calls + 1", [ctx.idempotencyKey, tenant]);
    return ctx.idempotencyKey;
  }, {
    name: "provider", retry: { attempts: 30 },
    compensate: async (k: string) => { await pool.query("INSERT INTO r3s_releases (idem_key) VALUES ($1) ON CONFLICT (idem_key) DO UPDATE SET n = r3s_releases.n + 1", [k]); },
  });
}
async function commitUse(tx: T, tenant: string, kind: string, idemKey: string) {
  await tx.db.query("INSERT INTO r3s_ledger (tx_id, tenant, kind, idem_key) VALUES ($1, $2, $3, $4)", [tx.id, tenant, kind, idemKey]);
  await tx.db.query("UPDATE r3s_spans SET ended_at = clock_timestamp() WHERE tx_id = $1", [tx.id]);
  await tx.spawn(async (ctx) => { await pool.query("INSERT INTO r3s_spawns (effect_id) VALUES ($1) ON CONFLICT DO NOTHING", [ctx.effectId]); });
}
const jobBody = async (tx: T, input: { tenant: string }) => {
  const k = await providerEffect(tx, input.tenant);
  await commitUse(tx, input.tenant, "job", k);
  return k;
};
A.define("r3s-job", jobBody);
B.define("r3s-job", jobBody);

const inline = (p: typeof A, tenant: string) => p.transaction(async (tx: T) => {
  const k = await providerEffect(tx, tenant);
  if (Math.random() < 0.1) throw new Error("declined after the provider call");
  await commitUse(tx, tenant, "inline", k);
}, { key: ["tenant", tenant] });
const idempotent = (p: typeof A, id: string, tenant: string) => p.transaction(async (tx: T) => {
  const k = await providerEffect(tx, tenant);
  await commitUse(tx, tenant, "idem", k);
}, { id });
const saga = (p: typeof A, tenant: string) => p.transaction(async (tx: T) => {
  const k1 = await providerEffect(tx, tenant);
  const k2 = await tx.effect(async () => "step2", { name: "step2", compensate: async () => {} });
  if (Math.random() < 0.5) await tx.effect(async () => { throw new Error("step 3 refused"); }, { name: "step3" });
  await commitUse(tx, tenant, "saga", k1);
  return k2;
}, { key: ["tenant", tenant] });

// ---------------------------------------------------------------- the child process
const CHILD = `
  import pg from "pg";
  import { PgTxn, RetryableError } from ${JSON.stringify(path.join(ROOT, "clients/typescript/client/src/index.ts"))};
  const pool = new pg.Pool({ connectionString: ${JSON.stringify(PG_URL)}, max: 8 });
  pool.on("error", () => {});
  const pgtxn = new PgTxn(pool, { leaseMs: ${LEASE}, pollMs: 100, onError: () => {} });
  const TENANTS = ${TENANTS};
  const provider = (tx, tenant) => tx.effect(async (ctx) => {
    await pool.query("INSERT INTO r3s_spans (tx_id, tenant, started_at) VALUES ($1, $2, clock_timestamp()) ON CONFLICT DO NOTHING", [ctx.txId, tenant]);
    if (Math.random() < 0.2) throw new RetryableError("flaky provider", { retryAfterMs: 5 });
    await pool.query("INSERT INTO r3s_provider (idem_key, tenant) VALUES ($1, $2) ON CONFLICT (idem_key) DO UPDATE SET calls = r3s_provider.calls + 1", [ctx.idempotencyKey, tenant]);
    return ctx.idempotencyKey;
  }, { name: "provider", retry: { attempts: 30 },
       compensate: async (k) => { await pool.query("INSERT INTO r3s_releases (idem_key) VALUES ($1) ON CONFLICT (idem_key) DO UPDATE SET n = r3s_releases.n + 1", [k]); } });
  const use = async (tx, tenant, kind, k) => {
    await tx.db.query("INSERT INTO r3s_ledger (tx_id, tenant, kind, idem_key) VALUES ($1, $2, $3, $4)", [tx.id, tenant, kind, k]);
    await tx.db.query("UPDATE r3s_spans SET ended_at = clock_timestamp() WHERE tx_id = $1", [tx.id]);
    await tx.spawn(async (ctx) => { await pool.query("INSERT INTO r3s_spawns (effect_id) VALUES ($1) ON CONFLICT DO NOTHING", [ctx.effectId]); });
  };
  pgtxn.define("r3s-job", async (tx, input) => { const k = await provider(tx, input.tenant); await use(tx, input.tenant, "job", k); return k; });
  let i = Math.floor(Math.random() * 1000);
  for (;;) {
    const tenant = "t" + (i++ % TENANTS);
    pgtxn.transaction(async (tx) => {
      const k = await provider(tx, tenant);
      if (Math.random() < 0.1) throw new Error("declined");
      await use(tx, tenant, "inline", k);
    }, { key: ["tenant", tenant] }).catch(() => {});
    if (i % 3 === 0) pgtxn.enqueue("r3s-job", { tenant }, { key: ["tenant", tenant] }).catch(() => {});
    await new Promise((r) => setTimeout(r, 40));
  }
`;
function startChild(): ChildProcess {
  return spawn(process.execPath, ["--no-warnings", "--input-type=module", "-e", CHILD], { cwd: ROOT, stdio: ["ignore", "ignore", "inherit"] });
}
const kill = (c: ChildProcess) => new Promise<void>((r) => { if (c.exitCode !== null) return r(); c.once("exit", () => r()); c.kill("SIGKILL"); });

test(`soak: ${WORKLOAD_MS / 1000} s of mixed workload with a SIGKILLed child, then the global invariants`, { timeout: 6 * 60_000 }, async () => {
  const t0 = Date.now();
  const enqueued: string[] = [];
  const inflight = new Set<Promise<unknown>>();
  const outcomes: Record<string, number> = {};
  const track = (p: Promise<unknown>, label: string) => {
    const q = p.then(() => { outcomes[`${label}:ok`] = (outcomes[`${label}:ok`] ?? 0) + 1; },
      (e: Error) => { const k = `${label}:${e.name}`; outcomes[k] = (outcomes[k] ?? 0) + 1; }).finally(() => inflight.delete(q));
    inflight.add(q);
  };
  let child = startChild();
  let kills = 0;
  const killer = (async () => {
    while (Date.now() - t0 < WORKLOAD_MS) {
      await sleep(8_000 + Math.random() * 6_000);
      if (Date.now() - t0 >= WORKLOAD_MS) break;
      await kill(child);
      kills++;
      child = startChild();
    }
  })();
  let i = 0;
  while (Date.now() - t0 < WORKLOAD_MS) {
    if (inflight.size < 40) {
      const p = i % 2 ? A : B;
      const tenant = tenantOf(i);
      switch (i % 7) {
        case 0: case 1: case 2: track(inline(p, tenant), "inline"); break;
        case 3: track(p.enqueue("r3s-job", { tenant }, { key: ["tenant", tenant] }).then((id) => { enqueued.push(id); }), "enqueue"); break;
        case 4: { const id = crypto.randomUUID(); track(idempotent(A, id, tenant), "idem"); track(sleep(10).then(() => idempotent(B, id, tenant)), "idem-dup"); break; }
        case 5: track(saga(p, tenant), "saga"); break;
        case 6: track(p.enqueue("r3s-job", { tenant }).then((id) => { enqueued.push(id); }), "enqueue"); break;
      }
      i++;
    }
    await sleep(15);
  }
  await killer;
  await Promise.allSettled([...inflight]);
  const producedFor = Date.now() - t0;
  // the last child dies for good: its inline transactions are abandoned, its
  // named ones resumed, its spawns and compensations become EffectLost
  await kill(child);
  const childDied = Date.now();
  console.log(`soak: produced ${i} operations in ${producedFor} ms, child killed ${kills + 1} times; outcomes ${JSON.stringify(outcomes)}`);

  // settle: named work finishes, abandoned ones are swept (lease + 5 s grace + a 5 s sweep)
  const deadline = Date.now() + 90_000;
  for (;;) {
    const r = (await pool.query(`SELECT count(*) FILTER (WHERE status = 'running')::int AS running,
                                        count(*) FILTER (WHERE status = 'running' AND name IS NOT NULL)::int AS named_running
                                   FROM txn.transactions WHERE created_at > $1`, [new Date(t0)])).rows[0];
    if (r.running === 0) break;
    if (Date.now() > deadline) break;
    await sleep(500);
  }
  // fail_lost_effects needs the dead process unseen for a minute
  await sleep(Math.max(0, childDied + 66_000 - Date.now()));
  await sleep(6_000);   // one more sweep

  // ---------------------------------------------------------------- invariants
  const since = new Date(t0);
  const q = async (sql: string, params: unknown[] = [since]) => (await pool.query(sql, params)).rows;
  const [tx] = await q(`SELECT count(*) FILTER (WHERE status = 'running')::int AS running,
                               count(*) FILTER (WHERE status = 'committed')::int AS committed,
                               count(*) FILTER (WHERE status = 'failed')::int AS failed,
                               count(*) FILTER (WHERE status = 'abandoned')::int AS abandoned,
                               count(*) FILTER (WHERE name = 'r3s-job')::int AS jobs,
                               count(*) FILTER (WHERE name = 'r3s-job' AND status = 'committed')::int AS jobs_committed,
                               count(*) FILTER (WHERE name = 'r3s-job' AND status = 'failed')::int AS jobs_failed,
                               count(*) FILTER (WHERE name = 'r3s-job' AND status = 'abandoned')::int AS jobs_abandoned
                          FROM txn.transactions WHERE created_at > $1`);
  const [keys] = await q(`SELECT count(*)::int AS total,
                                 count(*) FILTER (WHERE t.status <> 'running')::int AS of_ended
                            FROM txn.keys k JOIN txn.transactions t ON t.id = k.tx_id`, []);
  const [eff] = await q(`SELECT count(*) FILTER (WHERE kind <> 'call' AND status IN ('pending', 'retry_wait', 'running'))::int AS open_local,
                                count(*) FILTER (WHERE kind <> 'call' AND status IN ('pending', 'retry_wait', 'running') AND created_at < now() - interval '1 minute')::int AS open_local_old,
                                count(*) FILTER (WHERE kind = 'call' AND status IN ('pending', 'retry_wait', 'running'))::int AS open_calls,
                                count(*) FILTER (WHERE kind = 'compensation' AND status = 'succeeded')::int AS comp_ok,
                                count(*) FILTER (WHERE kind = 'compensation' AND status = 'failed' AND error->>'name' = 'EffectLost')::int AS comp_lost,
                                count(*) FILTER (WHERE kind = 'compensation' AND status = 'failed' AND error->>'name' <> 'EffectLost')::int AS comp_failed_other,
                                count(*) FILTER (WHERE kind = 'spawn' AND status = 'succeeded')::int AS spawn_ok,
                                count(*) FILTER (WHERE kind = 'spawn' AND status = 'failed')::int AS spawn_failed,
                                count(*) FILTER (WHERE kind = 'call' AND name = 'provider' AND status = 'orphaned' AND result IS NULL)::int AS provider_orphaned_no_result
                           FROM txn.effects WHERE created_at > $1`);
  const [money] = await q(`SELECT (SELECT count(*) FROM r3s_provider)::int AS provider,
                                  (SELECT count(*) FROM r3s_ledger)::int AS ledger,
                                  (SELECT count(*) FROM r3s_releases)::int AS released,
                                  (SELECT count(*) FROM r3s_releases WHERE n <> 1)::int AS released_more_than_once,
                                  (SELECT count(*) FROM r3s_ledger l JOIN r3s_releases r ON r.idem_key = l.idem_key)::int AS used_and_released,
                                  (SELECT count(*) FROM r3s_ledger l WHERE NOT EXISTS (SELECT 1 FROM r3s_provider p WHERE p.idem_key = l.idem_key))::int AS used_without_call,
                                  (SELECT count(*) FROM r3s_provider p WHERE NOT EXISTS (SELECT 1 FROM r3s_ledger l WHERE l.idem_key = p.idem_key)
                                                                        AND NOT EXISTS (SELECT 1 FROM r3s_releases r WHERE r.idem_key = p.idem_key))::int AS unaccounted,
                                  (SELECT count(*) FROM r3s_provider p JOIN txn.effects e ON e.id::text = p.idem_key
                                     WHERE e.result IS NOT NULL AND e.status = 'orphaned'
                                       AND NOT EXISTS (SELECT 1 FROM r3s_ledger l WHERE l.idem_key = p.idem_key)
                                       AND NOT EXISTS (SELECT 1 FROM r3s_releases r WHERE r.idem_key = p.idem_key))::int AS unaccounted_lost_compensation,
                                  (SELECT count(*) FROM r3s_provider p JOIN txn.effects e ON e.id::text = p.idem_key
                                     WHERE e.result IS NULL
                                       AND NOT EXISTS (SELECT 1 FROM r3s_ledger l WHERE l.idem_key = p.idem_key)
                                       AND NOT EXISTS (SELECT 1 FROM r3s_releases r WHERE r.idem_key = p.idem_key))::int AS unaccounted_no_result,
                                  (SELECT count(*) FROM txn.effect_attempts a JOIN txn.effects e ON e.id = a.effect_id
                                     WHERE a.outcome = 'stale' AND a.error IS NULL AND e.name = 'provider')::int AS stale_successes`, []);
  // committed KEYED transactions of one tenant never overlap (idempotent-id
  // transactions and the keyless enqueues are not serialized: excluded)
  const overlaps = await q(`SELECT a.tenant, a.tx_id AS x, b.tx_id AS y FROM r3s_spans a JOIN r3s_spans b
                              ON a.tenant = b.tenant AND a.tx_id < b.tx_id
                             AND a.ended_at IS NOT NULL AND b.ended_at IS NOT NULL
                             AND a.started_at < b.ended_at AND b.started_at < a.ended_at
                             JOIN txn.transactions ta ON ta.id = a.tx_id AND ta.status = 'committed' AND ta.keys IS NOT NULL
                             JOIN txn.transactions tb ON tb.id = b.tx_id AND tb.status = 'committed' AND tb.keys IS NOT NULL
                           LIMIT 5`, []);
  const spawnRows = (await q("SELECT count(*)::int AS n FROM r3s_spawns", []))[0].n;
  const doctor = await q("SELECT check_name, status, detail FROM txn.doctor()", []);
  const report = { tx, keys, eff, money, spawnRows, overlaps: overlaps.length, kills: kills + 1, doctor };
  console.log(`soak report: ${JSON.stringify(report, null, 1)}`);

  // transactions
  assert.equal(tx.running, 0, "running transactions left after settle");
  assert.equal(tx.jobs_failed, 0, "an enqueued job failed (they only fail transiently)");
  assert.equal(tx.jobs_abandoned, 0, "a named transaction was abandoned");
  assert.equal(tx.jobs_committed, tx.jobs, "every enqueued job committed (the child's included: resumed by A/B)");
  // keys
  assert.equal(keys.of_ended, 0, "keys of ended transactions");
  assert.equal(keys.total, 0, "keys held with nothing running");
  // effects
  assert.equal(eff.open_calls, 0, "call effects left open");
  assert.equal(eff.open_local_old, 0, "spawns/compensations of dead owners older than a minute not marked EffectLost");
  assert.equal(eff.comp_failed_other, 0, "a compensation failed for a reason other than its process dying");
  // spawns: every succeeded spawn wrote its row; a row without a succeeded
  // spawn can only come from one killed between its write and
  // complete_effect (failed as ambiguous/lost: documented); never from a
  // rolled-back run
  assert.ok(spawnRows >= eff.spawn_ok && spawnRows <= eff.spawn_ok + eff.spawn_failed,
    `spawn rows ${spawnRows} vs ${eff.spawn_ok} succeeded + ${eff.spawn_failed} failed spawn effects (a spawn from a rolled-back run, or a lost write)`);
  // money
  assert.equal(money.used_and_released, 0, "a provider call both used by a committed transaction and refunded");
  assert.equal(money.released_more_than_once, 0, "a compensation ran twice");
  assert.equal(money.used_without_call, 0, "a ledger row for a provider call that never happened");
  assert.equal(money.provider, money.ledger + money.released + money.unaccounted, "accounting identity");
  // unaccounted calls: documented (compensation function died with the child;
  // process killed mid-call) — and the round-3 stale-success bug class
  assert.equal(money.unaccounted, money.unaccounted_lost_compensation + money.unaccounted_no_result, "every unaccounted call is explained");
  assert.equal(money.unaccounted_lost_compensation, eff.comp_lost, "each lost compensation is one unaccounted call");
  console.log(`soak: unaccounted provider calls ${money.unaccounted} = ${money.unaccounted_lost_compensation} lost compensations (documented) + ${money.unaccounted_no_result} killed mid-call/late (${money.stale_successes} of them completed in the late process: r3-stale-success)`);
  // keys exclude
  assert.equal(overlaps.length, 0, `committed keyed transactions of one tenant overlapped: ${JSON.stringify(overlaps)}`);
  // doctor: warnings are expected for lost effects (the killed child); not for stalls or due work
  const bad = doctor.filter((d) => d.status !== "ok" && !/lost effects|orphaned effects|workers/.test(d.check_name));
  assert.deepEqual(bad, [], "doctor warnings beyond the documented EffectLost");
});
