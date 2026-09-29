// Hypothesis: when a process is late (its event loop was blocked by a
// synchronous effect, a GC pause, or it simply was not scheduled for longer
// than leaseMs + 5 s, 35 s by default) while an at-most-once effect is
// running, txn.abandon_expired (inline) or txn.lease_transactions (named)
// fences it. The effect's call then completes IN THE LATE PROCESS with a
// successful result (the charge went through), and txn.effect_done
// (sql:397-406) records the attempt as 'stale' and DISCARDS the result:
// txn.effects.result stays NULL, so
//   - txn._orphan (sql:291-293) schedules no compensation ("result IS NOT
//     NULL" is false), although the late process is alive and has the
//     compensation function (it is passed the effect id in #execute);
//   - txn.doctor's "orphaned effects" check (sql:856) requires result IS NOT
//     NULL, so it is silent;
//   - the effect_attempts 'stale' row has no result either (only an error
//     column).
// A successful charge is then neither used, nor refunded, nor reported.
// Expected: a late-but-successful outcome is kept as the effect's result (it
// is the only outcome that will ever exist for that idempotency key) and its
// compensation runs in the process that has the function; or, at the very
// least, it is stored in effect_attempts and reported by txn.doctor().
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId, waitFor, PG_URL } from "../helpers.ts";

const LEASE = 1500;
const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: LEASE });
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const effectRows = (id: string) => pool.query("SELECT status, result, error->>'name' AS error, compensation FROM txn.effects WHERE tx_id = $1 AND kind = 'call'", [id]);
const compensations = async (id: string) => (await pool.query("SELECT status FROM txn.effects WHERE tx_id = $1 AND kind = 'compensation'", [id])).rows;
const doctorWarnings = async () => (await pool.query("SELECT check_name, detail FROM txn.doctor() WHERE status <> 'ok'")).rows;

describe("a successful at-most-once effect whose process was late", () => {
  test("(lease expired mid-call, keyed inline) the charge's result is kept and its compensation runs", { timeout: 40_000 }, async () => {
    const id = crypto.randomUUID();
    const key = `late:${nextId()}`;
    const refunds: string[] = [];
    let charged = 0;
    const p = pgtxn.transaction(async (tx) => {
      await tx.effect(async () => {
        charged++;
        // a slow API; meanwhile this process "was late": its lease is expired
        // by the sweep of another replica (simulated here: abandon_expired on
        // an expired lease, the same statement any worker runs every 5 s)
        await sleep(1500);
        return `pay_${id.slice(0, 8)}`;
      }, { name: "charge", compensate: async (p) => { refunds.push(p); } });
      return "committed";
    }, { key, id }).then((v) => v, (e: Error) => `rejected: ${e.name}`);
    await sleep(400);
    // the process is late: from the database's point of view its lease lapsed
    // (heartbeats renew every max(1000, lease/3) ms; the expiry and the sweep
    // happen in one statement so the heartbeat cannot interleave)
    const c = await pool.connect();
    try {
      await c.query("BEGIN");
      await c.query("UPDATE txn.leases SET lease_until = now() - interval '10 seconds' WHERE tx_id = $1", [id]);
      await c.query("SELECT txn.abandon_expired()");
      await c.query("COMMIT");
    } finally {
      c.release();
    }
    const out = await p;
    assert.equal(out, "rejected: FencedError", "the late process must not commit");
    assert.equal(charged, 1);
    await sleep(1500);   // time for the worker to run a compensation, if any
    const [e] = (await effectRows(id)).rows;
    const comp = await compensations(id);
    const warnings = await doctorWarnings();
    const flagged = warnings.filter((w) => /orphan/.test(w.check_name));
    assert.ok(e.result !== null || comp.length > 0 || refunds.length > 0 || flagged.length > 0,
      `the charge succeeded (pay_${id.slice(0, 8)}) in this process but: effect row ${JSON.stringify(e)}, compensations ${JSON.stringify(comp)}, refunds ${JSON.stringify(refunds)}, doctor ${JSON.stringify(warnings)}`);
    assert.deepEqual(refunds, [`pay_${id.slice(0, 8)}`], "the compensation function is in this process and must run");
  });

  test("(lease expired mid-call, named, taken over by another worker) the result is not lost: ambiguous is not the right verdict when the outcome is known", { timeout: 40_000 }, async () => {
    const other = newPgTxn(pool, { leaseMs: LEASE, pollMs: 50 });
    const refunds: string[] = [];
    const calls: string[] = [];
    const body = async (tx: any, input: any) => {
      const p = await tx.effect(async () => { calls.push(input.who); await sleep(1500); return "pay_named"; },
        { name: "charge", compensate: async (r: string) => { refunds.push(r); } });
      await tx.db.query("SELECT 1");
      return p;
    };
    const settle = pgtxn.define("r3-late-named", body);
    other.define("r3-late-named", body);
    const id = crypto.randomUUID();
    const p = settle({ who: "first" }, { id }).then((v) => v, (e: Error) => `rejected: ${e.name}: ${e.message}`);
    await sleep(400);
    await pool.query("UPDATE txn.leases SET lease_until = now() - interval '10 seconds' WHERE tx_id = $1", [id]);
    // the other worker takes it over at its next poll (no grace for named transactions)
    const out = await p;
    const final = await waitFor(async () => {
      const r = (await pool.query("SELECT status, error FROM txn.transactions WHERE id = $1", [id])).rows[0];
      return r.status !== "running" ? r : null;
    }, "the transaction to end", 20_000);
    await sleep(1500);
    const [e] = (await effectRows(id)).rows;
    const comp = await compensations(id);
    // the first process's call did succeed (calls = ["first"]) and it holds the
    // compensation function; the takeover recorded AmbiguousEffectOutcome and
    // the result is gone
    assert.ok(e.result !== null || refunds.length > 0,
      `first process: ${out}; transaction ${JSON.stringify(final)}; effect ${JSON.stringify(e)}; compensations ${JSON.stringify(comp)}; calls ${JSON.stringify(calls)}; refunds ${JSON.stringify(refunds)}`);
  });

  test("(a real blocked event loop: a synchronous effect longer than lease + grace, in a child process) the charge is refunded or at least reported", { timeout: 60_000 }, async () => {
    const id = crypto.randomUUID();
    // a child whose effect is CPU-bound for lease + 5 s grace + a sweep: its heartbeats stop
    const code = `
      import pg from "pg";
      import { PgTxn } from ${JSON.stringify(path.join(ROOT, "clients/typescript/client/src/index.ts"))};
      const pool = new pg.Pool({ connectionString: ${JSON.stringify(PG_URL)}, max: 4 });
      pool.on("error", () => {});
      const pgtxn = new PgTxn(pool, { leaseMs: ${LEASE}, onError: () => {}, listen: false });
      const refunds = [];
      let out;
      try {
        out = await pgtxn.transaction(async (tx) => {
          await tx.effect(async () => {
            const until = Date.now() + ${LEASE + 5000 + 6500};
            while (Date.now() < until) { /* rendering a PDF synchronously */ }
            return "pay_blocked";
          }, { name: "charge", compensate: async (p) => { refunds.push(p); } });
          return "committed";
        }, { id: ${JSON.stringify(id)}, key: "blocked:" + ${JSON.stringify(id)} });
      } catch (e) { out = "rejected: " + e.name; }
      await new Promise((r) => setTimeout(r, 2000));
      console.log(JSON.stringify({ out, refunds }));
      await pgtxn.close(1000);
      await pool.end();
    `;
    const child = spawn(process.execPath, ["--no-warnings", "--input-type=module", "-e", code], { cwd: ROOT, stdio: ["ignore", "pipe", "inherit"] });
    let stdout = "";
    child.stdout.on("data", (d) => { stdout += d; });
    const exit = new Promise<number | null>((r) => child.on("exit", r));
    // this process's worker sweeps every 5 s (abandon_expired), like any replica
    await exit;
    const line = stdout.trim().split("\n").at(-1) ?? "{}";
    const r = JSON.parse(line);
    assert.equal(r.out, "rejected: FencedError", `child: ${line}`);
    const [e] = (await effectRows(id)).rows;
    const comp = await compensations(id);
    const warnings = await doctorWarnings();
    assert.ok(e.result !== null || r.refunds.length > 0 || warnings.some((w) => /orphan/.test(w.check_name)),
      `the charge succeeded in the child but: effect ${JSON.stringify(e)}, compensations ${JSON.stringify(comp)}, child refunds ${JSON.stringify(r.refunds)}, doctor ${JSON.stringify(warnings)}`);
  });
});
