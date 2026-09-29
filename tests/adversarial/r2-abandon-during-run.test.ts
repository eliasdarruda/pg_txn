// Hypothesis: the lease is not renewed while a run is open (index.ts:493-502,
// "no heartbeat while a run is open", a round-1 fix). An inline transaction
// with a key or an id has a durable row from txn.start, so when one of its
// runs lasts longer than leaseMs + 5 s, txn.abandon_expired (sql:706) run by
// ANY worker (this process's own included) marks it abandoned, releases its
// keys and orphans its effects, although its owner is alive and mid-run.
// Consequences:
//   1. another transaction with the same key starts and commits while the
//      first one's run is still executing user code ("Nobody interleaves");
//   2. the first one's commit fails at txn.finish with 55P03/fenced, so
//      pgtxn.transaction() rejects with FencedError, an error documented for
//      named transactions driven by another process.
// The TypeScript README says to keep runs "well under leaseMs" but does not
// say what happens otherwise, and the Elixir README says nothing.
// A named transaction with a slow run is taken over by another worker
// (lease_transactions, sql:683) before it can commit; the other worker's run
// is just as slow, so the first one takes it back, and so on: with two or
// more replicas it never commits, and user code runs once per leaseMs
// forever (generation 15 after 20 s in the test). The caller gets FencedError.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId, waitFor } from "../helpers.ts";

const LEASE = 1500;
const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: LEASE });
const other = newPgTxn(pool, { leaseMs: LEASE });

before(async () => {
  await pgtxn.ready();
  await other.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

describe("runs longer than the lease", () => {
  test("a keyed inline transaction whose run outlasts leaseMs + 5 s still excludes other holders of its key and commits", { timeout: 60_000 }, async () => {
    const key = `slow-run:${nextId()}`;
    let firstInRun = false;
    let overlap = false;
    const first = pgtxn.transaction(async (tx) => {
      firstInRun = true;
      await tx.db.query("SELECT 1");
      await sleep(LEASE + 5_000 + 6_000);   // abandon_expired runs on a 5 s sweep: lease + grace + one sweep
      firstInRun = false;
      return "first";
    }, { key });
    await sleep(LEASE + 5_000 + 500);
    // a second transaction on the same key: it must wait for the first one
    const second = other.transaction(async () => {
      if (firstInRun) overlap = true;
      return "second";
    }, { key, }).catch((e: Error) => `rejected: ${e.name}`);
    const r = await Promise.allSettled([first, second]);
    const status = (await pool.query("SELECT status, error->>'name' AS error FROM txn.transactions WHERE keys @> $1::text[]", [[key]])).rows;
    assert.equal(overlap, false, `the second keyed transaction ran while the first one's run was still open (rows: ${JSON.stringify(status)})`);
    assert.deepEqual(r.map((x) => (x.status === "fulfilled" ? x.value : `rejected: ${(x.reason as Error).name}`)), ["first", "second"]);
  });

  test("a named transaction whose run outlasts leaseMs commits when two workers define it (observed: a takeover livelock, never commits)", { timeout: 60_000 }, async () => {
    let calls = 0;
    const body = async (tx: any) => {
      calls++;
      await tx.db.query("SELECT 1");
      await sleep(LEASE + 1500);
      return await tx.effect(async () => "paid", { name: "charge" });
    };
    const settle = pgtxn.define("r2-slow-named", body);
    other.define("r2-slow-named", body);
    const id = crypto.randomUUID();
    let out: unknown;
    try {
      out = await settle({}, { id });
    } catch (e) {
      out = `rejected: ${(e as Error).name}`;
    }
    // the transaction does commit (on the other worker), and user code ran twice
    // observed: it never ends. Each worker's lease expires during its run, the
    // other worker takes it over (lease_transactions), fences it, and runs it
    // again: a livelock in which user code runs every ~leaseMs on alternating
    // replicas (generation keeps growing) and nothing commits.
    await sleep(20_000);
    const row = (await pool.query("SELECT status, output, generation, runs FROM txn.transactions WHERE id = $1", [id])).rows[0];
    await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE id = $1 AND status = 'running'", [id]);
    assert.equal(row.status, "committed", `after 20 s: ${JSON.stringify(row)}, user code ran ${calls} times`);
    assert.ok(calls >= 2, `user code ran ${calls} time(s)`);
    assert.equal(out, "paid", `the caller got ${JSON.stringify(out)} although the transaction committed`);
  });
});
