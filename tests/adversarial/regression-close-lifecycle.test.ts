// Hypotheses about the worker lifecycle:
//
// 1. close() (index.ts:723) stops the worker and waits only for #busy (work
//    the worker leased). A pgtxn.transaction() in flight is neither waited for
//    nor refused: it commits after the worker stopped, and its spawns and
//    compensations stay 'pending' forever in this process (EffectLost after a
//    minute, once txn.workers goes stale). Nothing is reported to the caller.
//    README: "await pgtxn.close() lets in-flight work finish".
// 2. transaction() after close() still works and loses its spawns silently.
// 3. #sweepLocal (index.ts:682-692) forgets the function of a pgtxn.spawn
//    registered more than 30 s ago whose row is not visible yet: a spawn
//    inside a user transaction (`{ trx }`) that lasts longer than that is
//    committed without its function and fails as EffectLost.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, waitFor } from "../helpers.ts";

const pool = makePool(10);

before(async () => {
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const effect = async (id: string) => (await pool.query("SELECT status, error->>'name' AS error FROM txn.effects WHERE id = $1", [id])).rows[0];

describe("close() and in-flight transactions", () => {
  test("close() drains a transaction in flight: its spawn and compensation still run", async () => {
    const p = newPgTxn(pool, { leaseMs: 1500 });   // closed by closeAll() too, so a failing assertion cannot leave the worker alive
    await p.ready();
    let spawned = false;
    let compensated = false;
    let spawnId = "";
    let run = 0;
    const inflight = p.transaction(async (tx) => {
      run++;
      await tx.effect(async () => "used", { name: "a" });
      // b is first reached in run 2 (deps [2]); run 3 reaches it with deps [3]: the run-2 call is orphaned at commit and must be compensated
      await tx.effect(async () => { await sleep(600); return "orphan"; }, { name: "b", deps: [Math.min(run, 3)], compensate: async () => { compensated = true; } });
      spawnId = await tx.spawn(async () => { spawned = true; });
    });
    await sleep(200);
    await p.close();          // graceful shutdown while the transaction is mid-effect
    await inflight;           // it commits anyway
    await sleep(1500);
    assert.ok(spawned, `the spawn of a transaction that committed during close() ran (row: ${JSON.stringify(await effect(spawnId))})`);
    assert.ok(compensated, "the orphaned effect of that transaction was compensated");
  });

  test("transaction() after close() either refuses or still runs its spawns; it must not lose them silently", async () => {
    const p = newPgTxn(pool, { leaseMs: 1500 });   // closed by closeAll() too, so a failing assertion cannot leave the worker alive
    await p.ready();
    await p.close();
    let spawned = false;
    let spawnId = "";
    let refused = false;
    try {
      await p.transaction(async (tx) => { spawnId = await tx.spawn(async () => { spawned = true; }); });
    } catch {
      refused = true;
    }
    await sleep(1000);
    assert.ok(refused || spawned, `neither refused nor run (row: ${spawnId && JSON.stringify(await effect(spawnId))})`);
  });
});

describe("pgtxn.spawn in a long user transaction", () => {
  test("a spawn in a user transaction open for more than 30 s still runs after the commit", { timeout: 90_000 }, async () => {
    const p = newPgTxn(pool, { leaseMs: 1500 });   // closed by closeAll() too, so a failing assertion cannot leave the worker alive
    await p.ready();
    let ran = false;
    const c = await pool.connect();
    let id = "";
    try {
      await c.query("BEGIN");
      id = await p.spawn(async () => { ran = true; }, { trx: c });
      await sleep(41_000);    // a long import; the worker sweeps every 5 s and forgets locals older than 30 s
      await c.query("COMMIT");
    } finally {
      c.release();
    }
    await sleep(2000);
    // observed: the row stays 'pending' forever. The worker never even leases it, because lease_effects
    // is only called while #local is non-empty (index.ts:635), and the function was swept.
    assert.ok(ran, `the spawned function did not run: ${JSON.stringify(await effect(id))}`);
  });
});
