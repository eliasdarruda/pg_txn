// Hypotheses about `id` ("Idempotent: an id that already ended returns its
// recorded output ... so a retried request is safe", clients/typescript/README.md):
//
// 1. Concurrent calls with the same id in one process share the owner, so
//    #ended (index.ts:435) lets both drive it. The second run finds the first
//    one's effect 'running' and prepare_effects (sql:320-330) fails it as
//    AmbiguousEffectOutcome: the effect ran once, both callers fail.
// 2. With keys, both calls reach txn.start and the second INSERT into
//    txn.transactions raises 23505 (sql:478).
// 3. run(name, input, { id }) never checks #ended (index.ts:394-401): a
//    finished id raises 23505 instead of returning the output.
// 4. Two processes: both see no row, both drive; the loser gets FencedError
//    instead of waiting for the output (index.ts:536).
// 5. A transaction without effects or keys leaves no record (transaction.test
//    asserts this), so its id is not idempotent at all: fn runs again.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, newOrder } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500 });
const other = newPgTxn(pool, { leaseMs: 1500 });

before(async () => {
  await pgtxn.ready();
  await other.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const settled = (r: PromiseSettledResult<unknown>) => (r.status === "fulfilled" ? r.value : `rejected: ${(r.reason as Error).name}: ${(r.reason as Error).message}`);

describe("idempotent ids", () => {
  test("concurrent calls with the same id (no keys, one process): the effect runs once and both callers get its output", async () => {
    const id = crypto.randomUUID();
    let calls = 0;
    const f = () => pgtxn.transaction(async (tx) => tx.effect(async () => { calls++; await sleep(300); return "receipt"; }), { id });
    const r = await Promise.allSettled([f(), sleep(100).then(f)]);
    assert.equal(calls, 1);
    assert.deepEqual(r.map(settled), ["receipt", "receipt"]);
  });

  test("concurrent calls with the same id and a key: no duplicate-key error", async () => {
    const id = crypto.randomUUID();
    const f = () => pgtxn.transaction(async (tx) => tx.effect(async () => { await sleep(300); return "receipt"; }), { id, key: `same:${id}` });
    const r = await Promise.allSettled([f(), f()]);
    assert.deepEqual(r.map(settled), ["receipt", "receipt"]);
  });

  test("a defined transaction run twice with the same id returns the recorded output", async () => {
    const pay = pgtxn.define("idem-pay", async (tx) => { await tx.effect(async () => 1); return "paid"; });
    const id = crypto.randomUUID();
    assert.equal(await pay({}, { id }), "paid");
    assert.equal(await pay({}, { id }), "paid");
  });

  test("two processes calling transaction({ id }) at once: one runs it, the other returns its output (not FencedError)", async () => {
    const id = crypto.randomUUID();
    let calls = 0;
    // the run reads for a while before its first effect: no durable record exists yet
    const f = (p: typeof pgtxn) => p.transaction(async (tx) => {
      await tx.db.query("SELECT pg_sleep(0.2)");
      return tx.effect(async () => { calls++; await sleep(300); return "receipt"; });
    }, { id });
    const r = await Promise.allSettled([f(pgtxn), sleep(100).then(() => f(other))]);
    assert.equal(calls, 1);
    assert.deepEqual(r.map(settled), ["receipt", "receipt"]);
  });

  test("an id is idempotent even when the transaction has no effects (a retried request must not write twice)", async () => {
    const id = crypto.randomUUID();
    const orderId = await newOrder(pool, { amount: 0 });
    const f = () => pgtxn.transaction(async (tx) => {
      await tx.db.query("UPDATE orders SET amount = amount + 1 WHERE id = $1", [orderId]);
      return "done";
    }, { id });
    assert.equal(await f(), "done");
    assert.equal(await f(), "done");
    assert.equal(Number((await pool.query("SELECT amount FROM orders WHERE id = $1", [orderId])).rows[0].amount), 1);
  });
});
