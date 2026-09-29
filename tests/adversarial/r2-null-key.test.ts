// Hypothesis: keysOf (index.ts:163-167) maps a key with JSON.stringify, which
// returns undefined for undefined (a missing field: `keys: [order.parentId]`),
// so the text[] sent to txn.enqueue holds a NULL element. txn.enqueue stores it
// (no check). Every later txn.lease_transactions (sql:672) that considers this
// row calls txn._claim, whose INSERT INTO txn.keys (key NOT NULL) raises, which
// aborts the whole statement: no worker can lease ANY named transaction whose
// name is in its definitions while this row is one of the first p_max*4
// candidates. One bad enqueue stops all background work of every replica.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500 });

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  // the poisoned row would keep breaking every other suite's workers
  await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE status = 'running' AND name = 'r2-poison'");
  await closeAll();
  await pool.end();
});

describe("a NULL key", () => {
  test("enqueue() refuses a key that has no JSON text (undefined) instead of storing a NULL key", async () => {
    pgtxn.define("r2-poison", async () => "ok");
    // TypeScript's type does not allow it, but a missing field is a runtime value
    const enqueued = pgtxn.enqueue("r2-poison", {}, { keys: [undefined as unknown as string] });
    await assert.rejects(enqueued, "a NULL key was stored");
  });

  test("one enqueued transaction with a NULL key does not stop other named transactions from being leased", { timeout: 30_000 }, async () => {
    pgtxn.define("r2-poison", async () => "ok");
    pgtxn.define("r2-healthy", async () => "healthy");
    // stored directly, as the client above does when it is not stopped
    await pool.query("SELECT txn.enqueue('r2-poison', '{}', NULL, ARRAY[NULL]::text[], NULL)").catch(() => {});
    const poisoned = (await pool.query("SELECT count(*)::int AS n FROM txn.transactions WHERE name = 'r2-poison' AND status = 'running' AND array_position(keys, NULL) IS NOT NULL")).rows[0].n;
    if (!poisoned) return;   // txn.enqueue refused it: nothing to poison
    // lease_transactions itself raises on the poisoned candidate
    const leasing = pool.query("SELECT * FROM txn.lease_transactions(gen_random_uuid(), ARRAY['r2-poison', 'r2-healthy'], 16, 30000)");
    const leaseError = await leasing.then(() => null, (e: Error) => e.message);
    const id = await pgtxn.enqueue("r2-healthy", {});
    const out = await pgtxn.wait(id, 8_000).catch((e: Error) => `rejected: ${e.message}`);
    assert.equal(out, "healthy", `the healthy transaction did not run; lease_transactions: ${leaseError}`);
  });
});
