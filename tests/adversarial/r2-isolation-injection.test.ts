// Hypothesis: txn.transactions.isolation is free text (sql:48; txn.enqueue
// stores p_isolation as is, and the TypeScript enqueue() does not validate
// options.isolation). The worker passes the stored value to
// db.transaction({ isolation }) (index.ts:740) and pgDb splices it into
// `BEGIN ISOLATION LEVEL ${isolation.toUpperCase()}` (db.ts:44) as a
// parameterless query, i.e. the simple protocol, which runs several
// statements. Whoever can write that column (or reach enqueue() with a
// user-controlled isolation) runs SQL as the application on every replica
// that defines the name. The Elixir client validates the level before it
// interpolates (loop.ex isolation!); the SQL and the TypeScript client do not.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, newOrder } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500 });

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE status = 'running' AND name = 'r2-isolation'");
  await closeAll();
  await pool.end();
});

describe("isolation as stored text", () => {
  test("the isolation level of an enqueued transaction is validated before the worker splices it into BEGIN", { timeout: 20_000 }, async () => {
    const orderId = await newOrder(pool);
    pgtxn.define("r2-isolation", async () => "ran");
    // uppercased by db.ts; unquoted identifiers fold, so the payload survives it
    const payload = `read committed; update orders set note = 'pwned' where id = ${orderId}; --`;
    const stored = await pool.query("SELECT txn.enqueue('r2-isolation', '{}', NULL, NULL, $1) AS id", [payload]).then((r) => r.rows[0].id, () => null);
    if (stored) {
      await pgtxn.wait(stored, 10_000).catch(() => {});
    }
    const note = (await pool.query("SELECT note FROM orders WHERE id = $1", [orderId])).rows[0].note;
    assert.equal(note, null, `the worker ran SQL taken from txn.transactions.isolation (note = ${note})`);
  });

  test("enqueue({ isolation }) rejects a value that is not an isolation level", async () => {
    await assert.rejects(
      pgtxn.enqueue("r2-isolation-typed", {}, { isolation: "serializable; select 1" as never }),
      "an arbitrary string was accepted as an isolation level and stored",
    );
  });
});
