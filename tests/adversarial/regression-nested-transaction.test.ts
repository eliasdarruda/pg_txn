// Hypothesis: the "inEffect" AsyncLocalStorage flag that forbids using the
// *outer* tx inside an effect leaks into any pg_txn transaction started from
// inside an effect or a spawned function (index.ts:155, :208, :573). A spawned
// function that chains another pgtxn.transaction (the natural way to fan out
// after a commit) therefore fails with "tx cannot be used inside an effect's
// or a spawned function", although it uses its own, new tx.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500 });

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const status = async (id: number) => (await pool.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status;

describe("nesting a new pg_txn transaction inside effects and spawns", () => {
  test("a spawned function can run its own pgtxn.transaction", async () => {
    const id = await newOrder(pool);
    let error = "";
    await pgtxn.transaction(async (tx) => {
      await tx.spawn(async () => {
        try {
          await pgtxn.transaction(async (inner) => {
            await inner.db.query("UPDATE orders SET status = 'followed-up' WHERE id = $1", [id]);
          });
        } catch (e) {
          error = (e as Error).message;
        }
      });
    });
    await sleep(800);
    assert.equal(error, "", "the inner transaction is a new one: it must not be refused");
    assert.equal(await status(id), "followed-up");
  });

  test("an effect can run its own pgtxn.transaction (e.g. a call into another aggregate)", async () => {
    const id = await newOrder(pool);
    let error = "";
    await pgtxn.transaction(async (tx) => {
      await tx.effect(async () => {
        try {
          await pgtxn.transaction(async (inner) => {
            await inner.db.query("UPDATE orders SET status = 'side' WHERE id = $1", [id]);
          });
        } catch (e) {
          error = (e as Error).message;
        }
      });
    });
    assert.equal(error, "");
    assert.equal(await status(id), "side");
  });

  test("a spawned function can spawn again (pgtxn.spawn inside a spawn)", async () => {
    let inner = false;
    let error = "";
    await pgtxn.transaction(async (tx) => {
      await tx.spawn(async () => {
        try {
          await pgtxn.transaction(async (t2) => { await t2.spawn(async () => { inner = true; }); });
        } catch (e) {
          error = (e as Error).message;
        }
      });
    });
    await waitFor(async () => inner || error, "the inner spawn");
    assert.equal(error, "");
    assert.ok(inner);
  });
});
