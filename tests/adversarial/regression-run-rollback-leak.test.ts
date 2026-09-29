// Hypothesis: when a run aborts for a missing effect, tx.db is still usable.
// #check (index.ts:207-212) only refuses tx.db after `ended`, which is set
// long after pgDb.transaction rolled back and released the client
// (db.ts:44-51). A query issued by the user's function while the abort
// propagates — e.g. a sibling of the effect in Promise.all that is mid-query
// when the effect throws NeedEffect — is queued behind ROLLBACK and then runs
// on the released client, in autocommit: a write from a run that "was rolled
// back" is committed.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep } from "../helpers.ts";

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

describe("a run that is rolled back", () => {
  test("queries of a rolled-back run never commit, even those issued while the abort propagates", async () => {
    const id = await newOrder(pool);
    let runs = 0;
    let stray: unknown = "not attempted";
    await pgtxn.transaction(async (tx) => {
      runs++;
      if (runs === 1) {
        await Promise.all([
          tx.effect(async () => "charged"),
          (async () => {
            await tx.db.query("SELECT pg_sleep(0.2)");
            // still "inside the run" from the user's point of view
            stray = await tx.db.query("UPDATE orders SET status = 'stray' WHERE id = $1", [id]).then(() => "ran", (e) => (e as Error).message);
          })(),
        ]);
      } else {
        await tx.effect(async () => "charged");
      }
    });
    await sleep(300);
    const status = (await pool.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status;
    assert.equal(status, "new", `the write of a rolled-back run committed (query outcome: ${stray})`);
  });
});
