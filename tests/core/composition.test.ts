// How pg_txn composes: with ordinary database transactions around it, with
// many effects and spawns in one transaction, with partial failures, and
// with misuse (tx outside its transaction).
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { EffectFailedError, PermanentError, TransactionFailedError } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool);

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const status = async (id: number) => (await pool.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status;
const runsOf = async (txId: string) => (await pool.query("SELECT runs FROM txn.transactions WHERE id = $1", [txId])).rows[0]?.runs;

async function inTransaction(f: (c: import("pg").PoolClient) => Promise<void>, commit = true) {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    await f(c);
    await c.query(commit ? "COMMIT" : "ROLLBACK");
  } finally {
    c.release();
  }
}

describe("inside ordinary database transactions", () => {
  test("pgtxn.spawn(fn, { trx }): runs iff the surrounding transaction commits", async () => {
    const ran: string[] = [];
    await inTransaction(async (c) => { await pgtxn.spawn(async () => { ran.push("committed"); }, { trx: c }); });
    await inTransaction(async (c) => { await pgtxn.spawn(async () => { ran.push("rolled back"); }, { trx: c }); }, false);
    await waitFor(async () => ran.includes("committed"), "spawn after commit");
    await sleep(300);
    assert.deepEqual(ran, ["committed"]);
  });

  test("a spawn in a savepoint that is rolled back does not run, though the transaction commits", async () => {
    const ran: string[] = [];
    await inTransaction(async (c) => {
      await pgtxn.spawn(async () => { ran.push("kept"); }, { trx: c });
      await c.query("SAVEPOINT s");
      await pgtxn.spawn(async () => { ran.push("undone"); }, { trx: c });
      await c.query("ROLLBACK TO SAVEPOINT s");
    });
    await waitFor(async () => ran.includes("kept"), "the kept spawn");
    await sleep(300);
    assert.deepEqual(ran, ["kept"]);
  });

  test("pgtxn.enqueue(…, { trx }): queued iff the surrounding transaction commits", async () => {
    pgtxn.define("compose-enqueue", async (tx, { id }: { id: number }) => {
      await tx.db.query("UPDATE orders SET status = 'done' WHERE id = $1", [id]);
      return id;
    });
    const a = await newOrder(pool);
    const b = await newOrder(pool);
    let kept = "";
    let dropped = "";
    await inTransaction(async (c) => { kept = await pgtxn.enqueue("compose-enqueue", { id: a }, { trx: c }); });
    await inTransaction(async (c) => { dropped = await pgtxn.enqueue("compose-enqueue", { id: b }, { trx: c }); }, false);
    assert.equal(await pgtxn.wait(kept), a);
    assert.equal(await status(a), "done");
    assert.equal((await pool.query("SELECT 1 FROM txn.transactions WHERE id = $1", [dropped])).rowCount, 0);
    assert.equal(await status(b), "new");
  });

  test("pgtxn.transaction called inside another database transaction is independent of it", async () => {
    const id = await newOrder(pool);
    await inTransaction(async () => {
      await pgtxn.transaction(async (tx) => {
        await tx.effect(async () => 1);
        await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id]);
      });
    }, false);
    assert.equal(await status(id), "paid", "committed on its own connection; the outer rollback does not undo it");
  });
});

describe("many effects and spawns in one transaction", () => {
  test("every spawn runs once, after the commit, with its own idempotency key", async () => {
    const id = await newOrder(pool);
    const seen: { n: number; key: string; committed: string }[] = [];
    await pgtxn.transaction(async (tx) => {
      await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id]);
      for (let n = 0; n < 10; n++) {
        await tx.spawn(async (ctx) => { seen.push({ n, key: ctx.idempotencyKey, committed: await status(id) }); });
      }
    });
    await waitFor(async () => seen.length === 10, "10 spawns");
    await sleep(300);
    assert.equal(seen.length, 10);
    assert.deepEqual(seen.map((s) => s.n).sort((a, b) => a - b), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert.equal(new Set(seen.map((s) => s.key)).size, 10);
    assert.ok(seen.every((s) => s.committed === "paid"), "each spawn saw the committed write");
  });

  test("effects and spawns interleaved over several rounds: each effect once, each spawn once", async () => {
    const calls: string[] = [];
    const spawned: string[] = [];
    const txId = crypto.randomUUID();
    await pgtxn.transaction(async (tx) => {
      await tx.spawn(async () => { spawned.push("s0"); });
      const a = await tx.effect(async () => { calls.push("a"); return 1; });
      await tx.spawn(async () => { spawned.push(`s1:${a}`); });
      const b = await tx.effect(async () => { calls.push("b"); return a + 1; });
      await tx.spawn(async () => { spawned.push(`s2:${b}`); });
      const c = await tx.effect(async () => { calls.push("c"); return b + 1; });
      await tx.spawn(async () => { spawned.push(`s3:${c}`); });
    }, { id: txId });
    assert.deepEqual(calls, ["a", "b", "c"]);
    assert.equal(await runsOf(txId), 4);
    await waitFor(async () => spawned.length === 4, "4 spawns");
    await sleep(300);
    assert.deepEqual(spawned.sort(), ["s0", "s1:1", "s2:2", "s3:3"]);
  });

  test("an effect's result decides the spawns: one per item, no duplicates across runs", async () => {
    const sent: string[] = [];
    await pgtxn.transaction(async (tx) => {
      const recipients = await tx.effect(async () => ["ana", "bo", "cy"]);
      for (const r of recipients) await tx.spawn(async () => { sent.push(r); });
      await tx.effect(async () => "audit");
    });
    await waitFor(async () => sent.length === 3, "3 spawns");
    await sleep(300);
    assert.deepEqual(sent.sort(), ["ana", "bo", "cy"]);
  });

  test("a data-dependent number of effects: sequential ones take N+1 runs, parallel ones 2", async () => {
    const items = [1, 2, 3, 4, 5];
    const seqId = crypto.randomUUID();
    const parId = crypto.randomUUID();
    let calls = 0;
    const seq = await pgtxn.transaction(async (tx) => {
      let sum = 0;
      for (const i of items) sum += await tx.effect(async () => { calls++; return i * 10; });
      return sum;
    }, { id: seqId });
    const par = await pgtxn.transaction(async (tx) =>
      (await Promise.all(items.map((i) => tx.effect(async () => { calls++; return i * 10; })))).reduce((a, b) => a + b), { id: parId });
    assert.deepEqual([seq, par, calls], [150, 150, 10]);
    assert.deepEqual([await runsOf(seqId), await runsOf(parId)], [6, 2]);
  });

  test("one spawn failing does not affect the others or the commit", async () => {
    const id = await newOrder(pool);
    const ran: string[] = [];
    let bad = "";
    await pgtxn.transaction(async (tx) => {
      await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id]);
      await tx.spawn(async () => { ran.push("a"); });
      bad = await tx.spawn(async () => { throw new PermanentError("mail server said no"); });
      await tx.spawn(async () => { ran.push("c"); });
    });
    assert.equal(await status(id), "paid");
    await waitFor(async () => ran.length === 2, "the good spawns");
    const row = await waitFor(async () => (await pool.query("SELECT status, error->>'message' AS m FROM txn.effects WHERE id = $1 AND status = 'failed'", [bad])).rows[0], "the failed spawn");
    assert.equal(row.m, "mail server said no");
  });

  test("a failure after several effects and spawns: no spawn runs, each effect is compensated once", async () => {
    const spawned: string[] = [];
    const undone: string[] = [];
    await assert.rejects(pgtxn.transaction(async (tx) => {
      for (const name of ["charge", "reserve", "notify-partner"]) {
        await tx.effect(async () => name, { name, compensate: async (r) => { undone.push(r); } });
        await tx.spawn(async () => { spawned.push(name); });
      }
      throw new Error("out of stock");
    }), /out of stock/);
    await waitFor(async () => undone.length === 3, "3 compensations");
    await sleep(300);
    assert.deepEqual(undone.sort(), ["charge", "notify-partner", "reserve"]);
    assert.deepEqual(spawned, []);
  });

  test("parallel effects where one fails: the other's result is kept; catching the failure still commits", async () => {
    let okCalls = 0;
    const out = await pgtxn.transaction(async (tx) => {
      const [ok, bad] = await Promise.allSettled([
        tx.effect(async () => { okCalls++; return "reserved"; }, { name: "reserve" }),
        tx.effect(async () => { throw new PermanentError("card declined"); }, { name: "charge" }),
      ]);
      assert.ok(bad.status === "rejected" && bad.reason instanceof EffectFailedError);
      return ok.status === "fulfilled" ? ok.value : null;
    });
    assert.equal(out, "reserved");
    assert.equal(okCalls, 1);
  });
});

describe("ids and misuse", () => {
  test("an id that already committed returns its recorded output without running again", async () => {
    const id = crypto.randomUUID();
    let calls = 0;
    const f = async (tx: any) => { await tx.effect(async () => { calls++; }); return "receipt-7"; };
    assert.equal(await pgtxn.transaction(f, { id }), "receipt-7");
    assert.equal(await pgtxn.transaction(f, { id }), "receipt-7");
    assert.equal(calls, 1);
  });

  test("an id that already failed throws its error without running again", async () => {
    const id = crypto.randomUUID();
    let calls = 0;
    const f = async (tx: any) => { calls++; await tx.effect(async () => 1); throw new Error("nope"); };
    await assert.rejects(pgtxn.transaction(f, { id }), /nope/);
    const before = calls;
    await assert.rejects(pgtxn.transaction(f, { id }), TransactionFailedError);
    assert.equal(calls, before);
  });

  test("tx inside an effect's function is refused with a clear error", async () => {
    let error = "";
    await pgtxn.transaction(async (tx) => {
      await tx.effect(async () => {
        try {
          await tx.db.query("SELECT 1");
        } catch (e) {
          error = (e as Error).message;
        }
      });
    });
    assert.match(error, /cannot be used inside an effect/);
  });

  test("tx after its transaction ended is refused with a clear error", async () => {
    let leaked: any;
    await pgtxn.transaction(async (tx) => { leaked = tx; });
    assert.throws(() => leaked.db, /has ended/);
    await assert.rejects(leaked.effect(async () => 1), /has ended/);
  });
});
