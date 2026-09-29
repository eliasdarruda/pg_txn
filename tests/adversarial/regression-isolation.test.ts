// Hypotheses about isolation levels:
//
// 1. The lease heartbeat (index.ts:454-456) UPDATEs the txn.transactions row
//    every max(1000, leaseMs / 3) ms, outside the run. txn.finish then does
//    SELECT ... FOR UPDATE on that row (sql:223). Under REPEATABLE READ or
//    SERIALIZABLE a row updated by a concurrent committed transaction after
//    the snapshot raises 40001 there, so any run that lasts longer than the
//    heartbeat interval and has a durable record (keys, or effects) can never
//    commit: it is retried up to 100 times and then fails.
// 2. The isolation option is not stored with the transaction: a named
//    transaction resumed by another process (index.ts:659, options `{}`) runs
//    at the default level, and enqueue() cannot set one at all.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, waitFor, nextId } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500 });   // heartbeat every 1000 ms

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

describe("isolation levels", () => {
  for (const isolation of ["repeatable read", "serializable"] as const) {
    test(`a keyed ${isolation} run longer than the heartbeat interval commits in one run`, async () => {
      let runs = 0;
      let out = "";
      try {
        out = await pgtxn.transaction(async (tx) => {
          runs++;
          if (runs > 3) throw new Error(`gave up after ${runs} runs`);
          await tx.db.query("SELECT pg_sleep(1.3)");
          return "committed";
        }, { key: `hb:${nextId()}`, isolation });
      } catch (e) {
        out = (e as Error).message;
      }
      assert.equal(out, "committed");
      assert.equal(runs, 1, "no serialization failure should come from pg_txn's own heartbeat");
    });
  }

  test("a named transaction resumed by another process keeps its isolation level", async () => {
    const a = newPgTxn(pool, { leaseMs: 30_000 });     // heartbeat far away: the takeover below is deterministic
    const b = newPgTxn(pool, { leaseMs: 1500 });
    const seen: Record<string, string> = {};
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const def = (who: string) => async (tx: any) => {
      seen[who] = (await tx.db.query("SHOW transaction_isolation")).rows[0].transaction_isolation;
      await tx.effect(async () => { if (who === "A") await gate; return 1; }, { retry: true });
      return "ok";
    };
    a.define("iso", def("A"));
    const id = crypto.randomUUID();
    const pa = a.run("iso", {}, { id, isolation: "serializable" }).catch((e) => e.name);
    await waitFor(async () => seen.A, "A's first run");
    // A stalls: its lease lapses and B, which defines the same name, resumes it
    await pool.query("UPDATE txn.transactions SET lease_until = now() - interval '1 second' WHERE id = $1", [id]);
    b.define("iso", def("B"));
    await waitFor(async () => seen.B, "B's run");
    release();
    await pa;
    assert.equal(seen.A, "serializable");
    assert.equal(seen.B, "serializable", "the resumed run must use the level the transaction was started with");
  });
});
