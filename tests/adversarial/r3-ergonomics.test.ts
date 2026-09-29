// Round 3: API ergonomics and small correctness issues of the TypeScript
// client, each with its hypothesis inline.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId, waitFor } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500, pollMs: 50 });

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

describe("close()", () => {
  // Hypothesis: close() (index.ts:819-831) counts every unfinished effect of
  // this process as "work in progress", a spawn with delayMs far in the
  // future included; it cannot run before its time, so close() sleeps for the
  // whole drainMs (30 s by default: the platform's stop timeout) and the
  // effect ends up EffectLost anyway. Expected: close() does not wait for
  // effects that are not due within the drain window.
  test("close() does not wait drainMs for a spawn delayed beyond the drain window", { timeout: 20_000 }, async () => {
    const p = newPgTxn(pool, { pollMs: 50 });
    await p.transaction(async (tx) => { await tx.spawn(async () => {}, { delayMs: 3_600_000 }); });
    const t0 = Date.now();
    await p.close(3_000);
    const took = Date.now() - t0;
    await pool.query("UPDATE txn.effects SET status = 'failed', completed_at = now() WHERE local_owner = $1 AND status = 'pending'", [p.owner]);
    assert.ok(took < 2_000, `close(3000) took ${took} ms waiting for a spawn due in an hour`);
  });
});

describe("option validation", () => {
  // Hypothesis: retry.attempts is not checked client-side (attemptsOf,
  // index.ts:35); the CHECK constraint on txn.effects.max_attempts (1..1000)
  // raises inside txn.prepare_effects, so the transaction FAILS (after
  // txn.fail_transaction) with a raw PostgreSQL constraint error rather than a
  // TypeError at the call. The Elixir client validates ([attempts: 1..1000]).
  test("retry: { attempts: 2000 } is rejected with a clear TypeError, not a constraint violation that fails the transaction", async () => {
    const id = crypto.randomUUID();
    let err: any;
    await pgtxn.transaction(async (tx) => tx.effect(async () => 1, { retry: { attempts: 2000 } }), { id }).catch((e) => { err = e; });
    const status = (await pool.query("SELECT status FROM txn.transactions WHERE id = $1", [id])).rows[0]?.status;
    assert.ok(err, "it must be refused");
    assert.ok(err instanceof TypeError && /attempts/.test(err.message),
      `got ${err.name} (${err.code ?? ""}): ${err.message}; transaction status: ${status}`);
  });

  // Hypothesis: keyText (index.ts:165-171) refuses numbers and booleans as
  // keys ("a key must be a string, an array or an object") while the Elixir
  // client accepts any durable value (key: 42 -> "42", key: true -> "true",
  // README: "any other durable value"). The same application ported between
  // the two clients behaves differently; the TypeScript type TxKey does not
  // allow number either, so this is at least an undocumented cross-client
  // difference. Also: a string key and a JSON key with the same text collide
  // by design ('["order",42]' as a string is ["order", 42]).
  test("holds: number and boolean keys are accepted, as in Elixir (42 is the key text \"42\"); null is refused; string/JSON key texts collide by design", async () => {
    assert.equal(await pgtxn.transaction(async () => 1, { key: 42 }), 1);
    assert.equal(await pgtxn.transaction(async () => 1, { keys: [true] }), 1);
    await assert.rejects((pgtxn as any).transaction(async () => 1, { key: null }), /a key must be a string, number, boolean, array or object/);
    const key = ["order", nextId()] as const;
    let overlapped = false;
    let inFirst = false;
    const a = pgtxn.transaction(async (tx) => { inFirst = true; await tx.effect(async () => { await sleep(500); }); inFirst = false; }, { key });
    await sleep(100);
    // the same text as a plain string: serialized with the array key
    await pgtxn.transaction(async () => { if (inFirst) overlapped = true; }, { key: JSON.stringify(key) });
    await a;
    assert.equal(overlapped, false, "'[\"order\",n]' as a string and [\"order\", n] are one key");
  });

  test("holds: serialize() error messages for keys name the offending value and path", async () => {
    const messages: string[] = [];
    for (const k of [new Map(), [() => 1], { a: Symbol("s") }, [new (class Foo {})()], [1, , 3], { d: new Date(NaN) }, [undefined]]) {
      await (pgtxn as any).transaction(async () => 1, { key: k }).then(() => {}, (e: any) => { messages.push(e.message); });
    }
    assert.equal(messages.length, 7, "every one is refused");
    assert.match(messages[0], /Map/);
    assert.match(messages[1], /function.*\[0\]/i);
    assert.match(messages[2], /symbol.*\ba\b/i);
    assert.match(messages[3], /Foo.*\[0\]/);
    assert.match(messages[4], /sparse.*\[1\]/);
    assert.match(messages[5], /invalid Date.*\bd\b/);
    assert.match(messages[6], /undefined/);
  });
});

describe("EffectContext.txId of a spawn", () => {
  // Hypothesis: txn.spawn (sql:219) links the spawn to the transaction only
  // if txn.transactions has a row for txn.current; an inline transaction
  // without keys, id or effects has none at that point, so its spawns get
  // tx_id NULL and the spawned function sees ctx.txId === null although it
  // was spawned inside a transaction (and txn.purge treats it as standalone,
  // deleting it by completed_at without its transaction). With an effect
  // before the spawn (or a key, or an id) ctx.txId is set. Undocumented.
  test("ctx.txId is the transaction id in a spawned function regardless of whether the transaction had an effect before it", async () => {
    let seen: unknown = "unset";
    let txId = "";
    await pgtxn.transaction(async (tx) => { txId = tx.id; await tx.spawn(async (ctx) => { seen = ctx.txId; }); });
    await waitFor(async () => seen !== "unset", "spawn");
    assert.equal(seen, txId, `ctx.txId is ${String(seen)} for a spawn in transaction ${txId} (no effect before it)`);
  });

  test("holds: with an effect before the spawn, ctx.txId is set", async () => {
    let seen: unknown = "unset";
    let txId = "";
    await pgtxn.transaction(async (tx) => { txId = tx.id; await tx.effect(async () => 1); await tx.spawn(async (ctx) => { seen = ctx.txId; }); });
    await waitFor(async () => seen !== "unset", "spawn");
    assert.equal(seen, txId);
  });
});

describe("idempotent ids and transient failures", () => {
  // Hypothesis (design gap, documented as "throws its error"): the outcome of
  // an id is final whatever ended it. A transaction that failed for a
  // TRANSIENT reason (here: a statement_timeout / a lost connection in the
  // user's function, before any effect ran) poisons the id: every later call
  // with the same id (a webhook redelivery, a client retry) throws the old
  // error and never processes the request, forever (until purge). The README
  // says "a retried request is safe"; it is refused. An abandoned transaction
  // (process crash before any effect) is the same. Expected: a transaction
  // that ended without any effect having run (nothing irreversible happened)
  // may be run again, or the docs say that ids are one-shot.
  test("an id whose transaction failed before any effect ran can be retried", async () => {
    const id = crypto.randomUUID();
    let attempts = 0;
    const handler = () => pgtxn.transaction(async (tx) => {
      attempts++;
      if (attempts === 1) {
        // a transient database error inside the function (a lock timeout on a hot row)
        await tx.db.query("SET LOCAL lock_timeout = '10ms'");
        const other = await pool.connect();
        try {
          await other.query("BEGIN");
          await other.query("SELECT 1 FROM txn.meta FOR UPDATE");
          await tx.db.query("SELECT 1 FROM txn.meta FOR UPDATE");
        } finally {
          await other.query("ROLLBACK");
          other.release();
        }
      }
      return "processed";
    }, { id });
    await assert.rejects(handler(), (e: any) => e.code === "55P03");
    const second = await handler().catch((e: Error) => `rejected: ${e.name}: ${e.message}`);
    assert.equal(second, "processed", `the retried request got ${second} (attempts: ${attempts})`);
  });
});

describe("hot-path statement counts", () => {
  // Not a bug: numbers for the report. Statements per pgtxn.transaction as
  // seen by the Db adapter (BEGIN/COMMIT not counted: they are the pool's).
  test("holds: statements per transaction: no effect / 1 effect / keyed / with id", async () => {
    let statements: string[] = [];
    // the worker's own periodic statements are not the transaction's
    const WORKER = /lease_effects|lease_transactions|worker_seen|abandon_expired|heartbeat/;
    const counting = {
      transaction: (fn: any, o: any) => pgtxn.db.transaction(fn, o),
      query: (trx: any, text: string, params: unknown[]) => { if (!WORKER.test(text)) statements.push(text.slice(0, 40)); return pgtxn.db.query(trx, text, params); },
    };
    const p = newPgTxn(counting as any, { pollMs: 1_000, listen: false });
    await p.ready();
    const count = async (f: () => Promise<unknown>) => { statements = []; await f(); console.log(statements); return statements.length; };
    const plain = await count(() => p.transaction(async (tx) => { await tx.db.query("SELECT 1"); }));
    const one = await count(() => p.transaction(async (tx) => { await tx.effect(async () => 1); }));
    const keyed = await count(() => p.transaction(async (tx) => { await tx.db.query("SELECT 1"); }, { key: `perf:${nextId()}` }));
    const withId = await count(() => p.transaction(async (tx) => { await tx.db.query("SELECT 1"); }, { id: crypto.randomUUID() }));
    await p.close(500);
    console.log(`statements per transaction (user query included): no effect ${plain}, one effect ${one}, keyed ${keyed}, with id ${withId}`);
    // one of them is #keepCompensations, run after EVERY transaction although
    // it can only matter when a compensation function was registered
    assert.ok(plain <= 4 && one <= 8 && keyed <= 5 && withId <= 5, `unexpected counts ${plain}/${one}/${keyed}/${withId}`);
  });
});
