// A transaction runs as ordinary database transactions ("runs"); effects are
// called between runs, once, and their recorded results reused.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import {
  EffectFailedError, PermanentError, RetryableError,
} from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor, nextId } from "../helpers.ts";

const pool = makePool(10);
const compensated: unknown[] = [];
const pgtxn = newPgTxn(pool);

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const status = async (id: string) => (await pool.query("SELECT status, runs FROM txn.transactions WHERE id = $1", [id])).rows[0];

describe("transactions", () => {
  test("without effects (and no id or key): one run, an ordinary transaction, nothing recorded", async () => {
    const id = await newOrder(pool);
    let runs = 0;
    const count = async () => (await pool.query("SELECT count(*)::int AS n FROM txn.transactions")).rows[0].n;
    const before = await count();
    const out = await pgtxn.transaction(async (tx) => {
      runs++;
      await tx.db.query("UPDATE orders SET status = 'seen' WHERE id = $1", [id]);
      return "done";
    });
    assert.equal(out, "done");
    assert.equal(runs, 1);
    assert.equal((await pool.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status, "seen");
    assert.equal(await count(), before, "no durable record for a transaction without effects");
  });

  test("N sequential effects: N+1 runs, each effect called once, results reused", async () => {
    const calls: Record<string, number> = { a: 0, b: 0, c: 0 };
    let runs = 0;
    const out = await pgtxn.transaction(async (tx) => {
      runs++;
      const a = await tx.effect(async () => { calls.a++; return 1 + 1; });
      const b = await tx.effect(async () => { calls.b++; return a * 10; });
      const c = await tx.effect(async () => { calls.c++; return `v${b}`; });
      return [a, b, c];
    });
    assert.deepEqual(out, [2, 20, "v20"]);
    assert.equal(runs, 4);
    assert.deepEqual(calls, { a: 1, b: 1, c: 1 });
  });

  test("effects started together run together: one extra run", async () => {
    let runs = 0;
    const started: number[] = [];
    const t0 = Date.now();
    const out = await pgtxn.transaction(async (tx) => {
      runs++;
      return Promise.all([1, 2, 3].map((n) =>
        tx.effect(async () => { started.push(Date.now() - t0); await sleep(300); return n * 2; }, { name: `p${n}` })));
    });
    assert.deepEqual(out, [2, 4, 6]);
    assert.equal(runs, 2);
    assert.equal(started.length, 3);
    assert.ok(Math.max(...started) - Math.min(...started) < 200, `effects ran concurrently (${started})`);
  });

  test("values round-trip: bigint, Date, bytes, nested", async () => {
    const value = { big: 18_446_744_073_709_551_616n, at: new Date("2026-01-02T03:04:05.678Z"), bytes: new Uint8Array([1, 2, 255]), list: [1, { x: "y" }] };
    const out = await pgtxn.transaction(async (tx) => tx.effect(async () => value));
    assert.deepEqual(out, value);
  });

  test("with a key, a re-run that calls the effect differently gets a new effect; the old one is compensated", async () => {
    let run = 0;
    const seen: number[] = [];
    await pgtxn.transaction(async (tx) => {
      run++;
      // input changes between runs (as if the data it came from changed)
      const amount = run === 1 ? 10 : 20;
      await tx.effect(async () => { seen.push(amount); return amount; }, { name: "charge", deps: { amount }, compensate: async (charged) => { compensated.push({ charged }); } });
    });
    assert.deepEqual(seen, [10, 20]);
    await waitFor(async () => compensated.find((c: any) => c.charged === 10), "refund of the unused charge");
  });

  test("a permanent failure is thrown into the transaction, which can handle it", async () => {
    let tries = 0;
    const out = await pgtxn.transaction(async (tx) => {
      try {
        await tx.effect(async () => { tries++; throw new PermanentError("card declined"); });
        return "charged";
      } catch (e) {
        assert.ok(e instanceof EffectFailedError);
        assert.match(e.message, /card declined/);
        return "declined";
      }
    });
    assert.equal(out, "declined");
    assert.equal(tries, 1);
  });

  test("an unhandled failure fails the transaction and compensates what already ran", async () => {
    const txId = crypto.randomUUID();
    const orderId = await newOrder(pool);
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.db.query("UPDATE orders SET status = 'charging' WHERE id = $1", [orderId]);
      const p = await tx.effect(async () => ({ id: "pay_1" }), { name: "charge", deps: { orderId }, compensate: async (p) => { compensated.push({ orderId, p }); } });
      await tx.effect(async () => { throw new PermanentError("warehouse closed"); }, { name: "ship" });
      return p;
    }, { id: txId }), (e: unknown) => e instanceof EffectFailedError);
    assert.equal((await status(txId)).status, "failed");
    assert.equal((await pool.query("SELECT status FROM orders WHERE id = $1", [orderId])).rows[0].status, "new", "no write committed");
    await waitFor(async () => compensated.find((c: any) => c.orderId === orderId && c.p.id === "pay_1"), "refund");
  });

  test("by default an effect is called once: no failure is retried, not even a RetryableError", async () => {
    let a = 0;
    let b = 0;
    await assert.rejects(pgtxn.transaction(async (tx) =>
      tx.effect(async () => { a++; throw new Error("503"); })), EffectFailedError);
    await assert.rejects(pgtxn.transaction(async (tx) =>
      tx.effect(async () => { b++; throw new RetryableError("429", { retryAfterMs: 10 }); })), EffectFailedError);
    assert.deepEqual([a, b], [1, 1]);
  });

  test("with retry, failures are retried with backoff until success", async () => {
    let tries = 0;
    const t0 = Date.now();
    const out = await pgtxn.transaction(async (tx) =>
      tx.effect(async () => {
        if (++tries < 3) throw new Error("503");
        return "ok";
      }, { retry: true }));
    assert.equal(out, "ok");
    assert.equal(tries, 3);
    assert.ok(Date.now() - t0 >= 550, "backoff 200 + 400 ms");
  });

  test("RetryableError sets the delay; max attempts ends in failure", async () => {
    let tries = 0;
    const t0 = Date.now();
    await assert.rejects(pgtxn.transaction(async (tx) =>
      tx.effect(async () => {
        tries++;
        throw new RetryableError("429", { retryAfterMs: 100 });
      }, { retry: { attempts: 3 } })), EffectFailedError);
    assert.equal(tries, 3);
    assert.ok(Date.now() - t0 < 1500);
  });

  test("with retry, a PermanentError stops the retries", async () => {
    let a = 0;
    await assert.rejects(pgtxn.transaction(async (tx) =>
      tx.effect(async () => { a++; throw new PermanentError("400"); }, { retry: true })), EffectFailedError);
    assert.equal(a, 1);
  });

  test("a slow effect times out and is retried; the signal is aborted", async () => {
    let tries = 0;
    let aborted = false;
    const out = await pgtxn.transaction(async (tx) =>
      tx.effect(async (ctx) => {
        if (++tries === 1) {
          await new Promise((r) => ctx.signal.addEventListener("abort", r));
          aborted = true;
        }
        return tries;
      }, { timeoutMs: 200, retry: true }));
    assert.equal(out, 2);
    assert.ok(aborted);
  });

  test("the effect id is stable across retries: the idempotency key", async () => {
    const keys: string[] = [];
    await pgtxn.transaction(async (tx) =>
      tx.effect(async (ctx) => {
        keys.push(ctx.idempotencyKey);
        if (keys.length < 3) throw new Error("again");
      }, { retry: true }));
    assert.equal(new Set(keys).size, 1);
  });

  test("now() and uuid() are the same in every run", async () => {
    const seen: string[] = [];
    await pgtxn.transaction(async (tx) => {
      seen.push(`${tx.now().toISOString()} ${tx.uuid()} ${tx.uuid()}`);
      await tx.effect(async () => 1);
    });
    assert.equal(seen.length, 2);
    assert.equal(seen[0], seen[1]);
    assert.match(seen[0], /[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/);
  });

  test("user code that catches the internal abort still gets correct results", async () => {
    let calls = 0;
    const out = await pgtxn.transaction(async (tx) => {
      let v = -1;
      try {
        v = await tx.effect(async () => { calls++; return 42; });
      } catch {
        // careless code
      }
      return v;
    });
    assert.equal(out, 42);
    assert.equal(calls, 1);
  });

  test("an error in user code before any effect: rolled back; with an id, the failure is recorded", async () => {
    const orderId = await newOrder(pool);
    const txId = crypto.randomUUID();
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.db.query("UPDATE orders SET status = 'x' WHERE id = $1", [orderId]);
      throw new Error("validation failed");
    }, { id: txId }), /validation failed/);
    assert.equal((await pool.query("SELECT status FROM orders WHERE id = $1", [orderId])).rows[0].status, "new");
    assert.equal((await status(txId)).status, "failed");
  });

  test("serializable transactions retry serialization failures by themselves", async () => {
    const a = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100)", [a]);
    const move = () => pgtxn.transaction(async (tx) => {
      const b = (await tx.db.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance;
      await sleep(20);
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [a, Number(b) - 10]);
    }, { isolation: "serializable" });
    await Promise.all([move(), move(), move(), move()]);
    assert.equal(Number((await pool.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance), 60);
  });
});
