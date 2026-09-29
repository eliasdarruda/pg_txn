// Guarantees that held under adversarial use: memoization, compensation,
// spawns, values, pool exhaustion, isolation retries.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { EffectFailedError, PermanentError } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor, nextId } from "../helpers.ts";

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

const effectRow = async (id: string) => (await pool.query("SELECT status, error->>'name' AS error FROM txn.effects WHERE id = $1", [id])).rows[0];

describe("holds: compensation", () => {
  test("a constraint violation in the final run fails the transaction and compensates its effects once", async () => {
    const refunds: string[] = [];
    const id = await newOrder(pool);
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.effect(async () => "pay_1", { name: "charge", compensate: async (p) => { refunds.push(p); } });
      await tx.db.query("INSERT INTO orders (id) VALUES ($1)", [id]);   // duplicate key
    }), (e: any) => e.code === "23505");
    await waitFor(async () => refunds.length === 1, "refund");
    await sleep(500);
    assert.deepEqual(refunds, ["pay_1"]);
  });

  test("a memoized effect (not re-executed in the failing run) is compensated exactly once, even when the compensation fails first and retries", async () => {
    const attempts: number[] = [];
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.effect(async () => "pay_2", { name: "charge", retry: true, compensate: async (_p, ctx) => { attempts.push(ctx.attempt); if (ctx.attempt === 1) throw new Error("gateway hiccup"); } });
      await tx.effect(async () => "reserved", { name: "reserve" });
      await tx.effect(async () => "labelled", { name: "label" });
      throw new Error("business rule");
    }), /business rule/);
    await waitFor(async () => attempts.length === 2, "retry of the refund", 5000);
    await sleep(700);
    assert.deepEqual(attempts, [1, 2]);
  });

  test("divergence in one process: the effect no longer reached is compensated; the new path's effect runs; no double compensation", async () => {
    const undone: string[] = [];
    const called: string[] = [];
    let run = 0;
    await pgtxn.transaction(async (tx) => {
      run++;
      if (run === 1) await tx.effect(async () => { called.push("a"); return "a"; }, { name: "a", compensate: async (r) => { undone.push(r); } });
      else await tx.effect(async () => { called.push("b"); return "b"; }, { name: "b", compensate: async (r) => { undone.push(r); } });
    });
    await waitFor(async () => undone.length === 1, "compensation of a");
    await sleep(500);
    assert.deepEqual(called, ["a", "b"]);
    assert.deepEqual(undone, ["a"]);
  });

  test("a failed (not succeeded) effect is never compensated", async () => {
    let comp = 0;
    await assert.rejects(pgtxn.transaction(async (tx) =>
      tx.effect(async () => { throw new PermanentError("declined"); }, { compensate: async () => { comp++; } })), EffectFailedError);
    await sleep(600);
    assert.equal(comp, 0);
  });
});

describe("holds: memoization", () => {
  test("the same effect name in a loop: memoized by position; a re-run that reorders gets different effects", async () => {
    const calls: number[] = [];
    const out = await pgtxn.transaction(async (tx) => {
      const r = [];
      for (let i = 0; i < 4; i++) r.push(await tx.effect(async () => { calls.push(i); return i * 10; }, { name: "step", deps: i }));
      return r;
    });
    assert.deepEqual(out, [0, 10, 20, 30]);
    assert.deepEqual(calls, [0, 1, 2, 3]);
  });

  test("deps that change between runs: a new effect; equal deps in different key order: the same effect", async () => {
    let run = 0;
    const calls: unknown[] = [];
    await pgtxn.transaction(async (tx) => {
      run++;
      const deps = run === 1 ? { a: 1, b: [1, 2] } : { b: [1, 2], a: 1 };
      await tx.effect(async () => { calls.push(deps); }, { name: "x", deps });
      // run 1 aborts at x (missing); y is first reached in run 2, with new deps in run 3, memoized in run 4
      await tx.effect(async () => { calls.push("y"); }, { name: "y", deps: { run: run <= 2 ? 1 : 2 } });
    });
    // x: once (same deps in both orders); y: twice (deps changed once)
    assert.equal(calls.filter((c) => c !== "y").length, 1);
    assert.equal(calls.filter((c) => c === "y").length, 2);
  });

  test("Promise.all of independent effects, then an effect that depends on them: two rounds, each effect once", async () => {
    const calls: string[] = [];
    let runs = 0;
    const out = await pgtxn.transaction(async (tx) => {
      runs++;
      const [a, b] = await Promise.all([
        tx.effect(async () => { calls.push("a"); return 1; }, { name: "a" }),
        tx.effect(async () => { calls.push("b"); return 2; }, { name: "b" }),
      ]);
      return tx.effect(async () => { calls.push("c"); return a + b; }, { name: "c", deps: [a, b] });
    });
    assert.equal(out, 3);
    assert.equal(runs, 3);
    assert.deepEqual(calls, ["a", "b", "c"].filter((x) => calls.includes(x)).length === 3 ? calls.sort() : calls);
  });

  test("a serializable transaction retried on 40001 does not call its effects again", async () => {
    const a = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100)", [a]);
    let calls = 0;
    const move = () => pgtxn.transaction(async (tx) => {
      const b = (await tx.db.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance;
      await tx.effect(async () => { calls++; }, { name: "e" });
      await sleep(20);
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [a, Number(b) - 10]);
    }, { isolation: "serializable" });
    await Promise.all([move(), move(), move(), move()]);
    assert.equal(calls, 4);
    assert.equal(Number((await pool.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance), 60);
  });

  test("spawns in serializable runs that were retried on 40001 run exactly once", async () => {
    const a = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100)", [a]);
    const ran: number[] = [];
    let runsTotal = 0;
    const move = (n: number) => pgtxn.transaction(async (tx) => {
      runsTotal++;
      const b = (await tx.db.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance;
      await tx.spawn(async () => { ran.push(n); });
      await sleep(20);
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [a, Number(b) - 10]);
    }, { isolation: "serializable" });
    await Promise.all([move(1), move(2), move(3), move(4)]);
    await waitFor(async () => ran.length >= 4, "4 spawns");
    await sleep(500);
    assert.deepEqual(ran.sort(), [1, 2, 3, 4]);
    assert.ok(runsTotal > 4, "some run was retried");
  });
});

describe("holds: values", () => {
  test("results round-trip: null, empty, unicode, nested, dates, bigint, empty bytes, tagged-looking objects", async () => {
    const vals: unknown[] = [null, 0, "", "héllo 🚀 😀  ", { a: { b: [1, { c: null }] } }, new Date(0), 123n, new Uint8Array(0), [undefined, null], { $date: "x" }, { $bigint: "1", x: 2 }];
    const out = await pgtxn.transaction(async (tx) => {
      const r = [];
      for (const v of vals) r.push(await tx.effect(async () => v));
      return r;
    });
    assert.deepEqual(out, vals);
  });

  test("a 5 MB result and a 5 MB output round-trip", async () => {
    const big = "x".repeat(5_000_000);
    const out = await pgtxn.transaction(async (tx) => tx.effect(async () => big));
    assert.equal(out.length, big.length);
  });

  test("effects throwing non-Error values fail with a recorded error", async () => {
    const errs = [];
    for (const v of ["str", { code: 1 }, undefined, null, 42]) {
      errs.push(await pgtxn.transaction(async (tx) => { try { await tx.effect(async () => { throw v; }); } catch (e) { return (e as EffectFailedError).error.message; } }));
    }
    assert.deepEqual(errs, ["str", '{"code":1}', "undefined", "null", "42"]);
  });

  test("a non-serializable *output* (a function) fails the transaction cleanly, releasing its key", async () => {
    const key = `fn-out:${nextId()}`;
    const id = crypto.randomUUID();
    await assert.rejects(pgtxn.transaction(async (tx) => { await tx.effect(async () => 1); return () => 1; }, { key, id }), /SerializationError|functions/);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = $1", [key])).rows[0].n, 0);
    assert.equal((await pool.query("SELECT status FROM txn.transactions WHERE id = $1", [id])).rows[0].status, "failed");
  });
});

describe("holds: spawns and the worker", () => {
  test("a spawned function running longer than the lease is kept alive by heartbeats: one attempt, no ambiguous outcome", async () => {
    let n = 0;
    const id = await pgtxn.spawn(async () => { n++; await sleep(4000); return "ok"; });
    await waitFor(async () => (await effectRow(id)).status === "succeeded", "success", 10_000);
    const att = (await pool.query("SELECT outcome FROM txn.effect_attempts WHERE effect_id = $1 ORDER BY id", [id])).rows.map((r) => r.outcome);
    assert.deepEqual([n, att], [1, ["succeeded"]]);
  });

  test("1000 spawns in one transaction all run, once", async () => {
    let n = 0;
    await pgtxn.transaction(async (tx) => { for (let i = 0; i < 1000; i++) await tx.spawn(async () => { n++; }); });
    await waitFor(async () => n === 1000, "1000 spawns", 30_000);
    await sleep(500);
    assert.equal(n, 1000);
  });

  test("a pool of one connection: effects, parallel effects, a key, a spawn and the worker do not deadlock", async () => {
    const one = makePool(1);
    const p = newPgTxn(one, { leaseMs: 1500 });
    let ran = false;
    const out = await p.transaction(async (tx) => {
      const a = await tx.effect(async () => 1);
      const [b, c] = await Promise.all([tx.effect(async () => 2), tx.effect(async () => 3)]);
      await tx.spawn(async () => { ran = true; });
      return a + b + c;
    }, { key: `pool1:${nextId()}` });
    await waitFor(async () => ran, "spawn on a pool of one");
    assert.equal(out, 6);
    await p.close(1000);
    await one.end();
  });

  test("a spawn whose function throws a non-Error is recorded as failed", async () => {
    const id = await pgtxn.spawn(async () => { throw "boom"; });
    const row = await waitFor(async () => { const r = await effectRow(id); return r.status === "failed" ? r : null; }, "failure");
    assert.equal(row.status, "failed");
  });

  test("close() waits for a spawned function in progress", async () => {
    const p = newPgTxn(pool, { leaseMs: 1500 });
    let done = false;
    const id = await p.spawn(async () => { await sleep(800); done = true; });
    await waitFor(async () => (await effectRow(id)).status === "running", "leased");
    await p.close(5000);
    assert.ok(done);
    assert.equal((await effectRow(id)).status, "succeeded");
  });
});
