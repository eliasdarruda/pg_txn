// Hypothesis: when the *outcome* of an effect cannot be recorded (its result
// is not a durable value, or the thrown value cannot be turned into JSON), the
// error is raised in #execute (index.ts:546-548, :579), i.e. in the catch
// branch of #runs (index.ts:501-503), so it escapes #runs without
// txn.fail_transaction. The caller gets the error, but the database keeps the
// transaction 'running' with its effect 'running', its keys held and its
// earlier effects uncompensated, until abandon_expired (lease + 5 s; 35 s by
// default) — and a named transaction is resumed elsewhere instead of failing.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { RetryableError } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId } from "../helpers.ts";

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

const keysHeld = async (key: string) => (await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = $1", [key])).rows[0].n;
const txStatus = async (id: string) => (await pool.query("SELECT status FROM txn.transactions WHERE id = $1", [id])).rows[0]?.status;

describe("an effect whose outcome cannot be recorded", () => {
  test("an effect returning a Map fails the transaction cleanly: keys released, status failed", async () => {
    const key = `map:${nextId()}`;
    const id = crypto.randomUUID();
    await assert.rejects(pgtxn.transaction(async (tx) => tx.effect(async () => new Map([[1, 2]])), { key, id }), /SerializationError|non-serializable/);
    assert.equal(await keysHeld(key), 0, "the key must be released when the transaction fails");
    assert.equal(await txStatus(id), "failed");
  });

  test("an effect returning a string with U+0000 fails cleanly and compensates the effects before it", async () => {
    const key = `nul:${nextId()}`;
    const id = crypto.randomUUID();
    const refunds: string[] = [];
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.effect(async () => "pay_1", { name: "charge", compensate: async (p) => { refunds.push(p); } });
      await tx.effect(async () => "bad\u0000", { name: "label" });
    }, { key, id }));
    assert.equal(await keysHeld(key), 0);
    assert.equal(await txStatus(id), "failed");
    await sleep(800);
    assert.deepEqual(refunds, ["pay_1"], "the charge was made and not used: it must be compensated");
  });

  test("an effect throwing a bigint fails cleanly", async () => {
    const key = `bigint:${nextId()}`;
    const id = crypto.randomUUID();
    await assert.rejects(pgtxn.transaction(async (tx) => tx.effect(async () => { throw 10n; }), { key, id }));
    assert.equal(await keysHeld(key), 0);
    assert.equal(await txStatus(id), "failed");
  });

  test("a RetryableError with an out-of-range retryAfterMs fails cleanly", async () => {
    const key = `retry-after:${nextId()}`;
    const id = crypto.randomUUID();
    await assert.rejects(pgtxn.transaction(async (tx) =>
      tx.effect(async () => { throw new RetryableError("429", { retryAfterMs: 1e12 }); }, { retry: true }), { key, id }));
    assert.equal(await keysHeld(key), 0);
    assert.equal(await txStatus(id), "failed");
  });

  test("a named transaction whose effect result is not durable is failed, not left running for another process to resume", async () => {
    const id = crypto.randomUUID();
    const run = pgtxn.define("bad-result", async (tx) => tx.effect(async () => new Map()));
    await assert.rejects(run({}, { id }));
    assert.equal(await txStatus(id), "failed");
  });
});
