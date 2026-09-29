// Hypothesis: close(drainMs) (index.ts:812) waits for #calls but a
// transaction waiting for a key can wait up to keyWaitMs (5 min by default),
// so close() returns at drainMs with the waiter still in #start (index.ts:411),
// which never checks #closing. When the key is released the waiter starts and
// commits after close() returned: the worker loop is gone, so its spawns and
// compensations stay pending in this process and become EffectLost. The
// caller of close() was told everything drained; the transaction's caller was
// told it committed.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId } from "../helpers.ts";

const pool = makePool(10);
const holder = newPgTxn(pool, { leaseMs: 1500 });

before(async () => {
  await holder.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

describe("close() during a key wait", () => {
  test("a transaction still waiting for a key when close() returns is refused, or its spawns still run", { timeout: 30_000 }, async () => {
    const closing = newPgTxn(pool, { leaseMs: 1500 });
    await closing.ready();
    const key = `close-wait:${nextId()}`;
    let release!: () => void;
    const held = new Promise<void>((r) => { release = r; });
    const first = holder.transaction(async (tx) => { await tx.effect(() => held); }, { key });
    await sleep(300);
    let spawned = false;
    let spawnId = "";
    const waiter = closing.transaction(async (tx) => {
      spawnId = await tx.spawn(async () => { spawned = true; });
      return "waited";
    }, { key }).then((v) => v, (e: Error) => `rejected: ${e.name}`);
    await sleep(300);
    const t0 = Date.now();
    await closing.close(500);
    const closeTook = Date.now() - t0;
    release();
    await first;
    const out = await waiter;
    await sleep(1500);
    const row = spawnId ? (await pool.query("SELECT status FROM txn.effects WHERE id = $1", [spawnId])).rows[0] : null;
    assert.ok(out !== "waited" || spawned,
      `close() returned after ${closeTook} ms; the waiter then ran and returned ${JSON.stringify(out)} but its spawn ${JSON.stringify(row)} never ran`);
  });
});
