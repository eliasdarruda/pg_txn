// SIGKILL at interesting points: holding keys mid-effect (inline), inside a
// compensation, inside a spawn. The documented outcomes held: keys are
// released by abandon_expired and a waiter proceeds; a compensation or spawn
// whose process died is reported EffectLost (a documented limitation).
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { fork, type ChildProcess } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { makePool, newPgTxn, closeAll, schema, waitFor, nextId, PG_URL } from "../helpers.ts";

const LEASE = 1500;
const pool = makePool(10);
const survivor = newPgTxn(pool, { leaseMs: LEASE });
const CHILD = path.join(path.dirname(fileURLToPath(import.meta.url)), "fixtures/child.ts");

before(async () => {
  await survivor.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

function start(mode: string, key: string) {
  const child = fork(CHILD, [JSON.stringify({ url: PG_URL, mode, key, leaseMs: LEASE })], { stdio: ["ignore", "ignore", "inherit", "ipc"] });
  const messages: any[] = [];
  child.on("message", (m) => messages.push(m));
  const next = (k: string) => waitFor(async () => messages.find((m) => k in m), k, 20_000);
  return { child, next };
}
const kill = (c: ChildProcess) => new Promise((r) => { c.once("exit", r); c.kill("SIGKILL"); });

describe("holds: crashes", () => {
  test("killed while holding two keys mid-effect: abandoned, both keys released, a waiter gets them (~lease + 5 s)", async () => {
    const key = `crash:${nextId()}`;
    const c = start("keys-in-effect", key);
    const m = await c.next("inEffect");
    await kill(c.child);
    const t0 = Date.now();
    const out = await survivor.transaction(async () => "mine now", { keys: [key, `${key}:2`] });
    assert.equal(out, "mine now");
    assert.ok(Date.now() - t0 < 15_000, `waited ${Date.now() - t0} ms`);
    const t = (await pool.query("SELECT status, error->>'name' AS error FROM txn.transactions WHERE id = $1", [m.inEffect])).rows[0];
    assert.deepEqual(t, { status: "abandoned", error: "AbandonedTransaction" });
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key LIKE $1", [`${key}%`])).rows[0].n, 0);
    const e = (await pool.query("SELECT status, error->>'name' AS error FROM txn.effects WHERE tx_id = $1", [m.inEffect])).rows[0];
    // failed by abandon_expired, then orphaned by _orphan (no result: no compensation)
    assert.deepEqual(e, { status: "orphaned", error: "AbandonedTransaction" });
  });

  test("killed inside a compensation: reported EffectLost after its lease expires (documented)", async () => {
    const id = crypto.randomUUID();
    const c = start("in-compensation", id);
    const m = await c.next("inCompensation");
    await kill(c.child);
    // ctx.effectId of a compensation is the compensation row itself
    const comp = (await pool.query("SELECT id FROM txn.effects WHERE id = $1 AND kind = 'compensation'", [m.inCompensation])).rows[0];
    // its lease expires: at-most-once → ambiguous; the transaction itself is failed
    const r = await waitFor(async () => { await pool.query("SELECT txn.expire_effect_leases()"); const x = (await pool.query("SELECT status, error->>'name' AS error FROM txn.effects WHERE id = $1", [comp.id])).rows[0]; return x.status === "failed" ? x : null; }, "expired", 20_000);
    assert.equal(r.error, "AmbiguousEffectOutcome");
    assert.equal((await pool.query("SELECT status FROM txn.transactions WHERE id = $1", [id])).rows[0].status, "failed");
  });

  test("killed inside a spawn: its lease expires, ambiguous (at-most-once); the commit stands", async () => {
    const id = crypto.randomUUID();
    const c = start("in-spawn", id);
    const m = await c.next("inSpawn");
    await kill(c.child);
    const r = await waitFor(async () => { await pool.query("SELECT txn.expire_effect_leases()"); const x = (await pool.query("SELECT status, error->>'name' AS error FROM txn.effects WHERE id = $1", [m.inSpawn])).rows[0]; return x.status === "failed" ? x : null; }, "expired", 20_000);
    assert.equal(r.error, "AmbiguousEffectOutcome");
    // a keyless transaction without effects leaves no txn.transactions row; the spawn row is the commit's witness
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.effects WHERE id = $1", [m.inSpawn])).rows[0].n, 1);
  });
});
