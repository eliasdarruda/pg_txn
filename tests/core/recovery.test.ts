// A process that stops mid-transaction loses nothing: named transactions are
// resumed by another process (recorded effects reused, the same idempotency
// key), inline ones are abandoned (rows released), and a stalled process that
// wakes up late cannot record anything (fencing).
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { fork, type ChildProcess } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { makePool, newPgTxn, closeAll, schema, newOrder, waitFor, PG_URL } from "../helpers.ts";

const LEASE = 1500;
const pool = makePool(10);
const refunds: any[] = [];
const survivor = newPgTxn(pool, { leaseMs: LEASE });
const CHILD = path.join(path.dirname(fileURLToPath(import.meta.url)), "fixtures/child.ts");

// the surviving process defines the same transaction
survivor.define("pay", async (tx, input: { orderId: number }) => {
  await tx.own("orders", input.orderId);
  const p = await tx.effect(async (ctx) => {
    await pool.query("INSERT INTO ledger (account_id, amount, ref) SELECT $1, 10, $2 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE ref = $2)",
      [input.orderId, ctx.idempotencyKey]);
    return { id: `pay_${ctx.idempotencyKey.slice(0, 8)}` };
  }, { name: "charge", key: input, retry: true, compensate: async (p) => { refunds.push(p); } });
  await tx.db.query("UPDATE orders SET status = 'paid', payment_id = $2 WHERE id = $1", [input.orderId, p.id]);
  return p.id;
});

before(async () => {
  await survivor.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

function start(mode: string, orderId: number, txId = crypto.randomUUID()) {
  const child = fork(CHILD, [JSON.stringify({ url: PG_URL, mode, txId, orderId, leaseMs: LEASE })], { stdio: ["ignore", "ignore", "inherit", "ipc"] });
  const messages: any[] = [];
  child.on("message", (m) => messages.push(m));
  const next = (key: string) => waitFor(async () => messages.find((m) => key in m), key, 20_000);
  return { child, txId, messages, next };
}
const kill = (c: ChildProcess) => new Promise((r) => { c.once("exit", r); c.kill("SIGKILL"); });
const order = async (id: number) => (await pool.query("SELECT status, payment_id FROM orders WHERE id = $1", [id])).rows[0];
const ledger = async (id: number) => (await pool.query("SELECT count(*)::int AS n FROM ledger WHERE account_id = $1", [id])).rows[0].n;
const outcomes = async (txId: string) => (await pool.query(
  "SELECT a.outcome FROM txn.effect_attempts a JOIN txn.effects e ON e.id = a.effect_id WHERE e.tx_id = $1 ORDER BY a.id", [txId])).rows.map((r) => r.outcome);

describe("recovery", () => {
  test("killed mid-call: another process resumes it with the same idempotency key; one charge", async () => {
    const id = await newOrder(pool);
    const c = start("hang-in-charge", id);
    await c.next("charged");
    await kill(c.child);
    const done = await waitFor(async () => (await order(id)).status === "paid" && order(id), "resumed and committed", 20_000);
    assert.match(done.payment_id, /^pay_/);
    assert.equal(await ledger(id), 1, "the external system saw the same key twice and charged once");
    assert.deepEqual(await outcomes(c.txId), ["lease_expired", "succeeded"]);
  });

  test("killed after the charge, before the commit: resumed without charging again", async () => {
    const id = await newOrder(pool);
    const c = start("hang-before-commit", id);
    await c.next("beforeCommit");
    await kill(c.child);
    await waitFor(async () => (await order(id)).status === "paid", "resumed and committed", 20_000);
    assert.equal(await ledger(id), 1);
    assert.deepEqual(await outcomes(c.txId), ["succeeded"], "the charge ran once; the recorded result was reused");
  });

  test("an inline transaction whose process stops is abandoned: rows released, its compensation reported lost", async () => {
    const id = await newOrder(pool);
    const c = start("inline-hang", id);
    await c.next("beforeCommit");
    await kill(c.child);
    // the compensation function lived in the killed process
    const comp = await waitFor(async () => (await pool.query(
      "SELECT id, local_owner, input->'result'->>'id' AS payment FROM txn.effects WHERE tx_id = $1 AND kind = 'compensation'", [c.txId])).rows[0],
      "compensation scheduled", 20_000);
    assert.match(comp.payment, /^pay_/);
    assert.notEqual(comp.local_owner, survivor.owner);
    await pool.query("UPDATE txn.effects SET created_at = now() - interval '2 minutes' WHERE id = $1", [comp.id]);
    await pool.query("UPDATE txn.workers SET seen_at = now() - interval '2 minutes' WHERE owner = $1", [comp.local_owner]);
    await pool.query("SELECT txn.fail_lost_effects()");
    const lost = (await pool.query("SELECT status, error->>'name' AS name FROM txn.effects WHERE id = $1", [comp.id])).rows[0];
    assert.deepEqual(lost, { status: "failed", name: "EffectLost" });
    assert.equal(refunds.length, 0);
    const t = (await pool.query("SELECT status, error->>'name' AS error FROM txn.transactions WHERE id = $1", [c.txId])).rows[0];
    assert.deepEqual(t, { status: "abandoned", error: "AbandonedTransaction" });
    await pool.query("UPDATE orders SET note = 'free again' WHERE id = $1", [id]);
    assert.equal((await order(id)).status, "new", "never marked paid");
  });

  test("a stalled process that wakes up late is fenced: its result is recorded as stale", async () => {
    const id = await newOrder(pool);
    const c = start("stall-in-charge", id);
    await c.next("charged");
    // the survivor takes over after the lease expires and finishes it
    await waitFor(async () => (await order(id)).status === "paid", "taken over", 20_000);
    const m = await c.next("error");
    assert.equal(m.error, "FencedError");
    assert.equal(await ledger(id), 1);
    const o = await outcomes(c.txId);
    assert.ok(o.includes("stale"), `stale attempt recorded: ${o}`);
  });
});
