// Hypothesis: txn.lease_transactions (sql:735-751) wraps each candidate in
// BEGIN ... EXCEPTION WHEN OTHERS and marks the transaction 'failed'
// (StartFailed) on ANY error, permanently. A transient error — lock_timeout
// or statement_timeout on the worker's session (both common defaults in
// managed PostgreSQL and in application pools), a cancel, a deadlock — that
// lands inside the block while it waits for a row lock (txn._lease's upsert
// waits for a heartbeat of the stale owner; txn._claim's insert waits for a
// txn.finish deleting the key row) therefore fails a healthy queued
// transaction that any later poll would have started. The error class is not
// distinguished (SQLERRM only, no SQLSTATE).
// Expected: only errors that make the transaction unstartable (e.g. a key
// that cannot be indexed) fail it; transient ones leave it queued.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId } from "../helpers.ts";

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

describe("transient errors inside txn.lease_transactions", () => {
  test("a lock_timeout while leasing leaves the queued transaction queued (observed: failed as StartFailed for good)", { timeout: 30_000 }, async () => {
    const name = `r3-lt-${nextId()}`;
    // an enqueued transaction with a stale lease row (as left by a driver
    // that died: enqueue creates none, but a previous driver did)
    const id = (await pool.query("SELECT txn.enqueue($1, '{}'::jsonb)::text AS id", [name])).rows[0].id;
    await pool.query("INSERT INTO txn.leases (tx_id, lease_until) VALUES ($1, now() - interval '1 minute')", [id]);
    // someone holds that lease row for a moment (a slow heartbeat statement of the old driver)
    const holder = await pool.connect();
    await holder.query("BEGIN");
    await holder.query("SELECT 1 FROM txn.leases WHERE tx_id = $1 FOR UPDATE", [id]);
    // a worker with the usual production lock_timeout leases: it waits on the row, times out
    const worker = await pool.connect();
    try {
      await worker.query("SET lock_timeout = '300ms'");
      const owner = crypto.randomUUID();
      const rows = (await worker.query("SELECT id FROM txn.lease_transactions($1, $2::text[], 16, 30000)", [owner, [name]])).rows;
      assert.equal(rows.length, 0, "not leased while the row is locked");
    } finally {
      worker.release();
      await holder.query("ROLLBACK");
      holder.release();
    }
    // the holder is gone: the next poll must be able to lease it
    const status = (await pool.query("SELECT status, error FROM txn.transactions WHERE id = $1", [id])).rows[0];
    const rows = (await pool.query("SELECT id FROM txn.lease_transactions($1, $2::text[], 16, 30000)", [crypto.randomUUID(), [name]])).rows;
    await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE id = $1 AND status = 'running'", [id]);
    assert.equal(status.status, "running", `a transient lock_timeout made it ${JSON.stringify(status)}`);
    assert.equal(rows.length, 1, "leased once the lock is gone");
  });

  test("holds: a statement_timeout (query_canceled) cancels the whole statement instead of being caught by the handler: the transaction stays queued", { timeout: 30_000 }, async () => {
    const name = `r3-st-${nextId()}`;
    const id = (await pool.query("SELECT txn.enqueue($1, '{}'::jsonb)::text AS id", [name])).rows[0].id;
    await pool.query("INSERT INTO txn.leases (tx_id, lease_until) VALUES ($1, now() - interval '1 minute')", [id]);
    const holder = await pool.connect();
    await holder.query("BEGIN");
    await holder.query("SELECT 1 FROM txn.leases WHERE tx_id = $1 FOR UPDATE", [id]);
    const worker = await pool.connect();
    let err: any = null;
    try {
      await worker.query("SET statement_timeout = '300ms'");
      await worker.query("SELECT id FROM txn.lease_transactions($1, $2::text[], 16, 30000)", [crypto.randomUUID(), [name]]).catch((e) => { err = e; });
    } finally {
      worker.release();
      await holder.query("ROLLBACK");
      holder.release();
    }
    const status = (await pool.query("SELECT status, error FROM txn.transactions WHERE id = $1", [id])).rows[0];
    await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE id = $1 AND status = 'running'", [id]);
    // either the statement was cancelled as a whole (fine: nothing changed) or
    // the cancel was swallowed by the handler and the transaction failed
    assert.equal(status.status, "running", `statement_timeout: error ${err?.code ?? "none"}; the queued transaction is ${JSON.stringify(status)}`);
  });

  test("holds: a key that cannot be indexed (random text over the btree limit) fails the enqueued transaction as StartFailed, and txn.start rejects it up front", { timeout: 30_000 }, async () => {
    const big = Array.from({ length: 3000 }, () => String.fromCharCode(0x4e00 + Math.floor(Math.random() * 20000))).join("");
    await assert.rejects(pgtxn.transaction(async () => 1, { key: big }), (e: any) => e.code === "54000");
    pgtxn.define("r3-bigkey", async () => "ran");
    const id = await pgtxn.enqueue("r3-bigkey", {}, { key: big });
    await assert.rejects(pgtxn.wait(id, 10_000), (e: any) => e.name === "TransactionFailedError" && /StartFailed/.test(e.message));
    await sleep(50);
  });
});
