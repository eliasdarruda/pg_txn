// Spawned effects: the transactional outbox, built in. A spawned function
// runs iff the transaction commits, right after the commit, in the process
// that committed it; attempts are recorded in txn.effects.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { PermanentError } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor } from "../helpers.ts";

const pool = makePool(10);
const received: { id: number; ctx: any; at: number }[] = [];
const pgtxn = newPgTxn(pool);
const sendReceipt = (id: number) => async (ctx: any) => { received.push({ id, ctx, at: Date.now() }); };

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const got = (id: number) => waitFor(async () => received.find((x) => x.id === id), "delivery");
const count = (id: number) => received.filter((x) => x.id === id).length;

describe("spawned effects", () => {
  test("spawned in a pg_txn transaction: runs after the commit, once", async () => {
    const id = await newOrder(pool);
    const t0 = Date.now();
    await pgtxn.transaction(async (tx) => {
      await tx.db.query("UPDATE orders SET status = 'placed' WHERE id = $1", [id]);
      await tx.spawn(async (ctx) => { received.push({ id, ctx, at: Date.now() }); });
    });
    const r = await got(id);
    assert.ok(r.at - t0 < 1000, `delivered ${r.at - t0} ms after the commit`);
    await sleep(300);
    assert.equal(count(id), 1);
    assert.match(r.ctx.idempotencyKey, /^[0-9a-f-]{36}$/);
  });

  test("spawned in a run that is re-run after an effect: runs once, from the committing run", async () => {
    const id = await newOrder(pool);
    let runs = 0;
    await pgtxn.transaction(async (tx) => {
      runs++;
      await tx.spawn(sendReceipt(id));
      await tx.effect(async () => 1);
      await tx.spawn(sendReceipt(id));
    });
    assert.equal(runs, 2);
    await got(id);
    await sleep(300);
    assert.equal(count(id), 2);
  });

  test("a rolled-back transaction spawns nothing", async () => {
    const id = await newOrder(pool);
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.spawn(sendReceipt(id));
      throw new Error("rollback");
    }));
    await sleep(600);
    assert.equal(count(id), 0);
  });

  test("pgtxn.spawn with a driver transaction (commit-gated)", async () => {
    const id = await newOrder(pool);
    const c = await pool.connect();
    try {
      await c.query("BEGIN");
      await pgtxn.spawn(sendReceipt(id), { trx: c });
      await c.query("ROLLBACK");
      await c.query("BEGIN");
      await pgtxn.spawn(sendReceipt(id + 0.5), { trx: c });
      await c.query("COMMIT");
    } finally {
      c.release();
    }
    await got(id + 0.5);
    await sleep(300);
    assert.equal(count(id), 0);
  });

  test("failures are retried with backoff; the history is in txn.effect_attempts", async () => {
    let n = 0;
    const id = await pgtxn.spawn(async () => { if (++n < 3) throw new Error("502"); }, { name: "webhook", retry: true });
    await waitFor(async () => (await pool.query("SELECT 1 FROM txn.effects WHERE id = $1 AND status = 'succeeded'", [id])).rows[0], "success");
    const attempts = (await pool.query("SELECT outcome FROM txn.effect_attempts WHERE effect_id = $1 ORDER BY id", [id])).rows.map((r) => r.outcome);
    assert.deepEqual(attempts, ["retry", "retry", "succeeded"]);
    assert.equal((await pool.query("SELECT name FROM txn.effects WHERE id = $1", [id])).rows[0].name, "webhook");
  });

  test("a permanent failure is recorded once and visible in txn.effect_errors", async () => {
    const id = await pgtxn.spawn(async () => { throw new PermanentError("400 bad request"); });
    const row = await waitFor(async () => (await pool.query("SELECT status, error FROM txn.effects WHERE id = $1 AND status = 'failed'", [id])).rows[0], "failure");
    assert.equal(row.error.message, "400 bad request");
    const errs = (await pool.query("SELECT error_name, error_message FROM txn.effect_errors WHERE effect_id = $1", [id])).rows;
    assert.deepEqual(errs, [{ error_name: "PermanentError", error_message: "400 bad request" }]);
  });

  test("delayMs defers the effect", async () => {
    const id = await newOrder(pool);
    const t0 = Date.now();
    await pgtxn.spawn(sendReceipt(id), { delayMs: 700 });
    const r = await got(id);
    assert.ok(r.at - t0 >= 650, `ran after ${r.at - t0} ms`);
  });

  test("a spawned effect whose process stopped is reported lost", async () => {
    const ghost = crypto.randomUUID();
    const r = await pool.query("SELECT txn.spawn($1, 'notify') AS id", [ghost]);
    await pool.query("UPDATE txn.effects SET created_at = now() - interval '2 minutes' WHERE id = $1", [r.rows[0].id]);
    assert.equal((await pool.query("SELECT txn.fail_lost_effects() AS n")).rows[0].n >= 1, true);
    const e = (await pool.query("SELECT status, error->>'name' AS name FROM txn.effects WHERE id = $1", [r.rows[0].id])).rows[0];
    assert.deepEqual(e, { status: "failed", name: "EffectLost" });
    const doctor = (await pool.query("SELECT status FROM txn.doctor() WHERE check_name = 'lost effects'")).rows[0];
    assert.equal(doctor.status, "warning");
  });

});
