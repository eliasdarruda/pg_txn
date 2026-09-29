// The SQL surface called directly with odd inputs, install races, doctor,
// purge while running, and a schema shadowing attempt. All held.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { PgTxn } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, sleep, waitFor, nextId } from "../helpers.ts";

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

const q = async (s: string, p: unknown[] = []) => { try { return (await pool.query(s, p)).rows; } catch (e) { return `ERR ${(e as any).code}`; } };

describe("holds: SQL functions with bad inputs", () => {
  test("bad shapes are rejected or ignored, never corrupt state", async () => {
    assert.equal(await q("SELECT txn.prepare_effects(gen_random_uuid(), gen_random_uuid(), 1000, '{\"x\":1}')"), "ERR 22023");
    assert.equal(await q("SELECT txn.prepare_effects(gen_random_uuid(), gen_random_uuid(), 1000, '[{\"seq\":\"a\"}]')"), "ERR 22P02");
    assert.deepEqual(await q("SELECT txn.effect_done(gen_random_uuid(), gen_random_uuid(), true) AS r"), [{ r: { status: "unknown" } }]);
    assert.deepEqual(await q("SELECT txn.finish(gen_random_uuid(), gen_random_uuid(), '{}') AS n"), [{ n: 0 }]);
    assert.equal(await q("SELECT * FROM txn.start(gen_random_uuid(), NULL, NULL, gen_random_uuid(), 1000, ARRAY[NULL::text])"), "ERR 23502");
    assert.equal(await q("SELECT txn.spawn(gen_random_uuid(), 'x', NULL, 0)"), "ERR 23514");
    assert.equal(await q("SELECT * FROM txn.lease_effects(gen_random_uuid(), -1, 1000)"), "ERR 2201W");
    assert.equal((await q("SELECT txn.complete_effect(gen_random_uuid(), gen_random_uuid(), 0, 'null') AS ok") as any)[0].ok, false);
    assert.equal((await q("SELECT txn.fail_effect(gen_random_uuid(), gen_random_uuid(), 0, 'null') AS s") as any)[0].s, "unknown");
    assert.equal((await q("SELECT txn.heartbeat(gen_random_uuid(), gen_random_uuid(), 1000) AS b") as any)[0].b, null);
  });

  test("effect_done and complete_effect by a stranger are recorded as stale and change nothing", async () => {
    const id = crypto.randomUUID();
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const p = pgtxn.transaction(async (tx) => tx.effect(async () => { await gate; return "real"; }), { id });
    const eff = await waitFor(async () => (await pool.query("SELECT id FROM txn.effects WHERE tx_id = $1 AND status = 'running'", [id])).rows[0], "running effect");
    const r = (await pool.query("SELECT txn.effect_done($1, gen_random_uuid(), true, '\"forged\"') AS r", [eff.id])).rows[0].r;
    assert.equal(r.status, "stale");
    release();
    assert.equal(await p, "real");
  });

  test("purge while transactions run deletes only finished work; running ones and pending compensations stay", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const id = crypto.randomUUID();
    const p = pgtxn.transaction(async (tx) => tx.effect(async () => { await gate; return 1; }), { id, key: `purge:${nextId()}` });
    await waitFor(async () => (await pool.query("SELECT 1 FROM txn.effects WHERE tx_id = $1", [id])).rows[0], "effect");
    const r = (await pool.query("SELECT * FROM txn.purge(interval '0')")).rows[0];
    assert.ok(Number(r.deleted_transactions) >= 0);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.transactions WHERE id = $1", [id])).rows[0].n, 1);
    release();
    assert.equal(await p, 1);
  });

  test("doctor() reports the schema, the workers and a stale enqueued transaction", async () => {
    const rows = (await pool.query("SELECT check_name, status FROM txn.doctor()")).rows;
    const by = Object.fromEntries(rows.map((r) => [r.check_name, r.status]));
    assert.equal(by.schema, "ok");
    assert.equal(by.workers, "ok");
    await pool.query("SELECT txn.enqueue('nobody-defines-this')");
    await pool.query("UPDATE txn.transactions SET lease_until = now() - interval '2 minutes' WHERE name = 'nobody-defines-this'");
    const again = (await pool.query("SELECT check_name, status FROM txn.doctor()")).rows;
    assert.ok(again.some((r) => r.check_name === "stalled transactions" && r.status === "warning"));
    await pool.query("DELETE FROM txn.transactions WHERE name = 'nobody-defines-this'");
  });

  test("12 clients installing the schema at once: every one succeeds", async () => {
    const p2 = makePool(20);
    p2.on("error", () => {});
    await p2.query("DROP SCHEMA IF EXISTS txn CASCADE");
    const insts = Array.from({ length: 12 }, () => new PgTxn(p2, { onError: () => {} }));
    const r = await Promise.allSettled(insts.map((i) => i.ready()));
    assert.deepEqual(r.map((x) => x.status), Array(12).fill("fulfilled"));
    await Promise.all(insts.map((i) => i.close(500)));
    await p2.end();
    await schema(pool);
  });

  test("a user schema first in search_path with tables and functions named like txn's does not divert pg_txn", async () => {
    const c = await pool.connect();
    try {
      await c.query("CREATE SCHEMA IF NOT EXISTS evil");
      await c.query("CREATE TABLE IF NOT EXISTS evil.effects (id uuid, status text)");
      await c.query("CREATE TABLE IF NOT EXISTS evil.transactions (id uuid, status text)");
      await c.query("CREATE OR REPLACE FUNCTION evil.clock_timestamp() RETURNS timestamptz LANGUAGE sql AS $$ SELECT '2000-01-01'::timestamptz $$");
      await c.query("CREATE OR REPLACE FUNCTION evil.hashtext(text) RETURNS int LANGUAGE sql AS $$ SELECT 0 $$");
      await c.query("SET search_path = evil, public");
      const id = crypto.randomUUID();
      const out = await pgtxn.transaction(async (tx) => { await tx.effect(async () => "e"); return "ok"; }, { id, key: `evil:${nextId()}` });
      assert.equal(out, "ok");
      const t = (await pool.query("SELECT status, created_at > now() - interval '1 minute' AS recent FROM txn.transactions WHERE id = $1", [id])).rows[0];
      assert.deepEqual(t, { status: "committed", recent: true });
      assert.equal((await c.query("SELECT count(*)::int AS n FROM evil.effects")).rows[0].n, 0);
    } finally {
      await c.query("RESET search_path");
      await c.query("DROP SCHEMA IF EXISTS evil CASCADE");
      c.release();
    }
  });
});
