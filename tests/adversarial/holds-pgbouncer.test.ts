// Through PgBouncer in transaction mode (port 55434): keys, effects, spawns,
// compensation, serializable, with and without LISTEN. All held (LISTEN
// simply does not deliver; the worker polls).
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, waitFor, nextId, newOrder } from "../helpers.ts";

const URL = process.env.PG_TXN_BOUNCER_URL ?? "postgres://app:app@localhost:55434/app";
const direct = makePool(5);
const pool = makePool(5, URL);

before(async () => {
  await schema(direct);
});
after(async () => {
  await closeAll();
  await pool.end();
  await direct.end();
});

describe("holds: PgBouncer transaction mode", () => {
  for (const listen of [false, true]) {
    test(`listen=${listen}: keyed serializable transaction with effect, compensation and spawn`, async () => {
      const p = newPgTxn(pool, { leaseMs: 1500, listen, pollMs: 100 });
      let ran = false;
      const refunds: string[] = [];
      const id = await newOrder(direct);
      let run = 0;
      const out = await p.transaction(async (tx) => {
        run++;
        await tx.effect(async () => `pay_${run}`, { name: "charge", deps: [Math.min(run, 2)], compensate: async (r) => { refunds.push(r); } });
        await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id]);
        await tx.spawn(async () => { ran = true; });
        return "ok";
      }, { key: `pgb:${nextId()}`, isolation: "serializable" });
      assert.equal(out, "ok");
      await waitFor(async () => ran, "spawn via pgbouncer");
      await waitFor(async () => refunds.length === 1, "compensation via pgbouncer");
      assert.deepEqual(refunds, ["pay_1"]);
      assert.equal((await direct.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status, "paid");
      await p.close(1000);
    });
  }

  test("enqueue + define through PgBouncer with keys", async () => {
    const p = newPgTxn(pool, { leaseMs: 1500, listen: false, pollMs: 100 });
    const id = await newOrder(direct, { amount: 0 });
    p.define("pgb-bump", async (tx) => {
      const a = Number((await tx.db.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount);
      await tx.effect(async () => sleep(20));
      await tx.db.query("UPDATE orders SET amount = $2 WHERE id = $1", [id, a + 1]);
    });
    const ids = await Promise.all(Array.from({ length: 5 }, () => p.enqueue("pgb-bump", {}, { key: ["pgb", id] })));
    for (const t of ids) await p.wait(t, 20_000);
    assert.equal(Number((await direct.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount), 5);
    await p.close(1000);
  });
});
