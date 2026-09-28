// The same transaction with three different database libraries: pg_txn only
// needs "run a callback in a transaction" and "run a statement".
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { drizzle } from "drizzle-orm/node-postgres";
import { pgTable, bigint, text, numeric } from "drizzle-orm/pg-core";
import { eq, sql } from "drizzle-orm";
import knexFactory from "knex";
import { PgTxn, type Tx } from "../../clients/typescript/client/src/index.ts";
import { drizzleDb } from "../../clients/typescript/drizzle/src/index.ts";
import { knexDb } from "../../clients/typescript/knex/src/index.ts";
import { makePool, closeAll, schema, newOrder, sleep, PG_URL } from "../helpers.ts";

const pool = makePool(10);
const orders = pgTable("orders", {
  id: bigint("id", { mode: "number" }).primaryKey(),
  status: text("status").notNull(),
  amount: numeric("amount").notNull(),
  paymentId: text("payment_id"),
});
const db = drizzle(pool);
const knex = knexFactory({ client: "pg", connection: PG_URL, pool: { min: 0, max: 5 } });
const instances: PgTxn[] = [];

before(async () => {
  await schema(pool);
});
after(async () => {
  await Promise.all(instances.map((p) => p.close(1000)));
  await closeAll();
  await knex.destroy();
  await pool.end();
});

type Stack<T> = {
  name: string;
  pgtxn: PgTxn<T>;
  read(tx: Tx<T>, id: number): Promise<{ status: string; amount: number }>;
  markPaid(tx: Tx<T>, id: number, paymentId: string): Promise<void>;
};

const stacks: Stack<any>[] = [
  {
    name: "node-postgres",
    pgtxn: new PgTxn(pool),
    read: async (tx, id) => (await tx.db.query("SELECT status, amount::float AS amount FROM orders WHERE id = $1", [id])).rows[0],
    markPaid: async (tx, id, p) => { await tx.db.query("UPDATE orders SET status = 'paid', payment_id = $2 WHERE id = $1", [id, p]); },
  },
  {
    name: "Drizzle",
    pgtxn: new PgTxn(drizzleDb(db)),
    read: async (tx, id) => {
      const [o] = await tx.db.select({ status: orders.status, amount: sql<number>`${orders.amount}::float` }).from(orders).where(eq(orders.id, id));
      return o;
    },
    markPaid: async (tx, id, p) => { await tx.db.update(orders).set({ status: "paid", paymentId: p }).where(eq(orders.id, id)); },
  },
  {
    name: "Knex",
    pgtxn: new PgTxn(knexDb(knex)),
    read: async (tx, id) => tx.db("orders").select("status", knex.raw("amount::float AS amount")).where({ id }).first(),
    markPaid: async (tx, id, p) => { await tx.db("orders").where({ id }).update({ status: "paid", payment_id: p }); },
  },
];
instances.push(...stacks.map((s) => s.pgtxn));

describe("database libraries", () => {
  for (const s of stacks) {
    test(`${s.name}: own, effect in the middle, write back atomically; plain SQL blocked meanwhile`, async () => {
      const id = await newOrder(pool, { amount: 30 });
      let charges = 0;
      let blocked = "";
      const out = await s.pgtxn.transaction(async (tx) => {
        await tx.own("orders", id);
        const o = await s.read(tx, id);
        if (o.status !== "new") return "skipped";
        const p = await tx.effect(async () => {
          charges++;
          blocked = await pool.query("UPDATE orders SET status = 'cancelled' WHERE id = $1", [id]).then(() => "no", (e) => e.code);
          await sleep(50);
          return { id: `pay_${o.amount}` };
        }, { name: "charge" });
        await s.markPaid(tx, id, p.id);
        return p.id;
      });
      assert.equal(out, "pay_30");
      assert.equal(charges, 1);
      assert.equal(blocked, "55P03");
      assert.deepEqual((await pool.query("SELECT status, payment_id FROM orders WHERE id = $1", [id])).rows[0], { status: "paid", payment_id: "pay_30" });
    });
  }
});
