// Drizzle and Knex: isolation levels, errors inside ORM queries (SQLSTATE
// found through `cause`), pgtxn.spawn with an ORM transaction. All held.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { drizzle } from "drizzle-orm/node-postgres";
import { pgTable, bigint, text, numeric } from "drizzle-orm/pg-core";
import { eq, sql } from "drizzle-orm";
import knexFactory from "knex";
import { PgTxn, EffectFailedError, PermanentError } from "../../clients/typescript/client/src/index.ts";
import { drizzleDb } from "../../clients/typescript/drizzle/src/index.ts";
import { knexDb } from "../../clients/typescript/knex/src/index.ts";
import { makePool, closeAll, schema, newOrder, sleep, waitFor, nextId, PG_URL } from "../helpers.ts";

const pool = makePool(10);
const orders = pgTable("orders", {
  id: bigint("id", { mode: "number" }).primaryKey(),
  status: text("status").notNull(),
  amount: numeric("amount").notNull(),
  paymentId: text("payment_id"),
});
const db = drizzle(pool);
const knex = knexFactory({ client: "pg", connection: PG_URL, pool: { min: 0, max: 5 } });
const dz = new PgTxn(drizzleDb(db), { onError: () => {}, leaseMs: 1500 });
const kx = new PgTxn(knexDb(knex), { onError: () => {}, leaseMs: 1500 });

before(async () => {
  await schema(pool);
  await dz.ready();
  await kx.ready();
});
after(async () => {
  await dz.close(1000);
  await kx.close(1000);
  await closeAll();
  await knex.destroy();
  await pool.end();
});

describe("holds: adapters", () => {
  test("Drizzle: serializable transactions with effects retry 40001 without re-calling the effect", async () => {
    const a = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100)", [a]);
    let calls = 0;
    const move = () => dz.transaction(async (tx) => {
      const [{ b }] = (await tx.db.execute(sql`SELECT balance AS b FROM accounts WHERE id = ${a}`)).rows as any[];
      await tx.effect(async () => { calls++; });
      await sleep(20);
      await tx.db.execute(sql`UPDATE accounts SET balance = ${Number(b) - 10} WHERE id = ${a}`);
    }, { isolation: "serializable" });
    await Promise.all([move(), move(), move()]);
    assert.equal(calls, 3);
    assert.equal(Number((await pool.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance), 70);
  });

  test("Knex: serializable transactions with effects retry 40001 without re-calling the effect", async () => {
    const a = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100)", [a]);
    let calls = 0;
    const move = () => kx.transaction(async (tx) => {
      const { b } = (await tx.db.raw("SELECT balance AS b FROM accounts WHERE id = ?", [a])).rows[0];
      await tx.effect(async () => { calls++; });
      await sleep(20);
      await tx.db.raw("UPDATE accounts SET balance = ? WHERE id = ?", [Number(b) - 10, a]);
    }, { isolation: "serializable" });
    await Promise.all([move(), move(), move()]);
    assert.equal(calls, 3);
    assert.equal(Number((await pool.query("SELECT balance FROM accounts WHERE id = $1", [a])).rows[0].balance), 70);
  });

  test("Drizzle: an error inside an ORM query in the final run fails the transaction, compensates, releases the key", async () => {
    const id = await newOrder(pool);
    const refunds: string[] = [];
    const key = `dz:${nextId()}`;
    await assert.rejects(dz.transaction(async (tx) => {
      await tx.effect(async () => "pay", { compensate: async (p) => { refunds.push(p); } });
      await tx.db.insert(orders).values({ id, status: "dup", amount: "1" });   // duplicate key
    }, { key }));
    await waitFor(async () => refunds.length === 1, "refund");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = $1", [key])).rows[0].n, 0);
  });

  test("Knex: EffectFailedError inside the transaction; a permanent failure is not retried", async () => {
    let tries = 0;
    const out = await kx.transaction(async (tx) => {
      try {
        await tx.effect(async () => { tries++; throw new PermanentError("no"); }, { retry: true });
        return "yes";
      } catch (e) {
        return e instanceof EffectFailedError ? "declined" : "other";
      }
    });
    assert.deepEqual([out, tries], ["declined", 1]);
  });

  test("pgtxn.spawn with a Drizzle transaction: runs iff it commits", async () => {
    const ran: string[] = [];
    await db.transaction(async (trx) => {
      await dz.spawn(async () => { ran.push("committed"); }, { trx });
    });
    await assert.rejects(db.transaction(async (trx) => {
      await dz.spawn(async () => { ran.push("rolled back"); }, { trx });
      throw new Error("abort");
    }));
    await waitFor(async () => ran.includes("committed"), "spawn");
    await sleep(400);
    assert.deepEqual(ran, ["committed"]);
  });

  test("Drizzle: tx.db.update inside pg_txn with a key and an effect, concurrently: one charge", async () => {
    const id = await newOrder(pool);
    let charges = 0;
    const checkout = () => dz.transaction(async (tx) => {
      const [o] = await tx.db.select({ status: orders.status }).from(orders).where(eq(orders.id, id));
      if (o.status !== "new") return "skip";
      await tx.effect(async () => { charges++; await sleep(50); });
      await tx.db.update(orders).set({ status: "paid" }).where(eq(orders.id, id));
      return "paid";
    }, { key: ["dz-order", id], isolation: "repeatable read" });
    const outs = await Promise.all([checkout(), checkout(), checkout()]);
    assert.deepEqual(outs.sort(), ["paid", "skip", "skip"]);
    assert.equal(charges, 1);
  });
});
