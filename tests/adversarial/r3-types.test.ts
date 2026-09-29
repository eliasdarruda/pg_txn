// TypeScript ergonomics: typical Drizzle / Knex / node-postgres usage, as a
// user writes it, typechecked with tsc. Usage files are generated into a
// temporary directory inside tests/ (so bare imports resolve) and removed.
import { test, after, describe } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const DIR = path.join(ROOT, "tests", ".r3-types");
const CLIENT = path.join(ROOT, "clients/typescript/client/src/index.ts");
const DRIZZLE = path.join(ROOT, "clients/typescript/drizzle/src/index.ts");
const KNEX = path.join(ROOT, "clients/typescript/knex/src/index.ts");

after(() => rmSync(DIR, { recursive: true, force: true }));

// returns tsc's errors for one usage file (empty when it typechecks)
function typecheck(name: string, source: string): string {
  mkdirSync(DIR, { recursive: true });
  const file = path.join(DIR, `${name}.ts`);
  writeFileSync(file, source);
  try {
    execFileSync(path.join(ROOT, ".tools/node/bin/node"), [path.join(ROOT, "node_modules/typescript/bin/tsc"), "--noEmit", "--strict",
      "--target", "ES2022", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--allowImportingTsExtensions", "--skipLibCheck",
      "--types", "node", file], { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    return "";
  } catch (e: any) {
    return String(e.stdout || e.stderr);
  }
}

describe("TS types for common usage", () => {
  test("holds: Drizzle (node-postgres) usage typechecks and tx.db is the Drizzle transaction", () => {
    const errors = typecheck("drizzle-usage", `
      import { drizzle } from "drizzle-orm/node-postgres";
      import { pgTable, bigint, text } from "drizzle-orm/pg-core";
      import { eq } from "drizzle-orm";
      import { PgTxn } from ${JSON.stringify(CLIENT)};
      import { drizzleDb } from ${JSON.stringify(DRIZZLE)};
      const orders = pgTable("orders", { id: bigint("id", { mode: "number" }).primaryKey(), status: text("status"), paymentId: text("payment_id") });
      const db = drizzle("postgres://x");
      const pgtxn = new PgTxn(drizzleDb(db));
      export const checkout = (orderId: number) =>
        pgtxn.transaction(async (tx) => {
          const [order] = await tx.db.select().from(orders).where(eq(orders.id, orderId));
          const payment = await tx.effect(async (ctx) => ({ id: ctx.idempotencyKey, total: order?.status }), { retry: true, deps: [order?.status], compensate: (p: { id: string }) => console.log(p.id) });
          await tx.db.update(orders).set({ status: "paid", paymentId: payment.id }).where(eq(orders.id, orderId));
          await tx.spawn(() => Promise.resolve(orderId));
          return payment.id;
        }, { key: ["order", orderId], isolation: "serializable" });
      export const settle = pgtxn.define("settle", async (tx, input: { invoiceId: number }) => { await tx.db.execute("select 1"); return input.invoiceId; });
      export const q = async () => {
        await settle({ invoiceId: 7 }, { key: ["invoice", 7] });
        const id = await pgtxn.enqueue("settle", { invoiceId: 7 }, { key: ["invoice", 7] });
        const out: number = await pgtxn.wait<number>(id);
        await db.transaction(async (trx) => { await pgtxn.spawn(() => 1, { trx }); await pgtxn.enqueue("settle", {}, { trx }); });
        return out;
      };
      // the transaction's result type flows out
      const s: Promise<string> = checkout(1);
      export { s };
    `);
    assert.equal(errors, "", errors);
  });

  test("holds: Knex usage typechecks and tx.db is a Knex.Transaction", () => {
    const errors = typecheck("knex-usage", `
      import knexFactory, { type Knex } from "knex";
      import { PgTxn } from ${JSON.stringify(CLIENT)};
      import { knexDb } from ${JSON.stringify(KNEX)};
      const knex = knexFactory({ client: "pg" });
      const pgtxn = new PgTxn(knexDb(knex));
      export const f = () => pgtxn.transaction(async (tx) => {
        const trx: Knex.Transaction = tx.db;
        await trx("orders").where({ id: 1 }).update({ status: "paid" });
        const r = await tx.effect(() => "x");
        return r.toUpperCase();
      }, { key: "k" });
      export const g = () => knex.transaction(async (trx) => { await pgtxn.spawn(() => 1, { trx }); });
    `);
    assert.equal(errors, "", errors);
  });

  // Hypothesis: `new PgTxn(pool)` picks the Pool overload but T stays `any`
  // (the class default), so `tx.db` is `any` although the README says "tx.db
  // is a PoolClient": a typo in `tx.db.qeury(...)` compiles. The user must
  // write `new PgTxn<pg.PoolClient>(pool)`, which the README does not show.
  test("new PgTxn(pool) types tx.db as pg.PoolClient, not any", () => {
    const errors = typecheck("pool-usage", `
      import pg from "pg";
      import { PgTxn } from ${JSON.stringify(CLIENT)};
      const pgtxn = new PgTxn(new pg.Pool());
      export const f = () => pgtxn.transaction(async (tx) => {
        // @ts-expect-error a typo must not compile if tx.db is a PoolClient
        await tx.db.qeury("SELECT 1");
      });
    `);
    assert.equal(errors, "", `tx.db is any: ${errors}`);
  });

  test("holds: EffectOptions.compensate receives the effect's result type only as any (documented shape); RunOptions/TxKey accept readonly tuples", () => {
    const errors = typecheck("misc-usage", `
      import pg from "pg";
      import { PgTxn, type Tx, type RunOptions, type TxKey } from ${JSON.stringify(CLIENT)};
      const pgtxn = new PgTxn<pg.PoolClient>(new pg.Pool());
      const opts: RunOptions = { key: ["a", 1] as const, keys: [{ b: 2 }, "c"], id: "x", isolation: "repeatable read" };
      const k: TxKey = ["order", 42] as const;
      export const f = (tx: Tx<pg.PoolClient>) => tx.db.query("SELECT 1");
      export const g = () => pgtxn.transaction(f, { ...opts, key: k });
    `);
    assert.equal(errors, "", errors);
  });
});
