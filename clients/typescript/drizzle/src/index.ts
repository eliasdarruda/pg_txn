// pg_txn for Drizzle (node-postgres driver):
//
//   const pgtxn = new PgTxn(drizzleDb(db))
//   await pgtxn.transaction(async (tx) => {
//     await tx.db.update(orders).set({ status: "paid" }).where(eq(orders.id, id))   // tx.db: the Drizzle transaction
//   })
import { sql, type SQL } from "drizzle-orm";
import { type Db, type TransactionOptions, isPgPool, pgDb } from "@pg-txn/client";

type Executor = { execute(query: SQL): Promise<unknown> };
type DrizzleDb = Executor & {
  transaction<R>(fn: (trx: any) => Promise<R>, config?: { isolationLevel?: TransactionOptions["isolation"] }): Promise<R>;
};

function rowsOf(r: unknown): any[] {
  if (Array.isArray(r)) return r;
  const o = r as { rows?: any[] };
  return o && Array.isArray(o.rows) ? o.rows : [];
}

// $n placeholders become bound parameters; without parameters the text is sent as is
function toSql(text: string, params: unknown[]): SQL {
  if (!params.length) return sql.raw(text);
  const parts = text.split(/\$(\d+)/);
  const chunks: SQL[] = [];
  for (let i = 0; i < parts.length; i++) {
    if (i % 2 === 0) {
      if (parts[i]) chunks.push(sql.raw(parts[i]));
    } else {
      // one bound parameter each (a bare array would expand into a list)
      chunks.push(sql`${sql.param(params[Number(parts[i]) - 1])}`);
    }
  }
  return sql.join(chunks);
}

/** A pg_txn Db for a Drizzle database; tx.db is the Drizzle transaction. */
export function drizzleDb<D extends DrizzleDb>(db: D): Db<Parameters<Parameters<D["transaction"]>[0]>[0]> {
  // on a node-postgres Pool, LISTEN wake-ups work as with a plain Pool
  const client = (db as { $client?: unknown }).$client;
  const listen = isPgPool(client) ? pgDb(client).listen : undefined;
  return {
    listen,
    transaction: (fn, options) => db.transaction(fn, options?.isolation ? { isolationLevel: options.isolation } : undefined),
    async query(trx, text, params) {
      return { rows: rowsOf(await ((trx as Executor | null) ?? db).execute(toSql(text, params))) };
    },
  };
}
