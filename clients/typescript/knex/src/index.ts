// pg_txn for Knex (and Objection, which runs on Knex), PostgreSQL client:
//
//   const pgtxn = new PgTxn(knexDb(knex))
//   await pgtxn.transaction(async (tx) => {
//     await Order.query(tx.db).patchAndFetchById(id, { status: "paid" })   // tx.db: the Knex transaction
//   })
import type { Knex } from "knex";
import type { Db } from "@pg-txn/client";

// $n placeholders become Knex's positional bindings
function toKnex(text: string, params: unknown[]): [string, unknown[]] {
  const bindings: unknown[] = [];
  const q = text.replace(/\$(\d+)/g, (_, n) => {
    bindings.push(params[Number(n) - 1]);
    return "?";
  });
  return [q, bindings];
}

/** A pg_txn Db for a Knex instance; tx.db is the Knex transaction. */
export function knexDb(knex: Knex): Db<Knex.Transaction> {
  return {
    transaction: (fn, options) =>
      knex.transaction((trx) => fn(trx), options?.isolation ? { isolationLevel: options.isolation } : undefined),
    async query(trx, text, params) {
      const r = params.length ? await (trx ?? knex).raw(...toKnex(text, params) as [string, any[]]) : await (trx ?? knex).raw(text);
      return { rows: (r as { rows?: any[] }).rows ?? [] };
    },
  };
}
