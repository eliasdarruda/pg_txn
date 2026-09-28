// A separate application process for the recovery tests. It reports its
// progress to the parent (process.send) and is killed or stalled on purpose.
import pg from "pg";
import { PgTxn, FencedError } from "../../../clients/typescript/client/src/index.ts";

const cfg = JSON.parse(process.argv[2]) as {
  url: string; mode: "hang-in-charge" | "hang-before-commit" | "inline-hang" | "stall-in-charge"; txId: string; orderId: number; leaseMs: number;
};
const pool = new pg.Pool({ connectionString: cfg.url, max: 4 });
pool.on("error", () => {});
const pgtxn = new PgTxn(pool, { leaseMs: cfg.leaseMs, onError: () => {} });
const send = (m: unknown) => process.send!(m);
const forever = () => new Promise(() => {});

// the external payment system: idempotent by key, like Stripe
async function charge(input: { orderId: number }, key: string) {
  await pool.query("INSERT INTO ledger (account_id, amount, ref) SELECT $1, 10, $2 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE ref = $2)",
    [input.orderId, key]);
  return { id: `pay_${key.slice(0, 8)}` };
}

async function pay(tx: any, input: { orderId: number }) {
  await tx.own("orders", input.orderId);
  const p = await tx.effect(async (ctx: any) => {
    const r = await charge(input, ctx.idempotencyKey);
    send({ charged: ctx.effectId });
    if (cfg.mode === "hang-in-charge") await forever();
    if (cfg.mode === "stall-in-charge") {
      const until = Date.now() + cfg.leaseMs * 3;      // the event loop is stuck: no heartbeats
      while (Date.now() < until) {}
    }
    return r;
  }, { name: "charge", key: input, retry: true, compensate: async () => {} });
  if (cfg.mode === "hang-before-commit" || cfg.mode === "inline-hang") {
    send({ beforeCommit: true });
    await forever();
  }
  await tx.db.query("UPDATE orders SET status = 'paid', payment_id = $2 WHERE id = $1", [input.orderId, p.id]);
  return p.id;
}

const define = pgtxn.define("pay", pay);
try {
  if (cfg.mode === "inline-hang") await pgtxn.transaction((tx) => pay(tx, { orderId: cfg.orderId }), { id: cfg.txId });
  else await define({ orderId: cfg.orderId }, { id: cfg.txId });
  send({ done: true });
} catch (e) {
  send({ error: (e as Error).name });
}
process.exit(0);
