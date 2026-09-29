// A separate process killed on purpose: it holds keys in an inline
// transaction mid-effect, is mid-compensation, or is mid-spawn.
import pg from "pg";
import { PgTxn } from "../../../clients/typescript/client/src/index.ts";

const cfg = JSON.parse(process.argv[2]) as { url: string; mode: "keys-in-effect" | "in-compensation" | "in-spawn"; key: string; leaseMs: number };
const pool = new pg.Pool({ connectionString: cfg.url, max: 4 });
pool.on("error", () => {});
const pgtxn = new PgTxn(pool, { leaseMs: cfg.leaseMs, onError: () => {} });
const send = (m: unknown) => process.send!(m);
const forever = () => new Promise(() => {});

try {
  if (cfg.mode === "keys-in-effect") {
    await pgtxn.transaction(async (tx) => {
      await tx.effect(async (ctx) => { send({ inEffect: ctx.txId }); await forever(); });
    }, { keys: [cfg.key, `${cfg.key}:2`] });
  } else if (cfg.mode === "in-compensation") {
    await pgtxn.transaction(async (tx) => {
      await tx.effect(async () => "pay", { name: "charge", compensate: async (_p, ctx) => { send({ inCompensation: ctx.effectId }); await forever(); } });
      throw new Error("fail after charge");
    }, { id: cfg.key }).catch(() => {});
    await forever();
  } else if (cfg.mode === "in-spawn") {
    await pgtxn.transaction(async (tx) => {
      await tx.spawn(async (ctx) => { send({ inSpawn: ctx.effectId }); await forever(); });
    }, { id: cfg.key });
    await forever();
  }
} catch (e) {
  send({ error: (e as Error).name });
}
