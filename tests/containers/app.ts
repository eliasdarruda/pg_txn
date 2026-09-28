// One application replica, as deployed in a container. The only pg_txn setup
// is the dependency and `new PgTxn(pool)`: this replica's worker runs its share
// of the transactions and effects, and resumes transactions of replicas that
// stop. Each replica enqueues JOBS "bump" transactions on the shared counters
// 1..ACTORS, one at a time per counter (a key): read the counter, call the
// receiver (the external API), write the new value back.
import os from "node:os";
import pg from "pg";
import { PgTxn } from "@pg-txn/client";

const env = (k: string) => {
  const v = process.env[k];
  if (!v) throw new Error(`${k} is required`);
  return v;
};
const pool = new pg.Pool({ connectionString: env("DATABASE_URL"), max: 8 });
pool.on("error", () => {});
const receiver = env("RECEIVER_URL");
// short leases so the tests' kills are recovered quickly
const pgtxn = new PgTxn(pool, { leaseMs: 3000 });

pgtxn.define("bump", async (tx, input: { counter: number }) => {
  const [c] = (await tx.db.query("SELECT n FROM counters WHERE id = $1", [input.counter])).rows;
  const receipt = await tx.effect(async (ctx) => {
    const res = await fetch(`${receiver}/tick`, {
      method: "POST", headers: { "idempotency-key": ctx.idempotencyKey }, body: String(c!.n + 1),
    });
    if (!res.ok) throw new Error(`receiver ${res.status}`);
    return ctx.effectId;
  }, { name: "tick", retry: true });   // safe: the receiver deduplicates by idempotency key
  await tx.db.query("UPDATE counters SET n = n + 1 WHERE id = $1", [input.counter]);
  return receipt;
});

process.on("SIGTERM", async () => {
  // stop taking work, let what is running finish
  await pgtxn.close(8000);
  process.exit(0);
});

await pgtxn.ready();
console.log(`replica ${os.hostname()} worker ${pgtxn.owner} started`);
const jobs = Number(env("JOBS"));
const actors = Number(env("ACTORS"));
for (let i = 0; i < jobs; i++) {
  const counter = 1 + ((i * 7 + os.hostname().length) % actors);
  await pgtxn.enqueue("bump", { counter }, { key: ["counter", counter] });
}
console.log(`replica ${os.hostname()} enqueued ${jobs}`);
// keep serving: the worker runs transactions for every replica
setInterval(() => {}, 1 << 30);
