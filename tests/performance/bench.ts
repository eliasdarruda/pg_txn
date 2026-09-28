// pg_txn costs next to the alternatives, on one PostgreSQL:
//   1. a transaction without effects vs the same statements in BEGIN…COMMIT
//   2. a transaction with one effect (an instant in-process call)
//   3. fire-and-forget: spawn() vs a hand-rolled LISTEN/NOTIFY outbox relay,
//      latency (COMMIT → delivery) and throughput
// Usage: node tests/performance/bench.ts > fragment.md   (env: N, BURST, CONC)
import pg from "pg";
import { PgTxn } from "../../clients/typescript/client/src/index.ts";
import { makePool, PG_URL, sleep } from "../helpers.ts";

const N = Number(process.env.N ?? 300);
const BURST = Number(process.env.BURST ?? 3000);
const CONC = Number(process.env.CONC ?? 16);
const pool = makePool(CONC + 8);
const pct = (xs: number[], p: number) => [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor((p / 100) * xs.length))];
const fmt = (xs: number[]) => `${pct(xs, 50).toFixed(2)} / ${pct(xs, 95).toFixed(2)} / ${pct(xs, 99).toFixed(2)}`;
const rows: string[][] = [];

async function timed(n: number, f: (i: number) => Promise<unknown>) {
  const xs: number[] = [];
  for (let i = 0; i < n; i++) {
    const t0 = performance.now();
    await f(i);
    xs.push(performance.now() - t0);
  }
  return xs;
}

async function throughput(total: number, f: (i: number) => Promise<unknown>) {
  let next = 0;
  const t0 = performance.now();
  await Promise.all(Array.from({ length: CONC }, async () => {
    while (next < total) await f(next++);
  }));
  return total / ((performance.now() - t0) / 1000);
}

await pool.query("DROP TABLE IF EXISTS bench_orders");
await pool.query("CREATE TABLE bench_orders (id bigint PRIMARY KEY, status text NOT NULL DEFAULT 'new', note text)");
await pool.query("INSERT INTO bench_orders (id) SELECT generate_series(1, $1)", [BURST + N + 10]);

// ---- 1 + 2: transactions
const pgtxn = new PgTxn(pool);
await pgtxn.ready();
const plain = async (i: number) => {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    await c.query("SELECT status FROM bench_orders WHERE id = $1", [i + 1]);
    await c.query("UPDATE bench_orders SET note = 'x' WHERE id = $1", [i + 1]);
    await c.query("COMMIT");
  } finally {
    c.release();
  }
};
const noEffect = (i: number) => pgtxn.transaction(async (tx) => {
  await tx.db.query("SELECT status FROM bench_orders WHERE id = $1", [i + 1]);
  await tx.db.query("UPDATE bench_orders SET note = 'x' WHERE id = $1", [i + 1]);
});
const oneEffect = (i: number) => pgtxn.transaction(async (tx) => {
  await tx.own("bench_orders", i + 1);
  const r = await tx.effect(async () => ({ ok: true }));
  await tx.db.query("UPDATE bench_orders SET note = $2 WHERE id = $1", [i + 1, String(r.ok)]);
});
for (const [name, f] of [["plain BEGIN … COMMIT (baseline)", plain], ["pg_txn, no effect", noEffect], ["pg_txn, own + 1 effect", oneEffect]] as const) {
  await timed(20, f);
  const lat = await timed(N, f);
  const tps = await throughput(BURST, f);
  rows.push([name, fmt(lat), tps.toFixed(0)]);
  await pool.query("DELETE FROM txn.transactions");
}
console.log(`### Transactions (N=${N} sequential, ${BURST} with ${CONC} concurrent)\n`);
console.log("| | latency p50 / p95 / p99 ms | throughput tx/s |\n|---|---:|---:|");
for (const r of rows) console.log(`| ${r.join(" | ")} |`);

// ---- 3: fire-and-forget delivery
type Delivery = { committedAt: Map<number, number>; delivered: Map<number, number> };
const fresh = (): Delivery => ({ committedAt: new Map(), delivered: new Map() });

async function measureSpawn() {
  const d = fresh();
  const p = new PgTxn(pool, { concurrency: CONC });
  await p.ready();
  await sleep(500);
  const produce = async (n: number) => {
    await p.transaction(async (tx) => {
      await tx.db.query("UPDATE bench_orders SET note = 'e' WHERE id = $1", [n % BURST + 1]);
      await tx.spawn(() => { d.delivered.set(n, performance.now()); });
    });
    d.committedAt.set(n, performance.now());
  };
  return { d, produce, close: () => p.close(2000) };
}

async function measureOutbox() {
  // a tuned hand-rolled outbox: LISTEN/NOTIFY-woken relay, CONC workers, SKIP LOCKED
  await pool.query("CREATE TABLE IF NOT EXISTS outbox (id bigserial PRIMARY KEY, payload jsonb, sent_at timestamptz)");
  await pool.query("TRUNCATE outbox");
  const d = fresh();
  let stop = false;
  let wake: (() => void) | null = null;
  const listener = new pg.Client({ connectionString: PG_URL });
  await listener.connect();
  listener.on("notification", () => wake?.());
  await listener.query("LISTEN outbox");
  const worker = async () => {
    while (!stop) {
      const c = await pool.connect();
      let got = false;
      try {
        await c.query("BEGIN");
        const r = await c.query("SELECT id, payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED");
        if (r.rows[0]) {
          got = true;
          d.delivered.set(r.rows[0].payload.n, performance.now());
          await c.query("UPDATE outbox SET sent_at = now() WHERE id = $1", [r.rows[0].id]);
        }
        await c.query("COMMIT");
      } finally {
        c.release();
      }
      if (!got) await new Promise<void>((res) => { wake = res; setTimeout(res, 1000); });
    }
  };
  const workers = Array.from({ length: CONC }, worker);
  const produce = async (n: number) => {
    const c = await pool.connect();
    try {
      await c.query("BEGIN");
      await c.query("UPDATE bench_orders SET note = 'e' WHERE id = $1", [n % BURST + 1]);
      await c.query("INSERT INTO outbox (payload) VALUES ($1)", [{ n }]);
      await c.query("NOTIFY outbox");
      await c.query("COMMIT");
    } finally {
      c.release();
    }
    d.committedAt.set(n, performance.now());
  };
  return { d, produce, close: async () => { stop = true; wake?.(); await Promise.all(workers); await listener.end(); } };
}

console.log(`\n### Fire-and-forget delivery (COMMIT → delivery; ${BURST}-event burst, ${CONC} producers and ${CONC} consumers)\n`);
console.log("| | latency p50 / p95 / p99 ms | throughput events/s |\n|---|---:|---:|");
for (const [name, make] of [["hand-rolled outbox, LISTEN/NOTIFY relay", measureOutbox], ["pg_txn spawn()", measureSpawn]] as const) {
  const m = await make();
  const lat: number[] = [];
  for (let i = 0; i < N; i++) {
    await m.produce(i);
    const t = m.d.committedAt.get(i)!;
    while (!m.d.delivered.has(i)) await sleep(0.2);
    lat.push(m.d.delivered.get(i)! - t);
  }
  const base = 1_000_000;
  const t0 = performance.now();
  let next = 0;
  await Promise.all(Array.from({ length: CONC }, async () => {
    while (next < BURST) await m.produce(base + next++);
  }));
  while (m.d.delivered.size < N + BURST) await sleep(5);
  const rate = BURST / ((performance.now() - t0) / 1000);
  console.log(`| ${name} | ${fmt(lat)} | ${rate.toFixed(0)} |`);
  await m.close();
}
await pgtxn.close(1000);
await pool.end();
