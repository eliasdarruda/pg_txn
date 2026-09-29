// Round 3: real-world scenarios written as a user would, run under load
// (100-1000 concurrent), checking the business invariants: money conserved,
// no double charges, every compensation exactly once, no stuck keys, no
// running transactions left. Mock providers are idempotent by key like
// Stripe (the same idempotency key returns the same object) and flaky.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { RetryableError, PermanentError, type Tx } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId, waitFor } from "../helpers.ts";
import type pg from "pg";

const pool = makePool(20);
const pgtxn = newPgTxn(pool, { leaseMs: 5_000, pollMs: 50 });
const replica = newPgTxn(pool, { leaseMs: 5_000, pollMs: 50 });
type T = Tx<pg.PoolClient>;

before(async () => {
  await pgtxn.ready();
  await replica.ready();
  await schema(pool);
  await pool.query(`CREATE TABLE IF NOT EXISTS r3_inventory (sku text PRIMARY KEY, stock int NOT NULL);
                    CREATE TABLE IF NOT EXISTS r3_reservations (id text PRIMARY KEY, sku text NOT NULL, qty int NOT NULL);
                    CREATE TABLE IF NOT EXISTS r3_events (event_id text PRIMARY KEY, applied text NOT NULL);
                    CREATE TABLE IF NOT EXISTS r3_counter (id int PRIMARY KEY, n int NOT NULL);
                    CREATE TABLE IF NOT EXISTS r3_jobs (job text PRIMARY KEY, tenant text NOT NULL, receipt text NOT NULL)`);
});
after(async () => {
  await closeAll();
  await pool.query("DROP TABLE IF EXISTS r3_inventory, r3_reservations, r3_events, r3_counter, r3_jobs");
  await pool.end();
});

const jitter = (max: number) => sleep(Math.random() * max);
const settledSummary = (rs: PromiseSettledResult<unknown>[]) => {
  const out: Record<string, number> = {};
  for (const r of rs) {
    const k = r.status === "fulfilled" ? `ok:${String(r.value)}` : `err:${(r.reason as Error).name}: ${(r.reason as Error).message.slice(0, 60)}`;
    out[k] = (out[k] ?? 0) + 1;
  }
  return out;
};

// a Stripe-like provider: idempotent by key, 10% transient failures, slow-ish
class Provider {
  charges = new Map<string, { id: string; amount: number; orderId: number; refunded: boolean }>();
  calls = 0;
  failRate: number;
  constructor(failRate = 0.1) { this.failRate = failRate; }
  async charge(idempotencyKey: string, orderId: number, amount: number) {
    this.calls++;
    await jitter(20);
    if (Math.random() < this.failRate) throw new RetryableError("503 from provider", { retryAfterMs: 5 + Math.random() * 20 });
    const existing = this.charges.get(idempotencyKey);
    if (existing) return { ...existing };
    const c = { id: `ch_${this.charges.size + 1}_${orderId}`, amount, orderId, refunded: false };
    this.charges.set(idempotencyKey, c);
    return { ...c };
  }
  async refund(idempotencyKey: string, chargeId: string) {
    this.calls++;
    await jitter(10);
    if (Math.random() < this.failRate) throw new RetryableError("503 from provider");
    const c = [...this.charges.values()].find((x) => x.id === chargeId);
    if (!c) throw new PermanentError(`no such charge ${chargeId}`);
    c.refunded = true;
    return { refund: `re_${idempotencyKey.slice(0, 8)}` };
  }
}

// keys, running transactions and pending effects left by THIS scenario
// (earlier suites leave orphaned rows on purpose, e.g. r3-worker-boot)
async function globalInvariants(label: string, since: Date) {
  const keys = (await pool.query("SELECT count(*)::int AS n FROM txn.keys k JOIN txn.transactions t ON t.id = k.tx_id WHERE t.created_at >= $1", [since])).rows[0].n;
  const running = (await pool.query("SELECT count(*)::int AS n FROM txn.transactions WHERE status = 'running' AND created_at >= $1", [since])).rows[0].n;
  const pending = (await pool.query("SELECT count(*)::int AS n FROM txn.effects WHERE status IN ('pending', 'running', 'retry_wait') AND created_at >= $1", [since])).rows[0].n;
  assert.equal(keys, 0, `${label}: keys still held`);
  assert.equal(running, 0, `${label}: transactions still running`);
  assert.equal(pending, 0, `${label}: effects still pending`);
}

describe("holds: scenario 1: order checkout with a Stripe-like provider", () => {
  test("300 concurrent checkouts over 100 orders (3 per order): every order charged once, money conserved, no stuck keys", { timeout: 120_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const provider = new Provider(0.1);
    const orders: number[] = [];
    for (let i = 0; i < 100; i++) orders.push(await (async () => { const id = nextId(); await pool.query("INSERT INTO orders (id, amount) VALUES ($1, $2)", [id, 10 + (i % 5)]); return id; })());
    const checkout = (orderId: number) => pgtxn.transaction(async (tx: T) => {
      const [order] = (await tx.db.query("SELECT * FROM orders WHERE id = $1", [orderId])).rows;
      if (order.status !== "new") return `already ${order.status}`;
      const payment = await tx.effect((ctx) => provider.charge(ctx.idempotencyKey, orderId, Number(order.amount)), {
        name: "charge", retry: true, deps: [Number(order.amount)],
        compensate: (p, ctx) => provider.refund(ctx.idempotencyKey, p.id),
      });
      await tx.db.query("UPDATE orders SET status = 'paid', payment_id = $2 WHERE id = $1", [orderId, payment.id]);
      await tx.db.query("INSERT INTO ledger (account_id, amount, ref) VALUES ($1, $2, $3)", [orderId, payment.amount, payment.id]);
      await tx.spawn(async () => { await jitter(5); }, { name: "receipt" });
      return "paid";
    }, { key: ["order", orderId] });
    const rs = await Promise.allSettled(orders.flatMap((o) => [checkout(o), jitter(30).then(() => checkout(o)), jitter(60).then(() => checkout(o))]));
    const summary = settledSummary(rs);
    assert.equal(summary["ok:paid"], 100, JSON.stringify(summary));
    assert.equal(summary["ok:already paid"], 200, JSON.stringify(summary));
    // money: one charge per order, the ledger equals the charges, nothing refunded
    const paid = (await pool.query("SELECT count(*)::int AS n, count(DISTINCT payment_id)::int AS d FROM orders WHERE id = ANY($1) AND status = 'paid'", [orders])).rows[0];
    assert.deepEqual(paid, { n: 100, d: 100 });
    assert.equal(provider.charges.size, 100, "exactly one charge per order (retries are deduplicated by the idempotency key)");
    assert.equal([...provider.charges.values()].filter((c) => c.refunded).length, 0, "no refund needed with keys");
    const ledger = (await pool.query("SELECT coalesce(sum(amount), 0)::int AS s, count(*)::int AS n FROM ledger WHERE account_id = ANY($1)", [orders])).rows[0];
    assert.equal(ledger.n, 100);
    assert.equal(ledger.s, [...provider.charges.values()].reduce((a, c) => a + c.amount, 0));
    await waitFor(async () => (await pool.query("SELECT count(*)::int AS n FROM txn.effects WHERE name = 'receipt' AND status <> 'succeeded'")).rows[0].n === 0, "receipts", 30_000);
    await globalInvariants("checkout", since);
    console.log(`checkout: provider calls ${provider.calls} for 100 charges (retries)`);
  });
});

describe("holds: scenario 2: inventory reservation with keys and compensation", () => {
  test("400 reservations over 20 SKUs of stock 5: stock never negative, every unused warehouse reservation released exactly once", { timeout: 120_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const skus = Array.from({ length: 20 }, (_, i) => `sku-${nextId()}-${i}`);
    for (const s of skus) await pool.query("INSERT INTO r3_inventory (sku, stock) VALUES ($1, 5)", [s]);
    const warehouse = { reserved: new Map<string, string>(), released: new Map<string, number>(), calls: 0 };
    let declined = 0;
    const reserve = (sku: string) => pgtxn.transaction(async (tx: T) => {
      const [inv] = (await tx.db.query("SELECT stock FROM r3_inventory WHERE sku = $1", [sku])).rows;
      if (inv.stock < 1) return "sold out";
      const r = await tx.effect(async (ctx) => {
        warehouse.calls++;
        await jitter(15);
        warehouse.reserved.set(ctx.idempotencyKey, sku);
        return { reservationId: `wh_${ctx.idempotencyKey}` };
      }, {
        name: "reserve",
        compensate: async (res: { reservationId: string }) => {
          await jitter(5);
          const k = res.reservationId.slice(3);
          warehouse.released.set(k, (warehouse.released.get(k) ?? 0) + 1);
        },
      });
      // a payment declined after the warehouse reserved: the reservation is released
      if (Math.random() < 0.15) { declined++; throw new PermanentError("card declined"); }
      await tx.db.query("UPDATE r3_inventory SET stock = stock - 1 WHERE sku = $1", [sku]);
      await tx.db.query("INSERT INTO r3_reservations (id, sku, qty) VALUES ($1, $2, 1)", [r.reservationId, sku]);
      return "reserved";
    }, { key: ["sku", sku] });
    const rs = await Promise.allSettled(Array.from({ length: 400 }, (_, i) => jitter(50).then(() => reserve(skus[i % 20]))));
    const summary = settledSummary(rs);
    assert.equal(summary["ok:reserved"], 100, JSON.stringify(summary));
    assert.equal(summary["err:PermanentError: card declined"] ?? 0, declined);
    const inv = (await pool.query("SELECT min(stock)::int AS mn, sum(stock)::int AS s FROM r3_inventory WHERE sku = ANY($1)", [skus])).rows[0];
    assert.equal(inv.mn, 0, "every SKU sold out, none oversold");
    assert.equal(inv.s, 0);
    const dbReservations = (await pool.query("SELECT count(*)::int AS n FROM r3_reservations WHERE sku = ANY($1)", [skus])).rows[0].n;
    assert.equal(dbReservations, 100);
    // every warehouse reservation is either in the database or released exactly once
    await waitFor(async () => warehouse.released.size >= declined, "compensations", 30_000);
    await sleep(500);
    const inDb = new Set((await pool.query("SELECT id FROM r3_reservations WHERE sku = ANY($1)", [skus])).rows.map((r) => r.id.slice(3)));
    for (const [k] of warehouse.reserved) {
      const released = warehouse.released.get(k) ?? 0;
      assert.ok(inDb.has(k) ? released === 0 : released === 1, `reservation ${k}: in db ${inDb.has(k)}, released ${released} times`);
    }
    assert.equal(warehouse.reserved.size, 100 + declined);
    await globalInvariants("inventory", since);
  });
});

describe("holds: scenario 3: a saga of 5 effects failing at step 4", () => {
  test("100 concurrent sagas: steps 1-3 compensated exactly once each, step 5 never called, status failed", { timeout: 120_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const calls = new Map<string, number[]>();
    const undone = new Map<string, number[]>();
    const bump = (m: Map<string, number[]>, saga: string, step: number) => { const a = m.get(saga) ?? [0, 0, 0, 0, 0]; a[step - 1]++; m.set(saga, a); };
    const saga = (id: string) => pgtxn.transaction(async (tx: T) => {
      for (let step = 1; step <= 5; step++) {
        await tx.effect(async () => {
          bump(calls, id, step);
          await jitter(10);
          if (step === 4) throw new PermanentError("step 4 refused");
          return { step };
        }, { name: `step${step}`, retry: true, compensate: async (r: { step: number }) => { await jitter(5); bump(undone, id, r.step); } });
      }
      return "done";
    }, { id });
    const ids = Array.from({ length: 100 }, () => crypto.randomUUID());
    const rs = await Promise.allSettled(ids.map((id) => jitter(40).then(() => saga(id))));
    const summary = settledSummary(rs);
    assert.equal(summary["err:EffectFailedError: effect step4 failed: PermanentError: step 4 refused"], 100, JSON.stringify(summary));
    await waitFor(async () => [...undone.values()].filter((a) => a[0] + a[1] + a[2] === 3).length === 100, "all compensations", 30_000);
    await sleep(500);
    for (const id of ids) {
      assert.deepEqual(calls.get(id), [1, 1, 1, 1, 0], `saga ${id} calls`);
      assert.deepEqual(undone.get(id), [1, 1, 1, 0, 0], `saga ${id} compensations`);
    }
    const st = (await pool.query("SELECT status, count(*)::int AS n FROM txn.transactions WHERE id = ANY($1) GROUP BY 1", [ids])).rows;
    assert.deepEqual(st, [{ status: "failed", n: 100 }]);
    const comps = (await pool.query("SELECT status, count(*)::int AS n FROM txn.effects WHERE tx_id = ANY($1) AND kind = 'compensation' GROUP BY 1", [ids])).rows;
    assert.deepEqual(comps, [{ status: "succeeded", n: 300 }]);
    await globalInvariants("saga", since);
  });
});

describe("holds: scenario 4: a webhook handler with duplicate deliveries", () => {
  test("100 events delivered 4 times concurrently and once more after: applied once each, every delivery gets the same result", { timeout: 120_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const applied = new Map<string, number>();
    // a uuid derived from the event id (as an application would: uuid v5 or a hash)
    const stableId = (eventId: string) => {
      const h = createHash("sha256").update(eventId).digest("hex");
      return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
    };
    const handle = (eventId: string) => pgtxn.transaction(async (tx: T) => {
      const result = await tx.effect(async () => {
        await jitter(20);
        applied.set(eventId, (applied.get(eventId) ?? 0) + 1);
        return { downstream: `d_${eventId}` };
      }, { name: "apply" });
      await tx.db.query("INSERT INTO r3_events (event_id, applied) VALUES ($1, $2)", [eventId, result.downstream]);
      return result.downstream;
    }, { id: stableId(eventId) });
    const events = Array.from({ length: 100 }, (_, i) => `evt_${nextId()}_${i}`);
    const rs = await Promise.allSettled(events.flatMap((e) => [handle(e), jitter(20).then(() => handle(e)), jitter(40).then(() => handle(e)), jitter(80).then(() => handle(e))]));
    const summary = settledSummary(rs);
    assert.equal(Object.keys(summary).filter((k) => k.startsWith("err:")).length, 0, JSON.stringify(summary));
    // a late redelivery, after everything ended
    for (const e of events.slice(0, 20)) assert.equal(await handle(e), `d_${e}`);
    for (const e of events) assert.equal(applied.get(e), 1, `event ${e} applied ${applied.get(e)} times`);
    const rows = (await pool.query("SELECT count(*)::int AS n FROM r3_events WHERE event_id = ANY($1)", [events])).rows[0].n;
    assert.equal(rows, 100);
    await globalInvariants("webhooks", since);
  });
});

describe("scenario 5: a high-contention counter", () => {
  // Correctness holds (the counter is exact, no effect runs twice), but the
  // throughput of ONE contended key is bounded by the key hand-off latency:
  // waiters poll txn.status(holder) with a backoff of up to 200 ms
  // (#waitFor, index.ts:668-675) and nobody listens to the 'txn_done' NOTIFY
  // that txn.finish/fail_transaction/abandon_expired emit (sql:273, 459, 783;
  // grep: no client subscribes to it). Each hand-off costs a poll interval:
  // 500 transactions on one key take minutes (the first version of this test
  // timed out at 180 s); the README table shows 6,400 tx/s uncontended.
  test("holds: 200 concurrent keyed increments with an effect each: the counter is exact and every effect called once", { timeout: 300_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const id = nextId();
    await pool.query("INSERT INTO r3_counter (id, n) VALUES ($1, 0)", [id]);
    let effects = 0;
    const inc = () => pgtxn.transaction(async (tx: T) => {
      const [{ n }] = (await tx.db.query("SELECT n FROM r3_counter WHERE id = $1", [id])).rows;
      const token = await tx.effect(async () => { effects++; await jitter(2); return `t${n}`; }, { deps: [n] });
      await tx.db.query("UPDATE r3_counter SET n = $2 WHERE id = $1", [id, n + 1]);
      return token;
    }, { key: ["counter", id] });
    const t0 = Date.now();
    const rs = await Promise.allSettled(Array.from({ length: 200 }, () => inc()));
    const took = Date.now() - t0;
    const summary = settledSummary(rs);
    assert.equal(Object.keys(summary).filter((k) => k.startsWith("err:")).length, 0, JSON.stringify(summary));
    const [{ n }] = (await pool.query("SELECT n FROM r3_counter WHERE id = $1", [id])).rows;
    assert.equal(n, 200);
    assert.equal(effects, 200, "no effect ran twice (a deps change would have re-called it and orphaned the old one)");
    console.log(`counter: 200 keyed transactions with an effect on ONE key in ${took} ms = ${(200_000 / took).toFixed(1)} tx/s per contended key`);
    await globalInvariants("counter", since);
  });

  test("one contended key sustains at least 50 transactions per second (each transaction takes ~10 ms of work)", { timeout: 300_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const id = nextId();
    await pool.query("INSERT INTO r3_counter (id, n) VALUES ($1, 0)", [id]);
    const inc = () => pgtxn.transaction(async (tx: T) => {
      const [{ n }] = (await tx.db.query("SELECT n FROM r3_counter WHERE id = $1", [id])).rows;
      await tx.effect(async () => `t${n}`, { deps: [n] });
      await tx.db.query("UPDATE r3_counter SET n = $2 WHERE id = $1", [id, n + 1]);
    }, { key: ["counter", id] });
    const t0 = Date.now();
    await Promise.all(Array.from({ length: 100 }, () => inc()));
    const took = Date.now() - t0;
    const rate = 100_000 / took;
    console.log(`counter: 100 keyed transactions on ONE key in ${took} ms = ${rate.toFixed(1)} tx/s`);
    assert.ok(rate >= 50, `${rate.toFixed(1)} tx/s on one contended key: the hand-off is a poll (up to 200 ms), not the txn_done NOTIFY`);
  });
});

describe("holds: scenario 6: a job queue with enqueue + keys + retries + a flaky worker", () => {
  test("200 jobs over 20 tenants on two replicas (one restarted midway): each job runs once per tenant in order, receipts unique, none lost", { timeout: 180_000 }, async () => {
    const since = new Date(Date.now() - 1000);
    const receipts = new Map<string, string>();      // idempotency key -> receipt (the provider)
    let providerCalls = 0;
    const active = new Map<string, string>();        // tenant -> job running now (in this process)
    let overlaps = 0;
    const body = async (tx: T, input: { job: string; tenant: string }) => {
      if (active.has(input.tenant) && active.get(input.tenant) !== input.job) overlaps++;
      active.set(input.tenant, input.job);
      try {
        const r = await tx.effect(async (ctx) => {
          providerCalls++;
          await jitter(10);
          if (Math.random() < 0.3) throw new RetryableError("flaky provider", { retryAfterMs: 5 });
          if (!receipts.has(ctx.idempotencyKey)) receipts.set(ctx.idempotencyKey, `rc_${input.job}`);
          return receipts.get(ctx.idempotencyKey)!;
        }, { name: "provider", retry: { attempts: 20 } });
        await tx.db.query("INSERT INTO r3_jobs (job, tenant, receipt) VALUES ($1, $2, $3)", [input.job, input.tenant, r]);
        return r;
      } finally {
        if (active.get(input.tenant) === input.job) active.delete(input.tenant);
      }
    };
    pgtxn.define("r3-job", body);
    let worker2 = newPgTxn(pool, { leaseMs: 5_000, pollMs: 50 });
    worker2.define("r3-job", body);
    const jobs = Array.from({ length: 200 }, (_, i) => ({ job: `job_${nextId()}_${i}`, tenant: `tenant_${i % 20}` }));
    const ids: string[] = [];
    for (const j of jobs) ids.push(await pgtxn.enqueue("r3-job", j, { key: ["tenant", j.tenant] }));
    // the flaky worker: restarted (gracefully) while jobs run
    await sleep(300);
    await worker2.close(10_000);
    worker2 = newPgTxn(pool, { leaseMs: 5_000, pollMs: 50 });
    worker2.define("r3-job", body);
    const outs = await Promise.all(ids.map((id) => pgtxn.wait<string>(id, 120_000)));
    assert.equal(new Set(outs).size, 200, "every job got its own receipt");
    const rows = (await pool.query("SELECT count(*)::int AS n, count(DISTINCT receipt)::int AS d FROM r3_jobs WHERE job = ANY($1)", [jobs.map((j) => j.job)])).rows[0];
    assert.deepEqual(rows, { n: 200, d: 200 });
    assert.equal(receipts.size, 200, "the provider issued one receipt per job (retries deduplicated by the idempotency key)");
    assert.equal(overlaps, 0, "two jobs of one tenant ran at the same time");
    const st = (await pool.query("SELECT status, count(*)::int AS n FROM txn.transactions WHERE id = ANY($1) GROUP BY 1", [ids])).rows;
    assert.deepEqual(st, [{ status: "committed", n: 200 }]);
    console.log(`jobs: provider calls ${providerCalls} for 200 receipts`);
    await globalInvariants("jobs", since);
  });
});
