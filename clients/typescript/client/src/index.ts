// pg_txn: transactions that include side effects.
//
//   const pgtxn = new PgTxn(pool)
//   await pgtxn.transaction(async (tx) => {
//     const [order] = (await tx.db.query("SELECT * FROM orders WHERE id = $1", [id])).rows
//     const payment = await tx.effect((ctx) => charge(order, ctx.idempotencyKey))  // no lock or connection held
//     await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id])
//     await tx.spawn(() => email.sendReceipt(id))                   // runs iff this commits
//   }, { key: ["order", id] })                                       // optional: one at a time per order
//
// Your function runs in an ordinary database transaction. When it reaches an
// effect that has not run yet, the transaction is rolled back, the effect is
// called outside of any transaction, its result is recorded, and your function
// runs again: effects that already ran return their recorded results. The run
// that reaches the end commits everything at once. See docs/protocol.md.
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import { type Db, type PgClient, type TransactionOptions, isPgPool, isolationLevel, pgDb } from "./db.ts";
import {
  EffectFailedError, FencedError, KeyTimeoutError, PermanentError, RetryableError, TransactionFailedError,
  errorJson, sqlDetail, sqlState,
} from "./errors.ts";
import { fromTagged, serialize, toTagged } from "./serialize.ts";
import { SCHEMA_SQL, SCHEMA_VERSION } from "./schema.ts";

export * from "./errors.ts";
export { pgDb, isPgPool, type Db, type PgClient, type TransactionOptions, type QueryResult } from "./db.ts";
export { SerializationError } from "./serialize.ts";

export type Retry = boolean | {
  /** Attempts in all, the first included (default 5). */
  attempts?: number;
};

const attemptsOf = (r: Retry | undefined) => {
  if (!r) return 1;
  if (r === true) return 5;
  const n = r.attempts ?? 5;
  if (!Number.isInteger(n) || n < 1 || n > 1000) throw new TypeError(`pg_txn: retry.attempts must be an integer from 1 to 1000, got ${n}`);
  return n;
};
const deliveryOf = (r: Retry | undefined) => (r ? "at-least-once" : "at-most-once");

export type EffectOptions = {
  /** A label for observability (txn.effects, txn.effect_errors); default: the function's name. */
  name?: string;
  /**
   * The data the call depends on, as JSON (e.g. [order.amount]). A re-run
   * reuses the recorded result only if it reaches the effect at the same
   * position with the same deps; if they changed (the data changed while the
   * call ran), it is a new effect and the old one is orphaned (and
   * compensated). Without deps, reuse is by position.
   */
  deps?: unknown;
  /**
   * Off by default: fn is called at most once, and a failure, a timeout or a
   * crash mid-call fails the effect. Turn it on only when fn is safe to call
   * again (e.g. it passes ctx.idempotencyKey to the API it calls): failures
   * other than PermanentError are then retried with backoff, and a call
   * interrupted by a crash is made again. true: 5 attempts.
   */
  retry?: Retry;
  /** Per attempt (default 30000). */
  timeoutMs?: number;
  /**
   * Undoes the effect if the transaction ends up not using its result (it
   * fails, or a re-run no longer calls it). Runs in this process, after the
   * transaction ends, with the effect's retry option.
   */
  compensate?: (result: any, ctx: EffectContext) => unknown;
};

export type SpawnOptions = {
  /** A label for observability (default: the function's name). */
  name?: string;
  /** As for effects: off by default (called at most once). */
  retry?: Retry;
  /** Start this long after the commit (default 0). */
  delayMs?: number;
  /** Per attempt (default 30000). */
  timeoutMs?: number;
};

export type EffectContext = {
  /** Stable across retries, re-runs and crashes: use it as the idempotency key. */
  effectId: string;
  idempotencyKey: string;
  attempt: number;
  /** Aborted when the attempt times out. */
  signal: AbortSignal;
  txId: string | null;
};

export interface Tx<T = any> {
  /** The logical transaction id (the same in every run). */
  readonly id: string;
  /** The database transaction of this run: use it for all your queries. */
  readonly db: T;
  /**
   * Calls fn once for the whole transaction (outside any database
   * transaction: no lock or connection is held while it runs) and returns its
   * recorded result in every run.
   */
  effect<R>(fn: (ctx: EffectContext) => Promise<R> | R, options?: EffectOptions): Promise<R>;
  /**
   * Calls fn iff the transaction commits, right after the commit, in this
   * process (retried on failure; recorded in txn.effects). Returns its id.
   */
  spawn(fn: (ctx: EffectContext) => unknown, options?: SpawnOptions): Promise<string>;
  /** When the transaction started: stable across runs. */
  now(): Date;
  /** A random-looking UUID that is the same in every run. */
  uuid(): string;
}

export type PgTxnOptions = {
  /** Spawned effects, compensations and background transactions run at once by this process (default 16). */
  concurrency?: number;
  /** Idle poll interval when no NOTIFY wake-up is available (default 250). */
  pollMs?: number;
  /** Install the txn schema if it is missing (default true). */
  install?: boolean;
  /** Lease of a transaction or effect this process drives (default 30000). */
  leaseMs?: number;
  /** Longest wait for another transaction with the same key (default 300000). */
  keyWaitMs?: number;
  /**
   * Wake the worker with LISTEN/NOTIFY (default true; enqueued transactions
   * start within milliseconds instead of the poll interval). false: poll only.
   * { connectionString }: listen on a direct database endpoint, e.g. behind
   * RDS Proxy or PgBouncer in transaction mode, which pin or drop LISTEN.
   */
  listen?: boolean | { connectionString: string };
  /** Worker errors (default: console.error). */
  onError?: (e: unknown) => void;
};

/** A transaction key: a string, or JSON (e.g. ["order", 42]). */
export type TxKey = string | number | boolean | readonly unknown[] | Record<string, unknown>;

export type RunOptions = TransactionOptions & {
  id?: string;
  /** Transactions with the same key run one at a time (the others wait, holding nothing). */
  key?: TxKey;
  /** Several keys, claimed all at once or none (no deadlocks), e.g. both accounts of a transfer. */
  keys?: readonly TxKey[];
};

class NeedEffect extends Error {
  constructor() {
    super("pg_txn: this run needs an effect result (internal; do not catch)");
    this.name = "NeedEffect";
  }
}

type Need = { seq: number; name: string; tagged: unknown; fn: (ctx: EffectContext) => unknown; options: EffectOptions };
type Local = { fn: (ctx: EffectContext, input: any) => unknown; timeoutMs?: number; since: number; txId: string | null; compensation: boolean };
type Action = { seq: number; id: string; action: "execute" | "wait" | "done"; attempt: number; wait_ms: number };

// the run whose effect is running: its tx must not be used there (other
// transactions, e.g. one started by a spawned function, may)
const inEffect = new AsyncLocalStorage<object | null>();

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const json = (v: unknown) => JSON.stringify(toTagged(v));
// pg_txn transactions live seconds to minutes: a longer retry delay fails the effect instead
const MAX_RETRY_AFTER_MS = 15 * 60_000;

// a key's text: a string as is, anything else as canonical JSON (sorted object
// keys), so that equal keys match whatever their key order or client
const keyText = (k: TxKey) => {
  if (typeof k === "string") return k;
  if (k === null || k === undefined || typeof k === "function" || typeof k === "symbol") {
    throw new TypeError(`pg_txn: a key must be a string, number, boolean, array or object, got ${String(k)}`);
  }
  const text = serialize(k);
  if (/"\$undefined"/.test(text)) throw new TypeError(`pg_txn: a key contains undefined: ${text}`);
  return text;
};
const keysOf = (o: { key?: TxKey; keys?: readonly TxKey[] }) => {
  const all = [...(o.key === undefined ? [] : [o.key]), ...(o.keys ?? [])].map(keyText);
  return all.length ? all : null;
};

const spawnParams = (owner: string, fn: Function, id: string, o: SpawnOptions) =>
  [owner, o.name ?? (fn.name || "spawn"), id, attemptsOf(o.retry), deliveryOf(o.retry), o.delayMs ?? 0];

type Outcome = { ok: boolean; result: unknown; error: unknown; retryable: boolean; retryAfterMs?: number };

// An outcome whose result can be stored; a result that cannot (a Map, a
// string with U+0000, …) becomes a permanent failure of the effect.
function storable(o: Outcome): Outcome & { stored?: string } {
  if (!o.ok) return o;
  try {
    return { ...o, stored: json(o.result) };
  } catch (e) {
    return { ok: false, result: undefined, retryable: false, retryAfterMs: undefined,
      error: { ...errorJson(e), message: `the effect's result cannot be stored: ${(e as Error).message}` } };
  }
}

function stableUuid(seed: string): string {
  const h = createHash("sha256").update(seed).digest("hex").split("");
  h[12] = "5";
  h[16] = ((parseInt(h[16], 16) & 0x3) | 0x8).toString(16);
  const s = h.join("");
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20, 32)}`;
}

class Run<T> implements Tx<T> {
  seq = 0;
  needs: Need[] = [];
  consumed: string[] = [];
  spawned: string[] = [];
  closed = false;
  ended = false;
  #inflight = 0;
  #idle: (() => void)[] = [];
  #uuids = 0;
  #db!: T;
  readonly id: string;
  private core: PgTxn<any>;
  private startedAt: Date;

  constructor(core: PgTxn<any>, id: string, startedAt: Date) {
    this.core = core;
    this.id = id;
    this.startedAt = startedAt;
  }

  get db(): T {
    this.#check();
    // a sibling branch still running after this run was aborted: its queries
    // would run outside the transaction, after its ROLLBACK
    if (this.closed) throw new NeedEffect();
    return this.#db;
  }

  set db(trx: T) {
    this.#db = trx;
  }

  #check(): void {
    if (inEffect.getStore() === this) {
      throw new Error("pg_txn: tx cannot be used inside an effect's or a spawned function: it runs outside the transaction. Return what you need from the effect and use it after");
    }
    if (this.ended) throw new Error("pg_txn: this transaction has ended; tx cannot be used after it");
  }

  async #q(text: string, params: unknown[]) {
    this.#check();
    if (this.closed) throw new NeedEffect();
    this.#inflight++;
    try {
      return await this.core.db.query(this.#db, text, params);
    } finally {
      if (--this.#inflight === 0) for (const r of this.#idle.splice(0)) r();
    }
  }

  // lets concurrent effect() calls of this run register their needs before the run aborts
  #quiesce(): Promise<void> {
    if (this.#inflight === 0) return Promise.resolve();
    return new Promise((r) => this.#idle.push(r));
  }

  async effect<R>(fn: (ctx: EffectContext) => Promise<R> | R, options: EffectOptions = {}): Promise<R> {
    if (typeof fn !== "function") throw new TypeError("tx.effect(fn, options?): fn must be a function");
    const seq = this.seq++;
    const name = options.name ?? (fn.name || "effect");
    const tagged = toTagged(options.deps === undefined ? null : options.deps);
    const r = (await this.#q("SELECT effect_id, status, result, error FROM txn.effect_lookup($1, $2, $3, $4::jsonb)",
      [this.id, seq, name, JSON.stringify(tagged)])).rows[0];
    if (r.status === "succeeded") {
      this.consumed.push(r.effect_id);
      if (options.compensate) this.core._compensation(r.effect_id, options, this.id);
      return fromTagged(r.result) as R;
    }
    if (r.status === "failed") {
      this.consumed.push(r.effect_id);
      throw new EffectFailedError(name, r.error);
    }
    this.needs.push({ seq, name, tagged, fn, options });
    await this.#quiesce();
    throw new NeedEffect();
  }

  async spawn(fn: (ctx: EffectContext) => unknown, options: SpawnOptions = {}): Promise<string> {
    if (typeof fn !== "function") throw new TypeError("tx.spawn(fn, options?): fn must be a function");
    const id = randomUUID();
    this.core._local(id, fn, options.timeoutMs, this.id, false);
    this.spawned.push(id);
    await this.#q("SELECT txn.spawn($1, $2, $3, $4, $5, $6)", spawnParams(this.core.owner, fn, id, options));
    return id;
  }

  now(): Date {
    this.#check();
    return new Date(this.startedAt);
  }

  uuid(): string {
    this.#check();
    return stableUuid(`${this.id}:${this.#uuids++}`);
  }
}

export class PgTxn<T = PgClient> {
  readonly db: Db<T>;
  /** This process's identity in leases. */
  readonly owner = randomUUID();
  #opts: Required<PgTxnOptions>;

  // the functions of this process's spawned effects (by effect id) and
  // compensations (by the id of the effect they undo)
  #local = new Map<string, Local>();
  #driving = new Set<string>();
  #definitions = new Map<string, (tx: Tx<T>, input: any) => unknown>();
  #ready: Promise<void> | null = null;
  #closed = false;
  #closing = false;
  #leaseNow = false;
  #keyTails = new Map<string, Promise<void>>();
  // callers waiting for a transaction to end, woken by NOTIFY txn_done
  #doneWaiters = new Map<string, Set<() => void>>();
  #calls = 0;
  #wake: (() => void) | null = null;
  #worker: Promise<void> | null = null;
  #unlisten: (() => Promise<void>) | null = null;
  #busy = new Set<Promise<unknown>>();

  /** A database library through its adapter (drizzleDb, knexDb, or your own Db): tx.db is its transaction. */
  constructor(db: Db<T>, options?: PgTxnOptions);
  /** A node-postgres Pool: tx.db is a PoolClient in a transaction. */
  constructor(pool: { connect(): Promise<unknown>; query(text: string, params?: unknown[]): Promise<unknown> }, options?: PgTxnOptions);
  constructor(db: unknown, options: PgTxnOptions = {}) {
    if (isPgPool(db)) this.db = pgDb(db) as unknown as Db<T>;
    else if (db && typeof (db as Db).transaction === "function" && typeof (db as Db).query === "function") this.db = db as Db<T>;
    else throw new TypeError("PgTxn: pass a pg Pool or a Db ({ transaction(fn), query(trx, sql, params) })");
    this.#opts = {
      concurrency: options.concurrency ?? 16,
      pollMs: options.pollMs ?? 250,
      install: options.install ?? true,
      leaseMs: options.leaseMs ?? 30_000,
      keyWaitMs: options.keyWaitMs ?? 300_000,
      onError: options.onError ?? ((e) => console.error("pg_txn worker:", e)),
      listen: options.listen ?? true,
    };
    this.#worker = this.#work();
  }

  // ------------------------------------------------------------------ setup

  /** Installs (if allowed) and checks the txn schema; runs once. */
  ready(): Promise<void> {
    // a failed install is tried again on the next call (e.g. the database was briefly down)
    this.#ready ??= this.#install().catch((e) => {
      this.#ready = null;
      throw e;
    });
    return this.#ready;
  }

  async #install(): Promise<void> {
    const q = (sql: string, params: unknown[] = []) => this.db.query(null, sql, params);
    const present = async (trx: T | null) =>
      // a catalog scan, not to_regclass: after waiting on the install lock the
      // backend's syscache may still hold "does not exist"
      (await this.db.query(trx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                  WHERE n.nspname = 'txn' AND c.relname = 'meta') AS ok`, [])).rows[0].ok;
    if (!(await present(null))) {
      if (!this.#opts.install) throw new Error("pg_txn: the txn schema is not installed (run extension/sql/pg_txn--1.0.sql, or allow install)");
      await this.db.transaction(async (trx) => {
        await this.db.query(trx, "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pg_txn install'))", []);
        if (!(await present(trx))) await this.db.query(trx, SCHEMA_SQL, []);
      });
    }
    const v = (await q("SELECT version FROM txn.meta")).rows[0]?.version;
    if (v !== SCHEMA_VERSION) throw new Error(`pg_txn: schema version ${v} in the database, this client needs ${SCHEMA_VERSION}`);
  }

  /** @internal */
  _compensation(effectId: string, options: EffectOptions, txId: string): void {
    const undo = options.compensate!;
    this._local(effectId, (ctx, input) => undo(fromTagged(input?.result), ctx), options.timeoutMs, txId, true);
  }

  /** @internal */
  _local(id: string, fn: Local["fn"], timeoutMs: number | undefined, txId: string | null, compensation: boolean): void {
    this.#local.set(id, { fn, timeoutMs, since: Date.now(), txId, compensation });
  }

  /**
   * A named transaction: if this process stops while running it, any process
   * that defines the same name resumes it (recorded effects are reused).
   */
  define<I = any, R = any>(name: string, fn: (tx: Tx<T>, input: I) => Promise<R> | R): (input: I, options?: RunOptions) => Promise<R> {
    this.#definitions.set(name, fn as (tx: Tx<T>, input: any) => unknown);
    this.#wake?.();
    return (input, options) => this.run(name, input, options);
  }

  // ------------------------------------------------------------------ running

  /**
   * Runs fn as one transaction that may include effects. With a key,
   * transactions with the same key run one at a time. With an id, it is
   * idempotent: an id that already ended returns its recorded output (or
   * throws its error) without running fn again.
   */
  async transaction<R>(fn: (tx: Tx<T>) => Promise<R> | R, options: RunOptions = {}): Promise<R> {
    isolationLevel(options.isolation);
    return this.#active(async () => {
      await this.ready();
      const id = options.id ?? randomUUID();
      // without an id or keys there is nothing to arbitrate: no durable record until an effect
      if (options.id === undefined && !keysOf(options)) return this.#drive(id, (tx) => fn(tx), new Date(), options);
      return this.#queued(keysOf(options), async () => {
        const started = await this.#start(id, null, null, options);
        if (started === "existing") return this.wait<R>(id, this.#opts.keyWaitMs);
        return this.#drive(id, (tx) => fn(tx), started, options);
      });
    });
  }

  // Records the transaction as running (named, with keys, or with an id) and
  // returns when it started. With a key another transaction holds, waits for
  // that one to end first; with an id that exists already (it ran, or runs
  // now elsewhere), returns "existing": wait for its outcome instead.
  async #start(id: string, name: string | null, input: unknown, options: RunOptions): Promise<Date | "existing"> {
    const keys = keysOf(options);
    const since = Date.now();
    for (;;) {
      const r = (await this.db.query(null,
        "SELECT created_at, holder, existing FROM txn.start($1, $2, $3::jsonb, $4, $5, $6::text[], $7)",
        [id, name, name === null ? null : json(input), this.owner, this.#opts.leaseMs, keys, options.isolation ?? null])).rows[0];
      if (r.existing) return "existing";
      if (!r.holder) return new Date(r.created_at);
      // wait for the holder, then for whoever took a key next, and only try
      // to start once the keys are free (waiters do not all insert at once)
      for (let holder: string | undefined = r.holder; holder;) {
        await this.#waitFor(holder, () => {
          if (this.#closing) throw new Error("pg_txn: this PgTxn is closed (while waiting for a key)");
          if (Date.now() - since > this.#opts.keyWaitMs) throw new KeyTimeoutError(keys!.join(", "), holder!, Date.now() - since);
        });
        holder = (await this.db.query(null, "SELECT tx_id FROM txn.keys WHERE key = ANY ($1::text[]) LIMIT 1", [keys])).rows[0]?.tx_id;
      }
    }
  }

  /** Runs a defined transaction now, in this process (resumable elsewhere if it stops). */
  async run<R = unknown>(name: string, input: unknown, options: RunOptions = {}): Promise<R> {
    const fn = this.#definitions.get(name);
    if (!fn) throw new Error(`pg_txn: no transaction named ${name} is defined in this process`);
    return this.#active(async () => {
      await this.ready();
      const id = options.id ?? randomUUID();
      return this.#queued(keysOf(options), async () => {
        const started = await this.#start(id, name, input, options);
        if (started === "existing") return this.wait<R>(id, this.#opts.keyWaitMs);
        return this.#drive(id, (tx) => fn(tx, input), started, options) as Promise<R>;
      });
    });
  }

  /**
   * Queues a defined transaction to run in the background on any process that
   * defines it. With trx (a database transaction), it is queued iff that commits.
   * With keys, it runs when no other transaction holds any of them.
   */
  async enqueue(name: string, input: unknown,
                options: { trx?: T; id?: string; key?: TxKey; keys?: readonly TxKey[]; isolation?: TransactionOptions["isolation"] } = {}): Promise<string> {
    this.#open();
    isolationLevel(options.isolation);
    await this.ready();
    const r = await this.db.query(options.trx ?? null, "SELECT txn.enqueue($1, $2::jsonb, $3, $4::text[], $5) AS id",
      [name, json(input), options.id ?? null, keysOf(options), options.isolation ?? null]);
    this.#wake?.();
    return r.rows[0].id;
  }

  /**
   * Calls fn in the background in this process, recorded and retried like
   * tx.spawn; with trx (your own database transaction), iff that commits.
   */
  async spawn(fn: (ctx: EffectContext) => unknown, options: SpawnOptions & { trx?: T } = {}): Promise<string> {
    if (typeof fn !== "function") throw new TypeError("pgtxn.spawn(fn, options?): fn must be a function");
    this.#open();
    await this.ready();
    const id = randomUUID();
    this._local(id, fn, options.timeoutMs, null, false);
    await this.db.query(options.trx ?? null, "SELECT txn.spawn($1, $2, $3, $4, $5, $6)", spawnParams(this.owner, fn, id, options));
    this.#wake?.();
    return id;
  }

  // Transactions of this process with a key in common run in order, one at
  // a time: only the first in line claims the keys in the database, so a
  // hand-off wakes one caller, not every waiter (keys in sorted order: no
  // cycles). Waiting here counts toward keyWaitMs.
  async #queued<R>(keys: string[] | null, f: () => Promise<R>): Promise<R> {
    if (!keys) return f();
    const since = Date.now();
    let release!: () => void;
    const mine = new Promise<void>((r) => { release = r; });
    const before: Promise<void>[] = [];
    for (const k of [...new Set(keys)].sort()) {
      before.push(this.#keyTails.get(k) ?? Promise.resolve());
      this.#keyTails.set(k, mine);
    }
    const ahead = Promise.all(before);
    try {
      for (;;) {
        const done = await Promise.race([ahead.then(() => true), sleep(250).then(() => false)]);
        if (done) break;
        if (this.#closing) throw new Error("pg_txn: this PgTxn is closed (while waiting for a key)");
        if (Date.now() - since > this.#opts.keyWaitMs) throw new KeyTimeoutError(keys.join(", "), "(this process)", Date.now() - since);
      }
      return await f();
    } finally {
      // those behind wait for those ahead of this one too (it may have given up early)
      ahead.then(release, release);
      for (const k of keys) if (this.#keyTails.get(k) === mine) ahead.then(() => { if (this.#keyTails.get(k) === mine) this.#keyTails.delete(k); });
    }
  }

  #open(): void {
    if (this.#closing) throw new Error("pg_txn: this PgTxn is closed");
  }

  // Runs a public call, so that close() waits for it.
  async #active<R>(f: () => Promise<R>): Promise<R> {
    this.#open();
    this.#calls++;
    try {
      return await f();
    } finally {
      this.#calls--;
    }
  }

  /** Waits for a transaction (e.g. an enqueued one) and returns its output. */
  async wait<R = unknown>(txId: string, timeoutMs = 60_000): Promise<R> {
    const deadline = Date.now() + timeoutMs;
    for (let ms = 10; ; ms = Math.min(ms * 1.5, 250)) {
      const r = (await this.db.query(null, "SELECT status, output, error FROM txn.status($1)", [txId])).rows[0];
      if (r?.status === "committed") return fromTagged(r.output) as R;
      if (r && r.status !== "running") throw new TransactionFailedError(txId, r.status, r.error);
      if (Date.now() > deadline) throw new Error(`pg_txn: transaction ${txId} did not finish within ${timeoutMs} ms`);
      await sleep(ms);
    }
  }

  async #drive(txId: string, fn: (tx: Tx<T>) => unknown, startedAt: Date, options: TransactionOptions): Promise<any> {
    // keeps the lease for as long as this process drives the transaction,
    // runs included (a no-op until the transaction has a durable record; the
    // lease is its own row, so this never conflicts with a run)
    const heartbeat = setInterval(() => {
      this.db.query(null, "SELECT txn.heartbeat($1, $2, $3)", [txId, this.owner, this.#opts.leaseMs]).catch(() => {});
    }, Math.max(1000, this.#opts.leaseMs / 3));
    this.#driving.add(txId);
    try {
      return await this.#runs(txId, fn, startedAt, options);
    } finally {
      clearInterval(heartbeat);
      this.#driving.delete(txId);
      await this.#keepCompensations(txId);
    }
  }

  // After a transaction ends, keeps only the compensation functions it
  // actually scheduled; compensations this process must run but has no
  // function for (a transaction resumed here took another path than the
  // process that called the effect) are failed as EffectLost at once.
  async #keepCompensations(txId: string): Promise<void> {
    const due = new Set((await this.db.query(null,
      "SELECT compensates::text AS id FROM txn.effects WHERE tx_id = $1 AND kind = 'compensation' AND local_owner = $2 AND status IN ('pending', 'retry_wait', 'running')",
      [txId, this.owner]).catch(() => ({ rows: [] }))).rows.map((r: any) => r.id));
    for (const [id, l] of this.#local) if (l.txId === txId && l.compensation && !due.has(id)) this.#local.delete(id);
    if (due.size) {
      if ([...due].some((id) => !this.#local.has(id))) this.#leaseNow = true;
      this.#wake?.();
    }
  }

  async #runs(txId: string, fn: (tx: Tx<T>) => unknown, startedAt: Date, options: TransactionOptions): Promise<any> {
    let retries = 0;
    let runs = 0;
    for (;;) {
      const run = new Run<T>(this, txId, startedAt);
      runs++;
      try {
        const out = await this.db.transaction(async (trx) => {
          run.db = trx;
          try {
            await this.db.query(trx, "SELECT txn.attempt($1, $2)", [txId, this.owner]);
            const result = await fn(run);
            if (run.needs.length) throw new NeedEffect();
            await this.db.query(trx, "SELECT txn.finish($1, $2, $3::uuid[], $4::jsonb, $5)", [txId, this.owner, run.consumed, json(result), runs]);
            return result;
          } finally {
            // before the COMMIT or ROLLBACK: from now on this run's tx refuses queries
            run.closed = true;
          }
        }, options);
        run.closed = true;
        run.ended = true;
        if (run.spawned.length) this.#wake?.();
        return out;
      } catch (e) {
        run.closed = true;
        for (const id of run.spawned) this.#local.delete(id);
        if (run.needs.length) {
          try {
            await this.#perform(txId, run);
          } catch (pe) {
            run.ended = true;
            if (!(pe instanceof FencedError)) {
              await this.db.query(null, "SELECT txn.fail_transaction($1, $2, $3::jsonb, $4)", [txId, this.owner, JSON.stringify(errorJson(pe)), runs])
                .catch(() => {});
            }
            throw pe;
          }
          continue;
        }
        const state = sqlState(e);
        const detail = sqlDetail(e) ?? "";
        run.ended = true;
        if (state === "55P03" && detail === "fenced") throw new FencedError(txId);
        if ((state === "40001" || state === "40P01") && retries++ < 100) {
          run.ended = false;
          await sleep(Math.min(1000, 5 * 2 ** Math.min(retries, 8)) * Math.random());
          continue;
        }
        await this.db.query(null, "SELECT txn.fail_transaction($1, $2, $3::jsonb, $4)", [txId, this.owner, JSON.stringify(errorJson(e)), runs])
          .catch(() => {});
        throw e;
      }
    }
  }

  async #prepare(txId: string, needs: Need[]): Promise<{ conflict?: any; effects?: Action[] }> {
    const effects = needs.map((n) => ({
      seq: n.seq, name: n.name, input: n.tagged, max_attempts: attemptsOf(n.options.retry),
      delivery: deliveryOf(n.options.retry),
      compensation: n.options.compensate ? (n.options.compensate.name || `undo ${n.name}`) : null,
    }));
    const r = await this.db.query(null, "SELECT txn.prepare_effects($1, $2, $3, $4::jsonb) AS r",
      [txId, this.owner, this.#opts.leaseMs, JSON.stringify(effects)]);
    return r.rows[0].r;
  }

  // Takes the lease and records the intents, then calls the needed effects
  // outside of any transaction.
  async #perform(txId: string, run: Run<T>): Promise<void> {
    const prep = await this.#prepare(txId, run.needs);
    if (prep.conflict) throw new FencedError(txId);
    await Promise.all(prep.effects!.map((a) => this.#execute(txId, run.needs.find((n) => n.seq === a.seq)!, a, run)));
  }

  async #execute(txId: string, need: Need, action: Action, run: object): Promise<void> {
    if (need.options.compensate) this._compensation(action.id, need.options, txId);
    for (;;) {
      if (action.action === "done") return;
      if (action.action === "execute") {
        const outcome = storable(await this.#call(need.fn, { retry: !!need.options.retry, timeoutMs: need.options.timeoutMs },
          action.id, action.attempt, txId, run));
        const done = (await this.db.query(null, "SELECT txn.effect_done($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7) AS r",
          [action.id, this.owner, outcome.ok, outcome.ok ? outcome.stored : null,
            outcome.ok ? null : JSON.stringify(outcome.error), outcome.retryable, outcome.retryAfterMs ?? null])).rows[0].r;
        if (done.compensate) {
          // the call succeeded too late (this process lost the transaction):
          // its compensation was scheduled here, where the function is
          this.#leaseNow = true;
          this.#wake?.();
        }
        if (done.status !== "retry_wait") return;
        await sleep(done.wait_ms);
      } else {
        await sleep(action.wait_ms);
      }
      const prep = await this.#prepare(txId, [need]);
      if (prep.conflict) throw new FencedError(txId);
      action = prep.effects![0];
    }
  }

  async #call(fn: (ctx: EffectContext) => unknown, options: { retry: boolean; timeoutMs?: number }, effectId: string, attempt: number,
              txId: string | null, run: object | null) {
    const timeoutMs = options.timeoutMs ?? 30_000;
    const ctrl = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        ctrl.abort();
        reject(Object.assign(new Error(`effect timed out after ${timeoutMs} ms`), { name: "EffectTimeout" }));
      }, timeoutMs);
    });

    try {
      const result = await Promise.race([
        inEffect.run(run, () => Promise.resolve().then(() => fn({ effectId, idempotencyKey: effectId, attempt, signal: ctrl.signal, txId }))),
        timeout,
      ]);
      return { ok: true as const, result, error: null, retryable: false, retryAfterMs: undefined };
    } catch (e) {
      let retryable = options.retry && !(e instanceof PermanentError);
      const error = errorJson(e);
      let after: number | undefined;
      if (e instanceof RetryableError && e.retryAfterMs !== undefined) {
        if (Number.isFinite(e.retryAfterMs) && e.retryAfterMs <= MAX_RETRY_AFTER_MS) {
          after = Math.max(0, Math.round(e.retryAfterMs));
        } else {
          retryable = false;
          error.message += ` (retryAfterMs ${e.retryAfterMs} is beyond the ${MAX_RETRY_AFTER_MS} ms a transaction may wait)`;
        }
      }
      return { ok: false as const, result: undefined, error, retryable, retryAfterMs: after };
    } finally {
      clearTimeout(timer);
    }
  }

  async #waitFor(txId: string, check: () => void): Promise<void> {
    let wake: (() => void) | null = null;
    const set = this.#doneWaiters.get(txId) ?? new Set();
    const waker = () => wake?.();
    set.add(waker);
    this.#doneWaiters.set(txId, set);
    try {
      for (let ms = 5; ; ms = Math.min(ms * 1.5, 250)) {
        const r = (await this.db.query(null, "SELECT status FROM txn.status($1)", [txId])).rows[0];
        if (!r || r.status !== "running") return;
        check();
        await new Promise<void>((resolve) => {
          const t = setTimeout(resolve, ms);
          wake = () => { clearTimeout(t); resolve(); };
        });
        wake = null;
      }
    } finally {
      set.delete(waker);
      if (!set.size) this.#doneWaiters.delete(txId);
    }
  }

  // ------------------------------------------------------------------ worker

  #track<P extends Promise<unknown>>(p: P): P {
    this.#busy.add(p);
    p.finally(() => this.#busy.delete(p)).catch(() => {});
    return p;
  }

  async #work(): Promise<void> {
    let lastSeen = 0;
    let lastSweep = 0;
    let running = 0;
    let trickle = false;
    // the database may be down at startup: keep trying (with backoff)
    for (let ms = 500; !this.#closed; ms = Math.min(ms * 2, 10_000)) {
      try {
        await this.ready();
        break;
      } catch (e) {
        this.#opts.onError(e);
        await this.#idle(ms);
      }
    }
    const listen = this.#opts.listen;
    if (this.db.listen && listen !== false && !this.#closed) {
      this.#unlisten = await this.db.listen(["txn_effects", "txn_done"], (channel, payload) => {
        if (channel === "txn_effects") this.#wake?.();
        else for (const r of this.#doneWaiters.get(payload) ?? []) r();
      }, typeof listen === "object" ? listen.connectionString : undefined).catch(() => null);
    }
    while (!this.#closed) {
      let found = 0;
      try {
        const now = Date.now();
        const defs = [...this.#definitions.keys()];
        if (now - lastSeen > 10_000) {
          lastSeen = now;
          await this.db.query(null, "SELECT txn.worker_seen($1, $2::jsonb)",
            [this.owner, JSON.stringify({ runtime: typeof (globalThis as any).Bun !== "undefined" ? "bun" : "node", pid: process.pid, defines: defs })]);
        }
        let tick = false;
        if (now - lastSweep > 5_000) {
          lastSweep = now;
          tick = true;
          await this.db.query(null, "SELECT txn.abandon_expired(), txn.expire_effect_leases(), txn.fail_lost_effects()", []);
          await this.#sweepLocal();
        }
        const free = this.#opts.concurrency - running;
        // on the tick too: effects of this process it no longer has a
        // function for (e.g. a compensation scheduled by a resumed
        // transaction) are failed as EffectLost instead of waiting forever
        if ((this.#local.size || tick || this.#leaseNow) && free > 0) {
          this.#leaseNow = false;
          // a trickle of work (the last lease got less than it asked for):
          // wait a moment so one lease picks up several effects
          if (trickle) await sleep(2);
          const rows = (await this.db.query(null,
            "SELECT id, kind, name, input, attempt, generation::text AS generation, tx_id, delivery, compensates FROM txn.lease_effects($1, $2, $3)",
            [this.owner, free, this.#opts.leaseMs])).rows;
          found += rows.length;
          trickle = rows.length > 0 && rows.length < free;
          for (const e of rows) {
            running++;
            // a free slot wakes the loop at once
            this.#track(this.#runLocal(e).catch(this.#opts.onError).finally(() => { running--; this.#wake?.(); }));
          }
        }
        const free2 = this.#opts.concurrency - running;
        if (defs.length && free2 > 0 && !this.#closing) {
          const rows = (await this.db.query(null, "SELECT id, name, input, created_at, isolation FROM txn.lease_transactions($1, $2::text[], $3, $4)",
            [this.owner, defs, free2, this.#opts.leaseMs])).rows;
          found += rows.length;
          for (const t of rows) {
            const fn = this.#definitions.get(t.name)!;
            const input = fromTagged(t.input);
            running++;
            this.#track(this.#drive(t.id, (tx) => fn(tx, input), new Date(t.created_at), { isolation: isolationLevel(t.isolation) })
              .catch((e) => { if (!(e instanceof FencedError)) this.#opts.onError(e); })
              .finally(() => { running--; this.#wake?.(); }));
          }
        }
      } catch (e) {
        if (!this.#closed) this.#opts.onError(e);
      }
      if (!found && !this.#closed) await this.#idle(this.#opts.pollMs);
    }
  }

  // Sleeps up to ms, or until woken (#wake).
  async #idle(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
      const t = setTimeout(done, ms);
      function done() {
        clearTimeout(t);
        resolve();
      }
      this.#wake = done;
    });
    this.#wake = null;
  }

  // Forgets functions whose effect is finished, or never became visible (its
  // transaction rolled back) within an hour.
  async #sweepLocal(): Promise<void> {
    const old = [...this.#local]
      .filter(([, l]) => Date.now() - l.since > 30_000 && !(l.txId && this.#driving.has(l.txId)))
      .map(([id]) => id);
    if (!old.length) return;
    const rows = (await this.db.query(null,
      `SELECT coalesce(compensates, id)::text AS id, status IN ('pending', 'retry_wait', 'running') AS live FROM txn.effects
        WHERE (id = ANY ($1::uuid[]) OR compensates = ANY ($1::uuid[])) AND kind <> 'call'`, [old])).rows;
    const seen = new Map(rows.map((r: any) => [r.id, r.live]));
    for (const id of old) {
      const live = seen.get(id);
      if (live === false || (live === undefined && Date.now() - this.#local.get(id)!.since > 3_600_000)) this.#local.delete(id);
    }
  }

  async #runLocal(e: { id: string; kind: string; name: string; input: unknown; attempt: number; generation: string;
                       tx_id: string | null; delivery: string; compensates: string | null }) {
    const key = e.kind === "compensation" ? e.compensates! : e.id;
    const local = this.#local.get(key);
    if (!local) {
      await this.db.query(null, "SELECT txn.fail_effect($1, $2, $3::bigint, $4::jsonb, false)",
        [e.id, this.owner, e.generation, JSON.stringify({ name: "EffectLost", message: "this process no longer has the effect's function" })]);
      return;
    }
    const heartbeat = setInterval(() => {
      this.db.query(null, "SELECT txn.heartbeat_effect($1, $2, $3::bigint, $4)", [e.id, this.owner, e.generation, this.#opts.leaseMs]).catch(() => {});
    }, Math.max(1000, this.#opts.leaseMs / 3));
    try {
      const outcome = storable(await this.#call((ctx) => local.fn(ctx, e.input), { retry: e.delivery === "at-least-once", timeoutMs: local.timeoutMs },
        e.id, e.attempt, e.tx_id ?? local.txId, null));
      let status = "succeeded";
      if (outcome.ok) {
        await this.db.query(null, "SELECT txn.complete_effect($1, $2, $3::bigint, $4::jsonb)", [e.id, this.owner, e.generation, outcome.stored]);
      } else {
        status = (await this.db.query(null, "SELECT txn.fail_effect($1, $2, $3::bigint, $4::jsonb, $5, $6) AS s",
          [e.id, this.owner, e.generation, JSON.stringify(outcome.error), outcome.retryable, outcome.retryAfterMs ?? null])).rows[0].s;
      }
      if (status !== "retry_wait" && status !== "stale") this.#local.delete(key);
    } finally {
      clearInterval(heartbeat);
    }
  }

  /**
   * Stops taking work and waits (up to drainMs) for work in progress: this
   * process's transactions, spawned functions and compensations, and the
   * background transactions it runs. New calls are refused.
   */
  async close(drainMs = 30_000): Promise<void> {
    this.#closing = true;
    const deadline = Date.now() + drainMs;
    while (Date.now() < deadline) {
      this.#wake?.();
      const pending = this.#local.size
        ? Number((await this.db.query(null,
          `SELECT count(*) AS n FROM txn.effects WHERE local_owner = $1 AND kind <> 'call' AND status IN ('pending', 'retry_wait', 'running')
            AND (status = 'running' OR next_attempt_at < now() + make_interval(secs => $2 / 1000.0))`,
          [this.owner, Math.max(0, deadline - Date.now())]).catch(() => ({ rows: [{ n: 0 }] }))).rows[0].n)
        : 0;
      if (!this.#calls && !this.#driving.size && !this.#busy.size && !pending) break;
      await sleep(50);
    }
    this.#closed = true;
    this.#wake?.();
    await this.#worker;
    await this.#unlisten?.();
  }
}
