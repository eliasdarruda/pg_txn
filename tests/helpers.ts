import pg from "pg";
import { PgTxn, type PgTxnOptions } from "../clients/typescript/client/src/index.ts";

// the non-superuser role of docker/test-initdb (as on a managed service)
export const PG_URL = process.env.PG_TXN_URL ?? "postgres://app:app@localhost:55432/app";

export function makePool(max = 10, url = PG_URL): pg.Pool {
  const pool = new pg.Pool({ connectionString: url, max });
  pool.on("error", () => {});
  return pool;
}

const pgtxns: PgTxn[] = [];

/** A PgTxn for a test, closed by closeAll(). */
export function newPgTxn(pool: pg.Pool, options: PgTxnOptions = {}): PgTxn<pg.PoolClient> {
  const p = new PgTxn<pg.PoolClient>(pool, { onError: () => {}, ...options });
  pgtxns.push(p);
  return p;
}

export async function closeAll(): Promise<void> {
  await Promise.all(pgtxns.splice(0).map((p) => p.close(2000)));
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function waitFor<T>(f: () => Promise<T | false | null | undefined>, what = "condition", timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await f();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(25);
  }
}

let seq = Date.now() % 1_000_000_000;
/** A fresh row id for tests sharing tables. */
export const nextId = () => ++seq;

/** Tables the suites use (created once; rows are per test). */
export async function schema(pool: pg.Pool): Promise<void> {
  // other suites (e.g. the Elixir client's) may have left tables of the same names
  await pool.query("DROP TABLE IF EXISTS orders, accounts, ledger CASCADE");
  await pool.query(`CREATE TABLE IF NOT EXISTS orders (
    id bigint PRIMARY KEY, status text NOT NULL DEFAULT 'new', amount numeric NOT NULL DEFAULT 10, payment_id text, note text)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS accounts (id bigint PRIMARY KEY, balance numeric NOT NULL)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS ledger (id bigserial PRIMARY KEY, account_id bigint, amount numeric, ref text)`);
}

export async function newOrder(pool: pg.Pool, fields: { status?: string; amount?: number } = {}): Promise<number> {
  const id = nextId();
  await pool.query("INSERT INTO orders (id, status, amount) VALUES ($1, $2, $3)", [id, fields.status ?? "new", fields.amount ?? 10]);
  return id;
}

export async function idleInTransaction(pool: pg.Pool): Promise<number> {
  return (await pool.query(
    "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'")).rows[0].n;
}
