// The only thing pg_txn needs from a database library: run a callback in a
// transaction, and run one SQL statement (in that transaction, or on its
// own). Any driver or ORM fits in a few lines; node-postgres is built in.

export type QueryResult = { rows: any[] };

export type TransactionOptions = {
  /** e.g. "serializable"; default: the database's default (read committed). */
  isolation?: "read committed" | "repeatable read" | "serializable";
};

export interface Db<T = unknown> {
  /** Runs fn in one database transaction: commit if it resolves, roll back if it throws. */
  transaction<R>(fn: (trx: T) => Promise<R>, options?: TransactionOptions): Promise<R>;
  /** Runs one statement on trx, or on its own (autocommit) when trx is null. */
  query(trx: T | null, text: string, params: unknown[]): Promise<QueryResult>;
  /**
   * Optional: calls onNotify for NOTIFY on channel (faster wake-ups than
   * polling). connectionString: a direct database endpoint for the listening
   * connection (poolers such as RDS Proxy pin LISTEN connections).
   */
  listen?(channel: string, onNotify: () => void, connectionString?: string): Promise<() => Promise<void>>;
}

type PgClient = { query(text: string, params?: unknown[]): Promise<QueryResult>; release(): void };
type PgPool = {
  connect(): Promise<PgClient>;
  query(text: string, params?: unknown[]): Promise<QueryResult>;
  options?: Record<string, unknown>;
};

export function isPgPool(x: unknown): x is PgPool {
  const p = x as PgPool;
  return !!p && typeof p.connect === "function" && typeof p.query === "function";
}

/** A Db for a node-postgres (pg) Pool. */
export function pgDb(pool: PgPool): Db<PgClient> {
  return {
    async transaction(fn, options) {
      const client = await pool.connect();
      try {
        await client.query(options?.isolation ? `BEGIN ISOLATION LEVEL ${options.isolation.toUpperCase()}` : "BEGIN");
        const out = await fn(client);
        await client.query("COMMIT");
        return out;
      } catch (e) {
        await client.query("ROLLBACK").catch(() => {});
        throw e;
      } finally {
        client.release();
      }
    },
    query(trx, text, params) {
      // no parameters: the simple protocol (allows multi-statement scripts)
      return params.length ? (trx ?? pool).query(text, params) : (trx ?? pool).query(text);
    },
    async listen(channel, onNotify, connectionString) {
      // a dedicated connection, outside the pool
      const pg = (await import("pg")).default;
      const client = new pg.Client((connectionString ? { connectionString } : pool.options) as never);
      client.on("error", () => {});
      await client.connect();
      client.on("notification", (m: { channel: string }) => {
        if (m.channel === channel) onNotify();
      });
      await client.query(`LISTEN ${channel}`);
      return async () => {
        await client.end().catch(() => {});
      };
    },
  };
}
