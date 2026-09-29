// Hypothesis: PgTxn#work (index.ts:685-695) awaits ready() once and, if it
// rejects (the database is down or unreachable when the process boots, a
// DNS blip, a pool that is not yet warm), calls onError and RETURNS: the
// worker loop is gone for the life of the process. ready() itself is retried
// on the next public call ("a failed install is tried again"), so a later
// pgtxn.spawn()/transaction() succeeds and records spawns and compensations
// for this process — but nobody leases them: they sit 'pending' until another
// replica's fail_lost_effects marks them EffectLost after a minute, enqueued
// transactions this process defines are never leased by it, and
// txn.worker_seen is never called (doctor: "no SDK worker"). The Elixir worker
// retries start every second (worker.ex start/1); the TypeScript one does not.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { PgTxn, type Db } from "../../clients/typescript/client/src/index.ts";
import { makePool, closeAll, schema, sleep, waitFor } from "../helpers.ts";

const pool = makePool(6);

before(async () => {
  const p = new PgTxn(pool, { onError: () => {} });
  await p.ready();
  await schema(pool);
  await p.close(1000);
});
after(async () => {
  await closeAll();
  await pool.end();
});

// a Db whose statements fail until `up` is set: the database seen from a
// process that boots before its network is ready
function flakyBoot(): { db: Db<unknown>; up: () => void } {
  let up = false;
  const down = () => Object.assign(new Error("connect ECONNREFUSED 10.0.0.5:5432"), { code: "ECONNREFUSED" });
  const db: Db<unknown> = {
    async transaction(fn, options) {
      if (!up) throw down();
      return pool.connect().then(async (c) => {
        try {
          await c.query(options?.isolation ? `BEGIN ISOLATION LEVEL ${options.isolation.toUpperCase()}` : "BEGIN");
          const r = await fn(c);
          await c.query("COMMIT");
          return r;
        } catch (e) {
          await c.query("ROLLBACK").catch(() => {});
          throw e;
        } finally {
          c.release();
        }
      });
    },
    async query(trx, text, params) {
      if (!up) throw down();
      return params.length ? (trx as any ?? pool).query(text, params) : (trx as any ?? pool).query(text);
    },
  };
  return { db, up: () => { up = true; } };
}

describe("a database that is unreachable when the process boots", () => {
  test("the worker recovers once the database is reachable: a later spawn runs and the process reports itself in txn.workers", { timeout: 30_000 }, async () => {
    const { db, up } = flakyBoot();
    const errors: unknown[] = [];
    const pgtxn = new PgTxn(db, { onError: (e) => errors.push(e), pollMs: 50, listen: false });
    // the worker's first ready() fails while the database is "down"
    await waitFor(async () => errors.length > 0, "the worker's boot error");
    up();
    // the application's own calls succeed now (ready() is retried)
    let ran = false;
    await pgtxn.transaction(async (tx) => { await tx.spawn(async () => { ran = true; }); });
    // ... and the worker should run the spawn and report itself
    let seen = false;
    try {
      await waitFor(async () => ran, "the spawned function", 5_000);
      seen = (await pool.query("SELECT count(*)::int AS n FROM txn.workers WHERE owner = $1", [pgtxn.owner])).rows[0].n > 0;
    } finally {
      const row = (await pool.query("SELECT status FROM txn.effects WHERE local_owner = $1 ORDER BY created_at DESC LIMIT 1", [pgtxn.owner])).rows[0];
      await pgtxn.close(500);
      // the spawn stays pending forever (until another replica marks it EffectLost after a minute)
      assert.equal(ran, true, `the spawned function never ran; the effect is ${JSON.stringify(row)} and the worker loop exited at boot: ${String(errors[0])}`);
      assert.equal(seen, true, "the worker never called txn.worker_seen");
    }
  });

  test("holds: the same boot with the database reachable: the spawn runs at once (control)", { timeout: 15_000 }, async () => {
    const { db, up } = flakyBoot();
    up();
    const pgtxn = new PgTxn(db, { onError: () => {}, pollMs: 50, listen: false });
    let ran = false;
    await pgtxn.transaction(async (tx) => { await tx.spawn(async () => { ran = true; }); });
    await waitFor(async () => ran, "spawn");
    await pgtxn.close(500);
    await sleep(10);
  });
});
