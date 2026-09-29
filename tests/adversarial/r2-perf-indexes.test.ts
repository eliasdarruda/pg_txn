// Hypothesis: three queries the clients run periodically scan the whole
// txn.effects table (measured with 300k rows: fail_lost_effects 600 ms,
// #sweepLocal 120 ms, close() 26 ms):
//   - txn.fail_lost_effects (sql:539), every 5 s per worker: its predicate
//     includes status 'running', which the partial index effects_due
//     (pending, retry_wait) does not cover, and there is no index on created_at;
//   - #sweepLocal / Worker.forget (index.ts:769): `compensates = ANY(...)` has
//     no index;
//   - close() (index.ts:819): local_owner + status incl. 'running', same as
//     the first.
// With 30 days of retention (docs/operations.md) and N replicas this is N
// full scans of a multi-million-row table every 5 s.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll } from "../helpers.ts";

const pool = makePool(4);
const pgtxn = newPgTxn(pool);

before(async () => {
  await pgtxn.ready();
  await pool.query(`INSERT INTO txn.transactions (id, name, status, finished_at)
                    SELECT gen_random_uuid(), 'r2-perf', 'committed', now() - interval '2 days' FROM generate_series(1, 100000)`);
  await pool.query(`INSERT INTO txn.effects (tx_id, kind, name, status, local_owner, completed_at)
                    SELECT id, 'spawn', 'r2-perf', 'succeeded', gen_random_uuid(), now() FROM txn.transactions WHERE name = 'r2-perf'`);
  await pool.query("ANALYZE txn.effects");
});
after(async () => {
  await pool.query("DELETE FROM txn.transactions WHERE name = 'r2-perf'");   // cascades to the effects
  await closeAll();
  await pool.end();
});

const seqScansEffects = (node: any): boolean =>
  (node["Node Type"] === "Seq Scan" && node["Relation Name"] === "effects") || (node.Plans ?? []).some(seqScansEffects);
// the plan, or null when it scans txn.effects sequentially
const plan = async (sql: string) => {
  const p = (await pool.query(`EXPLAIN (FORMAT JSON) ${sql}`)).rows[0]["QUERY PLAN"][0].Plan;
  return seqScansEffects(p) ? `sequential scan of txn.effects: ${JSON.stringify(p)}` : null;
};

describe("hot queries with 100k finished effects", () => {
  test("fail_lost_effects (every 5 s per worker) does not scan every effect", async () => {
    const p = await plan(`SELECT 1 FROM txn.effects e WHERE e.kind <> 'call' AND e.status IN ('pending', 'retry_wait', 'running')
      AND e.created_at < clock_timestamp() - interval '1 minute'
      AND NOT EXISTS (SELECT 1 FROM txn.workers w WHERE w.owner = e.local_owner AND w.seen_at > clock_timestamp() - interval '1 minute')`);
    assert.equal(p, null, p ?? "");
  });

  test("the local-function sweep (every 5 s) does not scan every effect", async () => {
    const p = await plan(`SELECT coalesce(compensates, id) FROM txn.effects
      WHERE (id = ANY ('{00000000-0000-0000-0000-000000000000}'::uuid[]) OR compensates = ANY ('{00000000-0000-0000-0000-000000000000}'::uuid[])) AND kind <> 'call'`);
    assert.equal(p, null, p ?? "");
  });

  test("close()'s pending count (every 50 ms while draining) does not scan every effect", async () => {
    const p = await plan(`SELECT count(*) FROM txn.effects WHERE local_owner = '00000000-0000-0000-0000-000000000000' AND kind <> 'call' AND status IN ('pending', 'retry_wait', 'running')`);
    assert.equal(p, null, p ?? "");
  });
});
