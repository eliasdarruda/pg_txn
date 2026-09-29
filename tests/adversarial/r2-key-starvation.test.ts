// Hypothesis: txn.lease_transactions (sql:680-688) looks at the p_max*4
// oldest runnable candidates (ORDER BY created_at LIMIT p_max*4) and skips
// those whose keys are held. With 200 transactions queued behind one held key
// and 10 younger ones on free keys, the 64 candidates are all blocked and the
// free ones are never considered until fewer than 64 blocked ones remain:
// they wait for ~140 serialized runs of the hot key although workers are idle.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 5000 });

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  // fail the queue so it does not keep the next suites' workers busy
  await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE status = 'running' AND name = 'r2-hot'");
  await closeAll();
  await pool.end();
});

describe("leasing behind a hot key", () => {
  test("transactions on free keys are leased promptly although 200 older ones wait on one held key", { timeout: 120_000 }, async () => {
    const hot = `hot:${nextId()}`;
    pgtxn.define("r2-hot", async () => { await sleep(500); return "hot"; });
    // 200 on the hot key, in one statement so they are created oldest-first
    await pool.query("SELECT txn.enqueue('r2-hot', '{}', NULL, ARRAY[$1]::text[], NULL) FROM generate_series(1, 200)", [hot]);
    const started = Date.now();
    const free = await Promise.all(Array.from({ length: 10 }, (_, i) => pgtxn.enqueue("r2-hot", {}, { key: `free:${hot}:${i}` })));
    const results = await Promise.all(free.map((id) => pgtxn.wait(id, 10_000).then(() => Date.now() - started, (e: Error) => `rejected: ${e.message}`)));
    const done = (await pool.query("SELECT count(*)::int AS n FROM txn.transactions WHERE name = 'r2-hot' AND status = 'committed' AND keys[1] = $1", [hot])).rows[0].n;
    assert.ok(results.every((r) => typeof r === "number" && r < 5_000), `free-key transactions after 10 s: ${JSON.stringify(results)}; hot ones committed meanwhile: ${done}`);
  });
});
