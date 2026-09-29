// Round 2: guarantees that were checked and held (idempotent-id arbitration
// across kinds and outcomes, key claims under hashtext collisions, odd and
// many keys, close() twice, purge vs pending work).
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId, waitFor } from "../helpers.ts";

const LEASE = 1500;
const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: LEASE });
const other = newPgTxn(pool, { leaseMs: LEASE, keyWaitMs: 1500 });

before(async () => {
  await pgtxn.ready();
  await other.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const settled = (r: PromiseSettledResult<unknown>) => (r.status === "fulfilled" ? r.value : `rejected: ${(r.reason as Error).name}`);

describe("holds: idempotent ids across kinds", () => {
  test("transaction({ id }) with the id of a committed enqueued transaction returns its output without running fn", async () => {
    pgtxn.define("r2-idem-named", async (_tx, input: any) => ({ got: input.n }));
    const id = await pgtxn.enqueue("r2-idem-named", { n: 4 });
    assert.deepEqual(await pgtxn.wait(id, 10_000), { got: 4 });
    let ran = false;
    assert.deepEqual(await pgtxn.transaction(async () => { ran = true; }, { id }), { got: 4 });
    // and run() of another name with that id, too
    pgtxn.define("r2-idem-other", async () => { ran = true; });
    assert.deepEqual(await pgtxn.run("r2-idem-other", {}, { id }), { got: 4 });
    assert.equal(ran, false);
  });

  test("the id of a transaction abandoned after an effect ran: TransactionFailedError (status abandoned), fn not run", async () => {
    const id = crypto.randomUUID();
    await pool.query("INSERT INTO txn.transactions (id, status, finished_at, error) VALUES ($1, 'abandoned', now(), '{\"name\": \"AbandonedTransaction\"}')", [id]);
    await pool.query("INSERT INTO txn.effects (tx_id, kind, seq, name, input_hash, status, result) VALUES ($1, 'call', 0, 'charge', 'h', 'orphaned', '{}')", [id]);
    await assert.rejects(pgtxn.transaction(async () => "ran", { id }), (e: any) => e.name === "TransactionFailedError" && e.status === "abandoned");
  });

  test("the id of a transaction abandoned before any effect ran: it runs again", async () => {
    const id = crypto.randomUUID();
    await pool.query("INSERT INTO txn.transactions (id, status, finished_at, error) VALUES ($1, 'abandoned', now(), '{\"name\": \"AbandonedTransaction\"}')", [id]);
    assert.equal(await pgtxn.transaction(async () => "ran", { id }), "ran");
  });

  test("the id of a running inline transaction whose process is gone: the waiter gets AbandonedTransaction after lease + grace", { timeout: 30_000 }, async () => {
    const id = crypto.randomUUID();
    // a row as txn.start leaves it for a process that died right after
    await pool.query("INSERT INTO txn.transactions (id, owner, generation) VALUES ($1, gen_random_uuid(), 1)", [id]);
    await pool.query("INSERT INTO txn.leases (tx_id, lease_until) VALUES ($1, now() + interval '1 second')", [id]);
    const t0 = Date.now();
    await assert.rejects(pgtxn.transaction(async () => "ran", { id }), (e: any) => e.name === "TransactionFailedError" && e.status === "abandoned");
    assert.ok(Date.now() - t0 < 15_000);
  });

  test("the id of an enqueued transaction nobody defines: the waiter times out after keyWaitMs (nothing runs)", async () => {
    const id = await other.enqueue("r2-nobody-defines", {});
    await assert.rejects(other.transaction(async () => "ran", { id }), /did not finish within 1500 ms/);
    await pool.query("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE id = $1", [id]);
  });

  test("an id plus a key another transaction holds: waits for the key, then runs once; a concurrent same-id caller gets the output", async () => {
    const key = `idem-key:${nextId()}`;
    const id = crypto.randomUUID();
    let calls = 0;
    const holder = pgtxn.transaction(async (tx) => { await tx.effect(async () => { await sleep(600); }); }, { key });
    await sleep(100);
    const f = () => pgtxn.transaction(async (tx) => { calls++; await tx.db.query("SELECT 1"); return "once"; }, { id, key });
    const r = await Promise.allSettled([f(), sleep(50).then(f), holder]);
    assert.deepEqual(r.slice(0, 2).map(settled), ["once", "once"]);
    assert.equal(calls, 1);
  });

  test("the same id with different keys: the second call returns the first one's output and claims nothing", async () => {
    const id = crypto.randomUUID();
    const a = pgtxn.transaction(async (tx) => { await tx.effect(async () => { await sleep(300); }); return "a"; }, { id, key: `k1:${id}` });
    await sleep(50);
    assert.equal(await pgtxn.transaction(async () => "b", { id, key: `k2:${id}` }), "a");
    assert.equal(await a, "a");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = $1", [`k2:${id}`])).rows[0].n, 0);
  });

  test("a transaction id reused as a spawn id and vice versa: no interaction (different tables)", async () => {
    const id = crypto.randomUUID();
    let spawned = false;
    await pool.query("SELECT txn.spawn($1, 'nobody', $2)", [crypto.randomUUID(), id]);   // a spawn of another process with that id
    assert.equal(await pgtxn.transaction(async (tx) => { await tx.spawn(async () => { spawned = true; }); return "ran"; }, { id }), "ran");
    await waitFor(async () => spawned, "spawn");
  });
});

describe("holds: keys", () => {
  test("two keys with the same hashtext ('k289282' and 'k191518') do not serialize each other", async () => {
    const [h1, h2] = (await pool.query("SELECT hashtext('k289282') AS a, hashtext('k191518') AS b")).rows.map((r) => [r.a, r.b])[0];
    assert.equal(h1, h2, "the collision pair changed?");
    let firstIn = false;
    let overlapped = false;
    const a = pgtxn.transaction(async (tx) => { firstIn = true; await tx.effect(async () => { await sleep(800); }); firstIn = false; }, { key: "k289282" });
    await sleep(100);
    const t0 = Date.now();
    await pgtxn.transaction(async () => { if (firstIn) overlapped = true; }, { key: "k191518" });
    assert.ok(Date.now() - t0 < 500, "the second key waited for the first one");
    assert.ok(overlapped);
    await a;
  });

  test("500 keys with odd text (quotes, unicode, empty, 10 kB) are claimed at once and all released", async () => {
    const n = nextId();
    const keys = [
      "", "'; DROP TABLE orders; --", "a\"b\\c", "😀", "é".normalize("NFD"), "x".repeat(10_000), "  ", "\t\n",
      ...Array.from({ length: 492 }, (_, i) => `many:${n}:${i}`),
    ];
    assert.equal(await pgtxn.transaction(async (tx) => { await tx.effect(async () => 1); return "ok"; }, { keys }), "ok");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = ANY($1)", [keys])).rows[0].n, 0);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM orders")).rows.length, 1);
  });

  test("keys are released by every ending: commit, throw before an effect, throw after an effect (compensated), fenced", async () => {
    const key = `ends:${nextId()}`;
    await pgtxn.transaction(async () => 1, { key });
    await assert.rejects(pgtxn.transaction(async () => { throw new Error("early"); }, { key }), /early/);
    let compensated = false;
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.effect(async () => "paid", { compensate: async () => { compensated = true; } });
      throw new Error("late");
    }, { key }), /late/);
    await waitFor(async () => compensated, "compensation");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = $1", [key])).rows[0].n, 0);
  });
});

describe("holds: lifecycle", () => {
  test("close() twice, and close() during ready()", async () => {
    const p = newPgTxn(pool, { leaseMs: LEASE });
    await p.close(500);
    await p.close(500);
    const q = newPgTxn(pool, { leaseMs: LEASE });
    const r = q.ready();
    await q.close(500);
    await r;
  });

  test("purge does not delete a committed transaction whose spawn is still pending, and deletes it once done", async () => {
    let release!: () => void;
    const held = new Promise<void>((r) => { release = r; });
    const id = crypto.randomUUID();
    await pgtxn.transaction(async (tx) => { await tx.spawn(() => held); }, { id });
    await sleep(200);
    await pool.query("UPDATE txn.transactions SET finished_at = now() - interval '2 days' WHERE id = $1", [id]);
    await pool.query("SELECT * FROM txn.purge(interval '1 day')");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.transactions WHERE id = $1", [id])).rows[0].n, 1);
    release();
    await waitFor(async () => (await pool.query("SELECT status FROM txn.effects WHERE tx_id = $1", [id])).rows[0]?.status === "succeeded", "spawn done");
    await pool.query("SELECT * FROM txn.purge(interval '1 day')");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM txn.transactions WHERE id = $1", [id])).rows[0].n, 0);
  });

  test("a spawned function that spawns during close(): the drain waits for the new spawn too", async () => {
    const p = newPgTxn(pool, { leaseMs: LEASE });
    await p.ready();
    let inner = false;
    await p.transaction(async (tx) => {
      await tx.spawn(async () => {
        await sleep(200);
        // pgtxn.spawn() would be refused while closing (documented: new calls are refused); a tx.spawn inside a running transaction is not
        await p.transaction(async (tx2) => { await tx2.spawn(async () => { inner = true; }); }).catch(() => {});
      });
    });
    await sleep(50);
    await p.close(5000);
    // either refused while closing, or drained
    const rows = (await pool.query("SELECT status FROM txn.effects WHERE local_owner = $1 AND kind <> 'call' AND status IN ('pending', 'running', 'retry_wait')", [p.owner])).rows;
    assert.deepEqual(rows, [], `pending after close(): ${JSON.stringify(rows)} (inner ran: ${inner})`);
  });
});
