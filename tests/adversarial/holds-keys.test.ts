// Keys under stress and in every release path. All of these held.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor, nextId } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool, { leaseMs: 1500 });

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const keysHeld = async (like: string) => (await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key LIKE $1", [like])).rows[0].n;

describe("holds: keys", () => {
  test("40 crossing transfers over 4 accounts, some refused: money conserved, no deadlock, no key left", async () => {
    const ids = [nextId(), nextId(), nextId(), nextId()];
    for (const id of ids) await pool.query("INSERT INTO accounts VALUES ($1, 1000)", [id]);
    const transfer = (from: number, to: number, fail: boolean) => pgtxn.transaction(async (tx) => {
      const x = Number((await tx.db.query("SELECT balance FROM accounts WHERE id = $1", [from])).rows[0].balance);
      const y = Number((await tx.db.query("SELECT balance FROM accounts WHERE id = $1", [to])).rows[0].balance);
      await tx.effect(async () => { await sleep(Math.random() * 30); });
      if (fail) throw new Error("refused");
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [from, x - 10]);
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [to, y + 10]);
    }, { keys: [["acc", from], ["acc", to]] }).catch((e) => { if (!/refused/.test(e.message)) throw e; });
    await Promise.all(Array.from({ length: 40 }, (_, i) => transfer(ids[i % 4], ids[(i + 1 + (i % 3)) % 4], i % 7 === 0)));
    assert.equal((await pool.query("SELECT sum(balance)::int AS s FROM accounts WHERE id = ANY($1)", [ids])).rows[0].s, 4000);
    assert.equal(await keysHeld('["acc",%'), 0);
  });

  test("inline, run() and enqueue() on one key: serialized, no lost update", async () => {
    const id = await newOrder(pool, { amount: 0 });
    let inside = 0;
    let overlap = 0;
    const body = async (tx: any) => {
      const a = Number((await tx.db.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount);
      await tx.effect(async () => { inside++; if (inside > 1) overlap++; await sleep(30); inside--; });
      await tx.db.query("UPDATE orders SET amount = $2 WHERE id = $1", [id, a + 1]);
    };
    pgtxn.define("mix", body);
    const ps: Promise<unknown>[] = [];
    for (let i = 0; i < 10; i++) {
      ps.push(pgtxn.transaction(body, { key: ["mix", id] }));
      ps.push(pgtxn.enqueue("mix", {}, { key: ["mix", id] }).then((t) => pgtxn.wait(t, 30_000)));
      ps.push(pgtxn.run("mix", {}, { key: ["mix", id] }));
    }
    await Promise.all(ps);
    assert.equal(Number((await pool.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount), 30);
    assert.equal(overlap, 0);
  });

  test("a keyed transaction that throws before any effect releases its key at once", async () => {
    const key = `early:${nextId()}`;
    await assert.rejects(pgtxn.transaction(async () => { throw new Error("early"); }, { key }), /early/);
    assert.equal(await keysHeld(key), 0);
  });

  test("a keyed named transaction taken over by another process: the new driver releases the key on commit", async () => {
    const a = newPgTxn(pool, { leaseMs: 30_000 });
    const b = newPgTxn(pool, { leaseMs: 1500 });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const def = (who: string) => async (tx: any) => { await tx.effect(async () => { if (who === "A") await gate; return 1; }, { retry: true }); return "ok"; };
    a.define("fenced-key", def("A"));
    const id = crypto.randomUUID();
    const key = `fenced:${id}`;
    const pa = a.run("fenced-key", {}, { id, key }).catch((e) => e.name);
    await sleep(300);
    await pool.query("UPDATE txn.transactions SET lease_until = now() - interval '1 second' WHERE id = $1", [id]);
    b.define("fenced-key", def("B"));
    assert.equal(await b.wait(id, 15_000), "ok");
    release();
    assert.equal(await pa, "FencedError");
    assert.equal(await keysHeld(key), 0);
  });

  test("a key held by an inline transaction whose process is gone is released by abandon_expired; the waiter proceeds", async () => {
    const ghost = crypto.randomUUID();
    const key = `ghost:${nextId()}`;
    const id = crypto.randomUUID();
    await pool.query("SELECT * FROM txn.start($1, NULL, NULL, $2, 100, $3::text[])", [id, ghost, [key]]);
    const out = await pgtxn.transaction(async () => "got it", { key });
    assert.equal(out, "got it");
    assert.equal((await pool.query("SELECT status FROM txn.transactions WHERE id = $1", [id])).rows[0].status, "abandoned");
  });

  test("keys are text: a string key and its JSON array form are different keys; duplicates in `keys` are fine", async () => {
    const n = nextId();
    const t0 = Date.now();
    await Promise.all([
      pgtxn.transaction(async (tx) => { await tx.effect(() => sleep(300)); }, { key: `k${n}` }),
      pgtxn.transaction(async (tx) => { await tx.effect(() => sleep(300)); }, { key: [`k${n}`] }),
      pgtxn.transaction(async (tx) => { await tx.effect(() => sleep(300)); }, { keys: [`dup${n}`, `dup${n}`, `dup${n}`] }),
    ]);
    assert.ok(Date.now() - t0 < 800, "distinct keys ran concurrently");
    assert.equal(await keysHeld(`%${n}%`), 0);
  });

  test("waiting is not FIFO (documented as cooperative, like advisory locks): a note, not a failure", async () => {
    const key = `fair:${nextId()}`;
    const order: string[] = [];
    const hold = (name: string, ms: number) => pgtxn.transaction(async (tx) => { await tx.effect(() => sleep(ms)); order.push(name); }, { key });
    const first = hold("first", 300);
    await sleep(50);
    const waiter = hold("waiter", 10);
    await sleep(50);
    const stream: Promise<unknown>[] = [];
    for (let i = 0; i < 6; i++) { stream.push(hold(`s${i}`, 120)); await sleep(60); }
    await Promise.all([first, waiter, ...stream]);
    assert.equal(order[0], "first");
    assert.equal(order.length, 8);
  });
});
