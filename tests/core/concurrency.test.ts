// Concurrency without locks: effects hold nothing; data that changes while
// an effect runs is seen by the re-run (optimistic); a key makes
// transactions with the same key run one at a time.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { KeyTimeoutError } from "../../clients/typescript/client/src/index.ts";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, idleInTransaction, nextId, waitFor } from "../helpers.ts";

const pool = makePool(10);
const pgtxn = newPgTxn(pool);

before(async () => {
  await pgtxn.ready();
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

const read = async (tx: any, id: number) => (await tx.db.query("SELECT status, amount::float AS amount FROM orders WHERE id = $1", [id])).rows[0];
const amount = async (id: number) => Number((await pool.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount);

describe("concurrency", () => {
  test("during an effect nothing is held: other writers go through, no idle transaction", async () => {
    const id = await newOrder(pool);
    let during: { update?: string; idle?: number } = {};
    await pgtxn.transaction(async (tx) => {
      await read(tx, id);
      await tx.effect(async () => {
        during = {
          update: await pool.query("UPDATE orders SET note = 'x' WHERE id = $1", [id]).then(() => "went through", (e) => e.code),
          idle: await idleInTransaction(pool),
        };
      });
      await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id]);
    });
    assert.deepEqual(during, { update: "went through", idle: 0 });
  });

  test("effects hold no connection: a 2-connection pool runs 20 concurrent 500 ms effects in about 500 ms", async () => {
    const small = makePool(2);
    const p = newPgTxn(small);
    const ids = await Promise.all(Array.from({ length: 20 }, () => newOrder(pool)));
    const t0 = Date.now();
    await Promise.all(ids.map((id) => p.transaction(async (tx) => {
      await tx.effect(() => sleep(500));
      await tx.db.query("UPDATE orders SET status = 'done' WHERE id = $1", [id]);
    })));
    const ms = Date.now() - t0;
    await small.end();
    assert.ok(ms < 2500, `took ${ms} ms (held connections would need 20 × 500 / 2 = 5000 ms)`);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM orders WHERE id = ANY($1) AND status = 'done'", [ids])).rows[0].n, 20);
  });

  test("the order was cancelled during the charge: the re-run sees it, and the unused charge is compensated", async () => {
    const id = await newOrder(pool);
    const refunds: string[] = [];
    const out = await pgtxn.transaction(async (tx) => {
      const o = await read(tx, id);
      if (o.status !== "new") return "skipped";
      const p = await tx.effect(async () => {
        await pool.query("UPDATE orders SET status = 'cancelled' WHERE id = $1", [id]);
        return { id: "pay_1" };
      }, { compensate: async (p) => { refunds.push(p.id); } });
      await tx.db.query("UPDATE orders SET status = 'paid', payment_id = $2 WHERE id = $1", [id, p.id]);
      return "paid";
    });
    assert.equal(out, "skipped");
    assert.equal((await pool.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status, "cancelled");
    await waitFor(async () => refunds.length === 1, "refund");
  });

  test("deps: the amount changed during the charge: a new charge on the new amount, the old one compensated", async () => {
    const id = await newOrder(pool, { amount: 10 });
    const charged: number[] = [];
    const refunded: number[] = [];
    await pgtxn.transaction(async (tx) => {
      const o = await read(tx, id);
      await tx.effect(async () => {
        charged.push(o.amount);
        if (charged.length === 1) await pool.query("UPDATE orders SET amount = 25 WHERE id = $1", [id]);
        return o.amount;
      }, { deps: [o.amount], compensate: async (a) => { refunded.push(a); } });
    });
    assert.deepEqual(charged, [10, 25]);
    await waitFor(async () => refunded.length === 1, "refund");
    assert.deepEqual(refunded, [10]);
  });

  test("the same key: one at a time, each sees the other's committed writes", async () => {
    const id = await newOrder(pool, { amount: 0 });
    const inside: string[] = [];
    let overlap = false;
    const bump = (name: string) => pgtxn.transaction(async (tx) => {
      await tx.effect(async () => {
        if (inside.length) overlap = true;
        inside.push(name);
        await sleep(100);
        inside.splice(inside.indexOf(name), 1);
      });
      await tx.db.query("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id]);
    }, { key: ["order", id] });
    await Promise.all([bump("A"), bump("B"), bump("C")]);
    assert.equal(overlap, false, "two transactions with the same key ran their effects at once");
    assert.equal(await amount(id), 3);
  });

  test("two checkouts of the same order with a key: one charge", async () => {
    const id = await newOrder(pool);
    let charges = 0;
    const checkout = () => pgtxn.transaction(async (tx) => {
      const o = await read(tx, id);
      if (o.status !== "new") return "already paid";
      await tx.effect(async () => { charges++; await sleep(100); });
      await tx.db.query("UPDATE orders SET status = 'paid' WHERE id = $1", [id]);
      return "paid";
    }, { key: `order:${id}` });
    const outs = await Promise.all([checkout(), checkout()]);
    assert.deepEqual(outs.sort(), ["already paid", "paid"]);
    assert.equal(charges, 1);
  });

  test("several keys, claimed together: crossing transfers never deadlock or interleave", async () => {
    const a = nextId();
    const b = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100), ($2, 100)", [a, b]);
    const inside: string[] = [];
    let overlap = false;
    const transfer = (from: number, to: number) => pgtxn.transaction(async (tx) => {
      const [x] = (await tx.db.query("SELECT balance::float AS balance FROM accounts WHERE id = $1", [from])).rows;
      const [y] = (await tx.db.query("SELECT balance::float AS balance FROM accounts WHERE id = $1", [to])).rows;
      await tx.effect(async () => {
        if (inside.length) overlap = true;
        inside.push(`${from}`);
        await sleep(80);
        inside.pop();
      }, { name: "fraud-check" });
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [from, x.balance - 10]);
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [to, y.balance + 10]);
    }, { keys: [["account", from], ["account", to]] });
    await Promise.all([transfer(a, b), transfer(b, a), transfer(a, b), transfer(b, a), transfer(a, b)]);
    assert.equal(overlap, false);
    const r = (await pool.query("SELECT balance FROM accounts WHERE id IN ($1, $2) ORDER BY id", [a, b])).rows.map((x) => Number(x.balance));
    assert.deepEqual(r, [90, 110], "money conserved, every transfer applied once");
  });

  test("different keys run concurrently", async () => {
    const t0 = Date.now();
    await Promise.all([1, 2, 3, 4].map((k) => pgtxn.transaction(async (tx) => {
      await tx.effect(() => sleep(300));
    }, { key: ["k", nextId(), k] })));
    assert.ok(Date.now() - t0 < 900, "they did not wait for each other");
  });

  test("a key is released when the transaction fails, or has no effects", async () => {
    const key = ["release", nextId()];
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.effect(async () => 1);
      throw new Error("business rule");
    }, { key }), /business rule/);
    await pgtxn.transaction(async () => "no effects", { key });
    const t0 = Date.now();
    await pgtxn.transaction(async () => "again", { key });
    assert.ok(Date.now() - t0 < 500);
    const held = (await pool.query("SELECT count(*)::int AS n FROM txn.keys WHERE key = $1", [JSON.stringify(key)])).rows[0].n;
    assert.equal(held, 0);
  });

  test("enqueued transactions with the same key run one at a time", async () => {
    const id = await newOrder(pool, { amount: 0 });
    const inside: number[] = [];
    let overlap = false;
    pgtxn.define("keyed-bump", async (tx, { n }: { n: number }) => {
      await tx.effect(async () => {
        if (inside.length) overlap = true;
        inside.push(n);
        await sleep(50);
        inside.splice(inside.indexOf(n), 1);
      });
      await tx.db.query("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id]);
    });
    const ids = [];
    for (let n = 0; n < 5; n++) ids.push(await pgtxn.enqueue("keyed-bump", { n }, { key: ["order", id] }));
    for (const t of ids) await pgtxn.wait(t, 15_000);
    assert.equal(overlap, false);
    assert.equal(await amount(id), 5);
  });

  test("enqueued transactions with the same key run one at a time", async () => {
    const id = await newOrder(pool, { amount: 0 });
    const inside: number[] = [];
    let overlap = false;
    pgtxn.define("keyed-bump", async (tx, { n }: { n: number }) => {
      await tx.effect(async () => {
        if (inside.length) overlap = true;
        inside.push(n);
        await sleep(50);
        inside.splice(inside.indexOf(n), 1);
      });
      await tx.db.query("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id]);
    });
    const ids = [];
    for (let n = 0; n < 5; n++) ids.push(await pgtxn.enqueue("keyed-bump", { n }, { key: ["order", id] }));
    for (const t of ids) await pgtxn.wait(t, 15_000);
    assert.equal(overlap, false);
    assert.equal(await amount(id), 5);
  });

  test("waiting longer than keyWaitMs fails with KeyTimeoutError", async () => {
    const impatient = newPgTxn(pool, { keyWaitMs: 200 });
    const key = `slow:${nextId()}`;
    const slow = pgtxn.transaction(async (tx) => { await tx.effect(() => sleep(800)); }, { key });
    await sleep(100);
    await assert.rejects(impatient.transaction(async () => 1, { key }), KeyTimeoutError);
    await slow;
  });
});
