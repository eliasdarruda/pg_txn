// Rows a transaction owns cannot change from the moment it read them to its
// commit, without locks, connections or open transactions held during its
// effects, and without deadlocks.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, idleInTransaction, nextId } from "../helpers.ts";

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

const orderStatus = async (id: number) => (await pool.query("SELECT status FROM orders WHERE id = $1", [id])).rows[0].status;

describe("ownership", () => {
  test("during an effect, writes to an owned row fail fast (55P03); nothing is held", async () => {
    const id = await newOrder(pool);
    let during: { update?: string; idle?: number } = {};
    const out = await pgtxn.transaction(async (tx) => {
      const order = await tx.own<{ status: string }>("orders", id);
      if (order!.status !== "new") return "skipped";
      const p = await tx.effect(async () => {
        during = {
          update: await pool.query("UPDATE orders SET status = 'cancelled' WHERE id = $1", [id]).then(() => "went through", (e) => e.code),
          idle: await idleInTransaction(pool),
        };
        return { id: "pay_1" };
      });
      await tx.db.query("UPDATE orders SET status = 'paid', payment_id = $2 WHERE id = $1", [id, p.id]);
      return "paid";
    });
    assert.equal(out, "paid");
    assert.deepEqual(during, { update: "55P03", idle: 0 });
    assert.equal(await orderStatus(id), "paid");
    await pool.query("UPDATE orders SET note = 'released' WHERE id = $1", [id]);
  });

  test("effects hold no connection: a 2-connection pool runs 20 concurrent 500 ms effects in about 500 ms", async () => {
    const small = makePool(2);
    const p = newPgTxn(small);
    const ids = await Promise.all(Array.from({ length: 20 }, () => newOrder(pool)));
    const t0 = Date.now();
    await Promise.all(ids.map((id) => p.transaction(async (tx) => {
      await tx.own("orders", id);
      await tx.effect(() => sleep(500));
      await tx.db.query("UPDATE orders SET status = 'done' WHERE id = $1", [id]);
    })));
    const ms = Date.now() - t0;
    await small.end();
    assert.ok(ms < 2500, `took ${ms} ms (held connections would need 20 × 500 / 2 = 5000 ms)`);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM orders WHERE id = ANY($1) AND status = 'done'", [ids])).rows[0].n, 20);
  });

  test("a row changed after it was read and before the claim: the effect does not run on stale data", async () => {
    const id = await newOrder(pool, { amount: 10 });
    const charged: number[] = [];
    let run = 0;
    await pgtxn.transaction(async (tx) => {
      run++;
      const order = await tx.own<{ amount: string }>("orders", id);
      // someone changes the order between this run's read and its first effect
      if (run === 1) await pool.query("UPDATE orders SET amount = 25 WHERE id = $1", [id]);
      await tx.effect(async () => { charged.push(Number(order!.amount)); });
    });
    assert.deepEqual(charged, [25], "charged once, on the current amount");
  });

  test("two transactions owning the same row run one after the other", async () => {
    const id = await newOrder(pool, { amount: 0 });
    const log: string[] = [];
    const bump = (name: string) => pgtxn.transaction(async (tx) => {
      const o = await tx.own<{ amount: string }>("orders", id);
      await tx.effect(async () => { log.push(`${name} start`); await sleep(200); log.push(`${name} end`); }, { key: Number(o!.amount) });
      await tx.db.query("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id]);
    });
    await Promise.all([bump("A"), sleep(50).then(() => bump("B"))]);
    assert.equal(Number((await pool.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount), 2);
    assert.deepEqual(log, ["A start", "A end", "B start", "B end"], "B waited for A (it never saw A's row mid-flight)");
  });

  test("claims made at the same moment: exactly one wins, the other waits (no double effect)", async () => {
    for (let round = 0; round < 5; round++) {
      const id = await newOrder(pool, { amount: 0 });
      const inside: string[] = [];
      let overlap = false;
      const claim = (name: string) => pgtxn.transaction(async (tx) => {
        await tx.own("orders", id);
        await tx.effect(async () => {
          if (inside.length) overlap = true;
          inside.push(name);
          await sleep(100);
          inside.splice(inside.indexOf(name), 1);
        }, { name: "exclusive" });
        await tx.db.query("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id]);
      });
      await Promise.all([claim("A"), claim("B"), claim("C")]);
      assert.equal(overlap, false, "two owners' effects ran at the same time");
      assert.equal(Number((await pool.query("SELECT amount FROM orders WHERE id = $1", [id])).rows[0].amount), 3);
    }
  });

  test("crossing claims never deadlock: all or nothing, nobody waits holding anything", async () => {
    const a = nextId();
    const b = nextId();
    await pool.query("INSERT INTO accounts VALUES ($1, 100), ($2, 100)", [a, b]);
    const transfer = (from: number, to: number) => pgtxn.transaction(async (tx) => {
      const x = await tx.own<{ balance: string }>("accounts", from);
      const y = await tx.own<{ balance: string }>("accounts", to);
      await tx.effect(() => sleep(100), { name: "fraud-check" });
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [from, Number(x!.balance) - 10]);
      await tx.db.query("UPDATE accounts SET balance = $2 WHERE id = $1", [to, Number(y!.balance) + 10]);
    });
    await Promise.all([transfer(a, b), transfer(b, a), transfer(a, b), transfer(b, a), transfer(a, b)]);
    const r = (await pool.query("SELECT id, balance FROM accounts WHERE id IN ($1, $2) ORDER BY id", [a, b])).rows.map((x) => Number(x.balance));
    assert.deepEqual(r, [90, 110], "money conserved, every transfer applied once");
  });

  test("own() after an effect is refused", async () => {
    const id = await newOrder(pool);
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.effect(async () => 1);
      await tx.own("orders", id);
    }), /before its first effect/);
  });

  test("owning a missing row returns null and claims nothing", async () => {
    const out = await pgtxn.transaction(async (tx) => {
      const o = await tx.own("orders", -1);
      await tx.effect(async () => 1);
      return o;
    });
    assert.equal(out, null);
  });

  test("rows are released when the transaction fails, too", async () => {
    const id = await newOrder(pool);
    await assert.rejects(pgtxn.transaction(async (tx) => {
      await tx.own("orders", id);
      await tx.effect(async () => 1);
      throw new Error("business rule");
    }), /business rule/);
    await pool.query("UPDATE orders SET status = 'free' WHERE id = $1", [id]);
    assert.equal(await orderStatus(id), "free");
  });

  test("composite and typed keys: 42 and '42' name the same row", async () => {
    await pool.query("CREATE TABLE IF NOT EXISTS seats (event text, seat int, holder text, PRIMARY KEY (event, seat))");
    await pool.query("INSERT INTO seats VALUES ('show', 7, NULL) ON CONFLICT DO NOTHING");
    const id = await newOrder(pool);
    let blocked = "";
    await pgtxn.transaction(async (tx) => {
      await tx.own("seats", { event: "show", seat: "7" });
      await tx.own("orders", String(id));
      await tx.effect(async () => {
        blocked = await pool.query("UPDATE seats SET holder = 'x' WHERE event = 'show' AND seat = 7").then(() => "no", (e) => e.code);
        blocked += await pool.query("UPDATE orders SET note = 'x' WHERE id = $1", [id]).then(() => " no", (e) => ` ${e.code}`);
      });
    });
    assert.equal(blocked, "55P03 55P03");
  });
});
