// Hypothesis: a named transaction resumed by another process, whose re-run
// diverges (the data changed) and no longer reaches an effect that succeeded
// in the first process, orphans that effect at txn.finish. The compensation
// row is created with local_owner = the resuming process (sql:263), which
// never registered the compensate function (it is only registered when the
// effect call site is reached, index.ts:240, :541). Worse, its worker never
// even leases it: lease_effects is only called when #local is non-empty
// (index.ts:635), so the row stays 'pending' forever while the process lives
// (fail_lost_effects needs a dead worker). The refund is neither made nor
// reported as EffectLost. docs/operations.md only documents the loss for
// inline transactions.
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, newOrder, sleep, waitFor } from "../helpers.ts";

const pool = makePool(10);

before(async () => {
  await schema(pool);
});
after(async () => {
  await closeAll();
  await pool.end();
});

describe("compensation after a divergent resume", () => {
  test("the charge of a transaction resumed elsewhere, whose re-run takes another path, is compensated (or at least reported lost)", async () => {
    const a = newPgTxn(pool, { leaseMs: 30_000 });   // its heartbeat is 10 s away: the takeover is deterministic
    const b = newPgTxn(pool, { leaseMs: 1500 });
    const refunds: string[] = [];
    const def = (who: string) => async (tx: any, input: { orderId: number }) => {
      const status = (await tx.db.query("SELECT status FROM orders WHERE id = $1", [input.orderId])).rows[0].status;
      if (status === "cancelled") return "cancelled";
      await tx.effect(async () => ({ id: "pay_1" }), { name: "charge", compensate: async (p: any) => { refunds.push(`${who}:${p.id}`); } });
      await tx.effect(async () => { if (who === "A") await new Promise(() => {}); return "shipped"; }, { name: "ship", retry: true });
      return "paid";
    };
    a.define("checkout", def("A"));
    const orderId = await newOrder(pool);
    const id = crypto.randomUUID();
    a.run("checkout", { orderId }, { id }).catch(() => {});
    await waitFor(async () => (await pool.query("SELECT 1 FROM txn.effects WHERE tx_id = $1 AND name = 'ship' AND status = 'running'", [id])).rows[0], "A stuck in ship");
    // meanwhile the order is cancelled and A stops driving (its lease lapses)
    await pool.query("UPDATE orders SET status = 'cancelled' WHERE id = $1", [orderId]);
    await pool.query("UPDATE txn.leases SET lease_until = now() - interval '1 second' WHERE tx_id = $1", [id]);
    b.define("checkout", def("B"));
    assert.equal(await b.wait(id, 15_000), "cancelled");
    const comp = await waitFor(async () => (await pool.query(
      "SELECT id, status, error->>'name' AS error, local_owner FROM txn.effects WHERE tx_id = $1 AND kind = 'compensation'", [id])).rows[0], "compensation row");
    assert.equal(comp.local_owner, b.owner);
    await sleep(2000);
    const after = (await pool.query("SELECT status, error->>'name' AS error FROM txn.effects WHERE id = $1", [comp.id])).rows[0];
    assert.ok(refunds.length === 1 || after.status === "failed",
      `the refund neither ran (${JSON.stringify(refunds)}) nor was reported: compensation is ${JSON.stringify(after)}`);
  });
});
