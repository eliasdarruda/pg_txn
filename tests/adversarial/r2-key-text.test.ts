// Hypothesis: keyText (index.ts:163) uses JSON.stringify, not the canonical
// (sorted-key) form used for every stored value (serialize.ts), so an object
// key depends on property order: { a, b } and { b, a } are two different keys
// and do not serialize each other. The Elixir client encodes maps sorted
// (Jason, small maps), so a map key agreed on by both clients only serializes
// across clients when TypeScript happens to insert its properties in sorted
// order, contrary to "the same key as in the other clients".
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { makePool, newPgTxn, closeAll, schema, sleep, nextId } from "../helpers.ts";

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

describe("object keys", () => {
  test("an object key is canonical: property order does not matter", async () => {
    const n = nextId();
    let inside = 0;
    let overlap = 0;
    const body = async (tx: any) => {
      inside++;
      try {
        if (inside > 1) overlap++;
        await tx.effect(async () => { await sleep(300); });
      } finally {
        inside--; // runs aborted at the effect leave too
      }
    };
    await Promise.all([
      pgtxn.transaction(body, { key: { kind: "order", id: n } }),
      pgtxn.transaction(body, { key: { id: n, kind: "order" } }),
    ]);
    assert.equal(overlap, 0, "the same object in another property order was a different key: both ran at once");
  });
});
