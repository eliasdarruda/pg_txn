// Hypothesis: the clients "refuse a version they were not built for"
// (docs/operations.md), but SCHEMA_VERSION stayed 1 across three commits
// that changed the schema (bf2fe6f, d6fd01b, eab5862: txn.leases, _lease,
// _lease_expired, _check_keys, the isolation CHECK, new indexes; the function
// signatures are identical, so nothing fails loudly). A database installed
// by a client from the previous commit (a migration run once; install:
// false; or simply the first replica to boot being an old one) passes the
// version check of the new client, which then runs with the OLD semantics of
// the round-2 bugs: no txn.leases (heartbeats during a run update
// txn.transactions again), no _check_keys, no CHECK on isolation, no
// StartFailed handling. Expected: a schema that differs from the one the
// client ships is refused (bump the version, or check a hash of the schema).
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PgTxn } from "../../clients/typescript/client/src/index.ts";
import { SCHEMA_VERSION } from "../../clients/typescript/client/src/schema.ts";
import { makePool, closeAll, sleep } from "../helpers.ts";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const pool = makePool(4);

let oldSchema: string | null = null;
before(() => {
  try {
    oldSchema = execFileSync("git", ["show", "d6fd01b:extension/sql/pg_txn--1.0.sql"], { cwd: ROOT, encoding: "utf8" });
  } catch {
    oldSchema = null;
  }
});
after(async () => {
  // leave the current schema installed for the next suites
  await pool.query("DROP SCHEMA IF EXISTS txn CASCADE");
  const p = new PgTxn(pool, { onError: () => {} });
  await p.ready();
  await p.close(500);
  await closeAll();
  await pool.end();
});

describe("schema version", () => {
  test("a database with an older schema (d6fd01b: version 1, no txn.leases) is refused by this client", { skip: oldSchema === null && "git history not available" }, async () => {
    await pool.query("DROP SCHEMA IF EXISTS txn CASCADE");
    await pool.query(`CREATE SCHEMA txn;\n${oldSchema}`);
    const v = (await pool.query("SELECT version FROM txn.meta")).rows[0].version;
    assert.notEqual(v, SCHEMA_VERSION, "schema changes bump the version (scripts/sync-schema.mjs enforces it)");
    const p = new PgTxn(pool, { onError: () => {}, listen: false });
    const ready = await p.ready().then(() => "accepted", (e: Error) => `refused: ${e.message}`);
    await p.close(500);
    await sleep(50);
    assert.match(ready, /^refused: pg_txn: schema version 1 in the database/);
  });
});
