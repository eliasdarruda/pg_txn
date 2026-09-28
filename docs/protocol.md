# The pg_txn protocol

pg_txn is a SQL schema (`txn`, see [`extension/sql/pg_txn--1.0.sql`](../extension/sql/pg_txn--1.0.sql))
plus a thin client loop in each language. Any database library works: the
client needs to **run a callback in a database transaction** and **run one
SQL statement** (inside that transaction, or on its own). This document is
the contract every SDK implements; the TypeScript client
(`clients/typescript/client/src/index.ts`) is the reference.

## Installation

The schema is installed by `CREATE EXTENSION pg_txn` (self-managed
PostgreSQL, or pg_tle), by running the file as a migration, or by an SDK on
first use: if `to_regclass('txn.meta')` is NULL, run `CREATE SCHEMA txn;` +
the file in one transaction after `pg_advisory_xact_lock(hashtext('pg_txn install'))`
(re-check after taking the lock). Then check `SELECT version FROM txn.meta`
against the version the SDK was built for. PostgreSQL 14+, no superuser,
no preloaded library.

## Values

Inputs, results and outputs are `jsonb`. SDKs may encode richer values
(the TypeScript and Elixir clients tag bigint, dates and bytes as
`{"$bigint": "..."}`, etc.). Effect keys are hashed **in SQL** from their
`jsonb` form (`txn._hash(name, key)`), so memoization never depends on a
client's JSON printer.

## A transaction

```
tx_id := new uuid; owner := this process's uuid; claimed := false
loop:
  run := { seq: 0, needs: [], consumed: [], claims: [], effect_called: false }
  BEGIN                                            -- the application's own transaction
    SELECT txn.attempt(tx_id, owner)               -- marks the session; fenced check
    result := user_function(tx)                    -- tx.effect / tx.own / tx.spawn below
    if run.needs not empty: ROLLBACK, goto perform
    SELECT txn.finish(tx_id, owner, run.consumed, output)
  COMMIT → return result
  on error:
    run.needs not empty          → perform
    55P03, DETAIL 'owner=<uuid>' → wait until txn.status(<uuid>) is not 'running', loop
    55P03, DETAIL 'fenced'       → another process drives this transaction: stop
    40001 / 40P01                → loop (with backoff)
    otherwise                    → SELECT txn.fail_transaction(tx_id, owner, error); rethrow
```

### tx.effect(fn, options)

`fn` runs outside any database transaction and receives a context (effect
id = idempotency key, attempt, abort signal). `options.name` (default: the
function's name, else `"effect"`) labels it; `options.key` (default `null`)
is JSON describing what it does: a re-run reuses the recorded result only if
it reaches the effect at the same position with the same name and key.

```
run.effect_called := true; seq := run.seq++
SELECT effect_id, status, result, error FROM txn.effect_lookup(tx_id, seq, name, key)
  'succeeded' → run.consumed += effect_id; return result
  'failed'    → run.consumed += effect_id; raise EffectFailed(error) in user code
  otherwise   → run.needs += {seq, name, key, fn, options}; abort the run
```

Effects started concurrently in one run (e.g. `Promise.all`) should all be
registered before the run aborts, so they are performed in one round.
Once a run has registered a need it must roll back even if user code
catches the abort.

### tx.own(table, key)

Only before the run's first `tx.effect` (otherwise an error).

```
SELECT "row", version, key, rel FROM txn.own(table::regclass, key)
  → 55P03 'owner=<uuid>' if another running transaction owns it (handled by the loop)
  no row → return null
  if not claimed: run.claims += {rel, key, version}
  return row
```

`rel` is the table's oid: pass it back as is (names would be resolved in
the schema functions' own `search_path`).

### perform (outside any transaction)

```
heartbeat every lease/3: SELECT txn.heartbeat(tx_id, owner, lease_ms)
r := SELECT txn.prepare_effects(tx_id, owner, lease_ms, effects, claims)
     effects: [{seq, name, input: key, max_attempts, delivery, compensation}]
     claims:  run.claims, only while not claimed
  r.conflict.reason 'owned'           → wait for r.conflict.owner; loop
  r.conflict.reason 'changed' | 'gone' → loop (re-run on fresh data)
  r.conflict.reason 'fenced'          → stop
claimed := true
for each r.effects[i] (concurrently):
  'done'    → nothing
  'wait'    → sleep wait_ms, prepare this effect again
  'execute' → call fn({effectId: id, attempt}) with a timeout
              SELECT txn.effect_done(id, owner, ok, result, error, retryable, retry_after_ms)
              'retry_wait' → sleep wait_ms, prepare this effect again
loop
```

Retries are opt-in: clients send `max_attempts` 1 and delivery
`at-most-once` unless the application asked for retries, then `max_attempts`
n and `at-least-once`. Retryable: with retries on, any error except an
explicit permanent one (an explicit retryable error may set the delay);
with retries off, nothing. Compensations get the policy of the effect they
undo. `prepare_effects`
claims every row all-or-nothing, each locked briefly in a fixed order and
checked unchanged (`xmin`) since the run read it: an effect only runs if the
data it was decided on is still current, and nobody waits while holding
anything (no deadlocks). A `running` effect found by a new driver (the
previous process stopped mid-call) is re-run (`at-least-once`) or failed as
`AmbiguousEffectOutcome` (`at-most-once`).

### txn.finish

Inside the committing run. Effects recorded for the transaction that this
run did not consume become `orphaned`; a succeeded orphan with a
`compensation` (a label; the function is the client's) gets a
`compensation` effect owned by the process driving the transaction
(`local_owner`), which has the function. Owned rows are released
and the outcome is stored, all in the application's commit.

## Spawned effects and background transactions

- `txn.spawn(owner, name, id, max_attempts, delivery, delay_ms)` inside any
  transaction: the effect exists iff it commits (a transactional outbox).
  Its code is a function the client keeps in memory under `id`; only the
  process `owner` runs it (`name` is a label).
- `txn.enqueue(name, input, id)`: a named transaction that runs in the
  background iff the surrounding transaction commits.
- Named transactions started in-process: `txn.start(tx_id, name, input, owner, lease_ms)`,
  then the loop above. If the process stops, the lease expires.

A worker (one per process, polling, woken by `NOTIFY txn_effects`):

```
every 10 s: SELECT txn.worker_seen(owner, info)
every 5 s:  SELECT txn.abandon_expired(),         -- inline transactions whose process stopped
                   txn.expire_effect_leases(),    -- effects whose call outlived its lease
                   txn.fail_lost_effects()        -- effects whose process is gone (EffectLost)
SELECT * FROM txn.lease_effects(owner, free_slots, lease_ms)   -- only this process's effects
  → run the function kept under id (spawn) or under compensates (compensation);
    missing: txn.fail_effect(..., {"name": "EffectLost"}, retryable false);
    txn.heartbeat_effect meanwhile;
    txn.complete_effect(id, owner, generation, result)
    or txn.fail_effect(id, owner, generation, error, retryable, retry_after_ms)
SELECT * FROM txn.lease_transactions(owner, definition_names, free_slots, lease_ms)
  → run the definition with its input through the loop (recorded effects are reused)
```

A compensation's input is `{effect, input, result}` of the orphaned call;
the client calls its function with `(result, context)`. Clients drop the
functions of a run that rolled back, and of effects no longer pending.

## Errors the loop recognizes

| SQLSTATE | DETAIL | meaning |
|---|---|---|
| `55P03` | `owner=<uuid>` | a row is owned by another running transaction |
| `55P03` | `fenced` | another process drives this transaction now |
| `40001`, `40P01` | | serialization failure / deadlock in a run: run again |
