# Operations

## Installing the schema

pg_txn on the database side is one schema, `txn`, of tables and plpgsql
functions. It adds nothing to your own tables: no triggers, no columns ([`extension/sql/pg_txn--1.0.sql`](../extension/sql/pg_txn--1.0.sql)).
PostgreSQL 14 or later; no superuser, no `shared_preload_libraries`, no
restart. Three ways to install it:

| | how | when |
|---|---|---|
| by the client (default) | on first use, in one transaction under an advisory lock, as the application's role | most applications |
| as a migration | run `CREATE SCHEMA txn;` + the file in your migration tool; clients with `install: false` | DDL only through migrations |
| as an extension | `make -C extension install` then `CREATE EXTENSION pg_txn` (trusted: the database owner may run it); on RDS/Aurora through pg_tle | extension management |

Clients check `txn.meta.version` against the version they were built for and
refuse to run against a different one.

**Roles.** Whoever installs the schema owns it and can use it. For another
role: `SELECT txn.grant_to('app_role')`.

## Client options

Options, and the options of effects and spawns, are in the client guides:
[TypeScript](../clients/typescript/README.md#options) and
[Elixir](../clients/elixir/README.md#options).

Spawned and compensation functions run in the process that committed (or
failed) the transaction, right after it; attempts are recorded (and retried,
with `retry`) there. If that process dies before one completes, it is failed as
`EffectLost` after a minute (`txn.doctor()` reports it). Work that must run
even then belongs in an enqueued defined transaction.

## Deployment

- **Replicas.** Every process that constructs a `PgTxn` (or starts a Repo
  with `use PgTxn.Repo`) runs its own spawned functions and its share of
  background transactions. Scale freely; leases are exclusive and fenced.
- **Shutdown.** Call `await pgtxn.close(drainMs)` on SIGTERM: the worker
  stops taking work and in-flight work finishes. Keep `drainMs` below the
  platform's stop timeout (ECS `stopTimeout`, Kubernetes
  `terminationGracePeriodSeconds`, both 30 s by default). Elixir drains when
  the Repo's supervisor stops.
- **Crashes.** A named transaction (`define` + `run`, or `enqueue`) whose
  process dies is resumed by another process that defines the same name,
  once its lease expires; recorded effects are reused and re-run calls keep
  their idempotency key. An inline `transaction(fn)` cannot be resumed
  elsewhere: it is marked abandoned and its keys are released; the
  compensation functions of its completed effects died with the process,
  so those are reported as `EffectLost`. The same happens to a named
  transaction resumed elsewhere whose re-run takes another path than the
  process that called an effect: the effect's compensation function is not
  in the resuming process.
- **Connections.** A run holds one connection while your function runs
  between effects (milliseconds); effects hold none. The worker uses the
  pool for short statements plus, with `listen`, one dedicated connection.
- **Poolers.** Session or transaction pooling both work (PgBouncer, RDS
  Proxy, Supabase, Neon): every statement is inside your transaction or a
  single autocommit statement, and the session marker is transaction-local
  (`set_config(..., true)`). `LISTEN` does not work through transaction
  poolers (RDS Proxy pins it): use `listen: false` or a direct endpoint.
- **Network.** PostgreSQL makes no outbound calls and holds no secrets; the
  application calls external services with its own network and credentials.

## Observability

| object | content |
|---|---|
| `txn.doctor()` | schema version, active workers, effects due for over a minute, effects lost with their process, stalled transactions, orphaned effects without compensation |
| `txn.running_transactions` | transactions in flight: name, keys, driving process, runs, lease, age |
| `txn.keys` | keys held, and the transaction holding each |
| `txn.pending_effects` | effects not finished: status, attempts, next attempt, last error |
| `txn.effect_attempts` | one row per finished attempt: outcome (`succeeded`, `retry`, `failed`, `lease_expired`, `stale`, `ambiguous`), error, process, timings |
| `txn.effect_errors` | failed attempts with `error_name`, `error_message`, duration |
| `txn.status(id)` | a transaction's status, output, error, runs |

## Retention

Finished transactions, their effects and attempts, and completed spawned
effects stay until purged:

```sql
SELECT * FROM txn.purge(interval '30 days');   -- at most 10000 of each per call; repeat while full
```

Run it periodically (pg_cron, or a job in your application).

## Upgrades

The schema is versioned (`txn.meta`). Deploy clients built for the new
version after running the new version's migration (or let the first new
client install it); clients refuse a version they were not built for.
