<h1 align="center">pg_txn</h1>

<p align="center">
  <b>Transactions that include side effects.</b><br>
  A transactional outbox that lives inside your transaction, plus external calls in the<br>
  middle of it that hold no locks, no connections and no open transactions.
</p>

<p align="center">
  <img alt="PostgreSQL 14+" src="https://img.shields.io/badge/PostgreSQL-14%2B-336791?logo=postgresql&logoColor=white">
  <img alt="Managed PostgreSQL" src="https://img.shields.io/badge/runs%20on-RDS%20%7C%20Cloud%20SQL%20%7C%20Supabase%20%7C%20Neon-555">
  <img alt="Clients" src="https://img.shields.io/badge/clients-Node.js%20%7C%20Bun%20%7C%20Elixir-3178c6">
  <img alt="Status: pre-release" src="https://img.shields.io/badge/status-pre--release-orange">
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

<p align="center">
  <img src="docs/assets/outbox-vs-pgtxn.svg" alt="The same checkout with a transactional outbox and with pg_txn" width="1000">
</p>

A checkout that charges a card usually turns into:

- an outbox table and a relay;
- a broker and a worker with its own deduplication;
- a dead-letter handler;
- a guard for writing the result back.

With pg_txn it is **one function**:

```ts
await pgtxn.transaction(async (tx) => {
  const order = await tx.own("orders", id)                      // nobody else can change it until commit
  const payment = await tx.effect(() => charge(order))          // runs holding no lock, connection or transaction
  await tx.db.update(orders).set({ paymentId: payment.id }).where(eq(orders.id, id))
  await tx.spawn(() => sendReceipt(id))                         // runs iff this commits
})
```

pg_txn is a SQL schema and a small library. There is no extension to build
and no service to run, and it works with any ORM on stock PostgreSQL.

- [Why pg_txn](#why-pg_txn)
- [Quick start](#quick-start)
- [effect and spawn](#effect-and-spawn)
- [Multi-row transactions without locks](#multi-row-transactions-without-locks)
- [How it works](#how-it-works)
- [Deployment](#deployment)
- [Observability](#observability)
- [Performance](#performance)
- [Compatibility](#compatibility)
- [Limitations](#limitations)
- [Documentation](#documentation)
- [Development](#development)

## Why pg_txn

| | transactional outbox | pg_txn |
|---|---|---|
| where the logic lives | relay + broker + worker + DLQ handler | one function |
| call an API **and use its result** in the transaction | a second transaction, a state column, a guard | `await tx.effect(…)`, then write |
| fire-and-forget after commit | outbox row + relay | `tx.spawn(() => …)` |
| data unchanged between decision and write-back | version checks on every write path | `tx.own(row)` |
| idempotency key stable across retries and crashes | yours to build | `ctx.idempotencyKey` |
| services to run | relay, broker, workers | none |

**Effects hold nothing.** Your function runs in a short database
transaction. When it reaches an effect that has not run yet:

1. pg_txn rolls that transaction back.
2. It calls the effect from your process and records the result.
3. It runs your function again. This time the effect returns the recorded
   result, and the run commits.

Slow APIs therefore pin no connections and block nobody. In the tests, 20
transactions with 500 ms effects finish in about 600 ms on a pool of
**two** connections.

## Quick start

```bash
npm install @pg-txn/client @pg-txn/drizzle
```

```ts
import { PgTxn } from "@pg-txn/client"
import { drizzleDb } from "@pg-txn/drizzle"

const pgtxn = new PgTxn(drizzleDb(db))   // your Drizzle instance
```

That is the whole setup. On first use the client creates the `txn` schema
as your application's role: no superuser, no restart, no worker service.
Every process runs its own share of the work.

Elixir takes one line in the Repo:

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
  use PgTxn.Repo
end

PgTxn.transaction(Repo, fn tx ->
  payment = PgTxn.effect(tx, fn -> Payments.charge(order) end)
  PgTxn.spawn(tx, fn -> Mailer.send_receipt(order.id) end)
end)
```

Full guides:
- **[TypeScript](clients/typescript/README.md):** Drizzle, Knex and
  node-postgres; the API and options.
- **[Elixir](clients/elixir/README.md):** Ecto and `Ecto.Multi`; the API and
  options.

## effect and spawn

| | `tx.effect(fn)` | `tx.spawn(fn)` |
|---|---|---|
| runs | before the commit, between runs | after the commit, iff it commits |
| result | returned to your transaction | not available |
| on failure | `EffectFailedError` in your function: catch it, or the transaction fails and completed effects are compensated | recorded as failed; the commit stands |
| retries | off: called at most once. `{ retry: true }` when the call is safe to repeat | same |

Retries are always explicit. Without `retry`, nothing is called twice, even
after a crash. With it, pass `ctx.idempotencyKey` to the API you call:

```ts
const payment = await tx.effect(
  (ctx) => stripe.paymentIntents.create(params, { idempotencyKey: ctx.idempotencyKey }),
  {
    retry: true,
    compensate: (p, ctx) => stripe.refunds.create({ payment_intent: p.id }, { idempotencyKey: ctx.idempotencyKey }),
  },
)
```

## Multi-row transactions without locks

<p align="center">
  <img src="docs/assets/multi-actor-transfer.svg" alt="A two-account transfer with a slow external call: row locks and a deadlock in plain PostgreSQL, owned rows and no locks with pg_txn" width="1000">
</p>

In plain PostgreSQL, a transfer that asks a fraud service before moving money
has two bad options:

- **Hold `FOR UPDATE` locks across the call:** writers are blocked,
  connections are pinned, and crossing transfers deadlock.
- **Don't lock:** a concurrent change is lost.

With pg_txn the transaction **owns** both rows until it commits, without
holding a lock:

```ts
await pgtxn.transaction(async (tx) => {
  const a = await tx.own("accounts", from)
  await tx.own("accounts", to)
  const verdict = await tx.effect(() => fraudCheck(from, to, amount))   // seconds or minutes
  if (verdict.blocked || a.balance < amount) throw new Error("refused")  // nothing is written
  await tx.db.update(accounts).set({ balance: sql`balance - ${amount}` }).where(eq(accounts.id, from))
  await tx.db.update(accounts).set({ balance: sql`balance + ${amount}` }).where(eq(accounts.id, to))
})
```

- **Nobody interleaves.** Other writes to an owned row fail at once with
  `55P03`. Another pg_txn transaction waits, outside any transaction.
- **No deadlocks.** A transaction claims all its rows at once or none, and
  nobody waits while holding anything.
- **Decisions on current data.** An effect only runs if the owned rows are
  unchanged since they were read. Otherwise your function first runs again
  on fresh data.

## How it works

pg_txn is a protocol between a SQL schema and a small client loop:

1. **Run.** Your function runs in an ordinary transaction. `tx.effect`
   looks up a recorded result, and `tx.own` reads the row with its version.
2. **Effect.** If an effect has not run, the run is rolled back.
   `txn.prepare_effects` claims the owned rows, all or nothing and
   version-checked. The client then calls the effect and records its result.
3. **Commit.** The next run reuses every recorded result. `txn.finish` then
   does three things in the run's own commit: it releases the rows,
   schedules compensations for unused results, and stores the outcome.
4. **Workers.** Each process runs its spawned functions and compensations
   right after the commit, and runs or resumes named transactions
   (`define`, `enqueue`) that any replica defines.

A client in any language is a few hundred lines. The contract is in
[docs/protocol.md](docs/protocol.md).

## Deployment

Every replica runs its share of the work, so add or remove replicas freely.

- **Graceful shutdown:** on SIGTERM, `await pgtxn.close()` lets in-flight
  work finish.
- **A killed replica:** its named transactions are resumed by the others,
  with the same idempotency keys. Its inline transactions are abandoned and
  their rows released.
- **Poolers:** everything works behind PgBouncer or RDS Proxy. Set
  `listen: false` so the optional LISTEN connection is not pinned.

This is tested with replicas in containers (Node on Debian and Alpine, and
Bun) and a Kubernetes Deployment. One replica is SIGKILLed mid-flight while
others are rolled and scaled. Every transaction commits exactly once. See
[docs/operations.md](docs/operations.md).

## Observability

```sql
SELECT * FROM txn.doctor();                                  -- workers, stuck, lost or orphaned work
SELECT * FROM txn.running_transactions;                      -- in flight, for how long
SELECT * FROM txn.owned;                                     -- owned rows, by whom
SELECT name, error_name, count(*) FROM txn.effect_errors     -- what is failing
 WHERE finished_at > now() - interval '15 min' GROUP BY 1, 2;
```

## Performance

These are single runs on one PostgreSQL 18 on a developer machine, with 16
concurrent clients
([`tests/performance/bench.ts`](tests/performance/bench.ts)):

| | latency p50 | throughput |
|---|---:|---:|
| plain `BEGIN … COMMIT` (a read and an update) | 0.86 ms | 7,500 tx/s |
| the same in pg_txn, no effect | 1.05 ms | 6,400 tx/s |
| pg_txn: own a row + one effect + write back | 4.2 ms | 760–1,440 tx/s |
| hand-rolled LISTEN/NOTIFY outbox, commit → delivery | 0.26 ms | 900–1,700 events/s |
| pg_txn `spawn()`, commit → delivery | 1–2 ms | ~1,800 events/s |

The effect's own duration is not in the table, because pg_txn holds nothing
while it runs.

## Compatibility

| | |
|---|---|
| PostgreSQL | 14+, self-managed or managed (RDS, Aurora, Cloud SQL, Azure, Supabase, Neon) |
| Poolers | direct, session or transaction pooling (PgBouncer, RDS Proxy) |
| TypeScript | Node.js 20+, Bun; node-postgres, Drizzle, Knex/Objection, or any driver through a two-function adapter |
| Elixir | 1.15+, Ecto SQL 3.12, `Ecto.Multi` |
| Tested | PostgreSQL 14 and 18, PgBouncer (transaction mode), Node.js 24, Bun 1, Elixir 1.17 |

## Limitations

- **Not a workflow engine.** pg_txn is for transactions that last seconds to
  minutes around a few calls. It is not for processes that sleep for days
  or wait on humans.
- **Your function runs once per round of effects.** Put side effects in
  `tx.effect` or `tx.spawn`; anything else repeats on every run.
- **Own before the first effect.** Other rows behave like ordinary rows.
- **Crash recovery needs a name.** `define`/`enqueue` transactions resume on
  another replica. An inline `transaction(fn)` is abandoned (its rows are
  released) if its process dies.
- **Spawned and compensation functions live in their process.** If the
  process dies between the commit and the call, the effect is marked
  `EffectLost` (see `txn.doctor()`). Work that must survive any crash
  belongs in an `enqueue`d transaction.
- **Effects need a running process.** Platforms that freeze or scale to zero
  (Lambda, Cloud Run with request-only CPU) run them only while a replica is
  up.

## Documentation

| | |
|---|---|
| [TypeScript guide](clients/typescript/README.md) | setup, API, options |
| [Elixir guide](clients/elixir/README.md) | setup, API, `Ecto.Multi`, options |
| [Operations](docs/operations.md) | installation, deployment, poolers, observability, retention |
| [Protocol](docs/protocol.md) | the SQL contract every client implements |
| [Schema](extension/sql/pg_txn--1.0.sql) | the database side of pg_txn, commented |

## Development

```bash
docker compose --profile matrix up -d     # PostgreSQL 18 and 14, PgBouncer
npm install
scripts/test-all.sh                       # every suite, with a summary
ONLY="unit core" scripts/test-all.sh      # groups: unit core compat bun elixir containers k8s
```

```
extension/sql/          the schema: the database side of pg_txn
clients/typescript/     @pg-txn/client, @pg-txn/drizzle, @pg-txn/knex
clients/elixir/         PgTxn, PgTxn.Repo, PgTxn.Multi
tests/                  core suites, containers, Kubernetes, benchmark
docs/                   protocol, operations
```

## License

[Apache License 2.0](LICENSE)
