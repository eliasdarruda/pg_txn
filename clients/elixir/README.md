# pg_txn for Elixir

`PgTxn` works with Ecto and Postgrex: plain Repo calls inside transactions,
and `Ecto.Multi`.

```elixir
{:pg_txn, "~> 0.1"}
```

## Setup

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
  use PgTxn.Repo
end
```

That is the whole setup. The Repo's child spec now also starts a
`PgTxn.Worker`. The worker installs the `txn` schema, runs this node's
spawned functions and background transactions, and drains before the Repo
stops.

## A checkout

```elixir
def checkout(order_id) do
  PgTxn.transaction(Repo, fn tx ->
    order = Repo.get!(Order, order_id)

    if order.status != "new" do
      {:already, order.status}                             # e.g. paid by a concurrent checkout
    else
      payment =
        PgTxn.effect(tx, fn ctx -> Payments.charge(order.total, idempotency_key: ctx.idempotency_key) end,
          retry: true,                                     # safe: the provider deduplicates by the key
          compensate: fn p, ctx -> Payments.refund(p["id"], idempotency_key: ctx.idempotency_key) end
        )

      Repo.update_all(from(o in Order, where: o.id == ^order_id), set: [status: "paid", payment_id: payment["id"]])
      PgTxn.spawn(tx, fn -> Mailer.send_receipt(order_id) end)   # runs iff this commits
      {:paid, payment["id"]}
    end
  end, key: {"order", order_id})                           # checkouts of one order run one at a time
end
```

Nothing is locked while the charge runs. If the order changes meanwhile
(say it is cancelled), the next run sees it and returns early; the unused
charge is then refunded by its `compensate`. The `:key` makes two checkouts
of the same order run one after the other, so the second sees the first's
`paid` status and charges nothing.

Inside the function, use the Repo as usual; each run is a real
`Repo.transaction`. It returns `{:ok, value}`. `Repo.rollback(reason)`
returns `{:error, reason}`, and a raise is re-raised. Either way nothing is
committed and completed effects are compensated.

## API

### `PgTxn.transaction(repo, fun, opts)`

| option | default | |
|---|---|---|
| `:key` | none | transactions with the same key run one at a time; the others wait, holding nothing |
| `:keys` | none | several keys, claimed all at once or none (no deadlocks), e.g. `keys: [{"account", from}, {"account", to}]` |
| `:isolation` | the database's | `:read_committed`, `:repeatable_read` or `:serializable`, for every run |
| `:id` | a new uuid | the transaction id; idempotent: an id that already exists is not run again, its outcome is returned (`{:ok, output}`, or `{:error, %PgTxn.TransactionFailedError{}}`) |

A key is a string, used as is, or any other durable value, stored as its
canonical JSON text (tuples as arrays, object keys sorted, no spaces), the
same key as in the other clients: `{"order", 42}` and `["order", 42]` are
both `["order",42]`; `%{b: 1, a: 2}` and `%{"a" => 2, "b" => 1}` are both
`{"a":2,"b":1}`; `42` is `42` (the same key as `"42"`). `nil`, and terms
that are not durable values, raise an `ArgumentError`. Transactions of one
node that share a key wait in line on the node (first come, first served);
across nodes, a waiter is woken when the holder ends (`NOTIFY txn_done`). A transaction that waits longer than `:key_wait_ms` raises
`PgTxn.KeyTimeoutError`. Other options go to `Repo.transaction/2`.

Use `tx` only in the transaction function, in the process running it. Using it
inside an effect's or a spawned function, or after the transaction ended,
raises an `ArgumentError`. Those functions may start transactions of their own.

When the Repo stops, its worker stops taking background transactions and
refuses new calls, but lets the transactions in progress, and their spawns
and compensations, finish (up to `:drain_ms`).

### `PgTxn.effect(tx, fun, opts)`

Calls `fun` (arity 0, or 1 with a ctx map) once for the whole transaction,
outside of any database transaction, and returns its recorded result.
`ctx.idempotency_key` is the same across retries and crashes.

| option | default | |
|---|---|---|
| `:retry` | off | `true` (5 attempts) or `[attempts: n]`. Without it, `fun` is called at most once, and an error, timeout or crash mid-call fails the effect. |
| `:compensate` | | `fn result -> … end` or `fn result, ctx -> … end`: undoes the effect if the transaction does not use its result. Uses the same `:retry`. |
| `:timeout_ms` | 30000 | per attempt |
| `:deps` | `nil` | the data the call is decided on; a re-run with different deps makes a new effect (the old one is compensated) |
| `:name` | `"effect"` | a label in `txn.effects` |

With `:retry` on:

- `PgTxn.PermanentError` stops the attempts.
- `PgTxn.RetryableError` with `retry_after_ms` sets the delay before the next
  attempt.

When the effect fails for good, `PgTxn.EffectFailedError` is raised in the
function.

Results are stored as JSON, so maps come back with string keys. Functions
must return a storable value (not a tuple).

### `PgTxn.spawn(tx_or_repo, fun, opts)`

Calls `fun` iff the transaction commits, on this node, right after the
commit. It is recorded in `txn.effects`. Options: `:retry` (off by default),
`:delay_ms`, `:timeout_ms` and `:name`.

With a Repo, it joins the surrounding `Repo.transaction`, if there is one.

### `Ecto.Multi`

```elixir
Ecto.Multi.new()
|> Ecto.Multi.insert(:user, changeset)
|> PgTxn.Multi.spawn(:welcome, fn %{user: u} -> Mailer.send_welcome(u.email) end)
|> PgTxn.Multi.enqueue(:onboard, "onboard", %{})
|> Repo.transaction()
```

### Named and background transactions

```elixir
PgTxn.define(Repo, "settle", fn tx, %{"invoice_id" => id} -> ... end)

PgTxn.run(Repo, "settle", %{invoice_id: 7})               # here; resumed elsewhere if this node dies
id = PgTxn.enqueue(Repo, "settle", %{invoice_id: 7})         # on any node that defines it
PgTxn.wait(Repo, id)
```

`run/4`, `enqueue/4` and `PgTxn.Multi.enqueue` take `:key`, `:keys` and
`:isolation` too (an enqueued transaction runs at its level wherever it runs).
An enqueued transaction starts once no other transaction holds its keys.

### `PgTxn.now(tx)` and `PgTxn.uuid(tx)`

A timestamp and UUIDs that are the same in every run.

## Options

Pass options to `use PgTxn.Repo, ...` or set them in the Repo config, which
wins:

```elixir
config :my_app, MyApp.Repo, pg_txn: [concurrency: 32]
```

| option | default | |
|---|---|---|
| `:concurrency` | 16 | spawned functions, compensations and background transactions run at once |
| `:lease_ms` | 30000 | lease of a transaction or effect this node drives |
| `:key_wait_ms` | 300000 | how long a transaction waits for another one holding its key |
| `:listen` | `true` | wake the worker with LISTEN; `false` behind RDS Proxy or a transaction pooler |
| `:poll_ms` | 250 | idle poll interval |
| `:drain_ms` | 30000 | how long shutdown waits for work in progress |
| `:install` | `true` | install the `txn` schema if it is missing |

## Testing with the Ecto SQL Sandbox

In `:manual` or shared sandbox mode every Repo call of a test runs inside
one sandbox transaction, so `PgTxn.transaction` raises "cannot run inside
another Repo transaction": each of its runs must commit or roll back on its
own, and effects and spawns run outside of it. Run the tests that use
pg_txn without the sandbox: `async: false`, with the sandbox in `:auto` mode
for them (`Ecto.Adapters.SQL.Sandbox.mode(Repo, :auto)`), cleaning up the
rows they create (e.g. `TRUNCATE` in `on_exit`). The same goes for
`PgTxn.spawn(Repo, ...)` and `PgTxn.enqueue(Repo, ...)`: nothing commits in
the sandbox, so the worker (which uses connections of its own) would never
see them.

More in [docs/operations.md](../../docs/operations.md).
