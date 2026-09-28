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
    order = PgTxn.own(tx, Order, order_id)                 # nobody else can change it until this commits

    payment =
      PgTxn.effect(tx, fn ctx -> Payments.charge(order["total"], idempotency_key: ctx.idempotency_key) end,
        retry: true,                                       # safe: the provider deduplicates by the key
        compensate: fn p, ctx -> Payments.refund(p["id"], idempotency_key: ctx.idempotency_key) end
      )

    Repo.update_all(from(o in Order, where: o.id == ^order_id), set: [status: "paid", payment_id: payment["id"]])
    PgTxn.spawn(tx, fn -> Mailer.send_receipt(order_id) end)   # runs iff this commits
    payment["id"]
  end)
end
```

Inside the function, use the Repo as usual; each run is a real
`Repo.transaction`. It returns `{:ok, value}`. `Repo.rollback(reason)`
returns `{:error, reason}`, and a raise is re-raised. Either way nothing is
committed and completed effects are compensated.

## API

### `PgTxn.own(tx, table, key)`

Reads a row and protects it until the transaction ends. Other writers get a
`Postgrex.Error` (`:lock_not_available`) at once, and another pg_txn
transaction waits.

- `table` is a table name or an Ecto schema module.
- It returns the row as a map with string keys, or `nil`.
- Call it before the first effect.

### `PgTxn.effect(tx, fun, opts)`

Calls `fun` (arity 0, or 1 with a ctx map) once for the whole transaction,
outside of any database transaction, and returns its recorded result.
`ctx.idempotency_key` is the same across retries and crashes.

| option | default | |
|---|---|---|
| `:retry` | off | `true` (5 attempts) or `[attempts: n]`. Without it, `fun` is called at most once, and an error, timeout or crash mid-call fails the effect. |
| `:compensate` | | `fn result -> … end` or `fn result, ctx -> … end`: undoes the effect if the transaction does not use its result. Uses the same `:retry`. |
| `:timeout_ms` | 30000 | per attempt |
| `:key` | `nil` | describes the call; a re-run with a different key makes a new effect |
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
| `:owner_wait_ms` | 300000 | how long to wait for a row owned by another transaction |
| `:listen` | `true` | wake the worker with LISTEN; `false` behind RDS Proxy or a transaction pooler |
| `:poll_ms` | 250 | idle poll interval |
| `:drain_ms` | 30000 | how long shutdown waits for work in progress |
| `:install` | `true` | install the `txn` schema if it is missing |

More in [docs/operations.md](../../docs/operations.md).
