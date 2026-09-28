defmodule PgTxn do
  @moduledoc """
  pg_txn for Ecto: database transactions that include side effects.

      PgTxn.transaction(Repo, fn tx ->
        # protected until commit
        order = PgTxn.own(tx, "orders", id)

        # no lock or connection held while it runs
        payment =
          PgTxn.effect(tx, fn ctx -> Payments.charge(order, ctx.idempotency_key) end,
            name: "charge", key: order["amount"])

        Repo.query!("UPDATE orders SET status = 'paid', payment = $2 WHERE id = $1", [id, payment["id"]])

        # runs iff this commits
        PgTxn.spawn(tx, fn -> Mailer.send_receipt(id) end)
      end)

  The function runs in an ordinary `Repo.transaction/2`, so it uses the Repo
  as usual (queries, schemas, changesets). When it reaches an effect that has
  not run yet, the transaction is rolled back, the effect is called outside
  of any transaction (no lock or connection held), its result is recorded,
  and the function runs again: effects that already ran return their
  recorded results. The run that reaches the end commits everything at once.
  The function must therefore be deterministic given the database and the
  effects' results: use `now/1` and `uuid/1` instead of the clock and random
  ids. See `docs/protocol.md`.

  Setup is `use PgTxn.Repo` in the Repo (see `PgTxn.Repo`), which also
  starts this node's `PgTxn.Worker` (it runs spawned effects and
  compensations, which are functions in this node's memory) and installs
  the `txn` schema.

  Values (inputs, results, outputs) are stored as JSON (`PgTxn.DJSON`): maps
  come back with string keys; integers beyond 2^53, `DateTime`s and
  `{:bytes, binary}` round-trip exactly.
  """
  alias PgTxn.{Call, Config, DJSON, Local, Loop, Registry, Schema, SQL, Tx, TransactionFailedError, Worker}

  @type tx :: Tx.t()
  @type repo :: module

  @typedoc "`fn -> ... end` or `fn ctx -> ... end` (see `effect/3`)."
  @type effect_fun :: (-> term) | (map -> term)

  @typedoc "`fn result -> ... end` or `fn result, ctx -> ... end` (see `effect/3`)."
  @type compensate_fun :: (term -> term) | (term, map -> term)

  # ------------------------------------------------------------------ transactions

  @doc """
  Runs `fun` as one transaction that may include effects.

  Returns `{:ok, value}` when it commits and `{:error, reason}` when `fun`
  calls `repo.rollback(reason)`; an exception raised by `fun` is re-raised
  (like `Repo.transaction/2`). Either way the failure is recorded
  (`txn.fail_transaction`): effects that ran are orphaned and compensated.

  Options: `:id` (the transaction id, default a new uuid), `:lease_ms`, and
  any `Repo.transaction/2` option (e.g. `:timeout`). Cannot be called inside
  another Repo transaction.
  """
  @spec transaction(repo, (tx -> result), keyword) :: {:ok, result} | {:error, term} when result: term
  def transaction(repo, fun, opts \\ []) when is_function(fun, 1) do
    Loop.drive(repo, opts[:id] || Ecto.UUID.generate(), fun, DateTime.utc_now(), opts)
  end

  @doc """
  Calls `fun` once for the whole transaction and returns its recorded result.

  `fun` takes no argument, or a context map with `:effect_id` and
  `:idempotency_key` (the same across retries, re-runs and crashes: pass it
  to the target API), `:attempt` and `:tx_id`. It returns `{:ok, result}`,
  `{:error, reason}` or a plain result, or raises. It runs in its own
  process, outside of any database transaction. The result must be a durable
  value (see `PgTxn.DJSON`); maps come back with string keys.

  An effect is identified by its position in the transaction and its
  `:name`. With a `:key` (any durable value, e.g. the data the call is
  decided on), a re-run that reaches it with a different key is a new
  effect, and the old one is orphaned (and compensated) when the
  transaction commits.

  Options:

    * `:name` - names the effect in `txn.effects` and for reuse (default `"effect"`)
    * `:key` - see above (default `nil`: reuse by position and name only)
    * `:retry` - off by default: `fun` is called at most once, and an
      error, a timeout or a crash mid-call fails the effect. Turn it on only
      when `fun` is safe to call again (e.g. it passes `ctx.idempotency_key`
      to the API it calls): `true` (5 attempts) or `[attempts: n]`. Errors
      other than a `PgTxn.PermanentError` are then retried with backoff (a
      `PgTxn.RetryableError`'s `:retry_after_ms` sets the delay), and a call
      interrupted by a crash is made again.
    * `:timeout_ms` - per attempt (default 30000)
    * `:compensate` - `fn result -> ... end` or `fn result, ctx -> ... end`:
      undoes the effect if the transaction ends up not using its result (it
      fails, or a re-run no longer calls it). It runs on this node's worker,
      after the transaction ends, with the effect's `:retry` option, the
      effect's recorded result (and the context map of the compensation).
      Recorded as `"undo <name>"`.

  When the effect fails for good, `PgTxn.EffectFailedError` is raised here.
  """
  @spec effect(tx, effect_fun, keyword) :: term
  def effect(%Tx{} = tx, fun, opts \\ []) when is_function(fun, 0) or is_function(fun, 1) do
    Call.validate!(opts)

    with undo when not is_nil(undo) and not is_function(undo, 1) and not is_function(undo, 2) <- opts[:compensate] do
      raise ArgumentError, "pg_txn: :compensate must be a function of arity 1 or 2, got #{inspect(undo)}"
    end

    Tx.effect(tx, fun, opts)
  end

  @doc """
  Reads a row and protects it until the transaction commits: nobody else can
  change it meanwhile (they get a `Postgrex.Error` with code
  `:lock_not_available` at once), and effects only run if it did not change
  since it was read. Another transaction owning the same row waits for this
  one.

  `table` is a table name (`"orders"`, `"shop.orders"`) or an Ecto schema
  module; `key` is the primary key value, or a map of its columns. Returns
  the row as a map with string keys (its `jsonb` form), or `nil`. Call it
  before the transaction's first effect.
  """
  @spec own(tx, String.t() | module, term) :: map | nil
  def own(%Tx{} = tx, table, key), do: Tx.own(tx, table, key)

  @doc "When the transaction started: the same in every run."
  @spec now(tx) :: DateTime.t()
  def now(%Tx{started_at: t}), do: t

  @doc """
  A random-looking UUID that is the same in every run (the n-th call of a run
  returns the n-th id).
  """
  @spec uuid(tx) :: String.t()
  def uuid(%Tx{} = tx), do: Tx.uuid(tx)

  # ------------------------------------------------------------------ spawn / enqueue

  @doc """
  Calls `fun` iff the surrounding transaction commits, right after the
  commit, on this node's worker (retried on failure; recorded in
  `txn.effects`). `fun` takes no argument or the context map of `effect/3`,
  and returns or raises like an effect; like an effect it is called at most
  once unless `:retry` is on. It is a function in this node's
  memory: if the node stops before it completes, it fails as `EffectLost`
  (for work that must survive the node, `enqueue/4` a named transaction).

  The first argument is a pg_txn `tx`, or a Repo: then it joins the
  `Repo.transaction/2` (or `Ecto.Multi`, see `PgTxn.Multi`) the calling
  process is in, or is spawned at once outside of one. Returns the effect id.

  Options: `:name` (a label in `txn.effects`, default `"spawn"`), `:retry`
  (as in `effect/3`), `:delay_ms` (default 0), `:timeout_ms` (per attempt,
  default 30000).
  """
  @spec spawn(tx | repo, effect_fun, keyword) :: String.t()
  def spawn(tx_or_repo, fun, opts \\ []) when is_function(fun, 0) or is_function(fun, 1) do
    Call.validate!(opts)
    repo = repo!(tx_or_repo)
    unless match?(%Tx{}, tx_or_repo), do: Schema.ensure!(repo)
    id = Ecto.UUID.generate()
    owner = Config.owner(repo)
    call = fn ctx, _input -> if is_function(fun, 0), do: fun.(), else: fun.(ctx) end
    tx_id = with %Tx{id: tx_id} <- tx_or_repo, do: tx_id, else: (_ -> nil)
    Local.put(repo, id, :spawn, owner, tx_id, call, opts[:timeout_ms])

    params = [
      owner,
      to_string(Keyword.get(opts, :name, "spawn")),
      id,
      Call.attempts(opts[:retry]),
      Call.delivery(opts[:retry]),
      Keyword.get(opts, :delay_ms, 0)
    ]

    sql = "SELECT txn.spawn($1::text::uuid, $2, $3::text::uuid, $4, $5, $6)"

    case tx_or_repo do
      %Tx{} = tx ->
        # a run that does not commit forgets it (see PgTxn.Loop)
        Tx.mark_spawned(tx, id)
        SQL.all(repo, sql, params)

      _repo ->
        try do
          SQL.all(repo, sql, params)
        rescue
          e ->
            Local.delete(repo, [id])
            reraise e, __STACKTRACE__
        end

        unless repo.in_transaction?(), do: Worker.wake(repo)
    end

    id
  end

  @doc """
  Queues the named transaction `name` (see `define/3`) to run in the
  background on any node that defines it, iff the surrounding transaction
  commits (like `spawn/3`: pass a `tx` or a Repo). Returns its id, for
  `wait/3`. Option: `:id`.
  """
  @spec enqueue(tx | repo, String.t() | atom, term, keyword) :: String.t()
  def enqueue(tx_or_repo, name, input \\ %{}, opts \\ []) do
    repo = repo!(tx_or_repo)
    unless match?(%Tx{}, tx_or_repo), do: Schema.ensure!(repo)

    id =
      SQL.value(repo, "SELECT txn.enqueue($1, $2::text::jsonb, $3::text::uuid)::text",
        [to_string(name), DJSON.encode!(input), opts[:id]])

    case tx_or_repo do
      %Tx{} = tx -> Tx.mark_spawned(tx)
      _ -> unless repo.in_transaction?(), do: Worker.wake(repo)
    end

    id
  end

  # ------------------------------------------------------------------ named transactions

  @doc """
  Defines the named transaction `name`: `fun.(tx, input)` is its function.
  If the process running it stops, any node that defines the same name
  resumes it (recorded effects are reused). Define it on every node, at
  startup. `input` is always given in its stored form (maps with string
  keys), in the first run and in resumed ones alike.
  """
  @spec define(repo, String.t() | atom, (tx, term -> term)) :: :ok
  def define(repo, name, fun) when is_atom(repo) and is_function(fun, 2) do
    Registry.put_definition(repo, name, fun)
    Worker.wake(repo)
  end

  @doc """
  Runs the named transaction `name` now, in this process (resumable by any
  node that defines it if this one stops). Returns like `transaction/3`; its
  output must be a durable value. Options: `:id`, `:lease_ms`.
  """
  @spec run(repo, String.t() | atom, term, keyword) :: {:ok, term} | {:error, term}
  def run(repo, name, input, opts \\ []) do
    name = to_string(name)
    fun = Registry.definition(repo, name) || raise ArgumentError, "pg_txn: no transaction named #{name} is defined for #{inspect(repo)}"
    Schema.ensure!(repo)
    id = opts[:id] || Ecto.UUID.generate()
    lease_ms = opts[:lease_ms] || Config.get(repo, :lease_ms)
    encoded = DJSON.encode!(input)
    owner = Ecto.UUID.generate()

    # its created_at: now/1 of the first run and of resumed ones agree
    started_at =
      SQL.value(repo, "SELECT txn.start($1::text::uuid, $2, $3::text::jsonb, $4::text::uuid, $5)",
        [id, name, encoded, owner, lease_ms])

    stored = DJSON.decode!(encoded)
    Loop.drive(repo, id, fn tx -> fun.(tx, stored) end, started_at, Keyword.merge(opts, id: id, named: true, owner: owner))
  end

  @doc """
  Waits up to `timeout_ms` for a transaction (e.g. an enqueued one) to end.
  Returns `{:ok, output}` when it committed, `{:error, %PgTxn.TransactionFailedError{}}`
  when it failed or was abandoned, and `{:error, :timeout}`.
  """
  @spec wait(repo, String.t(), non_neg_integer) :: {:ok, term} | {:error, TransactionFailedError.t() | :timeout}
  def wait(repo, tx_id, timeout_ms \\ 60_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait(repo, tx_id, deadline, 10)
  end

  defp wait(repo, tx_id, deadline, ms) do
    case SQL.one(repo, "SELECT status, output, error FROM txn.status($1::text::uuid)", [tx_id]) do
      %{"status" => "committed", "output" => output} ->
        {:ok, DJSON.from_tagged(output)}

      %{"status" => status, "error" => error} when status != "running" ->
        {:error, %TransactionFailedError{tx_id: tx_id, status: status, error: error}}

      _running_or_not_yet ->
        if System.monotonic_time(:millisecond) > deadline do
          {:error, :timeout}
        else
          Process.sleep(ms)
          wait(repo, tx_id, deadline, min(round(ms * 1.5), 250))
        end
    end
  end

  defp repo!(%Tx{repo: repo}), do: repo
  defp repo!(repo) when is_atom(repo), do: repo
end
