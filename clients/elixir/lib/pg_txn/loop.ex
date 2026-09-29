defmodule PgTxn.Loop do
  @moduledoc false
  # The client loop of docs/protocol.md: run the function in a Repo
  # transaction; when it needs an effect, roll back, call the effect outside
  # of any transaction, record its result, and run again. The run that
  # reaches the end commits.
  require Logger
  alias PgTxn.{Call, Config, DJSON, FencedError, KeyTimeoutError, Local, NeedEffect, Schema, SQL, Tx}

  @own_opts [:id, :lease_ms, :named, :owner, :key, :keys, :start, :started_at, :isolation]
  @isolations %{
    "read committed" => "read committed",
    "repeatable read" => "repeatable read",
    "serializable" => "serializable",
    read_committed: "read committed",
    repeatable_read: "repeatable read",
    serializable: "serializable"
  }

  @doc """
  Drives transaction `tx_id` to its end. Returns `{:ok, value}` when it
  commits, `{:error, reason}` when the function called `repo.rollback(reason)`,
  and re-raises whatever the function raised (after recording the failure).

  Options (besides `Repo.transaction/2` ones): `:owner`, `:lease_ms`,
  `:named`, `:isolation`, `:started_at` (of a resumed transaction), or
  `start: {name, encoded_input}` with `:key`/`:keys` to record it as running
  first (`txn.start`, waiting for the keys). If `txn.start` finds the id
  exists already (it ran, or runs now elsewhere), nothing is run: this waits
  for its outcome, like `PgTxn.wait/3`.
  """
  def drive(repo, tx_id, fun, opts) do
    if repo.in_transaction?() do
      raise ArgumentError,
            "pg_txn: PgTxn.transaction/3 cannot run inside another Repo transaction (each run commits or rolls back on its own)"
    end

    Schema.ensure!(repo)
    # the Erlang process driving it: if it dies, its lease expires and any
    # worker (this node's too) resumes a named transaction
    owner = opts[:owner] || Ecto.UUID.generate()
    lease_ms = opts[:lease_ms] || Config.get(repo, :lease_ms)
    isolation = isolation!(opts[:isolation])

    case opts[:start] do
      nil ->
        drive(repo, tx_id, fun, opts, owner, lease_ms, isolation, opts[:started_at] || DateTime.utc_now())

      {name, input} ->
        case start(repo, tx_id, name, input, owner, lease_ms, keys(opts), isolation) do
          :existing -> PgTxn.wait(repo, tx_id, Config.get(repo, :key_wait_ms))
          started_at -> drive(repo, tx_id, fun, opts, owner, lease_ms, isolation, started_at)
        end
    end
  end

  @doc "An isolation level as stored in `txn.transactions.isolation` (nil: the database default)."
  def isolation!(nil), do: nil

  def isolation!(level) do
    Map.get(@isolations, level) ||
      raise ArgumentError, "pg_txn: :isolation must be :read_committed, :repeatable_read or :serializable, got #{inspect(level)}"
  end

  defp drive(repo, tx_id, fun, opts, owner, lease_ms, isolation, started_at) do
    ctx = %{
      repo: repo,
      tx_id: tx_id,
      owner: owner,
      lease_ms: lease_ms,
      durable: Keyword.get(opts, :named, false) or Keyword.has_key?(opts, :id),
      repo_opts: Keyword.drop(opts, @own_opts),
      # every run is rolled back but the last, so the count is kept here
      runs: 0,
      isolation: isolation,
      run: nil,
      started_at: DateTime.truncate(started_at, :millisecond)
    }

    # the lease (txn.leases, apart from the row a run updates) is kept for
    # the whole time this process drives the transaction, runs included
    heartbeat =
      every(max(1000, div(ctx.lease_ms, 3)), fn ->
        SQL.all(repo, "SELECT txn.heartbeat($1::text::uuid, $2::text::uuid, $3)", [tx_id, ctx.owner, ctx.lease_ms])
      end)

    Local.driving(repo, tx_id)

    try do
      loop(ctx, fun, 0)
    after
      PgTxn.Proc.shutdown(heartbeat)
      Local.driven(repo, tx_id)
      keep_compensations(ctx)
    end
  end

  # after a transaction ends, keeps only the compensation functions it
  # actually scheduled; compensations this driver must run but has no
  # function for (a transaction resumed here took another path than the
  # process that called the effect) are leased at once, to fail as EffectLost
  defp keep_compensations(ctx) do
    due =
      ctx.repo
      |> SQL.all(
        "SELECT compensates::text AS id FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation' AND local_owner = $2::text::uuid AND status IN ('pending', 'retry_wait', 'running')",
        [ctx.tx_id, ctx.owner])
      |> MapSet.new(& &1["id"])

    ids = Local.compensations(ctx.repo, ctx.tx_id)
    Local.delete(ctx.repo, Enum.reject(ids, &MapSet.member?(due, &1)))
    # a function registered by an earlier driver of this transaction (on this
    # node) runs under this driver's lease now
    Enum.each(due, &Local.set_owner(ctx.repo, &1, ctx.owner))

    cond do
      Enum.any?(due, &(&1 not in ids)) -> PgTxn.Worker.lease_now(ctx.repo, ctx.owner)
      MapSet.size(due) > 0 -> PgTxn.Worker.wake(ctx.repo)
      true -> :ok
    end
  rescue
    _ -> :ok
  end

  @doc """
  The keys of options `:key` (one) and `:keys` (a list) as stored, or nil
  for none: a string as is, any other term as its canonical DJSON text
  (sorted object keys, tuples as arrays), like the other clients:
  `["order", 42]` is `["order",42]`, `%{b: 1, a: 2}` is `{"a":2,"b":1}`.
  nil, and terms that are not durable values, raise an ArgumentError.
  """
  @spec keys(keyword) :: [String.t()] | nil
  def keys(opts) do
    one = if Keyword.has_key?(opts, :key), do: [opts[:key]], else: []

    case Enum.map(one ++ (opts[:keys] || []), &key_text/1) do
      [] -> nil
      keys -> keys
    end
  end

  defp key_text(key) when is_binary(key), do: key
  defp key_text(nil), do: raise(ArgumentError, "pg_txn: a key cannot be nil")

  defp key_text(key) do
    key |> lists() |> DJSON.encode!()
  rescue
    e in ArgumentError -> reraise ArgumentError, "pg_txn: #{inspect(key)} is not a valid key: #{Exception.message(e)}", __STACKTRACE__
  end

  defp lists(t) when is_tuple(t), do: t |> Tuple.to_list() |> lists()
  defp lists(l) when is_list(l), do: Enum.map(l, &lists/1)
  defp lists(%{__struct__: _} = s), do: s
  defp lists(m) when is_map(m), do: Map.new(m, fn {k, v} -> {k, lists(v)} end)
  defp lists(v), do: v

  # records the transaction as running (named, with keys, or with an id) and
  # returns when it started; with a key another transaction holds, waits for
  # that one to end first; with an id that exists already, returns :existing
  defp start(repo, tx_id, name, input, owner, lease_ms, keys, isolation) do
    wait_ms = Config.get(repo, :key_wait_ms)
    since = System.monotonic_time(:millisecond)

    Stream.repeatedly(fn ->
      SQL.one(repo,
        "SELECT created_at, holder::text AS holder, existing FROM txn.start($1::text::uuid, $2, $3::text::jsonb, $4::text::uuid, $5, $6::text[], $7)",
        [tx_id, name, input, owner, lease_ms, keys, isolation])
    end)
    |> Enum.find_value(fn
      %{"existing" => true} ->
        :existing

      %{"holder" => nil, "created_at" => created_at} ->
        created_at

      %{"holder" => holder} ->
        wait_for(repo, holder, fn ->
          # the worker is shutting down (or stopped): do not start later
          Local.open!(repo)
          waited = System.monotonic_time(:millisecond) - since
          if waited > wait_ms, do: raise(KeyTimeoutError, key: Enum.join(keys, ", "), holder: holder, waited_ms: waited)
        end)

        nil
    end)
  end

  defp loop(ctx, fun, retries) do
    ctx = %{ctx | runs: ctx.runs + 1}
    tx = Tx.new(ctx.repo, ctx.tx_id, ctx.owner, ctx.started_at)

    outcome =
      try do
        ctx.repo.transaction(fn -> run(ctx, tx, fun) end, ctx.repo_opts)
      catch
        kind, reason -> {:caught, kind, reason, __STACKTRACE__}
      end

    state = Tx.close(tx)
    unless match?({:ok, _}, outcome) and state.needs == [], do: Local.delete(ctx.repo, state.spawn_ids)

    if state.needs != [] do
      # a run that registered a need rolls back whatever user code did with the abort
      perform(%{ctx | run: tx.ref}, state)
      loop(ctx, fun, retries)
    else
      finish(ctx, fun, retries, outcome, state)
    end
  end

  defp run(ctx, tx, fun) do
    if ctx.isolation, do: Ecto.Adapters.SQL.query!(ctx.repo, "SET TRANSACTION ISOLATION LEVEL #{String.upcase(ctx.isolation)}")
    SQL.all(ctx.repo, "SELECT txn.attempt($1::text::uuid, $2::text::uuid)", [ctx.tx_id, ctx.owner])
    result = fun.(tx)
    state = Tx.state(tx)
    if state.needs != [], do: ctx.repo.rollback(NeedEffect)

    SQL.all(ctx.repo, "SELECT txn.finish($1::text::uuid, $2::text::uuid, $3::text[]::uuid[], $4::text::jsonb, $5)",
      [ctx.tx_id, ctx.owner, state.consumed, output(result, ctx.durable), ctx.runs])

    result
  end

  # the output of a transaction that can be asked for again (named, or with
  # an id: PgTxn.wait/3, an idempotent re-call) must be a durable value, or
  # the transaction fails; one nobody can ask for again stores it when it is
  defp output(result, true) do
    DJSON.encode!(result)
  rescue
    e in ArgumentError ->
      reraise ArgumentError,
              "pg_txn: the transaction's output cannot be stored (named, or with an :id, it is returned again to a later call): #{Exception.message(e)}",
              __STACKTRACE__
  end

  defp output(result, false) do
    DJSON.encode!(result)
  rescue
    ArgumentError -> "null"
  end

  defp finish(ctx, _fun, _retries, {:ok, result}, state) do
    if state.spawned, do: PgTxn.Worker.wake(ctx.repo)
    {:ok, result}
  end

  defp finish(ctx, _fun, _retries, {:error, reason}, _state) do
    fail(ctx, %{"name" => "Rollback", "message" => inspect(reason)})
    {:error, reason}
  end

  defp finish(ctx, fun, retries, {:caught, kind, reason, stack}, _state) do
    case kind == :error && SQL.pg_error(reason) do
      {"55P03", "fenced"} ->
        raise FencedError, tx_id: ctx.tx_id

      {code, _} when code in ["40001", "40P01"] and retries < 100 ->
        Process.sleep(:rand.uniform(min(1000, 5 * 2 ** min(retries + 1, 8))))
        loop(ctx, fun, retries + 1)

      _ ->
        fail(ctx, Call.error_json(if kind == :error, do: reason, else: %{"name" => to_string(kind), "message" => inspect(reason)}))
        :erlang.raise(kind, reason, stack)
    end
  end

  defp fail(ctx, error) do
    SQL.all(ctx.repo, "SELECT txn.fail_transaction($1::text::uuid, $2::text::uuid, $3::text::jsonb, $4)",
      [ctx.tx_id, ctx.owner, Jason.encode!(error), ctx.runs])
  rescue
    e -> Logger.warning("pg_txn: could not record the failure of #{ctx.tx_id}: #{Exception.message(e)}")
  end

  # ------------------------------------------------------------------ perform

  # takes the lease and records the intents, then calls the needed effects
  # outside of any transaction; anything failing here fails the transaction
  # (releasing its keys) rather than leaving it running
  defp perform(ctx, state) do
    case prepare(ctx, state.needs) do
      %{"effects" => actions} -> execute_all(ctx, state.needs, actions)
      %{"conflict" => _fenced} -> raise FencedError, tx_id: ctx.tx_id
    end
  catch
    :error, %FencedError{} = e ->
      reraise e, __STACKTRACE__

    kind, reason ->
      stack = __STACKTRACE__
      fail(ctx, Call.error_json(if kind == :error, do: reason, else: %{"name" => to_string(kind), "message" => inspect(reason)}))
      :erlang.raise(kind, reason, stack)
  end

  defp prepare(ctx, needs) do
    effects =
      Enum.map(needs, fn n ->
        %{
          seq: n.seq,
          name: n.name,
          input: n.tagged,
          max_attempts: Call.attempts(n.opts[:retry]),
          delivery: Call.delivery(n.opts[:retry]),
          compensation: if(n.opts[:compensate], do: "undo #{n.name}")
        }
      end)

    SQL.value(ctx.repo, "SELECT txn.prepare_effects($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb)",
      [ctx.tx_id, ctx.owner, ctx.lease_ms, Jason.encode!(effects)])
  end

  defp execute_all(ctx, needs, [action]), do: execute(ctx, need(needs, action), action)

  defp execute_all(ctx, needs, actions) do
    actions
    |> Enum.map(fn a ->
      PgTxn.Proc.async(fn ->
        try do
          execute(ctx, need(needs, a), a)
        catch
          kind, reason -> {:raise, kind, reason, __STACKTRACE__}
        end
      end)
    end)
    |> Enum.map(&PgTxn.Proc.await/1)
    |> Enum.each(fn
      {:ok, {:raise, kind, reason, stack}} -> :erlang.raise(kind, reason, stack)
      {:ok, _} -> :ok
      {:exit, reason} -> exit(reason)
    end)
  end

  defp need(needs, %{"seq" => seq}), do: Enum.find(needs, &(&1.seq == seq))

  defp execute(ctx, need, action) do
    if need.opts[:compensate], do: Local.compensation(ctx.repo, action["id"], need.opts, ctx.owner, ctx.tx_id)
    step(ctx, need, action)
  end

  defp step(_ctx, _need, %{"action" => "done"}), do: :ok

  defp step(ctx, need, %{"action" => "wait", "wait_ms" => ms}) do
    Process.sleep(ms)
    again(ctx, need)
  end

  defp step(ctx, need, %{"action" => "execute", "id" => id, "attempt" => attempt}) do
    call_ctx = %{effect_id: id, idempotency_key: id, attempt: attempt, tx_id: ctx.tx_id}
    fun = need.fun
    thunk = if is_function(fun, 0), do: fun, else: fn -> fun.(call_ctx) end
    o = Call.call(thunk, retry: !!need.opts[:retry], timeout_ms: Keyword.get(need.opts, :timeout_ms, 30_000), run: ctx.run)

    done =
      SQL.value(ctx.repo,
        "SELECT txn.effect_done($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb, $5::text::jsonb, $6, $7)",
        [id, ctx.owner, o.ok, if(o.ok, do: Jason.encode!(o.result)), if(!o.ok, do: Jason.encode!(o.error)),
         o.retryable, o.retry_after_ms])

    case done do
      %{"status" => "retry_wait", "wait_ms" => ms} ->
        Process.sleep(ms)
        again(ctx, need)

      _ ->
        :ok
    end
  end

  defp again(ctx, need) do
    case prepare(ctx, [need]) do
      %{"effects" => [action]} -> step(ctx, need, action)
      _conflict -> raise FencedError, tx_id: ctx.tx_id
    end
  end

  # ------------------------------------------------------------------ helpers

  # waits until transaction `tx_id` is no longer running; `check` may raise
  defp wait_for(repo, tx_id, check), do: wait_for(repo, tx_id, check, 5)

  defp wait_for(repo, tx_id, check, ms) do
    case SQL.one(repo, "SELECT status FROM txn.status($1::text::uuid)", [tx_id]) do
      %{"status" => "running"} ->
        check.()
        Process.sleep(ms)
        wait_for(repo, tx_id, check, min(round(ms * 1.5), 200))

      _ ->
        :ok
    end
  end

  @doc false
  # runs fun every `ms` in a process that dies with the caller; stop it
  # with PgTxn.Proc.shutdown/1
  def every(ms, fun) do
    PgTxn.Proc.async(fn -> tick(ms, fun) end)
  end

  defp tick(ms, fun) do
    Process.sleep(ms)

    try do
      fun.()
    catch
      _, _ -> :ok
    end

    tick(ms, fun)
  end
end
