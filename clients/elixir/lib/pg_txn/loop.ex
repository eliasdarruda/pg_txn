defmodule PgTxn.Loop do
  @moduledoc false
  # The client loop of docs/protocol.md: run the function in a Repo
  # transaction; when it needs an effect, roll back, call the effect outside
  # of any transaction, record its result, and run again. The run that
  # reaches the end commits.
  require Logger
  alias PgTxn.{Call, Config, DJSON, FencedError, Local, NeedEffect, OwnershipTimeoutError, Schema, SQL, Tx}

  @own_opts [:id, :lease_ms, :named, :owner]

  @doc """
  Drives transaction `tx_id` to its end. Returns `{:ok, value}` when it
  commits, `{:error, reason}` when the function called `repo.rollback(reason)`,
  and re-raises whatever the function raised (after recording the failure).
  """
  def drive(repo, tx_id, fun, %DateTime{} = started_at, opts) do
    if repo.in_transaction?() do
      raise ArgumentError,
            "pg_txn: PgTxn.transaction/3 cannot run inside another Repo transaction (each run commits or rolls back on its own)"
    end

    Schema.ensure!(repo)

    ctx = %{
      repo: repo,
      tx_id: tx_id,
      # the Erlang process driving it: if it dies, its lease expires and any
      # worker (this node's too) resumes a named transaction
      owner: opts[:owner] || Ecto.UUID.generate(),
      lease_ms: opts[:lease_ms] || Config.get(repo, :lease_ms),
      owner_wait_ms: Config.get(repo, :owner_wait_ms),
      named: Keyword.get(opts, :named, false),
      repo_opts: Keyword.drop(opts, @own_opts),
      started_at: DateTime.truncate(started_at, :millisecond)
    }

    # the lease is kept for the whole time this process drives the transaction
    heartbeat =
      every(max(1000, div(ctx.lease_ms, 3)), fn ->
        SQL.all(repo, "SELECT txn.heartbeat($1::text::uuid, $2::text::uuid, $3)", [tx_id, ctx.owner, ctx.lease_ms])
      end)

    Local.driving(repo, tx_id)

    try do
      loop(ctx, fun, false, 0)
    after
      Task.shutdown(heartbeat, :brutal_kill)
      Local.driven(repo, tx_id)
      keep_compensations(ctx)
    end
  end

  # after a transaction ends, keeps only the compensation functions it
  # actually scheduled
  defp keep_compensations(ctx) do
    case Local.compensations(ctx.repo, ctx.tx_id) do
      [] ->
        :ok

      ids ->
        due =
          ctx.repo
          |> SQL.all(
            "SELECT compensates::text AS id FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation' AND status IN ('pending', 'retry_wait', 'running')",
            [ctx.tx_id])
          |> MapSet.new(& &1["id"])

        Local.delete(ctx.repo, Enum.reject(ids, &MapSet.member?(due, &1)))
        if MapSet.size(due) > 0, do: PgTxn.Worker.wake(ctx.repo)
    end
  rescue
    _ -> :ok
  end

  defp loop(ctx, fun, claimed, retries) do
    tx = Tx.new(ctx.repo, ctx.tx_id, ctx.owner, ctx.started_at, claimed)

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
      claimed = perform(ctx, state) or claimed
      loop(ctx, fun, claimed, retries)
    else
      finish(ctx, fun, claimed, retries, outcome, state)
    end
  end

  defp run(ctx, tx, fun) do
    SQL.all(ctx.repo, "SELECT txn.attempt($1::text::uuid, $2::text::uuid)", [ctx.tx_id, ctx.owner])
    result = fun.(tx)
    state = Tx.state(tx)
    if state.needs != [], do: ctx.repo.rollback(NeedEffect)

    SQL.all(ctx.repo, "SELECT txn.finish($1::text::uuid, $2::text::uuid, $3::text[]::uuid[], $4::text::jsonb)",
      [ctx.tx_id, ctx.owner, state.consumed, output(result, ctx.named)])

    result
  end

  # named transactions store their output for PgTxn.wait/3 (it must be a
  # durable value); inline ones store it when it is one
  defp output(result, true), do: DJSON.encode!(result)

  defp output(result, false) do
    DJSON.encode!(result)
  rescue
    ArgumentError -> "null"
  end

  defp finish(ctx, _fun, _claimed, _retries, {:ok, result}, state) do
    if state.spawned, do: PgTxn.Worker.wake(ctx.repo)
    {:ok, result}
  end

  defp finish(ctx, _fun, _claimed, _retries, {:error, reason}, _state) do
    fail(ctx, %{"name" => "Rollback", "message" => inspect(reason)})
    {:error, reason}
  end

  defp finish(ctx, fun, claimed, retries, {:caught, kind, reason, stack}, _state) do
    case kind == :error && SQL.pg_error(reason) do
      {"55P03", "fenced"} ->
        raise FencedError, tx_id: ctx.tx_id

      {"55P03", "owner=" <> owner} ->
        wait_for(ctx, owner)
        loop(ctx, fun, claimed, retries)

      {code, _} when code in ["40001", "40P01"] and retries < 100 ->
        Process.sleep(:rand.uniform(min(1000, 5 * 2 ** min(retries + 1, 8))))
        loop(ctx, fun, claimed, retries + 1)

      _ ->
        fail(ctx, Call.error_json(if kind == :error, do: reason, else: %{"name" => to_string(kind), "message" => inspect(reason)}))
        :erlang.raise(kind, reason, stack)
    end
  end

  defp fail(ctx, error) do
    SQL.all(ctx.repo, "SELECT txn.fail_transaction($1::text::uuid, $2::text::uuid, $3::text::jsonb)",
      [ctx.tx_id, ctx.owner, Jason.encode!(error)])
  rescue
    e -> Logger.warning("pg_txn: could not record the failure of #{ctx.tx_id}: #{Exception.message(e)}")
  end

  # ------------------------------------------------------------------ perform

  # Claims the rows (once, all or nothing), then calls the needed effects
  # outside of any transaction. Returns whether the claims are now in place.
  defp perform(ctx, state) do
    case claim(ctx, state) do
      %{"conflict" => %{"reason" => "fenced"}} ->
        raise FencedError, tx_id: ctx.tx_id

      %{"conflict" => %{"reason" => "owned", "owner" => owner}} ->
        wait_for(ctx, owner)
        false

      %{"conflict" => _changed_or_gone} ->
        false

      %{"effects" => actions} ->
        execute_all(ctx, state.needs, actions)
        true
    end
  end

  # txn.prepare_effects raises 55P03 'owner=<uuid>' when it loses a claim
  # race to a transaction claiming the same row at the same time: an owned
  # conflict like any other
  defp claim(ctx, state) do
    prepare(ctx, state.needs, state.claims)
  rescue
    e in Postgrex.Error ->
      case SQL.pg_error(e) do
        {"55P03", "owner=" <> owner} -> %{"conflict" => %{"reason" => "owned", "owner" => owner}}
        _ -> reraise e, __STACKTRACE__
      end
  end

  defp prepare(ctx, needs, claims) do
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

    SQL.value(ctx.repo, "SELECT txn.prepare_effects($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb, $5::text::jsonb)",
      [ctx.tx_id, ctx.owner, ctx.lease_ms, Jason.encode!(effects), Jason.encode!(claims)])
  end

  defp execute_all(ctx, needs, [action]), do: execute(ctx, need(needs, action), action)

  defp execute_all(ctx, needs, actions) do
    actions
    |> Enum.map(fn a ->
      Task.async(fn ->
        try do
          execute(ctx, need(needs, a), a)
        rescue
          e -> {:raise, e, __STACKTRACE__}
        end
      end)
    end)
    |> Task.await_many(:infinity)
    |> Enum.each(fn
      {:raise, e, stack} -> reraise e, stack
      _ -> :ok
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
    o = Call.call(thunk, retry: !!need.opts[:retry], timeout_ms: Keyword.get(need.opts, :timeout_ms, 30_000))

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
    case prepare(ctx, [need], []) do
      %{"effects" => [action]} -> step(ctx, need, action)
      _conflict -> raise FencedError, tx_id: ctx.tx_id
    end
  end

  # ------------------------------------------------------------------ helpers

  @doc "Waits until transaction `owner` is no longer running."
  def wait_for(ctx, owner) do
    start = System.monotonic_time(:millisecond)
    wait_for(ctx, owner, start, 5)
  end

  defp wait_for(ctx, owner, start, ms) do
    case SQL.one(ctx.repo, "SELECT status FROM txn.status($1::text::uuid)", [owner]) do
      %{"status" => "running"} ->
        waited = System.monotonic_time(:millisecond) - start
        if waited > ctx.owner_wait_ms, do: raise(OwnershipTimeoutError, owner: owner, waited_ms: waited)
        Process.sleep(ms)
        wait_for(ctx, owner, start, min(round(ms * 1.5), 200))

      _ ->
        :ok
    end
  end

  @doc false
  # runs fun every `ms` in a process linked to the caller (it dies with it)
  def every(ms, fun) do
    Task.async(fn -> tick(ms, fun) end)
  end

  defp tick(ms, fun) do
    Process.sleep(ms)

    try do
      fun.()
    rescue
      _ -> :ok
    end

    tick(ms, fun)
  end
end
