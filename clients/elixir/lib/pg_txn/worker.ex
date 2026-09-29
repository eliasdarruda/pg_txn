defmodule PgTxn.Worker do
  @moduledoc """
  This node's pg_txn worker for a Repo: runs this node's spawned effects
  and compensations (functions kept in its memory, see `PgTxn.spawn/3`), and
  drives named transactions that nobody drives (enqueued ones, and ones
  whose process stopped).

  Started by `use PgTxn.Repo`, or directly:

      children = [MyApp.Repo, {PgTxn.Worker, repo: MyApp.Repo}]

  It installs the `txn` schema on start, polls (woken early by
  `LISTEN txn_effects` and by this node's own spawns), reports itself with
  `txn.worker_seen` every 10 s, every 5 s abandons inline transactions
  whose process stopped, expires the leases of spawned effects whose worker
  stopped, fails those whose node is gone (`EffectLost`) and forgets
  functions that no effect needs anymore, and heartbeats the leases of the
  work it runs. On shutdown it stops leasing and waits up to `:drain_ms` for
  work in progress. See `PgTxn.Config` for the options.
  """
  use GenServer
  require Logger
  alias PgTxn.{Call, Config, DJSON, FencedError, Local, Loop, Registry, Schema, SQL}

  @seen_every 10_000

  @doc false
  # the worker and the Repo's PgTxn.KeyQueue, which outlives the worker's
  # shutdown (callers waiting in it then leave)
  def child_spec(opts) do
    repo = Keyword.fetch!(opts, :repo)
    shutdown = Config.get(repo, :drain_ms) + 5_000

    worker = %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, repo, [name: name(repo)]]},
      # draining happens in terminate/2
      shutdown: shutdown
    }

    %{
      id: {__MODULE__, repo},
      type: :supervisor,
      start: {Supervisor, :start_link, [[{PgTxn.KeyQueue, repo}, worker], [strategy: :rest_for_one]]},
      shutdown: shutdown + 1_000
    }
  end

  @doc "Starts the worker of `opts[:repo]` (and its key queue)."
  def start_link(opts) do
    %{start: {m, f, a}} = child_spec(opts)
    apply(m, f, a)
  end

  @doc "The registered name of a Repo's worker."
  def name(repo), do: Module.concat(repo, PgTxnWorker)

  @doc "Makes the Repo's worker (if running on this node) poll now."
  @spec wake(module) :: :ok
  def wake(repo) do
    case Process.whereis(name(repo)) do
      nil -> :ok
      pid -> send(pid, :wake)
    end

    :ok
  end

  @doc false
  # leases the effects of `owner` (a transaction this node drove) now: its
  # compensations this node has no function for fail as EffectLost at once
  def lease_now(repo, owner) do
    case Process.whereis(name(repo)) do
      nil -> :ok
      pid -> send(pid, {:lease_now, owner})
    end

    :ok
  end

  # ------------------------------------------------------------------ server

  @impl true
  def init(repo) do
    Process.flag(:trap_exit, true)
    Local.new(repo)

    state = %{
      repo: repo,
      owner: Config.owner(repo),
      opts: Config.get(repo),
      tasks: %{},
      timer: nil,
      ready: false,
      listener: nil,
      last_seen: nil,
      last_maintain: nil
    }

    {:ok, state, {:continue, :start}}
  end

  @impl true
  def handle_continue(:start, state), do: {:noreply, start(state)}

  @impl true
  def handle_info(:start, state), do: {:noreply, start(state)}

  def handle_info(msg, %{ready: true} = state) when msg in [:poll, :wake] do
    if state.timer, do: Process.cancel_timer(state.timer)
    {found, state} = poll(state)
    delay = if found > 0, do: 0, else: state.opts[:poll_ms]
    {:noreply, %{state | timer: Process.send_after(self(), :poll, delay)}}
  end

  def handle_info({:lease_now, owner}, %{ready: true} = state) do
    {_n, state} =
      try do
        lease_effects(state, owner, 0)
      rescue
        e ->
          Logger.error("pg_txn worker: #{Exception.message(e)}")
          {0, state}
      end

    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, "txn_done", tx_id}, state) do
    Local.notify_done(state.repo, tx_id)
    {:noreply, state}
  end

  def handle_info({:notification, _pid, _ref, _channel, _payload}, state) do
    send(self(), :wake)
    {:noreply, state}
  end

  def handle_info({ref, _result}, state) when is_map_key(state.tasks, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | tasks: Map.delete(state.tasks, ref)}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_map_key(state.tasks, ref) do
    Logger.error("pg_txn worker: #{inspect(state.tasks[ref])} crashed: #{inspect(reason)}")
    {:noreply, %{state | tasks: Map.delete(state.tasks, ref)}}
  end

  def handle_info({:EXIT, pid, reason}, %{listener: pid} = state) do
    Logger.warning("pg_txn worker: LISTEN connection stopped (#{inspect(reason)}); polling only")
    {:noreply, %{state | listener: nil}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Shutting down: no more background transactions are leased and new calls
  # are refused, but this node's spawned functions and compensations keep
  # running until the transactions in progress (and their spawns and
  # compensations) are done, for up to :drain_ms.
  @impl true
  def terminate(_reason, state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    Local.closing(state.repo)
    drain(state, System.monotonic_time(:millisecond) + state.opts[:drain_ms])
  end

  defp drain(state, deadline) do
    state = await_tasks(state, 0)

    cond do
      idle?(state, deadline) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Logger.warning("pg_txn worker: work still in progress after drain_ms")

      true ->
        state =
          try do
            if state.ready, do: state |> lease_effects(false) |> elem(1), else: state
          rescue
            _ -> state
          end

        drain(await_tasks(state, 50), deadline)
    end
  end

  defp await_tasks(state, timeout) do
    receive do
      {ref, _} when is_map_key(state.tasks, ref) ->
        Process.demonitor(ref, [:flush])
        await_tasks(%{state | tasks: Map.delete(state.tasks, ref)}, 0)

      {:DOWN, ref, :process, _, _} when is_map_key(state.tasks, ref) ->
        await_tasks(%{state | tasks: Map.delete(state.tasks, ref)}, 0)
    after
      timeout -> state
    end
  end

  defp idle?(state, deadline) do
    map_size(state.tasks) == 0 and Local.busy(state.repo) == 0 and pending_local(state, deadline) == 0
  end

  # this node's spawns and compensations due within the drain window (one
  # delayed by an hour does not hold the shutdown)
  defp pending_local(state, deadline) do
    if Local.any?(state.repo) do
      owners = Enum.uniq([state.owner | Local.compensation_owners(state.repo)])
      remaining = max(0, deadline - System.monotonic_time(:millisecond))

      SQL.value(state.repo, """
      SELECT count(*) FROM txn.effects WHERE local_owner = ANY ($1::text[]::uuid[]) AND kind <> 'call'
         AND status IN ('pending', 'retry_wait', 'running')
         AND (status = 'running' OR next_attempt_at < now() + make_interval(secs => $2 / 1000.0))
      """, [owners, remaining])
    else
      0
    end
  rescue
    _ -> 0
  end

  defp start(state) do
    Schema.ensure!(state.repo)
    send(self(), :poll)
    %{state | ready: true, listener: listen(state)}
  rescue
    e ->
      Logger.error("pg_txn worker: cannot start for #{inspect(state.repo)}: #{Exception.message(e)}; retrying")
      Process.send_after(self(), :start, 1_000)
      state
  end

  defp listen(%{opts: opts, repo: repo}) do
    if opts[:listen] do
      config = repo.config() |> Keyword.drop([:pool, :pool_size, :name]) |> Keyword.put(:auto_reconnect, true)

      with {:ok, pid} <- Postgrex.Notifications.start_link(config),
           {:ok, _ref} <- Postgrex.Notifications.listen(pid, "txn_effects"),
           # wakes callers waiting for a transaction to end (a key's holder)
           {:ok, _ref} <- Postgrex.Notifications.listen(pid, "txn_done") do
        pid
      else
        _ -> nil
      end
    end
  end

  # ------------------------------------------------------------------ polling

  defp poll(state) do
    {state, tick} = housekeeping(state)
    {n1, state} = lease_effects(state, tick)
    {n2, state} = lease_transactions(state)
    {n1 + n2, state}
  rescue
    e ->
      Logger.error("pg_txn worker: #{Exception.message(e)}")
      {0, state}
  end

  defp housekeeping(state) do
    now = System.monotonic_time(:millisecond)

    state =
      if due?(state.last_seen, now, @seen_every) do
        info = %{runtime: "elixir", node: to_string(node()), pid: System.pid(), defines: Map.keys(Registry.definitions(state.repo))}
        SQL.all(state.repo, "SELECT txn.worker_seen($1::text::uuid, $2::text::jsonb)", [state.owner, Jason.encode!(info)])

        # the owners of transactions this node drove (or drives): their
        # compensations are this node's to run
        for owner <- Local.compensation_owners(state.repo), owner != state.owner do
          SQL.all(state.repo, "SELECT txn.worker_seen($1::text::uuid, $2::text::jsonb)",
            [owner, Jason.encode!(%{runtime: "elixir", node: to_string(node()), worker: state.owner})])
        end

        %{state | last_seen: now}
      else
        state
      end

    if due?(state.last_maintain, now, state.opts[:maintain_ms]) do
      SQL.all(state.repo, "SELECT txn.abandon_expired(), txn.expire_effect_leases(), txn.fail_lost_effects()")
      forget(state)
      {%{state | last_maintain: now}, true}
    else
      {state, false}
    end
  end

  # forgets functions whose effect is finished, or never became visible (its
  # transaction rolled back) within :forget_invisible_ms; one whose
  # transaction is still open (e.g. a spawn in a long Repo.transaction) is kept
  defp forget(state) do
    case Local.older_than(state.repo, state.opts[:forget_after_ms]) do
      [] ->
        :ok

      old ->
        seen =
          state.repo
          |> SQL.all("""
          SELECT coalesce(compensates, id)::text AS id, status IN ('pending', 'retry_wait', 'running') AS live FROM txn.effects
           WHERE (id = ANY ($1::text[]::uuid[]) OR compensates = ANY ($1::text[]::uuid[])) AND kind <> 'call'
          """, [Enum.map(old, &elem(&1, 0))])
          |> Map.new(&{&1["id"], &1["live"]})

        forget =
          for {id, age} <- old, Map.get(seen, id) == false or (not Map.has_key?(seen, id) and age > state.opts[:forget_invisible_ms]),
              do: id

        Local.delete(state.repo, forget)
    end
  end

  defp due?(nil, _now, _every), do: true
  defp due?(last, now, every), do: now - last > every

  defp free(state), do: state.opts[:concurrency] - map_size(state.tasks)

  # on the maintenance tick too, even with no functions: effects of this
  # node it no longer has a function for fail as EffectLost instead of
  # waiting forever
  defp lease_effects(state, tick) do
    owners =
      cond do
        Local.any?(state.repo) -> Enum.uniq([state.owner | Local.compensation_owners(state.repo)])
        tick -> [state.owner]
        true -> []
      end

    Enum.reduce(owners, {0, state}, fn owner, {n, st} -> lease_effects(st, owner, n) end)
  end

  defp lease_effects(state, owner, n) do
    if free(state) > 0 do
      rows =
        SQL.all(state.repo,
          "SELECT id::text AS id, kind, name, input, attempt, generation, tx_id::text AS tx_id, delivery, compensates::text AS compensates FROM txn.lease_effects($1::text::uuid, $2, $3)",
          [owner, free(state), state.opts[:lease_ms]])

      state = Enum.reduce(rows, state, fn e, st -> start_task(st, {:effect, e["name"], e["id"]}, fn -> run_local(st, owner, e) end) end)
      {n + length(rows), state}
    else
      {n, state}
    end
  end

  defp lease_transactions(state) do
    definitions = Registry.definitions(state.repo)

    if map_size(definitions) > 0 and free(state) > 0 do
      # a fresh owner per lease: each driving process has its own lease, so
      # one that dies is resumed (by this node too) once its lease expires
      owner = Ecto.UUID.generate()

      rows =
        SQL.all(state.repo,
          "SELECT id::text AS id, name, input, created_at, isolation FROM txn.lease_transactions($1::text::uuid, $2::text[], $3, $4)",
          [owner, Map.keys(definitions), free(state), state.opts[:lease_ms]])

      state =
        Enum.reduce(rows, state, fn t, st ->
          start_task(st, {:transaction, t["name"], t["id"]}, fn -> drive(st, owner, definitions[t["name"]], t) end)
        end)

      {length(rows), state}
    else
      {0, state}
    end
  end

  defp start_task(state, what, fun) do
    %Task{ref: ref} = Task.async(fun)
    %{state | tasks: Map.put(state.tasks, ref, what)}
  end

  # ------------------------------------------------------------------ work

  defp run_local(state, owner, e) do
    repo = state.repo
    key = if e["kind"] == "compensation", do: e["compensates"], else: e["id"]

    case Local.get(repo, key) do
      nil ->
        SQL.all(repo, "SELECT txn.fail_effect($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb, false)",
          [e["id"], owner, e["generation"],
           Jason.encode!(%{name: "EffectLost", message: "this node no longer has the effect's function"})])

      local ->
        lease_ms = state.opts[:lease_ms]

        heartbeat =
          Loop.every(max(1000, div(lease_ms, 3)), fn ->
            SQL.all(repo, "SELECT txn.heartbeat_effect($1::text::uuid, $2::text::uuid, $3, $4)", [e["id"], owner, e["generation"], lease_ms])
          end)

        try do
          # a spawn made before its transaction had a durable row has tx_id NULL
          ctx = %{effect_id: e["id"], idempotency_key: e["id"], attempt: e["attempt"], tx_id: e["tx_id"] || local.tx_id}
          o = Call.call(fn -> local.fun.(ctx, e["input"]) end, run: nil, retry: e["delivery"] == "at-least-once", timeout_ms: local.timeout_ms || 30_000)

          status =
            if o.ok do
              SQL.all(repo, "SELECT txn.complete_effect($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb)",
                [e["id"], owner, e["generation"], Jason.encode!(o.result)])

              "succeeded"
            else
              SQL.value(repo, "SELECT txn.fail_effect($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb, $5, $6)",
                [e["id"], owner, e["generation"], Jason.encode!(o.error), o.retryable, o.retry_after_ms])
            end

          if status not in ["retry_wait", "stale"], do: Local.delete(repo, [key])
        after
          PgTxn.Proc.shutdown(heartbeat)
        end
    end
  end

  defp drive(state, owner, fun, t) do
    input = DJSON.from_tagged(t["input"])
    Loop.drive(state.repo, t["id"], fn tx -> fun.(tx, input) end,
      started_at: t["created_at"], named: true, owner: owner, isolation: t["isolation"])
  rescue
    FencedError -> :fenced
    e -> Logger.error("pg_txn worker: transaction #{t["name"]} #{t["id"]} failed: #{Exception.message(e)}")
  end
end
