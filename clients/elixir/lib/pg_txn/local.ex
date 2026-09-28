defmodule PgTxn.Local do
  @moduledoc false
  # The functions of a Repo's spawned effects (by effect id) and
  # compensations (by the id of the effect they undo), run by its worker,
  # plus the transactions this node drives now. An ETS table owned by the
  # Repo's PgTxn.Worker: the rows of txn.effects point at these functions
  # through local_owner, so they only live as long as the worker.
  #
  #   {id, kind, owner, tx_id, since_ms, fun, timeout_ms}   kind: :spawn | :compensation
  #   {{:driving, tx_id}, pid}

  @doc false
  def table(repo), do: Module.concat(repo, PgTxnLocal)

  @doc false
  def new(repo) do
    :ets.new(table(repo), [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
  end

  @doc "Registers `fun.(ctx, input)` under effect id `id` (the worker's owner `owner` leases it)."
  def put(repo, id, kind, owner, tx_id, fun, timeout_ms) do
    :ets.insert(table(repo), {id, kind, owner, tx_id, now(), fun, timeout_ms})
    :ok
  rescue
    ArgumentError ->
      reraise ArgumentError,
              "pg_txn: no PgTxn.Worker is running for #{inspect(repo)} (use PgTxn.Repo, or start {PgTxn.Worker, repo: #{inspect(repo)}})",
              __STACKTRACE__
  end

  @doc "Registers the `:compensate` function of effect options `opts` for call effect `effect_id`."
  def compensation(repo, effect_id, opts, owner, tx_id) do
    undo = Keyword.fetch!(opts, :compensate)

    fun = fn ctx, input ->
      result = PgTxn.DJSON.from_tagged(input["result"])
      if is_function(undo, 1), do: undo.(result), else: undo.(result, ctx)
    end

    put(repo, effect_id, :compensation, owner, tx_id, fun, opts[:timeout_ms])
  end

  def get(repo, id) do
    case :ets.lookup(table(repo), id) do
      [{^id, kind, owner, tx_id, _since, fun, timeout_ms}] -> %{kind: kind, owner: owner, tx_id: tx_id, fun: fun, timeout_ms: timeout_ms}
      _ -> nil
    end
  rescue
    ArgumentError -> nil
  end

  def delete(repo, ids) do
    t = table(repo)
    Enum.each(ids, &:ets.delete(t, &1))
  rescue
    ArgumentError -> :ok
  end

  @doc "Whether any function is registered."
  def any?(repo), do: :ets.select_count(table(repo), [{{:_, :_, :_, :_, :_, :_, :_}, [], [true]}]) > 0

  @doc "The owners of registered compensations (the processes that drove their transactions)."
  def compensation_owners(repo) do
    table(repo) |> :ets.match({:_, :compensation, :"$1", :_, :_, :_, :_}) |> List.flatten() |> Enum.uniq()
  end

  @doc "The ids of the compensations registered for transaction `tx_id`."
  def compensations(repo, tx_id) do
    table(repo) |> :ets.match({:"$1", :compensation, :_, tx_id, :_, :_, :_}) |> List.flatten()
  rescue
    ArgumentError -> []
  end

  @doc "Ids registered more than `age_ms` ago, except those of transactions driven now."
  def older_than(repo, age_ms) do
    t = table(repo)
    cutoff = now() - age_ms

    t
    |> :ets.select([{{:"$1", :_, :_, :"$2", :"$3", :_, :_}, [{:<, :"$3", cutoff}], [{{:"$1", :"$2"}}]}])
    |> Enum.reject(fn {_id, tx_id} -> tx_id && driving?(t, tx_id) end)
    |> Enum.map(&elem(&1, 0))
  end

  def driving(repo, tx_id) do
    :ets.insert(table(repo), {{:driving, tx_id}, self()})
  rescue
    ArgumentError -> :ok
  end

  def driven(repo, tx_id) do
    :ets.delete(table(repo), {:driving, tx_id})
  rescue
    ArgumentError -> :ok
  end

  defp driving?(t, tx_id) do
    case :ets.lookup(t, {:driving, tx_id}) do
      [{_, pid}] -> Process.alive?(pid)
      [] -> false
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
