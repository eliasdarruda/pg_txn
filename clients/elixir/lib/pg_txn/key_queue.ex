defmodule PgTxn.KeyQueue do
  @moduledoc false
  # Transactions of this node with a key in common run in order, one at a
  # time: only the first in line claims the keys in the database, so a
  # hand-off wakes one caller instead of every waiter polling txn.start.
  # One per Repo, started with its worker (and stopped after it, so callers
  # waiting here during the worker's shutdown can leave).
  use GenServer

  def child_spec(repo), do: %{id: {__MODULE__, repo}, start: {__MODULE__, :start_link, [repo]}}

  def start_link(repo), do: GenServer.start_link(__MODULE__, nil, name: name(repo))

  def name(repo), do: Module.concat(repo, PgTxnKeyQueue)

  @doc """
  Runs `fun` once every transaction of this node ahead of this one on any of
  `keys` is done. While waiting, `check` is called every 250 ms (it raises
  to give up: timeout, shutdown).
  """
  def run(repo, keys, check, fun) do
    case Process.whereis(name(repo)) do
      nil ->
        fun.()

      server ->
        ref = make_ref()
        :ok = GenServer.call(server, {:enter, keys |> Enum.uniq() |> Enum.sort(), ref}, :infinity)

        try do
          await_turn(ref, check)
          fun.()
        after
          try do
            GenServer.call(server, {:leave, ref}, :infinity)
          catch
            :exit, _ -> :ok
          end

          # a turn sent before the leave was handled (messages between two
          # processes arrive in order, so it is here by now)
          receive do
            {__MODULE__, ^ref, :turn} -> :ok
          after
            0 -> :ok
          end
        end
    end
  end

  defp await_turn(ref, check) do
    receive do
      {__MODULE__, ^ref, :turn} -> :ok
    after
      250 ->
        check.()
        await_turn(ref, check)
    end
  end

  # ------------------------------------------------------------------ server

  @impl true
  def init(nil), do: {:ok, %{queues: %{}, entries: %{}}}

  @impl true
  def handle_call({:enter, keys, ref}, {pid, _}, state) do
    mon = Process.monitor(pid)
    queues = Enum.reduce(keys, state.queues, fn k, qs -> Map.update(qs, k, :queue.from_list([ref]), &:queue.in(ref, &1)) end)
    state = %{state | queues: queues, entries: Map.put(state.entries, ref, %{pid: pid, keys: keys, mon: mon, granted: false})}
    {:reply, :ok, grant(state, ref)}
  end

  def handle_call({:leave, ref}, _from, state), do: {:reply, :ok, remove(state, ref)}

  @impl true
  def handle_info({:DOWN, mon, :process, _, _}, state) do
    case Enum.find(state.entries, fn {_ref, e} -> e.mon == mon end) do
      {ref, _} -> {:noreply, remove(state, ref)}
      nil -> {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp remove(state, ref) do
    case Map.pop(state.entries, ref) do
      {nil, _} ->
        state

      {entry, entries} ->
        Process.demonitor(entry.mon, [:flush])

        queues =
          Enum.reduce(entry.keys, state.queues, fn k, qs ->
            q = :queue.delete(ref, Map.fetch!(qs, k))
            if :queue.is_empty(q), do: Map.delete(qs, k), else: Map.put(qs, k, q)
          end)

        state = %{state | queues: queues, entries: entries}

        entry.keys
        |> Enum.flat_map(fn k -> with {:ok, q} <- Map.fetch(queues, k), {:value, head} <- :queue.peek(q), do: [head], else: (_ -> []) end)
        |> Enum.uniq()
        |> Enum.reduce(state, &grant(&2, &1))
    end
  end

  # the entry's turn when it heads the queue of every key it has
  defp grant(state, ref) do
    entry = Map.fetch!(state.entries, ref)

    if not entry.granted and Enum.all?(entry.keys, &(:queue.peek(Map.fetch!(state.queues, &1)) == {:value, ref})) do
      send(entry.pid, {__MODULE__, ref, :turn})
      %{state | entries: Map.put(state.entries, ref, %{entry | granted: true})}
    else
      state
    end
  end
end
