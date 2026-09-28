defmodule PgTxn.Multi do
  @moduledoc """
  pg_txn steps for `Ecto.Multi`. They run inside the Multi's transaction and
  take effect only if it commits:

      Ecto.Multi.new()
      |> Ecto.Multi.insert(:order, order_changeset)
      |> PgTxn.Multi.spawn(:receipt, fn %{order: o} -> Mailer.send_receipt(o.id) end)
      |> Repo.transaction()

  (A `PgTxn.transaction/3` cannot run inside a Multi: its runs commit and
  roll back on their own.)
  """
  alias Ecto.Multi

  @doc """
  Calls `fun` iff the Multi commits (see `PgTxn.spawn/3`) with the Multi's
  changes: `fn changes -> ... end` or `fn changes, ctx -> ... end`. Options
  as in `PgTxn.spawn/3` (e.g. `retry: true`). The step's value is the
  effect id.
  """
  @spec spawn(Multi.t(), Multi.name(), (map -> term) | (map, map -> term), keyword) :: Multi.t()
  def spawn(multi, step, fun, opts \\ []) when is_function(fun, 1) or is_function(fun, 2) do
    Multi.run(multi, step, fn repo, changes ->
      call = if is_function(fun, 1), do: fn -> fun.(changes) end, else: fn ctx -> fun.(changes, ctx) end
      {:ok, PgTxn.spawn(repo, call, opts)}
    end)
  end

  @doc "A named transaction queued iff the Multi commits (see `PgTxn.enqueue/4`); the step's value is its id."
  @spec enqueue(Multi.t(), Multi.name(), String.t(), term | (map -> term), keyword) :: Multi.t()
  def enqueue(multi, step, name, input, opts \\ []) do
    Multi.run(multi, step, fn repo, changes -> {:ok, PgTxn.enqueue(repo, name, resolve(input, changes), opts)} end)
  end

  defp resolve(f, changes) when is_function(f, 1), do: f.(changes)
  defp resolve(v, _changes), do: v
end
