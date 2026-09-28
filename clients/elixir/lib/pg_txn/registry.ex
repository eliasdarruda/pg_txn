defmodule PgTxn.Registry do
  @moduledoc false
  # Definitions (named transactions) per Repo, in :persistent_term:
  # registered once at startup, read on every worker poll.

  def put_definition(repo, name, fun) do
    :global.trans({__MODULE__, repo}, fn ->
      :persistent_term.put({__MODULE__, repo}, Map.put(definitions(repo), to_string(name), fun))
    end, [node()])

    :ok
  end

  def definitions(repo), do: :persistent_term.get({__MODULE__, repo}, %{})

  def definition(repo, name), do: Map.get(definitions(repo), name)
end
