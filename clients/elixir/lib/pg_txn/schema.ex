defmodule PgTxn.Schema do
  @moduledoc """
  The `txn` schema this client needs (`priv/schema.sql`, generated from
  `extension/sql/pg_txn--1.0.sql`). It is installed on first use unless the
  Repo config says `pg_txn: [install: false]`; the role only needs to own
  the database (no superuser).
  """
  alias PgTxn.SQL

  @path Path.expand("../../priv/schema.sql", __DIR__)
  @external_resource @path
  @sql File.read!(@path)
  @version @sql |> then(&Regex.run(~r/INSERT INTO txn\.meta VALUES \((\d+)\)/, &1)) |> Enum.at(1) |> String.to_integer()

  @doc "The schema version this client is built for."
  def version, do: @version

  @doc """
  Installs (if allowed and missing) and checks the schema; once per Repo.
  Runs on its own connection, so it is safe inside a Repo transaction.
  """
  @spec ensure!(module) :: :ok
  def ensure!(repo) do
    if :persistent_term.get({__MODULE__, repo}, false) do
      :ok
    else
      result =
        PgTxn.Proc.async(fn ->
          try do
            install!(repo)
          catch
            kind, reason -> {:raise, kind, reason, __STACKTRACE__}
          end
        end)
        |> PgTxn.Proc.await()

      case result do
        {:ok, {:raise, kind, reason, stack}} -> :erlang.raise(kind, reason, stack)
        {:ok, _} -> :ok
        {:exit, reason} -> exit(reason)
      end

      :persistent_term.put({__MODULE__, repo}, true)
    end
  end

  defp install!(repo) do
    unless present?(repo) do
      unless PgTxn.Config.get(repo, :install) do
        raise "pg_txn: the txn schema is not installed (run extension/sql/pg_txn--1.0.sql, or allow install)"
      end

      {:ok, _} =
        repo.transaction(fn ->
          SQL.all(repo, "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pg_txn install'))")
          # no parameters, several statements: the simple query protocol
          unless present?(repo), do: Ecto.Adapters.SQL.query!(repo, @sql, [], query_type: :text)
        end)
    end

    case SQL.value(repo, "SELECT version FROM txn.meta") do
      @version -> :ok
      v -> raise "pg_txn: schema version #{inspect(v)} in the database, this client needs #{@version}"
    end
  end

  # a catalog scan, not to_regclass: after waiting on the install lock the
  # backend's syscache may still say "does not exist"
  defp present?(repo) do
    SQL.value(repo, """
    SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'txn' AND c.relname = 'meta')
    """)
  end
end
