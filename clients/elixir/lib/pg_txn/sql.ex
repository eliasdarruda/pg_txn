defmodule PgTxn.SQL do
  @moduledoc false
  # One statement through the Repo: inside the calling process's transaction
  # when there is one, on its own (autocommit) otherwise. Uuids travel as
  # text (`$1::text::uuid`, `x::text`), jsonb as Jason-encoded terms.

  @doc "Runs a statement and returns its rows as maps (column name => value)."
  def all(repo, sql, params \\ [], opts \\ []) do
    %{columns: columns, rows: rows} = Ecto.Adapters.SQL.query!(repo, sql, params, opts)
    Enum.map(rows, fn row -> columns |> Enum.zip(row) |> Map.new() end)
  end

  @doc "The first row as a map, or nil."
  def one(repo, sql, params \\ [], opts \\ []), do: repo |> all(sql, params, opts) |> List.first()

  @doc "The first column of the first row."
  def value(repo, sql, params \\ [], opts \\ []) do
    %{rows: [[v | _] | _]} = Ecto.Adapters.SQL.query!(repo, sql, params, opts)
    v
  end

  @doc "SQLSTATE and DETAIL of a PostgreSQL error, or nil."
  def pg_error(%Postgrex.Error{postgres: %{pg_code: code} = pg}), do: {code, pg[:detail] || ""}
  def pg_error(_), do: nil
end
