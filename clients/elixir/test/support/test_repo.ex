defmodule PgTxn.TestRepo do
  use Ecto.Repo, otp_app: :pg_txn, adapter: Ecto.Adapters.Postgres
  use PgTxn.Repo
end

defmodule PgTxn.Test.Order do
  @moduledoc false
  use Ecto.Schema

  schema "orders" do
    field :amount, :integer, default: 0
    field :status, :string, default: "new"
    field :n, :integer, default: 0
  end
end

defmodule PgTxn.DrainRepo do
  @moduledoc false
  # started and stopped by a test: its worker drains on shutdown
  use Ecto.Repo, otp_app: :pg_txn, adapter: Ecto.Adapters.Postgres
  use PgTxn.Repo, poll_ms: 20
end
