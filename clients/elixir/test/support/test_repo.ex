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

defmodule PgTxn.ImpatientRepo do
  @moduledoc false
  # started by a test: waits 200 ms at most for a key
  use Ecto.Repo, otp_app: :pg_txn, adapter: Ecto.Adapters.Postgres
  use PgTxn.Repo, key_wait_ms: 200
end

defmodule PgTxn.SweepRepo do
  @moduledoc false
  # started by tests: frequent maintenance, short function lifetimes
  use Ecto.Repo, otp_app: :pg_txn, adapter: Ecto.Adapters.Postgres
  use PgTxn.Repo, poll_ms: 20, maintain_ms: 50, forget_after_ms: 100, forget_invisible_ms: 1_000
end
