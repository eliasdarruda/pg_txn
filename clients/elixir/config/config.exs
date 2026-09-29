import Config

# a vanilla PostgreSQL with a non-superuser role owning the database; the
# SDK installs the txn schema itself
config :pg_txn, PgTxn.TestRepo,
  url: System.get_env("PG_TXN_ECTO_URL", "ecto://app:app@localhost:55461/app"),
  pool_size: 10,
  pg_txn: [poll_ms: 50]

config :pg_txn, ecto_repos: [PgTxn.TestRepo]
config :logger, level: :warning

config :pg_txn, PgTxn.DrainRepo,
  url: System.get_env("PG_TXN_ECTO_URL", "ecto://app:app@localhost:55461/app"),
  pool_size: 2,
  pg_txn: [drain_ms: 5_000, listen: false]

config :pg_txn, PgTxn.ImpatientRepo,
  url: System.get_env("PG_TXN_ECTO_URL", "ecto://app:app@localhost:55461/app"),
  pool_size: 2,
  pg_txn: [listen: false]

config :pg_txn, PgTxn.SweepRepo,
  url: System.get_env("PG_TXN_ECTO_URL", "ecto://app:app@localhost:55461/app"),
  pool_size: 3,
  pg_txn: [listen: false]
