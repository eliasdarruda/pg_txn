# a fresh database state each run: the SDK installs the txn schema itself
# (as the non-superuser owner of the database)
url = Application.fetch_env!(:pg_txn, PgTxn.TestRepo)[:url]
{:ok, conn} = Postgrex.start_link(Ecto.Repo.Supervisor.parse_url(url))
Postgrex.query!(conn, "DROP SCHEMA IF EXISTS txn CASCADE", [])
Postgrex.query!(conn, "DROP TABLE IF EXISTS orders", [])

Postgrex.query!(conn, """
CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  amount integer NOT NULL DEFAULT 0,
  status text NOT NULL DEFAULT 'new',
  n integer NOT NULL DEFAULT 0
)
""", [])

GenServer.stop(conn)

# the Repo's child spec also starts its PgTxn.Worker (use PgTxn.Repo)
{:ok, _} = Supervisor.start_link([PgTxn.TestRepo], strategy: :one_for_one)
ExUnit.start()
