defmodule PgTxn.Repo do
  @moduledoc """
  Adds pg_txn to an Ecto Repo:

      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
        use PgTxn.Repo
      end

  The Repo's child spec then also starts a `PgTxn.Worker` for it, so the
  application's existing `children = [MyApp.Repo, ...]` is the whole setup.
  The worker runs this node's spawned effects and compensations, stops
  (draining work in progress) before the Repo does, and installs the `txn`
  schema when it starts.

  Options given here are defaults for the Repo config under `:pg_txn` (see
  `PgTxn.Config`), which wins:

      use PgTxn.Repo, concurrency: 32
      config :my_app, MyApp.Repo, pg_txn: [poll_ms: 50]
  """

  defmacro __using__(opts) do
    quote do
      @doc false
      def __pg_txn_options__, do: unquote(opts)

      defoverridable child_spec: 1

      def child_spec(opts) do
        PgTxn.Repo.__child_spec__(__MODULE__, super(opts))
      end
    end
  end

  @doc false
  def __child_spec__(repo, repo_spec) do
    %{
      id: repo,
      type: :supervisor,
      # a Repo restart restarts the worker; a worker restart leaves the Repo alone
      start: {Supervisor, :start_link, [[repo_spec, {PgTxn.Worker, repo: repo}], [strategy: :rest_for_one]]}
    }
  end
end
