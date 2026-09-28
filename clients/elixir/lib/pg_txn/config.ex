defmodule PgTxn.Config do
  @moduledoc """
  pg_txn options of a Repo: the defaults, overridden by the options given to
  `use PgTxn.Repo`, overridden by the Repo config under `:pg_txn`:

      config :my_app, MyApp.Repo, pg_txn: [concurrency: 32]

    * `:install` - install the `txn` schema when missing (default `true`)
    * `:lease_ms` - lease of a transaction or effect this node drives (default 30000)
    * `:owner_wait_ms` - longest wait for a row owned by another transaction (default 300000)
    * `:concurrency` - spawned effects and background transactions run at once (default 16)
    * `:poll_ms` - idle poll interval of the worker (default 250)
    * `:drain_ms` - how long the worker waits for work in progress on shutdown (default 30000)
    * `:listen` - wake the worker with `LISTEN txn_effects` (default `true`)
  """

  @defaults [
    install: true,
    lease_ms: 30_000,
    owner_wait_ms: 300_000,
    concurrency: 16,
    poll_ms: 250,
    drain_ms: 30_000,
    listen: true
  ]

  @doc "The options of `repo` (read once, then cached)."
  @spec get(module) :: keyword
  def get(repo) do
    case :persistent_term.get({__MODULE__, repo}, nil) do
      nil ->
        opts = load(repo)
        :persistent_term.put({__MODULE__, repo}, opts)
        opts

      opts ->
        opts
    end
  end

  @doc "One option of `repo`."
  def get(repo, key), do: Keyword.fetch!(get(repo), key)

  @doc false
  def load(repo) do
    use_opts =
      if Code.ensure_loaded?(repo) and function_exported?(repo, :__pg_txn_options__, 0),
        do: repo.__pg_txn_options__(),
        else: []

    repo_opts = if function_exported?(repo, :config, 0), do: repo.config()[:pg_txn] || [], else: []
    @defaults |> Keyword.merge(use_opts) |> Keyword.merge(repo_opts)
  end

  @doc """
  The identity of `repo`'s worker on this node (one uuid per Repo per
  running VM): in `txn.worker_seen`, and the `local_owner` of spawned
  effects, whose functions only this worker has. Each process driving a
  transaction has an owner of its own.
  """
  @spec owner(module) :: String.t()
  def owner(repo) do
    key = {__MODULE__, :owner, repo}

    case :persistent_term.get(key, nil) do
      nil ->
        :global.trans({__MODULE__, repo}, fn ->
          case :persistent_term.get(key, nil) do
            nil ->
              id = Ecto.UUID.generate()
              :persistent_term.put(key, id)
              id

            id ->
              id
          end
        end, [node()])

      id ->
        id
    end
  end
end
