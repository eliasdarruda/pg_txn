defmodule PgTxn.Tx do
  @moduledoc """
  One run of a pg_txn transaction, passed to the transaction function.

  `:id` is the logical transaction id (the same in every run) and `:repo` the
  Repo whose transaction the run is in. The run's bookkeeping (effect
  sequence, needed and consumed effects, claimed rows) lives in the process
  dictionary of the process running it: call `PgTxn.effect/3`, `PgTxn.own/3`
  and friends from that process, like any Repo call inside a transaction.
  """
  alias PgTxn.{DJSON, EffectFailedError, Local, NeedEffect, SQL}

  @enforce_keys [:repo, :id, :owner, :started_at, :ref]
  defstruct [:repo, :id, :owner, :started_at, :ref, claimed: false]

  @type t :: %__MODULE__{
          repo: module,
          id: String.t(),
          owner: String.t(),
          started_at: DateTime.t(),
          ref: reference,
          claimed: boolean
        }

  @doc false
  def new(repo, id, owner, started_at, claimed) do
    tx = %__MODULE__{repo: repo, id: id, owner: owner, started_at: started_at, ref: make_ref(), claimed: claimed}
    Process.put(key(tx), %{seq: 0, needs: [], consumed: [], claims: [], effect_called: false, spawned: false, spawn_ids: [], uuids: 0})
    tx
  end

  @doc false
  def state(tx), do: state!(tx)

  @doc false
  # the run's final state (and forgets it)
  def close(tx), do: Process.delete(key(tx))

  @doc false
  def effect(tx, fun, opts) do
    state = state!(tx)
    seq = state.seq
    put(tx, %{state | seq: seq + 1, effect_called: true})
    name = to_string(Keyword.get(opts, :name, "effect"))
    tagged = DJSON.to_tagged(Keyword.get(opts, :key))

    row =
      SQL.one(tx.repo, "SELECT effect_id::text AS effect_id, status, result, error FROM txn.effect_lookup($1::text::uuid, $2, $3, $4::text::jsonb)",
        [tx.id, seq, name, Jason.encode!(tagged)])

    case row do
      %{"status" => "succeeded", "effect_id" => id, "result" => result} ->
        update(tx, &%{&1 | consumed: [id | &1.consumed]})
        if opts[:compensate], do: Local.compensation(tx.repo, id, opts, tx.owner, tx.id)
        DJSON.from_tagged(result)

      %{"status" => "failed", "effect_id" => id, "error" => error} ->
        update(tx, &%{&1 | consumed: [id | &1.consumed]})
        raise EffectFailedError, effect: name, error: error

      _missing ->
        need = %{seq: seq, name: name, tagged: tagged, fun: fun, opts: opts}
        update(tx, &%{&1 | needs: &1.needs ++ [need]})
        raise NeedEffect
    end
  end

  @doc false
  def own(tx, table, key) do
    if state!(tx).effect_called do
      raise ArgumentError,
            "pg_txn: own(#{inspect(table)}) after an effect: own the rows a transaction depends on before its first effect"
    end

    row =
      SQL.one(tx.repo, ~s{SELECT "row", version, key, rel::bigint AS rel FROM txn.own($1::text::regclass, $2::text::jsonb)},
        [table_name(table), Jason.encode!(key)])

    case row do
      nil ->
        nil

      %{"row" => data, "version" => version, "key" => k, "rel" => rel} ->
        unless tx.claimed, do: update(tx, &%{&1 | claims: &1.claims ++ [%{rel: rel, key: k, version: version}]})
        data
    end
  end

  @doc false
  def mark_spawned(tx), do: update(tx, &%{&1 | spawned: true})

  @doc false
  def mark_spawned(tx, id), do: update(tx, &%{&1 | spawned: true, spawn_ids: [id | &1.spawn_ids]})

  @doc false
  def uuid(tx) do
    n = state!(tx).uuids
    update(tx, &%{&1 | uuids: n + 1})
    stable_uuid("#{tx.id}:#{n}")
  end

  @doc false
  # sha256(seed) as a UUID with version nibble 5 and variant bits 10, like
  # the TypeScript client's stableUuid
  def stable_uuid(seed) do
    <<a::binary-size(12), _::binary-size(1), b::binary-size(3), v::binary-size(1), c::binary-size(15), _::binary>> =
      :crypto.hash(:sha256, seed) |> Base.encode16(case: :lower)

    variant = Integer.to_string(Bitwise.bor(Bitwise.band(String.to_integer(v, 16), 0x3), 0x8), 16) |> String.downcase()
    h = a <> "5" <> b <> variant <> c
    <<p1::binary-size(8), p2::binary-size(4), p3::binary-size(4), p4::binary-size(4), p5::binary-size(12)>> = h
    Enum.join([p1, p2, p3, p4, p5], "-")
  end

  defp table_name(table) when is_atom(table) do
    if Code.ensure_loaded?(table) and function_exported?(table, :__schema__, 1) do
      case table.__schema__(:prefix) do
        nil -> ~s("#{table.__schema__(:source)}")
        prefix -> ~s("#{prefix}"."#{table.__schema__(:source)}")
      end
    else
      Atom.to_string(table)
    end
  end

  defp table_name(table) when is_binary(table), do: table

  defp key(%__MODULE__{ref: ref}), do: {__MODULE__, ref}

  defp state!(tx) do
    Process.get(key(tx)) ||
      raise ArgumentError,
            "pg_txn: this transaction is not running in this process (use tx only inside its transaction function, in the process running it)"
  end

  defp put(tx, state), do: Process.put(key(tx), state)
  defp update(tx, fun), do: put(tx, fun.(state!(tx)))
end
