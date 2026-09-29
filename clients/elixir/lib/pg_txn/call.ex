defmodule PgTxn.Call do
  @moduledoc false
  # Calls an effect, spawned or compensation function once, in its own process, with a
  # timeout, and classifies the outcome for txn.effect_done / txn.fail_effect.
  alias PgTxn.{DJSON, PermanentError, RetryableError}

  # pg_txn transactions live seconds to minutes: a longer retry delay fails the effect instead
  @max_retry_after_ms 15 * 60_000

  @type outcome :: %{
          ok: boolean,
          result: term,
          error: map | nil,
          retryable: boolean,
          retry_after_ms: non_neg_integer | nil
        }

  @doc """
  Calls `thunk` (which applies the user's function). It returns
  `{:ok, result}`, `{:error, reason}` or a plain result, or raises. `result`
  in the outcome is already tagged (a durable value).

  Options: `:timeout_ms`, `:retry`, `:run` (the ref of the run whose tx must
  not be used inside the function, or nil).
  """
  @spec call((-> term), keyword) :: outcome
  def call(thunk, opts) do
    timeout = opts[:timeout_ms] || 30_000
    retry = Keyword.get(opts, :retry, false)
    proc =
      PgTxn.Proc.async(fn ->
        Process.put(in_effect_key(), Keyword.get(opts, :run))
        invoke(thunk)
      end)

    reply =
      case PgTxn.Proc.yield(proc, timeout) do
        {:ok, reply} ->
          reply

        {:exit, reason} ->
          {:error, %{"name" => "Exit", "message" => inspect(reason)}}

        nil ->
          PgTxn.Proc.shutdown(proc)
          {:error, %{"name" => "EffectTimeout", "message" => "effect timed out after #{timeout} ms"}}
      end

    classify(reply, retry)
  end

  @doc "Set in the process running an effect or spawned function: the ref of the run whose tx it must not use."
  def in_effect_key, do: {__MODULE__, :in_effect}

  defp invoke(thunk) do
    case thunk.() do
      {:ok, result} -> tag(result)
      {:error, reason} -> {:error, reason}
      result -> tag(result)
    end
  rescue
    e -> {:error, e}
  catch
    kind, reason -> {:error, %{"name" => to_string(kind), "message" => inspect(reason)}}
  end

  # a result that cannot be stored is a deterministic failure: never retried
  # (retrying would repeat the side effect for nothing)
  defp tag(result) do
    {:ok, DJSON.to_tagged(result)}
  rescue
    e in ArgumentError ->
      {:error, %PermanentError{message: "the effect's result cannot be stored: #{Exception.message(e)}"}}
  end

  defp classify({:ok, tagged}, _), do: %{ok: true, result: tagged, error: nil, retryable: false, retry_after_ms: nil}

  defp classify({:error, reason}, retry) do
    retryable = retry and not match?(%PermanentError{}, reason)
    error = error_json(reason)

    case reason do
      %RetryableError{retry_after_ms: ms} when is_number(ms) and ms <= @max_retry_after_ms ->
        %{ok: false, result: nil, error: error, retryable: retryable, retry_after_ms: max(0, round(ms))}

      %RetryableError{retry_after_ms: ms} when not is_nil(ms) ->
        note = " (retry_after_ms #{inspect(ms)} is beyond the #{@max_retry_after_ms} ms a transaction may wait)"
        %{ok: false, result: nil, error: Map.update!(error, "message", &(&1 <> note)), retryable: false, retry_after_ms: nil}

      _ ->
        %{ok: false, result: nil, error: error, retryable: retryable, retry_after_ms: nil}
    end
  end

  @doc "`max_attempts` of a `:retry` option (`nil`/`false`: 1, `true`: 5, `[attempts: n]`)."
  def attempts(retry) when retry in [nil, false], do: 1
  def attempts(true), do: 5
  def attempts(retry) when is_list(retry), do: max(1, Keyword.get(retry, :attempts, 5))

  @doc "`delivery` of a `:retry` option."
  def delivery(retry) when retry in [nil, false], do: "at-most-once"
  def delivery(_retry), do: "at-least-once"

  @doc "Checks the options of an effect or a spawn."
  def validate!(opts) do
    case opts[:retry] do
      r when is_boolean(r) or is_nil(r) -> :ok
      [attempts: n] when is_integer(n) and n >= 1 and n <= 1000 -> :ok
      r -> raise ArgumentError, "pg_txn: :retry must be true, false or [attempts: 1..1000], got #{inspect(r)}"
    end

    case opts[:timeout_ms] do
      nil -> :ok
      ms when is_integer(ms) and ms > 0 -> :ok
      ms -> raise ArgumentError, "pg_txn: :timeout_ms must be a positive integer (or nil: 30000), got #{inspect(ms)}"
    end
  end

  @doc """
  An error as stored in `txn.effects.error` / `txn.transactions.error`:
  always storable (anything that is not falls back to `inspect/1`).
  """
  @spec error_json(term) :: map
  def error_json(reason) do
    reason |> raw_error() |> Map.new(fn {k, v} -> {text(k), storable(v)} end)
  rescue
    _ -> %{"name" => "Error", "message" => text(safe_inspect(reason))}
  end

  defp raw_error(%{"name" => _, "message" => _} = e), do: e

  defp raw_error(e) when is_exception(e) do
    name = e.__struct__ |> Module.split() |> Enum.join(".")

    message =
      try do
        Exception.message(e)
      rescue
        _ -> safe_inspect(e)
      end

    base = %{"name" => name, "message" => message}

    case e do
      %Postgrex.Error{postgres: %{pg_code: code}} -> Map.put(base, "code", code)
      _ -> base
    end
  end

  defp raw_error(reason) when is_binary(reason), do: %{"name" => "Error", "message" => reason}
  defp raw_error(reason), do: %{"name" => "Error", "message" => safe_inspect(reason)}

  defp storable(v) when is_binary(v), do: text(v)
  defp storable(v) when is_number(v) or is_boolean(v) or is_nil(v), do: v
  defp storable(v), do: text(safe_inspect(v))

  # jsonb text: valid UTF-8 without U+0000
  defp text(s) when is_binary(s) do
    if String.valid?(s), do: String.replace(s, <<0>>, "\\u0000"), else: text(inspect(s))
  end

  defp text(a) when is_atom(a), do: a |> Atom.to_string() |> text()
  defp text(v), do: v |> safe_inspect() |> text()

  defp safe_inspect(v) do
    inspect(v)
  rescue
    _ -> "#<uninspectable>"
  end
end
