defmodule PgTxn.Call do
  @moduledoc false
  # Calls an effect, spawned or compensation function once, in its own process, with a
  # timeout, and classifies the outcome for txn.effect_done / txn.fail_effect.
  alias PgTxn.{DJSON, PermanentError, RetryableError}

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
  """
  @spec call((-> term), keyword) :: outcome
  def call(thunk, opts) do
    timeout = Keyword.get(opts, :timeout_ms, 30_000)
    retry = Keyword.get(opts, :retry, false)
    task = Task.async(fn -> invoke(thunk) end)

    reply =
      case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
        {:ok, reply} -> reply
        {:exit, reason} -> {:error, %{"name" => "Exit", "message" => inspect(reason)}}
        nil -> {:error, %{"name" => "EffectTimeout", "message" => "effect timed out after #{timeout} ms"}}
      end

    classify(reply, retry)
  end

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
      {:error, %PermanentError{message: "the effect's result is not a durable value: #{Exception.message(e)}"}}
  end

  defp classify({:ok, tagged}, _), do: %{ok: true, result: tagged, error: nil, retryable: false, retry_after_ms: nil}

  defp classify({:error, reason}, retry) do
    retryable = retry and not match?(%PermanentError{}, reason)

    retry_after = with %RetryableError{retry_after_ms: ms} <- reason, do: ms, else: (_ -> nil)
    %{ok: false, result: nil, error: error_json(reason), retryable: retryable, retry_after_ms: retry_after}
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
  end

  @doc "An error as stored in `txn.effects.error` / `txn.transactions.error`."
  @spec error_json(term) :: map
  def error_json(%{"name" => _, "message" => _} = e), do: e

  def error_json(e) when is_exception(e) do
    name = e.__struct__ |> Module.split() |> Enum.join(".")
    base = %{"name" => name, "message" => Exception.message(e)}

    case e do
      %Postgrex.Error{postgres: %{pg_code: code}} -> Map.put(base, "code", code)
      _ -> base
    end
  end

  def error_json(reason) when is_binary(reason), do: %{"name" => "Error", "message" => reason}
  def error_json(reason), do: %{"name" => "Error", "message" => inspect(reason)}
end
