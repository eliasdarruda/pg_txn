defmodule PgTxn.RetryableError do
  @moduledoc """
  Raise (or return as `{:error, %PgTxn.RetryableError{}}`) from an effect
  with `retry:` on to retry it after `:retry_after_ms` (default: backoff).
  Without `retry:` it fails the effect like any other error.
  """
  defexception message: "retryable error", retry_after_ms: nil
end

defmodule PgTxn.PermanentError do
  @moduledoc """
  Raise (or return as `{:error, %PgTxn.PermanentError{}}`) from an effect
  with `retry:` on to fail it now, without more attempts.
  """
  defexception message: "permanent error"
end

defmodule PgTxn.EffectFailedError do
  @moduledoc """
  Raised inside the transaction function when an effect failed for good (no
  more retries). `:error` is the recorded error (`%{"name" => .., "message" => ..}`).
  """
  defexception [:effect, :error]

  @impl true
  def message(%{effect: effect, error: error}) do
    "effect #{effect} failed: #{error["name"]}: #{error["message"]}"
  end
end

defmodule PgTxn.FencedError do
  @moduledoc "Another process drives this transaction now (this one lost its lease)."
  defexception [:tx_id]

  @impl true
  def message(%{tx_id: id}), do: "transaction #{id} is now driven by another process"
end

defmodule PgTxn.KeyTimeoutError do
  @moduledoc """
  The transaction's key stayed held by another transaction (`:holder`) for
  longer than `:key_wait_ms`.
  """
  defexception [:key, :holder, :waited_ms]

  @impl true
  def message(%{key: key, holder: holder, waited_ms: ms}) do
    "key #{key} is still held by transaction #{holder} after #{ms} ms"
  end
end

defmodule PgTxn.TransactionFailedError do
  @moduledoc "A named or enqueued transaction ended without committing (see `PgTxn.wait/3`)."
  defexception [:tx_id, :status, :error]

  @impl true
  def message(%{tx_id: id, status: status, error: error}) do
    detail = if is_map(error), do: ": #{error["name"] || "Error"}: #{error["message"]}", else: ""
    "transaction #{id} #{status}#{detail}"
  end
end

defmodule PgTxn.NeedEffect do
  @moduledoc false
  # Aborts a run that reached an effect with no recorded result. Internal:
  # the loop rolls the run back even if user code rescues this.
  defexception message: "pg_txn: this run needs an effect result (internal; do not rescue)"
end
