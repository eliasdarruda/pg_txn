defmodule PgTxn.Proc do
  @moduledoc false
  # Like Task.async/yield/shutdown, without leaving messages in the caller:
  # the process is linked to the caller (it dies with it) but unlinks itself
  # before it ends, so a caller that traps exits gets no {:EXIT, pid, _}.
  # The function must not raise (callers rescue inside it).

  defstruct [:pid, :mref, :tag]

  def async(fun) do
    parent = self()
    tag = make_ref()

    {pid, mref} =
      spawn_monitor(fn ->
        Process.link(parent)
        result = fun.()
        Process.unlink(parent)
        send(parent, {tag, result})
      end)

    %__MODULE__{pid: pid, mref: mref, tag: tag}
  end

  @doc "`{:ok, result}`, `{:exit, reason}`, or nil after `timeout` (the process keeps running)."
  def yield(%__MODULE__{pid: pid, mref: mref, tag: tag}, timeout) do
    receive do
      {^tag, result} ->
        Process.demonitor(mref, [:flush])
        {:ok, result}

      {:DOWN, ^mref, :process, _, reason} ->
        flush_exit(pid)
        {:exit, reason}
    after
      timeout -> nil
    end
  end

  def await(p), do: yield(p, :infinity)

  @doc "Kills the process (if still running) and drops whatever it sent."
  def shutdown(%__MODULE__{pid: pid, mref: mref, tag: tag}) do
    Process.unlink(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^mref, :process, _, _} -> :ok
    end

    receive do
      {^tag, _} -> :ok
    after
      0 -> :ok
    end

    flush_exit(pid)
    :ok
  end

  defp flush_exit(pid) do
    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      0 -> :ok
    end
  end
end
