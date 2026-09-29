defmodule PgTxn.AdversarialR3Test do
  # Round 3 of the adversarial review: PgTxn.Proc (linked effect processes),
  # callers dying mid-effect, option validation, cross-client key texts.
  # `BUG:` tests fail on the current code; `holds:` tests pass.
  use ExUnit.Case, async: false
  alias PgTxn.TestRepo, as: Repo

  defp scalar(sql, params) do
    %{rows: [[v]]} = Repo.query!(sql, params)
    v
  end

  defp wait_until(fun, ms) do
    cond do
      fun.() -> :ok
      ms <= 0 -> flunk("condition not met in time")
      true -> Process.sleep(20); wait_until(fun, ms - 20)
    end
  end

  # ------------------------------------------------------------------ PgTxn.Proc and dying callers

  # Hypothesis: the effect runs in a PgTxn.Proc child linked to the caller
  # (call.ex). When the caller is killed mid-effect the child dies with it
  # (no orphaned process keeps calling the provider), nothing is recorded
  # for the effect, the heartbeat process (also linked) dies, and the inline
  # transaction is abandoned after lease + grace, releasing its key.
  test "holds: a caller killed mid-effect takes the effect process down; the transaction is abandoned and its key released" do
    parent = self()
    key = "r3-dying-caller-#{System.unique_integer([:positive])}"
    id = Ecto.UUID.generate()

    {:ok, caller} =
      Task.start(fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn ->
            send(parent, {:in_effect, self()})
            Process.sleep(:infinity)
          end)
        end, key: key, id: id, lease_ms: 1_000)
      end)

    assert_receive {:in_effect, effect_pid}, 5_000
    assert scalar("SELECT count(*) FROM txn.keys WHERE key = $1", [key]) == 1
    Process.exit(caller, :kill)
    wait_until(fn -> not Process.alive?(effect_pid) end, 1_000)
    # the maintenance sweep of any worker abandons it once its lease lapsed (1 s) plus the 5 s grace
    start_supervised!(PgTxn.SweepRepo)
    wait_until(fn -> scalar("SELECT status FROM txn.transactions WHERE id = $1::text::uuid", [id]) == "abandoned" end, 15_000)
    assert scalar("SELECT count(*) FROM txn.keys WHERE key = $1", [key]) == 0
    assert scalar("SELECT status FROM txn.effects WHERE tx_id = $1::text::uuid", [id]) == "orphaned"
  end

  test "holds: an effect that times out has its process killed (no orphan keeps running) and the effect fails as EffectTimeout" do
    parent = self()

    {:ok, out} =
      PgTxn.transaction(Repo, fn tx ->
        try do
          PgTxn.effect(tx, fn ->
            send(parent, {:effect_pid, self()})
            Process.sleep(:infinity)
          end, timeout_ms: 200)
        rescue
          e in PgTxn.EffectFailedError -> e.error["name"]
        end
      end)

    assert out == "EffectTimeout"
    assert_receive {:effect_pid, pid}, 1_000
    refute Process.alive?(pid)
  end

  # Hypothesis: Loop.step reads `Keyword.get(opts, :timeout_ms, 30_000)`, so
  # an explicit `timeout_ms: nil` (e.g. a config default that was not set)
  # reaches `receive ... after nil` in PgTxn.Proc.yield/2, which is a
  # :timeout_value error: the caller crashes with an ErlangError (recorded as
  # the transaction's failure), Proc.shutdown never runs, and the effect
  # process is left running. Call.validate!/1 checks :retry but not
  # :timeout_ms. Expected: an ArgumentError up front (or nil = the default).
  test "timeout_ms: nil is refused up front (or means the default) instead of crashing the caller and orphaning the effect process" do
    parent = self()

    result =
      try do
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn ->
            send(parent, {:effect_pid, self()})
            Process.sleep(300)
            :ok
          end, timeout_ms: nil)
        end)
      rescue
        e -> {:raised, e}
      end

    case result do
      # the effect's :ok is stored as JSON, so it comes back as "ok"
      {:ok, ok} when ok in [:ok, "ok"] ->
        :ok

      {:raised, %ArgumentError{}} ->
        :ok

      other ->
        pid = receive do: ({:effect_pid, p} -> p), after: (0 -> nil)
        Process.sleep(100)
        alive = pid && Process.alive?(pid)
        flunk("timeout_ms: nil gave #{inspect(other)}; effect process still alive: #{inspect(alive)}")
    end
  end

  # Hypothesis: retry: [attempts: 2000] is refused by Call.validate! (the
  # TypeScript client lets the CHECK constraint fail the transaction).
  test "holds: retry: [attempts: 2000] raises an ArgumentError before anything runs" do
    assert_raise ArgumentError, ~r/attempts/, fn ->
      PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> 1 end, retry: [attempts: 2000]) end)
    end
  end

  # Task.async semantics, documented here for the report: a process linked
  # from inside an effect that crashes takes the Proc child down, and the
  # caller with it (it is linked to the child). The TypeScript client turns
  # any error inside the effect into a failed effect. In Elixir the caller
  # (a request process, a GenServer) dies and the transaction is recorded only
  # by abandon_expired later. Not a bug per se (BEAM semantics), a doc gap.
  test "holds (doc gap): a crash of a process linked from inside an effect kills the caller (Task-like), nothing is recorded until abandon" do
    parent = self()
    id = Ecto.UUID.generate()

    {:ok, caller} =
      Task.start(fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn ->
            spawn_link(fn -> raise "helper crashed" end)
            Process.sleep(500)
            :ok
          end)
        end, id: id, lease_ms: 1_000)
        send(parent, :caller_returned)
      end)

    ref = Process.monitor(caller)
    assert_receive {:DOWN, ^ref, :process, _, reason}, 5_000
    refute reason == :normal, "the caller returned normally: #{inspect(reason)}"
    refute_received :caller_returned
    assert scalar("SELECT status FROM txn.transactions WHERE id = $1::text::uuid", [id]) == "running"
    Repo.query!("UPDATE txn.transactions SET status = 'failed', finished_at = now() WHERE id = $1::text::uuid", [id])
  end

  test "holds: a caller that dies mid-effect in a named transaction: the worker resumes it; at-most-once -> AmbiguousEffectOutcome, at-least-once -> re-run" do
    parent = self()
    calls = :counters.new(1, [])

    PgTxn.define(Repo, "r3-resume-once", fn tx, _ ->
      PgTxn.effect(tx, fn ->
        :counters.add(calls, 1, 1)
        send(parent, :in_effect)
        Process.sleep(2_000)
        "paid"
      end, name: "charge")
    end)

    PgTxn.define(Repo, "r3-resume-retry", fn tx, _ ->
      PgTxn.effect(tx, fn ->
        :counters.add(calls, 1, 1)
        send(parent, :in_effect)
        if :counters.get(calls, 1) == 1, do: Process.sleep(2_000)
        "paid"
      end, name: "charge", retry: true)
    end)

    for {name, expect} <- [{"r3-resume-once", :ambiguous}, {"r3-resume-retry", :rerun}] do
      :counters.put(calls, 1, 0)
      id = Ecto.UUID.generate()
      {:ok, caller} = Task.start(fn -> PgTxn.run(Repo, name, %{}, id: id, lease_ms: 1_000) end)
      assert_receive :in_effect, 5_000
      Process.exit(caller, :kill)
      wait_until(fn -> scalar("SELECT status FROM txn.transactions WHERE id = $1::text::uuid", [id]) != "running" end, 15_000)
      %{rows: [[status, error]]} = Repo.query!("SELECT status, error->>'name' FROM txn.transactions WHERE id = $1::text::uuid", [id])
      eff = scalar("SELECT error->>'name' FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'call'", [id])

      case expect do
        :ambiguous ->
          assert status == "failed"
          assert error == "PgTxn.EffectFailedError"
          assert eff == "AmbiguousEffectOutcome"
          assert :counters.get(calls, 1) == 1

        :rerun ->
          assert status == "committed"
          assert :counters.get(calls, 1) == 2
      end
    end
  end

  # ------------------------------------------------------------------ keys across clients

  # The TypeScript client refuses numbers and booleans as keys ("a key must be
  # a string, an array or an object"); this client encodes any durable value,
  # so `key: 42` and `key: "42"` are one key here and a TypeError there. A
  # cross-client difference the READMEs do not mention.
  test "holds (documented difference): number and boolean keys are accepted here, refused by the TypeScript client" do
    assert PgTxn.Loop.keys(key: 42) == ["42"]
    assert PgTxn.Loop.keys(key: "42") == ["42"]
    assert PgTxn.Loop.keys(key: true) == ["true"]
    assert PgTxn.Loop.keys(key: 1.5) == ["1.5"]
  end

  # ------------------------------------------------------------------ the stale-success class (shared SQL root cause)

  # Hypothesis (same root cause as tests/adversarial/r3-stale-success.test.ts):
  # a process that is late (its lease lapsed while an at-most-once effect was
  # running) sees its successful outcome recorded as 'stale' by
  # txn.effect_done and dropped: the effect is orphaned with result NULL, so
  # no compensation is scheduled although this node has the function, and
  # txn.doctor is silent.
  test "a successful at-most-once effect of a late process is compensated (or at least kept) rather than dropped" do
    parent = self()
    id = Ecto.UUID.generate()
    key = "r3-late-#{System.unique_integer([:positive])}"

    task =
      Task.async(fn ->
        try do
          PgTxn.transaction(Repo, fn tx ->
            PgTxn.effect(tx, fn ->
              send(parent, :in_effect)
              Process.sleep(1_200)
              "pay_late"
            end, name: "charge", compensate: fn p -> send(parent, {:refunded, p}) end)

            :committed
          end, key: key, id: id, lease_ms: 1_500)
        rescue
          e -> {:raised, e.__struct__}
        end
      end)

    assert_receive :in_effect, 5_000
    # the process is late: its lease lapsed and another replica's sweep ran, in one transaction
    Repo.transaction(fn ->
      Repo.query!("UPDATE txn.leases SET lease_until = now() - interval '10 seconds' WHERE tx_id = $1::text::uuid", [id])
      Repo.query!("SELECT txn.abandon_expired()")
    end)

    out = Task.await(task, 10_000)
    assert out == {:raised, PgTxn.FencedError}, "the late process must not commit: #{inspect(out)}"
    Process.sleep(1_500)
    %{rows: [[status, result, err]]} = Repo.query!("SELECT status, result, error->>'name' FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'call'", [id])
    comps = scalar("SELECT count(*) FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation'", [id])
    refunded = receive do: ({:refunded, p} -> p), after: (0 -> nil)

    assert result != nil or comps > 0 or refunded != nil,
           "the charge succeeded in this node but the effect is #{status}/#{err} with result #{inspect(result)}, #{comps} compensation(s), refunded: #{inspect(refunded)}"
  end
end
