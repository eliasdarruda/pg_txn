defmodule PgTxn.AdversarialTest do
  # Round 2 of the adversarial review: the Elixir client, which round 1 did
  # not look at. `BUG:` tests fail on the current code; `holds:` tests passed.
  use ExUnit.Case, async: false
  alias PgTxn.TestRepo, as: Repo

  defp scalar(sql, params \\ []) do
    %{rows: [[v]]} = Repo.query!(sql, params)
    v
  end

  defp wait_until(fun, ms \\ 5_000) do
    cond do
      fun.() -> :ok
      ms <= 0 -> flunk("condition not met in time")
      true -> Process.sleep(20); wait_until(fun, ms - 20)
    end
  end

  # ------------------------------------------------------------------ process hygiene

  # Hypothesis: PgTxn.Loop.every, PgTxn.Call.call and Loop.execute_all use
  # Task.async in the CALLER's process (loop.ex, call.ex). A task that ends
  # normally or is brutal-killed sends the caller an {:EXIT, pid, reason}
  # message when the caller traps exits (a GenServer with trap_exit, e.g. one
  # that manages resources), and Task.shutdown/yield flush only the reply
  # and :DOWN. Those messages reach handle_info/2: a GenServer without a
  # catch-all clause crashes; one with it logs noise on every transaction.
  test "PgTxn.transaction leaves no stray messages in a caller that traps exits" do
    Process.flag(:trap_exit, true)
    {:messages, before} = Process.info(self(), :messages)

    {:ok, 3} =
      PgTxn.transaction(Repo, fn tx ->
        a = PgTxn.effect(tx, fn -> 1 end, name: "a")
        b = PgTxn.effect(tx, fn -> 2 end, name: "b")
        a + b
      end)

    Process.sleep(200)
    {:messages, after_} = Process.info(self(), :messages)
    Process.flag(:trap_exit, false)
    assert after_ -- before == [], "stray messages in the caller's mailbox: #{inspect(after_ -- before)}"
  end

  # Hypothesis: PgTxn.Local keeps the count of public calls in an ETS table
  # owned by the worker (local.ex :calls). When the worker crashes and is
  # restarted (rest_for_one), the table is recreated with {:calls, 0} while
  # calls are in flight; each of them decrements on exit, so the count goes
  # negative and Local.busy/1 never reaches 0 again: the worker's terminate/2
  # then always waits the full :drain_ms (worker.ex idle?/1).
  test "after a worker restart, finished calls leave the busy count at 0" do
    parent = self()

    t =
      Task.async(fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn ->
            send(parent, {:in_effect, self()})
            receive do: (:go -> :ok)
            1
          end)
        end)
      end)

    assert_receive {:in_effect, effect_pid}, 5_000
    worker = Process.whereis(PgTxn.Worker.name(Repo))
    ref = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}, 5_000
    wait_until(fn -> pid = Process.whereis(PgTxn.Worker.name(Repo)); pid != nil and pid != worker end)
    wait_until(fn -> :ets.whereis(PgTxn.Local.table(Repo)) != :undefined end)
    send(effect_pid, :go)
    assert {:ok, 1} = Task.await(t, 10_000)
    assert PgTxn.Local.busy(Repo) == 0
  end

  # ------------------------------------------------------------------ values

  test "holds: a float with an integral value comes back as an integer (JSON has one number type; undocumented)" do
    {:ok, v} = PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> %{"x" => 1.0, "y" => 2.5} end) end)
    assert v == %{"x" => 1, "y" => 2.5}
    assert PgTxn.DJSON.decode!(PgTxn.DJSON.encode!(1.0)) === 1
  end

  test "a map with an atom key and the same string key is rejected, not silently merged" do
    assert_raise ArgumentError, fn -> PgTxn.DJSON.encode!(%{"a" => 2, a: 1}) end
  end

  test "holds: a Decimal, a tuple and a NaiveDateTime are refused as durable values (the effect fails for good, not retried)" do
    for bad <- [Decimal.new("1.5"), {:a, 1}, ~N[2026-01-01 00:00:00]] do
      {:ok, out} =
        PgTxn.transaction(Repo, fn tx ->
          try do
            PgTxn.effect(tx, fn -> bad end, retry: true)
          rescue
            e in PgTxn.EffectFailedError -> {:failed, e.error["name"]}
          end
        end)

      assert {:failed, "PgTxn.PermanentError"} = out
    end
  end

  # Hypothesis: Loop.output/2 stores "null" when an inline transaction's
  # result is not a durable value (a tuple, a struct), and the caller gets the
  # real value. A retried call with the same :id takes the :existing path and
  # returns the stored output: {:ok, nil}, which the caller cannot tell from a
  # transaction that returned nil.
  # Fixed as in the TypeScript client: with an :id (or named), an output
  # that cannot be stored fails the transaction. Since round 3 an id that
  # failed with no effect result recorded runs again (txn.start).
  test "an idempotent re-call of a transaction whose output is not durable does not return {:ok, nil}" do
    id = Ecto.UUID.generate()
    assert_raise ArgumentError, ~r/output cannot be stored/, fn -> PgTxn.transaction(Repo, fn _tx -> {:paid, 7} end, id: id) end
    second = PgTxn.transaction(Repo, fn _tx -> "second" end, id: id)
    assert second == {:ok, "second"}
    refute match?({:ok, nil}, second), "the retried call got #{inspect(second)}"
  end

  # ------------------------------------------------------------------ keys

  test "holds: key text of tuples, lists and strings is the TypeScript client's" do
    assert PgTxn.Loop.keys(key: {"order", 42}) == ["[\"order\",42]"]
    assert PgTxn.Loop.keys(key: ["order", 42]) == ["[\"order\",42]"]
    assert PgTxn.Loop.keys(key: "plain") == ["plain"]
  end

  # Hypothesis: Loop.key_text encodes a map key with Jason, i.e. in the map's
  # internal order: for atom keys that is atom-creation order (OTP 26+), which
  # differs between nodes and from the same map with string keys; the
  # TypeScript client uses JSON.stringify (property order). DJSON sorts keys
  # for values, but keys of transactions are not canonical in either client.
  test "a map key has one canonical text (sorted keys, like DJSON values)" do
    assert PgTxn.Loop.keys(key: %{b: 1, a: 2}) == ["{\"a\":2,\"b\":1}"]
    assert PgTxn.Loop.keys(key: %{b: 1, a: 2}) == PgTxn.Loop.keys(key: %{"a" => 2, "b" => 1})
  end

  # changed in round 2: nil is refused (txn.start refuses a NULL key too)
  test "holds: key: nil is refused; an atom key is quoted" do
    assert_raise ArgumentError, ~r/cannot be nil/, fn -> PgTxn.Loop.keys(key: nil) end
    assert_raise ArgumentError, ~r/not a valid key/, fn -> PgTxn.Loop.keys(keys: [["a", self()]]) end
    assert PgTxn.Loop.keys(key: :order) == ["\"order\""]
  end

  # ------------------------------------------------------------------ isolation and tx use

  test "holds: :isolation sets the level of every run and refuses anything else (the TypeScript client and txn.enqueue do not validate it)" do
    {:ok, level} =
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn -> :ok end)
        scalar("SHOW transaction_isolation")
      end, isolation: :serializable)

    assert level == "serializable"
    assert_raise ArgumentError, fn -> PgTxn.transaction(Repo, fn _ -> :ok end, isolation: "serializable; select 1") end
    assert_raise ArgumentError, fn -> PgTxn.enqueue(Repo, "x", %{}, isolation: "read committed; select 1") end
  end

  test "holds: tx used from another process (a Task inside the function) raises instead of running outside the run" do
    parent = self()

    {:ok, :raised} =
      PgTxn.transaction(Repo, fn tx ->
        spawn(fn ->
          send(parent, try do
            PgTxn.effect(tx, fn -> 1 end)
          rescue
            e in ArgumentError -> {:raised, Exception.message(e)}
          end)
        end)

        receive do
          {:raised, msg} -> assert msg =~ "not running in this process"; :raised
          other -> other
        after
          5_000 -> :timeout
        end
      end)
  end

  # ------------------------------------------------------------------ Multi and local functions

  test "holds: a Multi.spawn in a Multi that fails is not run, and its function is forgotten after :forget_invisible_ms" do
    start_supervised!(PgTxn.SweepRepo)
    parent = self()

    {:error, :boom, _, _} =
      Ecto.Multi.new()
      |> PgTxn.Multi.spawn(:hello, fn _changes -> send(parent, :spawned) end)
      |> Ecto.Multi.run(:boom, fn _, _ -> {:error, :boom} end)
      |> PgTxn.SweepRepo.transaction()

    refute_receive :spawned, 500
    assert PgTxn.Local.any?(PgTxn.SweepRepo)
    wait_until(fn -> not PgTxn.Local.any?(PgTxn.SweepRepo) end, 3_000)
  end

  # ------------------------------------------------------------------ cross-client values

  test "holds: an input enqueued in the TypeScript encoding is decoded here, and the output is stored in it" do
    PgTxn.define(Repo, "r2-xclient", fn _tx, input -> input end)

    ts_input = ~s({"n":{"$bigint":"9007199254740993"},"d":{"$date":"2026-01-01T00:00:00.000Z"},"b":{"$bytes":"AQI="},"u":{"$undefined":true},"e":{"$escape":{"$date":"x"}}})
    id = scalar("SELECT txn.enqueue('r2-xclient', $1::text::jsonb)::text", [ts_input])

    assert {:ok, out} = PgTxn.wait(Repo, id, 10_000)
    assert out["n"] === 9_007_199_254_740_993
    assert out["d"] == ~U[2026-01-01 00:00:00.000Z]
    assert out["b"] == {:bytes, <<1, 2>>}
    assert out["u"] == nil
    assert out["e"] == %{"$date" => "x"}
    stored = scalar("SELECT output::text FROM txn.transactions WHERE id = $1::text::uuid", [id])
    assert stored =~ ~s("n": {"$bigint": "9007199254740993"})
    assert stored =~ ~s("b": {"$bytes": "AQI="})
    assert stored =~ ~s("e": {"$escape": {"$date": "x"}})
  end
end
