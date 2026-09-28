defmodule PgTxnTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias PgTxn.TestRepo, as: Repo
  alias Ecto.Multi

  # ------------------------------------------------------------------ helpers

  # an in-memory "external API": counts calls per name
  defp counter, do: start_supervised!({Agent, fn -> %{} end}, id: make_ref())
  defp bump(c, k), do: Agent.get_and_update(c, fn m -> n = Map.get(m, k, 0) + 1; {n, Map.put(m, k, n)} end)
  defp count(c, k), do: Agent.get(c, &Map.get(&1, k, 0))

  defp insert_order(attrs \\ []) do
    Repo.insert!(struct(PgTxn.Test.Order, attrs)).id
  end

  defp order(id), do: Repo.get!(PgTxn.Test.Order, id)

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

  defp tx_status(id), do: scalar("SELECT status FROM txn.transactions WHERE id = $1::text::uuid", [id])

  # ------------------------------------------------------------------ the loop

  test "installs the schema as a non-superuser and checks its version" do
    refute scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
    assert scalar("SELECT version FROM txn.meta") == PgTxn.Schema.version()
    # the worker reports itself and is woken by NOTIFY
    assert scalar("SELECT count(*) FROM txn.workers") >= 1
    assert scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'LISTEN%txn_effects%'") == 1
  end

  test "memoization: N effects run the function N+1 times and each effect once" do
    c = counter()
    id = insert_order(amount: 5)

    {:ok, {total, uuids, now}} =
      PgTxn.transaction(Repo, fn tx ->
        bump(c, :runs)
        u = PgTxn.uuid(tx)
        Agent.update(c, &Map.update(&1, :uuids, [u], fn l -> [u | l] end))
        a = PgTxn.effect(tx, fn -> bump(c, :a); {:ok, 1} end, name: "a")
        b = PgTxn.effect(tx, fn ctx -> bump(c, :b); {:ok, %{"x" => a + 1, "attempt" => ctx.attempt}} end, name: "b")
        d = PgTxn.effect(tx, fn -> bump(c, :d); b["x"] + 1 end, name: "d")
        Repo.query!("UPDATE orders SET n = n + 1 WHERE id = $1", [id])
        {a + b["x"] + d, Agent.get(c, & &1.uuids), PgTxn.now(tx)}
      end)

    assert total == 6
    assert count(c, :runs) == 4
    assert {count(c, :a), count(c, :b), count(c, :d)} == {1, 1, 1}
    # the writes of the committing run only; uuid/now are stable across runs
    assert order(id).n == 1
    assert [u, u, u, u] = uuids
    assert u =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    assert %DateTime{} = now
  end

  test "uuid/1 is the TypeScript client's stableUuid" do
    # sha256("tx:0") hex with h[12] = "5" and h[16] = (h[16] & 3) | 8
    hex = :crypto.hash(:sha256, "tx:0") |> Base.encode16(case: :lower)
    <<a::binary-size(12), _, b::binary-size(3), v, c::binary-size(15), _::binary>> = hex
    variant = Integer.to_string(Bitwise.bor(Bitwise.band(String.to_integer(<<v>>, 16), 3), 8), 16) |> String.downcase()
    <<p1::binary-size(8), p2::binary-size(4), p3::binary-size(4), p4::binary-size(4), p5::binary>> = a <> "5" <> b <> variant <> c
    assert PgTxn.Tx.stable_uuid("tx:0") == Enum.join([p1, p2, p3, p4, p5], "-")
    assert PgTxn.Tx.stable_uuid("tx:0") =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
  end

  test "during an effect nothing is held: other writers go through, no idle transaction" do
    id = insert_order()
    test = self()

    {:ok, "ok"} =
      PgTxn.transaction(Repo, fn tx ->
        Repo.get!(PgTxn.Test.Order, id)

        PgTxn.effect(tx, fn ->
          Repo.query!("UPDATE orders SET status = 'touched' WHERE id = $1", [id])

          idle =
            scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state LIKE 'idle in transaction%'")

          send(test, {:idle, idle})
          :ok
        end)
      end)

    assert_received {:idle, 0}
    assert order(id).status == "touched"
  end

  test "the order was cancelled during the charge: the re-run returns early and the charge is compensated" do
    test = self()
    id = insert_order()

    assert {:ok, "skipped"} =
             PgTxn.transaction(Repo, fn tx ->
               if order(id).status != "new" do
                 "skipped"
               else
                 p =
                   PgTxn.effect(tx, fn ->
                     Repo.query!("UPDATE orders SET status = 'cancelled' WHERE id = $1", [id])
                     %{"id" => "pay_1"}
                   end, name: "charge", compensate: fn p -> send(test, {:refund, p["id"]}); :ok end)

                 Repo.query!("UPDATE orders SET status = 'paid' WHERE id = $1", [id])
                 p["id"]
               end
             end, key: {"order", id})

    assert order(id).status == "cancelled"
    assert_receive {:refund, "pay_1"}, 5_000
  end

  test "deps: a re-run with different deps orphans the old effect and compensates it" do
    test = self()
    c = counter()
    id = insert_order(amount: 10)
    tx_id = Ecto.UUID.generate()

    {:ok, charged} =
      PgTxn.transaction(Repo, fn tx ->
        %{rows: [[amount]]} = Repo.query!("SELECT amount FROM orders WHERE id = $1", [id])

        PgTxn.effect(tx, fn ->
          # the row changes between runs (nothing is locked)
          if bump(c, :charge) == 1, do: Repo.query!("UPDATE orders SET amount = 20 WHERE id = $1", [id])
          {:ok, %{"charged" => amount}}
        end, name: "charge", deps: %{amount: amount}, compensate: fn result, ctx -> send(test, {:refund, result, ctx}); :ok end)
      end, id: tx_id)

    assert charged == %{"charged" => 20}
    assert count(c, :charge) == 2

    assert_receive {:refund, %{"charged" => 10}, %{tx_id: ^tx_id, attempt: 1}}, 5_000
    refute_receive {:refund, _, _}, 300
    assert scalar("SELECT count(*) FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'call' AND status = 'orphaned'", [tx_id]) == 1

    %{rows: [[name, input, status]]} =
      Repo.query!("SELECT name, input, status FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation'", [tx_id])

    assert name == "undo charge"
    assert %{"effect" => "charge", "input" => %{"amount" => 10}} = input
    assert status == "succeeded"
  end

  test "a failed transaction compensates the effects that ran, once each" do
    test = self()
    c = counter()
    tx_id = Ecto.UUID.generate()

    assert_raise RuntimeError, "out of stock", fn ->
      PgTxn.transaction(Repo, fn tx ->
        a = PgTxn.effect(tx, fn -> bump(c, :reserve); %{"hold" => 1} end, name: "reserve", compensate: fn r -> send(test, {:release, r}); :ok end)
        # a second effect: the first one's result is memoized in the re-run
        PgTxn.effect(tx, fn -> a["hold"] + 1 end, name: "plain")
        PgTxn.effect(tx, fn -> "ok" end, name: "unused", compensate: fn _ -> send(test, :unused) end, deps: 1)
        raise "out of stock"
      end, id: tx_id)
    end

    assert count(c, :reserve) == 1
    assert_receive {:release, %{"hold" => 1}}, 5_000
    assert_receive :unused, 5_000
    refute_receive {:release, _}, 300
    assert scalar("SELECT count(*) FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation' AND status = 'succeeded'", [tx_id]) == 2
    # the functions are forgotten once they ran
    assert PgTxn.Local.compensations(Repo, tx_id) == []
  end

  test "a committed transaction keeps no compensation functions" do
    tx_id = Ecto.UUID.generate()

    {:ok, 1} =
      PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> 1 end, compensate: fn _ -> :ok end) end, id: tx_id)

    assert PgTxn.Local.compensations(Repo, tx_id) == []
    assert scalar("SELECT count(*) FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation'", [tx_id]) == 0
  end

  test "a permanent effect failure raises EffectFailedError and fails the transaction" do
    c = counter()
    tx_id = Ecto.UUID.generate()

    error =
      assert_raise PgTxn.EffectFailedError, fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn -> bump(c, :boom); raise PgTxn.PermanentError, "card declined" end, name: "boom")
        end, id: tx_id)
      end

    assert error.effect == "boom"
    assert error.error["message"] == "card declined"
    assert count(c, :boom) == 1
    assert tx_status(tx_id) == "failed"
    assert scalar("SELECT error->>'name' FROM txn.transactions WHERE id = $1::text::uuid", [tx_id]) == "PgTxn.EffectFailedError"
  end

  test "{:error, %PermanentError{}} fails at once too" do
    assert_raise PgTxn.EffectFailedError, ~r/nope/, fn ->
      PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> {:error, %PgTxn.PermanentError{message: "nope"}} end) end)
    end
  end

  test "a rescued effect failure lets the transaction commit" do
    assert {:ok, :fallback} =
             PgTxn.transaction(Repo, fn tx ->
               try do
                 PgTxn.effect(tx, fn -> {:error, %PgTxn.PermanentError{}} end)
               rescue
                 PgTxn.EffectFailedError -> :fallback
               end
             end)
  end

  test "retryable errors are retried with backoff until success" do
    c = counter()
    tx_id = Ecto.UUID.generate()
    t0 = System.monotonic_time(:millisecond)

    assert {:ok, 3} =
             PgTxn.transaction(Repo, fn tx ->
               PgTxn.effect(tx, fn ->
                 n = bump(c, :flaky)
                 if n < 3, do: raise("flaky #{n}"), else: {:ok, n}
               end, name: "flaky", retry: true)
             end, id: tx_id)

    # default backoff: 200 ms, then 400 ms
    assert System.monotonic_time(:millisecond) - t0 >= 600

    outcomes =
      Repo.query!(
        "SELECT a.outcome FROM txn.effect_attempts a JOIN txn.effects e ON e.id = a.effect_id WHERE e.tx_id = $1::text::uuid ORDER BY a.id",
        [tx_id]
      ).rows

    assert outcomes == [["retry"], ["retry"], ["succeeded"]]
  end

  test "by default a failing effect is called once, even with a RetryableError" do
    c = counter()
    tx_id = Ecto.UUID.generate()

    assert_raise PgTxn.EffectFailedError, fn ->
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn -> bump(c, :once); raise PgTxn.RetryableError, retry_after_ms: 10 end, name: "once")
      end, id: tx_id)
    end

    assert count(c, :once) == 1

    assert Repo.query!("SELECT max_attempts, delivery FROM txn.effects WHERE tx_id = $1::text::uuid", [tx_id]).rows ==
             [[1, "at-most-once"]]
  end

  test "with retry, a permanent error stops further attempts; retry_after_ms sets the delay" do
    c = counter()

    assert_raise PgTxn.EffectFailedError, ~r/declined/, fn ->
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn ->
          if bump(c, :card) == 1, do: raise(PgTxn.RetryableError, retry_after_ms: 20), else: raise(PgTxn.PermanentError, "declined")
        end, name: "card", retry: [attempts: 10])
      end)
    end

    assert count(c, :card) == 2
  end

  test "Repo.rollback/1 in the function returns {:error, reason} and fails the transaction" do
    tx_id = Ecto.UUID.generate()

    assert {:error, :out_of_stock} =
             PgTxn.transaction(Repo, fn tx ->
               PgTxn.effect(tx, fn -> 1 end)
               Repo.rollback(:out_of_stock)
             end, id: tx_id)

    assert tx_status(tx_id) == "failed"
  end

  test "an exception from the function is re-raised and recorded" do
    tx_id = Ecto.UUID.generate()

    assert_raise RuntimeError, "bad", fn ->
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn -> 1 end)
        raise "bad"
      end, id: tx_id)
    end

    assert tx_status(tx_id) == "failed"
  end

  test "cannot run inside another Repo transaction" do
    assert_raise ArgumentError, ~r/inside another Repo transaction/, fn ->
      Repo.transaction(fn -> PgTxn.transaction(Repo, fn _ -> :ok end) end)
    end
  end

  # ------------------------------------------------------------------ spawn

  test "spawn runs iff the surrounding transaction commits" do
    test = self()
    receipt = fn label -> fn -> send(test, {:receipt, label}); :ok end end

    {:error, :nope} =
      Repo.transaction(fn ->
        send(self(), {:spawned, PgTxn.spawn(Repo, receipt.("rolled back"))})
        Repo.rollback(:nope)
      end)

    assert_received {:spawned, rolled_back_id}
    {:ok, _} = Repo.transaction(fn -> PgTxn.spawn(Repo, receipt.("committed"), name: "send_receipt") end)

    {:error, :fail, :x, _} =
      Multi.new()
      |> PgTxn.Multi.spawn(:receipt, fn _changes -> send(test, {:receipt, "multi rolled back"}); :ok end)
      |> Multi.run(:fail, fn _, _ -> {:error, :x} end)
      |> Repo.transaction()

    {:ok, %{receipt: multi_id}} =
      Multi.new()
      |> Multi.run(:n, fn _, _ -> {:ok, 1} end)
      |> PgTxn.Multi.spawn(:receipt, fn %{n: n}, ctx -> send(test, {:receipt, "multi #{n}", ctx.effect_id}); :ok end)
      |> Repo.transaction()

    assert_raise RuntimeError, fn ->
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.spawn(tx, receipt.("pg_txn failed"))
        raise "no"
      end)
    end

    {:ok, _} = PgTxn.transaction(Repo, fn tx -> PgTxn.spawn(tx, receipt.("pg_txn committed")) end)

    assert_receive {:receipt, "committed"}, 5_000
    assert_receive {:receipt, "multi 1", ^multi_id}, 5_000
    assert_receive {:receipt, "pg_txn committed"}, 5_000
    refute_receive {:receipt, _}, 500

    assert scalar("SELECT count(*) FROM txn.effects WHERE id = $1::text::uuid", [rolled_back_id]) == 0
    assert scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [multi_id]) == "succeeded"
    assert scalar("SELECT count(*) FROM txn.effects WHERE kind = 'spawn' AND name = 'send_receipt' AND status = 'succeeded'") == 1
  end

  test "a spawn in a rolled-back transaction never runs; one in a committed transaction runs once" do
    c = counter()
    rolled_back = Ecto.UUID.generate()

    {:error, :cancelled} =
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.spawn(tx, fn -> bump(c, :rolled_back) end)
        PgTxn.effect(tx, fn -> 1 end)
        Repo.rollback(:cancelled)
      end, id: rolled_back)

    committed = Ecto.UUID.generate()

    {:ok, id} =
      PgTxn.transaction(Repo, fn tx ->
        # spawned in every run: only the committing run's spawn exists
        id = PgTxn.spawn(tx, fn ctx -> bump(c, {:committed, ctx.effect_id}) end, name: "once")
        PgTxn.effect(tx, fn -> 1 end, name: "a")
        PgTxn.effect(tx, fn -> 2 end, name: "b")
        id
      end, id: committed)

    wait_until(fn -> count(c, {:committed, id}) == 1 end)
    Process.sleep(300)
    assert Agent.get(c, & &1) == %{{:committed, id} => 1}
    assert scalar("SELECT count(*) FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'spawn'", [rolled_back]) == 0
    assert scalar("SELECT count(*) FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'spawn'", [committed]) == 1
    # the functions of the runs that rolled back are forgotten at once
    assert :ets.select_count(PgTxn.Local.table(Repo), [{{:_, :spawn, :_, :"$1", :_, :_, :_}, [{:"=:=", :"$1", rolled_back}], [true]}]) == 0
    assert :ets.select_count(PgTxn.Local.table(Repo), [{{:_, :spawn, :_, :"$1", :_, :_, :_}, [{:"=:=", :"$1", committed}], [true]}]) == 0
  end

  test "spawn standalone: a failing function is called once by default, retried with retry:" do
    test = self()
    failing = PgTxn.spawn(Repo, fn -> send(test, :failing); raise "boom" end)
    assert_receive :failing, 5_000
    refute_receive :failing, 500
    assert scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [failing]) == "failed"

    c = counter()

    id =
      PgTxn.spawn(Repo, fn ctx ->
        if bump(c, :h) < 2, do: raise(PgTxn.RetryableError, retry_after_ms: 10)
        send(test, {:handled, ctx.attempt})
        :ok
      end, retry: true)

    assert_receive {:handled, 2}, 5_000
    wait_until(fn -> scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [id]) == "succeeded" end)
    assert PgTxn.Local.get(Repo, id) == nil
  end

  test "a spawned effect whose function this node does not have fails as EffectLost" do
    test = self()
    owner = PgTxn.Config.owner(Repo)
    ghost = scalar("SELECT txn.spawn($1::text::uuid, 'ghost')::text", [owner])
    PgTxn.spawn(Repo, fn -> send(test, :real) end)
    assert_receive :real, 5_000
    wait_until(fn -> scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [ghost]) == "failed" end)
    assert scalar("SELECT error->>'name' FROM txn.effects WHERE id = $1::text::uuid", [ghost]) == "EffectLost"
  end

  test "the Repo's child spec starts its worker" do
    assert %{start: {Supervisor, :start_link, [[_repo, {PgTxn.Worker, repo: PgTxn.TestRepo}], _]}} = PgTxn.TestRepo.child_spec([])
  end

  test "the worker drains work in progress on shutdown" do
    test = self()
    start_supervised!(PgTxn.DrainRepo)

    id =
      PgTxn.spawn(PgTxn.DrainRepo, fn ->
        send(test, :handler_started)
        Process.sleep(500)
        send(test, :handler_done)
      end)

    assert_receive :handler_started, 5_000
    :ok = stop_supervised(PgTxn.DrainRepo)
    assert_received :handler_done
    assert scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [id]) == "succeeded"
  end

  # ------------------------------------------------------------------ named transactions

  test "a named transaction resumes on the worker after its process is killed" do
    test = self()
    c = counter()

    PgTxn.define(Repo, "slow_order", fn tx, %{"k" => k} ->
      a = PgTxn.effect(tx, fn -> bump(c, :fast); {:ok, k} end, name: "fast")

      b =
        PgTxn.effect(tx, fn ->
          if bump(c, :slow) == 1 do
            send(test, :slow_started)
            Process.sleep(60_000)
          end

          {:ok, 2}
        end, name: "slow", retry: true)

      a + b
    end)

    tx_id = Ecto.UUID.generate()
    pid = spawn(fn -> PgTxn.run(Repo, "slow_order", %{k: 1}, id: tx_id, lease_ms: 1500) end)
    assert_receive :slow_started, 5_000
    Process.exit(pid, :kill)

    assert {:ok, 3} = PgTxn.wait(Repo, tx_id, 15_000)
    # the completed effect's recorded result was reused; the interrupted one ran again
    assert count(c, :fast) == 1
    assert count(c, :slow) == 2
    assert tx_status(tx_id) == "committed"

    assert scalar("SELECT count(*) FROM txn.effect_attempts a JOIN txn.effects e ON e.id = a.effect_id WHERE e.tx_id = $1::text::uuid AND a.outcome = 'lease_expired'", [tx_id]) == 1
  end

  test "a named transaction's compensations run on this node (its own owner)" do
    test = self()

    PgTxn.define(Repo, "doomed_booking", fn tx, _ ->
      PgTxn.effect(tx, fn -> "seat 1" end, name: "book", compensate: fn seat -> send(test, {:cancel, seat}); :ok end)
      raise "payment declined"
    end)

    tx_id = Ecto.UUID.generate()
    assert_raise RuntimeError, fn -> PgTxn.run(Repo, "doomed_booking", %{}, id: tx_id) end
    assert_receive {:cancel, "seat 1"}, 5_000

    assert scalar("SELECT local_owner::text FROM txn.effects WHERE tx_id = $1::text::uuid AND kind = 'compensation'", [tx_id]) !=
             PgTxn.Config.owner(Repo)
  end

  test "run/4 drives a named transaction in this process" do
    PgTxn.define(Repo, "echo", fn tx, input -> [PgTxn.effect(tx, fn -> input end, deps: input), PgTxn.now(tx)] end)
    tx_id = Ecto.UUID.generate()
    assert {:ok, [%{"x" => 1}, now]} = PgTxn.run(Repo, "echo", %{x: 1}, id: tx_id)
    # now/1 is the stored start time, as in a resumed run
    created_at = scalar("SELECT created_at FROM txn.transactions WHERE id = $1::text::uuid", [tx_id])
    assert now == DateTime.truncate(created_at, :millisecond)
  end

  test "enqueue + wait" do
    PgTxn.define(Repo, "double", fn tx, %{"n" => n} ->
      %{"doubled" => PgTxn.effect(tx, fn -> n * 2 end, name: "mul")}
    end)

    PgTxn.define(Repo, "doomed", fn _tx, _ -> raise "doomed" end)

    id = PgTxn.enqueue(Repo, "double", %{n: 21})
    assert {:ok, %{"doubled" => 42}} = PgTxn.wait(Repo, id, 10_000)

    # queued iff the surrounding transaction commits
    {:error, :no} = Repo.transaction(fn -> send(self(), {:id, PgTxn.enqueue(Repo, "double", %{n: 1})}); Repo.rollback(:no) end)
    assert_received {:id, never}
    assert {:error, :timeout} = PgTxn.wait(Repo, never, 300)

    {:ok, %{job: job}} = Multi.new() |> PgTxn.Multi.enqueue(:job, "double", %{n: 2}) |> Repo.transaction()
    assert {:ok, %{"doubled" => 4}} = PgTxn.wait(Repo, job, 10_000)

    capture_log(fn ->
      id = PgTxn.enqueue(Repo, "doomed", %{})
      assert {:error, %PgTxn.TransactionFailedError{status: "failed"}} = PgTxn.wait(Repo, id, 10_000)
    end)
  end

  # ------------------------------------------------------------------ keys

  # records effects entering and leaving (to detect overlap)
  defp tracked(c, label, ms) do
    Agent.update(c, &Map.update(&1, :log, [{:in, label}], fn l -> [{:in, label} | l] end))
    Process.sleep(ms)
    Agent.update(c, &Map.update!(&1, :log, fn l -> [{:out, label} | l] end))
    :ok
  end

  defp overlapped?(c) do
    c
    |> Agent.get(&Enum.reverse(Map.get(&1, :log, [])))
    |> Enum.reduce_while(0, fn
      {:in, _}, 0 -> {:cont, 1}
      {:in, _}, _ -> {:halt, :overlap}
      {:out, _}, n -> {:cont, n - 1}
    end) == :overlap
  end

  test "keys are stored as text: strings as is, other terms as JSON like JSON.stringify" do
    assert PgTxn.Loop.keys(key: "order:1") == ["order:1"]
    assert PgTxn.Loop.keys(key: {"order", 42}) == [~s(["order",42])]
    assert PgTxn.Loop.keys(key: ["order", 42], keys: [["account", 1], "x"]) == [~s(["order",42]), ~s(["account",1]), "x"]
    assert PgTxn.Loop.keys([]) == nil
    assert PgTxn.Loop.keys(keys: []) == nil

    tx_id = Ecto.UUID.generate()
    {:ok, 1} = PgTxn.transaction(Repo, fn _ -> 1 end, key: ["order", 42], id: tx_id)
    assert scalar("SELECT keys FROM txn.transactions WHERE id = $1::text::uuid", [tx_id]) == [~s(["order",42])]
  end

  test "the same key: one at a time, each sees the other's committed writes" do
    c = counter()
    id = insert_order()

    bump = fn label ->
      Task.async(fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn -> tracked(c, label, 100) end)
          Repo.query!("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id])
        end, key: ["order", id])
      end)
    end

    [bump.(:a), bump.(:b), bump.(:c)] |> Task.await_many(15_000)
    refute overlapped?(c)
    assert order(id).amount == 3
  end

  test "two checkouts of the same order with a key: one charge" do
    c = counter()
    id = insert_order()

    checkout = fn ->
      Task.async(fn ->
        PgTxn.transaction(Repo, fn tx ->
          if order(id).status != "new" do
            "already paid"
          else
            PgTxn.effect(tx, fn -> bump(c, :charge); Process.sleep(100); :ok end, name: "charge")
            Repo.query!("UPDATE orders SET status = 'paid' WHERE id = $1", [id])
            "paid"
          end
        end, key: "order:#{id}")
      end)
    end

    outs = [checkout.(), checkout.()] |> Task.await_many(15_000) |> Enum.map(fn {:ok, o} -> o end)
    assert Enum.sort(outs) == ["already paid", "paid"]
    assert count(c, :charge) == 1
  end

  test "different keys run concurrently" do
    t0 = System.monotonic_time(:millisecond)

    for k <- 1..4 do
      Task.async(fn -> PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> Process.sleep(300); :ok end) end, key: ["k", insert_order(), k]) end)
    end
    |> Task.await_many(15_000)

    assert System.monotonic_time(:millisecond) - t0 < 900
  end

  test "a key is released when the transaction fails, or has no effects" do
    key = ["release", insert_order()]

    assert_raise RuntimeError, "business rule", fn ->
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn -> 1 end)
        raise "business rule"
      end, key: key)
    end

    assert {:ok, "no effects"} = PgTxn.transaction(Repo, fn _ -> "no effects" end, key: key)
    t0 = System.monotonic_time(:millisecond)
    assert {:ok, "again"} = PgTxn.transaction(Repo, fn _ -> "again" end, key: key)
    assert System.monotonic_time(:millisecond) - t0 < 500
    assert scalar("SELECT count(*) FROM txn.keys WHERE key = $1", [Jason.encode!(key)]) == 0
  end

  test "waiting longer than :key_wait_ms raises KeyTimeoutError" do
    start_supervised!(PgTxn.ImpatientRepo)
    test = self()
    key = "slow:#{insert_order()}"

    slow =
      Task.async(fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn -> send(test, :holding); Process.sleep(800); :ok end)
        end, key: key)
      end)

    assert_receive :holding, 5_000
    error = assert_raise PgTxn.KeyTimeoutError, fn -> PgTxn.transaction(PgTxn.ImpatientRepo, fn _ -> 1 end, key: key) end
    assert error.key == key
    assert {:ok, "ok"} = Task.await(slow, 5_000)
  end

  test "enqueued transactions with the same key run one at a time" do
    c = counter()
    id = insert_order()

    PgTxn.define(Repo, "keyed_bump", fn tx, %{"n" => n} ->
      PgTxn.effect(tx, fn -> tracked(c, n, 50) end)
      Repo.query!("UPDATE orders SET amount = amount + 1 WHERE id = $1", [id])
      n
    end)

    ids = for n <- 1..5, do: PgTxn.enqueue(Repo, "keyed_bump", %{n: n}, key: ["order", id])
    for t <- ids, do: assert({:ok, _} = PgTxn.wait(Repo, t, 15_000))
    refute overlapped?(c)
    assert order(id).amount == 5
  end

  test "crossing transfers with keys on both accounts: one at a time, money conserved" do
    c = counter()
    a = insert_order(amount: 100)
    b = insert_order(amount: 100)

    transfer = fn from, to ->
      Task.async(fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn -> tracked(c, {from, to}, 30) end, name: "ledger")
          Repo.query!("UPDATE orders SET amount = amount - 10 WHERE id = $1", [from])
          Repo.query!("UPDATE orders SET amount = amount + 10 WHERE id = $1", [to])
        end, keys: [["account", from], ["account", to]])
      end)
    end

    tasks = for i <- 1..10, do: if(rem(i, 2) == 0, do: transfer.(a, b), else: transfer.(b, a))
    assert Enum.all?(Task.await_many(tasks, 30_000), &match?({:ok, _}, &1))
    refute overlapped?(c)
    assert order(a).amount + order(b).amount == 200
    assert {order(a).amount, order(b).amount} == {100, 100}
  end
end
