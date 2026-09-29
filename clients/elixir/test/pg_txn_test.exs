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

  # ------------------------------------------------------------------ composition

  defp runs_of(tx_id), do: scalar("SELECT runs FROM txn.transactions WHERE id = $1::text::uuid", [tx_id])

  defp collect(tag, n, timeout \\ 5_000) do
    for _ <- 1..n do
      receive do
        {^tag, v} -> v
      after
        timeout -> flunk("expected #{n} #{inspect(tag)} messages")
      end
    end
  end

  test "composition: spawn with a Repo inside Repo.transaction runs iff it commits" do
    test = self()
    {:ok, _} = Repo.transaction(fn -> PgTxn.spawn(Repo, fn -> send(test, {:ran, "committed"}); :ok end) end)
    {:error, :no} = Repo.transaction(fn -> PgTxn.spawn(Repo, fn -> send(test, {:ran, "rolled back"}); :ok end); Repo.rollback(:no) end)
    assert collect(:ran, 1) == ["committed"]
    refute_receive {:ran, _}, 300
  end

  test "composition: a spawn after a savepoint that is rolled back does not run, though the transaction commits" do
    test = self()

    {:ok, _} =
      Repo.transaction(fn ->
        PgTxn.spawn(Repo, fn -> send(test, {:ran, "kept"}); :ok end)
        Repo.query!("SAVEPOINT s")
        PgTxn.spawn(Repo, fn -> send(test, {:ran, "undone"}); :ok end)
        Repo.query!("ROLLBACK TO SAVEPOINT s")
      end)

    assert collect(:ran, 1) == ["kept"]
    refute_receive {:ran, _}, 300
  end

  test "composition: enqueue inside Repo.transaction is queued iff it commits" do
    PgTxn.define(Repo, "compose_enqueue", fn _tx, %{"id" => id} ->
      Repo.query!("UPDATE orders SET status = 'done' WHERE id = $1", [id])
      id
    end)

    a = insert_order()
    b = insert_order()
    {:ok, kept} = Repo.transaction(fn -> PgTxn.enqueue(Repo, "compose_enqueue", %{id: a}) end)
    {:error, {:dropped, dropped}} = Repo.transaction(fn -> Repo.rollback({:dropped, PgTxn.enqueue(Repo, "compose_enqueue", %{id: b})}) end)
    assert {:ok, ^a} = PgTxn.wait(Repo, kept, 10_000)
    assert order(a).status == "done"
    assert scalar("SELECT count(*) FROM txn.transactions WHERE id = $1::text::uuid", [dropped]) == 0
    assert order(b).status == "new"
  end

  test "composition: PgTxn.transaction inside Repo.transaction is refused" do
    assert_raise ArgumentError, ~r/cannot run inside another Repo transaction/, fn ->
      Repo.transaction(fn -> PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> 1 end) end) end)
    end
  end

  test "composition: 10 spawns run once each, after the commit, each seeing it, with distinct idempotency keys" do
    test = self()
    id = insert_order()

    {:ok, _} =
      PgTxn.transaction(Repo, fn tx ->
        Repo.query!("UPDATE orders SET status = 'paid' WHERE id = $1", [id])
        for n <- 0..9, do: PgTxn.spawn(tx, fn ctx -> send(test, {:seen, {n, ctx.idempotency_key, order(id).status}}); :ok end)
      end)

    seen = collect(:seen, 10)
    refute_receive {:seen, _}, 300
    assert seen |> Enum.map(&elem(&1, 0)) |> Enum.sort() == Enum.to_list(0..9)
    assert seen |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> length() == 10
    assert Enum.all?(seen, &(elem(&1, 2) == "paid"))
  end

  test "composition: effects and spawns interleaved over 3 rounds: each once, 4 runs" do
    test = self()
    c = counter()
    tx_id = Ecto.UUID.generate()
    sp = fn label -> fn -> send(test, {:spawned, label}); :ok end end

    {:ok, _} =
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.spawn(tx, sp.("s0"))
        a = PgTxn.effect(tx, fn -> bump(c, :a); 1 end)
        PgTxn.spawn(tx, sp.("s1:#{a}"))
        b = PgTxn.effect(tx, fn -> bump(c, :b); a + 1 end)
        PgTxn.spawn(tx, sp.("s2:#{b}"))
        d = PgTxn.effect(tx, fn -> bump(c, :c); b + 1 end)
        PgTxn.spawn(tx, sp.("s3:#{d}"))
      end, id: tx_id)

    assert {count(c, :a), count(c, :b), count(c, :c)} == {1, 1, 1}
    assert runs_of(tx_id) == 4
    assert Enum.sort(collect(:spawned, 4)) == ["s0", "s1:1", "s2:2", "s3:3"]
    refute_receive {:spawned, _}, 300
  end

  test "composition: an effect's result decides the spawns, one per item" do
    test = self()

    {:ok, _} =
      PgTxn.transaction(Repo, fn tx ->
        for r <- PgTxn.effect(tx, fn -> ["ana", "bo", "cy"] end), do: PgTxn.spawn(tx, fn -> send(test, {:sent, r}); :ok end)
        PgTxn.effect(tx, fn -> "audit" end)
      end)

    assert Enum.sort(collect(:sent, 3)) == ["ana", "bo", "cy"]
    refute_receive {:sent, _}, 300
  end

  # effects are sequential in Elixir (the run's state lives in the process
  # running it), so only the sequential half of the TS test applies
  test "composition: a data-dependent number of sequential effects takes N+1 runs" do
    c = counter()
    tx_id = Ecto.UUID.generate()

    {:ok, 150} =
      PgTxn.transaction(Repo, fn tx ->
        Enum.sum(for i <- 1..5, do: PgTxn.effect(tx, fn -> bump(c, :calls); i * 10 end))
      end, id: tx_id)

    assert count(c, :calls) == 5
    assert runs_of(tx_id) == 6
  end

  test "composition: one spawn failing permanently does not affect the others or the commit" do
    test = self()
    id = insert_order()

    {:ok, bad} =
      PgTxn.transaction(Repo, fn tx ->
        Repo.query!("UPDATE orders SET status = 'paid' WHERE id = $1", [id])
        PgTxn.spawn(tx, fn -> send(test, {:ran, "a"}); :ok end)
        bad = PgTxn.spawn(tx, fn -> raise PgTxn.PermanentError, "mail server said no" end)
        PgTxn.spawn(tx, fn -> send(test, {:ran, "c"}); :ok end)
        bad
      end)

    assert order(id).status == "paid"
    assert Enum.sort(collect(:ran, 2)) == ["a", "c"]
    wait_until(fn -> scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [bad]) == "failed" end)
    assert scalar("SELECT error->>'message' FROM txn.effects WHERE id = $1::text::uuid", [bad]) == "mail server said no"
  end

  test "composition: a failure after 3 compensated effects and 3 spawns: 3 compensations, no spawn" do
    test = self()
    tx_id = Ecto.UUID.generate()

    assert_raise RuntimeError, "out of stock", fn ->
      PgTxn.transaction(Repo, fn tx ->
        for name <- ["charge", "reserve", "notify-partner"] do
          PgTxn.effect(tx, fn -> name end, name: name, compensate: fn r -> send(test, {:undone, r}); :ok end)
          PgTxn.spawn(tx, fn -> send(test, {:spawned, name}); :ok end)
        end

        raise "out of stock"
      end, id: tx_id)
    end

    assert Enum.sort(collect(:undone, 3)) == ["charge", "notify-partner", "reserve"]
    refute_receive {:undone, _}, 300
    refute_received {:spawned, _}
    assert runs_of(tx_id) == 4
  end

  test "ids: an id that already committed returns its recorded output without running again" do
    c = counter()
    id = Ecto.UUID.generate()
    f = fn tx -> PgTxn.effect(tx, fn -> bump(c, :calls) end); bump(c, :fun); "receipt-7" end
    assert {:ok, "receipt-7"} = PgTxn.transaction(Repo, f, id: id)
    fun_calls = count(c, :fun)
    assert {:ok, "receipt-7"} = PgTxn.transaction(Repo, f, id: id)
    assert count(c, :calls) == 1
    assert count(c, :fun) == fun_calls
  end

  test "ids: an id that already failed returns its recorded error without running again" do
    c = counter()
    id = Ecto.UUID.generate()
    f = fn tx -> bump(c, :fun); PgTxn.effect(tx, fn -> 1 end); raise "nope" end
    assert_raise RuntimeError, "nope", fn -> PgTxn.transaction(Repo, f, id: id) end
    before = count(c, :fun)

    assert {:error, %PgTxn.TransactionFailedError{status: "failed", error: %{"message" => "nope"}}} =
             PgTxn.transaction(Repo, f, id: id)

    assert count(c, :fun) == before
  end

  test "misuse: tx inside an effect's or a spawned function is refused with a clear error" do
    test = self()

    {:ok, _} =
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn ->
          try do
            PgTxn.effect(tx, fn -> 1 end)
          rescue
            e -> send(test, {:error, Exception.message(e)})
          end

          :ok
        end)

        PgTxn.spawn(tx, fn ->
          try do
            PgTxn.uuid(tx)
          rescue
            e -> send(test, {:error, Exception.message(e)})
          end

          :ok
        end)
      end)

    assert [a, b] = collect(:error, 2)
    assert a =~ "cannot be used inside an effect"
    assert b =~ "cannot be used inside an effect"
  end

  test "misuse: tx after its transaction ended is refused with a clear error" do
    {:ok, leaked} = PgTxn.transaction(Repo, fn tx -> tx end)
    assert_raise ArgumentError, ~r/has ended/, fn -> PgTxn.effect(leaked, fn -> 1 end) end
    assert_raise ArgumentError, ~r/has ended/, fn -> PgTxn.spawn(leaked, fn -> 1 end) end
    assert_raise ArgumentError, ~r/has ended/, fn -> PgTxn.now(leaked) end
  end

  # ------------------------------------------------------------------ adversarial fixes

  test "a transaction can be started inside an effect's or a spawned function" do
    test = self()

    {:ok, outer} =
      PgTxn.transaction(Repo, fn tx ->
        inner = PgTxn.effect(tx, fn -> {:ok, inner} = PgTxn.transaction(Repo, fn t2 -> PgTxn.effect(t2, fn -> "inner" end) end); inner end)

        PgTxn.spawn(tx, fn ->
          {:ok, v} = PgTxn.transaction(Repo, fn t3 -> PgTxn.effect(t3, fn -> "from spawn" end) end)
          send(test, {:nested, v})
          :ok
        end)

        inner <> "+outer"
      end)

    assert outer == "inner+outer"
    assert collect(:nested, 1) == ["from spawn"]
  end

  test "an effect result that cannot be stored fails the effect for good, once" do
    c = counter()

    for {label, result} <- [tuple: {:a, 1}, nul: "a\0b", pid: self()] do
      tx_id = Ecto.UUID.generate()

      error =
        assert_raise PgTxn.EffectFailedError, fn ->
          PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> bump(c, label); result end, retry: true) end, id: tx_id)
        end

      assert error.error["message"] =~ "cannot be stored"
      assert count(c, label) == 1
      assert tx_status(tx_id) == "failed"
    end
  end

  defmodule BadMessageError do
    defexception []
    @impl true
    def message(_), do: raise("no message")
  end

  test "errors that cannot be encoded are stored as inspected text" do
    for reason <- [%{"name" => "X", "message" => {:not, :text}}, %BadMessageError{}, "nul\0byte", <<255, 0>>] do
      error =
        assert_raise PgTxn.EffectFailedError, fn ->
          PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> {:error, reason} end) end)
        end

      assert is_binary(error.error["message"])
    end
  end

  test "a failure while performing effects fails the transaction and releases its keys" do
    key = "perform:#{insert_order()}"
    tx_id = Ecto.UUID.generate()

    assert_raise Postgrex.Error, fn ->
      PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> 1 end, name: "bad\0name") end, key: key, id: tx_id)
    end

    assert tx_status(tx_id) == "failed"
    t0 = System.monotonic_time(:millisecond)
    assert {:ok, 1} = PgTxn.transaction(Repo, fn _ -> 1 end, key: key)
    assert System.monotonic_time(:millisecond) - t0 < 500
  end

  test "retry_after_ms beyond 15 minutes fails the effect; a negative one is clamped to 0" do
    c = counter()

    error =
      assert_raise PgTxn.EffectFailedError, fn ->
        PgTxn.transaction(Repo, fn tx ->
          PgTxn.effect(tx, fn -> bump(c, :far); raise PgTxn.RetryableError, retry_after_ms: 16 * 60_000 end, retry: true)
        end)
      end

    assert count(c, :far) == 1
    assert error.error["message"] =~ "beyond"

    assert_raise PgTxn.EffectFailedError, fn ->
      PgTxn.transaction(Repo, fn tx ->
        PgTxn.effect(tx, fn -> raise PgTxn.RetryableError, retry_after_ms: :infinity end, retry: true)
      end)
    end

    t0 = System.monotonic_time(:millisecond)

    assert {:ok, "ok"} =
             PgTxn.transaction(Repo, fn tx ->
               PgTxn.effect(tx, fn -> if bump(c, :neg) == 1, do: raise(PgTxn.RetryableError, retry_after_ms: -500), else: "ok" end, retry: true)
             end)

    assert System.monotonic_time(:millisecond) - t0 < 1_000
  end

  test "a spawn inside a Repo.transaction open longer than the sweep threshold still runs; a rolled-back one is forgotten" do
    start_supervised!(PgTxn.SweepRepo)
    test = self()

    {:ok, id} =
      PgTxn.SweepRepo.transaction(fn ->
        id = PgTxn.spawn(PgTxn.SweepRepo, fn -> send(test, :late_spawn); :ok end)
        # several maintenance ticks while the spawn is invisible
        Process.sleep(400)
        id
      end)

    assert_receive :late_spawn, 5_000
    wait_until(fn -> PgTxn.Local.get(PgTxn.SweepRepo, id) == nil end)

    {:error, {:rolled_back, gone}} =
      PgTxn.SweepRepo.transaction(fn -> PgTxn.SweepRepo.rollback({:rolled_back, PgTxn.spawn(PgTxn.SweepRepo, fn -> :ok end)}) end)

    assert PgTxn.Local.get(PgTxn.SweepRepo, gone) != nil
    wait_until(fn -> PgTxn.Local.get(PgTxn.SweepRepo, gone) == nil end, 5_000)
  end

  test "effects of this node it has no function for fail as EffectLost on the maintenance tick" do
    start_supervised!(PgTxn.SweepRepo)
    %{rows: [[ghost]]} = PgTxn.SweepRepo.query!("SELECT txn.spawn($1::text::uuid, 'ghost')::text", [PgTxn.Config.owner(PgTxn.SweepRepo)])
    wait_until(fn -> scalar("SELECT status FROM txn.effects WHERE id = $1::text::uuid", [ghost]) == "failed" end, 3_000)
    assert scalar("SELECT error->>'name' FROM txn.effects WHERE id = $1::text::uuid", [ghost]) == "EffectLost"
  end

  test "a resumed transaction that diverged gets its compensation run at once" do
    test = self()
    flag = counter()

    PgTxn.define(Repo, "diverge", fn tx, _ ->
      deps = if count(flag, :resumed) == 0, do: 1, else: 2
      PgTxn.effect(tx, fn -> "charge #{deps}" end, name: "charge", deps: deps, compensate: fn r -> send(test, {:refund, r}); :ok end)

      if deps == 1 do
        PgTxn.effect(tx, fn -> send(test, :blocked); Process.sleep(60_000) end, name: "block")
      end

      deps
    end)

    tx_id = Ecto.UUID.generate()
    pid = spawn(fn -> PgTxn.run(Repo, "diverge", %{}, id: tx_id, lease_ms: 1500) end)
    assert_receive :blocked, 5_000
    bump(flag, :resumed)
    Process.exit(pid, :kill)

    assert {:ok, 2} = PgTxn.wait(Repo, tx_id, 15_000)
    assert_receive {:refund, "charge 1"}, 3_000
  end

  test "the worker's shutdown lets transactions in progress and their spawns finish, then refuses new calls" do
    start_supervised!(PgTxn.DrainRepo)
    test = self()

    task =
      Task.async(fn ->
        PgTxn.transaction(PgTxn.DrainRepo, fn tx ->
          PgTxn.effect(tx, fn -> send(test, :in_effect); Process.sleep(300); 1 end)
          PgTxn.spawn(tx, fn -> Process.sleep(200); send(test, :spawn_done); :ok end)
        end)
      end)

    assert_receive :in_effect, 5_000
    :ok = stop_supervised(PgTxn.DrainRepo)
    assert {:ok, _} = Task.await(task)
    assert_received :spawn_done
    assert_raise ArgumentError, ~r/no PgTxn.Worker is running/, fn -> PgTxn.transaction(PgTxn.DrainRepo, fn _ -> 1 end) end
  end

  test "an enqueued transaction runs at its isolation level; so does transaction/3 with :isolation" do
    level = fn -> Repo.query!("SELECT current_setting('transaction_isolation')").rows |> hd() |> hd() end
    PgTxn.define(Repo, "iso", fn tx, _ -> PgTxn.effect(tx, fn -> 1 end); level.() end)

    id = PgTxn.enqueue(Repo, "iso", %{}, isolation: :serializable)
    assert {:ok, "serializable"} = PgTxn.wait(Repo, id, 10_000)
    assert scalar("SELECT isolation FROM txn.transactions WHERE id = $1::text::uuid", [id]) == "serializable"

    assert {:ok, "repeatable read"} =
             PgTxn.transaction(Repo, fn tx -> PgTxn.effect(tx, fn -> 1 end); level.() end, isolation: :repeatable_read, lease_ms: 3000)
  end

  test "concurrent calls with the same id get the one outcome; the function runs once" do
    c = counter()
    id = Ecto.UUID.generate()

    f = fn tx ->
      bump(c, :fun)
      PgTxn.effect(tx, fn -> bump(c, :effect); Process.sleep(200); "paid" end)
    end

    results = for(_ <- 1..5, do: Task.async(fn -> PgTxn.transaction(Repo, f, id: id) end)) |> Task.await_many(15_000)
    assert Enum.uniq(results) == [{:ok, "paid"}]
    assert count(c, :effect) == 1

    # an id without effects is idempotent too
    id2 = Ecto.UUID.generate()
    assert {:ok, "first"} = PgTxn.transaction(Repo, fn _ -> "first" end, id: id2)
    assert {:ok, "first"} = PgTxn.transaction(Repo, fn _ -> "second" end, id: id2)
  end
end
