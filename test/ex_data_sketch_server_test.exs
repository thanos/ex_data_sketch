defmodule ExDataSketch.ServerTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Server

  alias ExDataSketch.{Errors, HLL, Server, Storage, Window}

  describe "start_link/1" do
    test "starts with a registry atom" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      assert %HLL{} = Server.sketch(pid)
    end

    test "starts with a module directly" do
      {:ok, pid} = Server.start_link(sketch: HLL, sketch_opts: [p: 10])
      assert %HLL{} = Server.sketch(pid)
    end

    test "registers under :name" do
      name = :"server_test_#{System.unique_integer([:positive])}"
      {:ok, _pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10], name: name)
      assert %HLL{} = Server.sketch(name)
    end

    test "starts windowed when :window is given" do
      {:ok, pid} =
        Server.start_link(sketch: :hll, sketch_opts: [p: 10], window: [every: 60_000, keep: 5])

      assert %Window{} = Server.sketch(pid)
    end
  end

  describe "update/2 and update_sync/2" do
    test "update_sync/2 is applied before returning" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      :ok = Server.update_sync(pid, "a")
      assert Server.estimate(pid) > 0.0
    end

    test "update/2 is eventually applied" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      Server.update(pid, "a")
      :ok = Server.update_sync(pid, "b")
      assert Server.estimate(pid) >= 1.0
    end

    test "update_many/2 applies every item" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      Server.update_many(pid, ["a", "b", "c"])
      :ok = Server.update_sync(pid, "d")
      assert_in_delta Server.estimate(pid), 4.0, 0.5
    end

    test "windowed server updates both the window and the all-time accumulator" do
      {:ok, pid} =
        Server.start_link(
          sketch: :hll,
          sketch_opts: [p: 10],
          window: [every: 60_000, keep: 5, track_all_time: true]
        )

      :ok = Server.update_sync(pid, "a")
      assert Server.estimate(pid) > 0.0
      assert Server.estimate(pid, window: :all) > 0.0
    end
  end

  describe "estimate/2 with window: :all" do
    test "raises UnsupportedOperationError without track_all_time: true" do
      {:ok, pid} =
        Server.start_link(sketch: :hll, sketch_opts: [p: 10], window: [every: 60_000, keep: 5])

      assert_raise Errors.UnsupportedOperationError, ~r/track_all_time/, fn ->
        Server.estimate(pid, window: :all)
      end
    end

    test "dispatches to count/1 for quantile families when not windowed" do
      {:ok, pid} = Server.start_link(sketch: :kll, sketch_opts: [])
      :ok = Server.update_sync(pid, 1.0)
      assert Server.estimate(pid) == 1
    end

    test "raises UnsupportedOperationError for a family with no single-value estimate" do
      {:ok, pid} = Server.start_link(sketch: :cms, sketch_opts: [])

      assert_raise Errors.UnsupportedOperationError, fn ->
        Server.estimate(pid)
      end
    end
  end

  describe "sketch/1" do
    test "returns the bare sketch struct when not windowed" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      assert %HLL{} = Server.sketch(pid)
    end

    test "returns the %Window{} struct when windowed" do
      {:ok, pid} =
        Server.start_link(sketch: :hll, sketch_opts: [p: 10], window: [every: 60_000, keep: 5])

      assert %Window{} = Server.sketch(pid)
    end
  end

  describe "merge/2" do
    test "merges an already-built sketch into a non-windowed server" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      partial = HLL.new(p: 10) |> HLL.update("a")
      :ok = Server.merge(pid, partial)
      assert Server.estimate(pid) > 0.0
    end

    test "raises UnsupportedOperationError on a windowed server" do
      {:ok, pid} =
        Server.start_link(sketch: :hll, sketch_opts: [p: 10], window: [every: 60_000, keep: 5])

      partial = HLL.new(p: 10) |> HLL.update("a")

      assert_raise Errors.UnsupportedOperationError, ~r/windowed/, fn ->
        Server.merge(pid, partial)
      end
    end
  end

  describe "reset/1" do
    test "clears a non-windowed server" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      :ok = Server.update_sync(pid, "a")
      :ok = Server.reset(pid)
      assert Server.estimate(pid) == 0.0
    end

    test "clears both the window and the all-time accumulator" do
      {:ok, pid} =
        Server.start_link(
          sketch: :hll,
          sketch_opts: [p: 10],
          window: [every: 60_000, keep: 5, track_all_time: true]
        )

      :ok = Server.update_sync(pid, "a")
      :ok = Server.reset(pid)
      assert Server.estimate(pid) == 0.0
      assert Server.estimate(pid, window: :all) == 0.0
    end
  end

  describe "max_queue" do
    test "drops casts once the mailbox is at or above the threshold" do
      test_pid = self()

      :telemetry.attach(
        "server-drop-test-#{System.unique_integer([:positive])}",
        [:ex_data_sketch, :server, :drop],
        fn _event, measurements, _metadata, _config -> send(test_pid, {:drop, measurements}) end,
        nil
      )

      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10], max_queue: 0)
      Server.update(pid, "a")
      # update_sync/2 is a call, never subject to max_queue, so it is
      # always applied -- used here only to know the cast ahead of it has
      # already been handled (dropped or not) by the time this returns.
      :ok = Server.update_sync(pid, "sync-item")

      assert_receive {:drop, %{queue_len: _}}
      # Only "sync-item" landed; "a" was dropped.
      assert_in_delta Server.estimate(pid), 1.0, 0.5
    end

    test "update_sync/2 is never subject to max_queue" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 10], max_queue: 0)
      :ok = Server.update_sync(pid, "a")
      assert Server.estimate(pid) > 0.0
    end
  end

  describe "snapshot: crash recovery" do
    test "restores from the last snapshot after a restart" do
      table = :"server_snapshot_test_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      opts = [
        sketch: :hll,
        sketch_opts: [p: 10],
        snapshot: [to: {Storage.ETS, table, "counter"}, every: :infinity]
      ]

      {:ok, pid1} = Server.start_link(opts)
      :ok = Server.update_sync(pid1, "a")
      :ok = Server.update_sync(pid1, "b")
      :ok = GenServer.stop(pid1, :normal)

      {:ok, pid2} = Server.start_link(opts)
      assert_in_delta Server.estimate(pid2), 2.0, 0.5

      :ets.delete(table)
    end

    test "an untrappable kill loses only updates since the last periodic snapshot" do
      table = :"server_snapshot_kill_test_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      opts = [
        sketch: :hll,
        sketch_opts: [p: 10],
        snapshot: [to: {Storage.ETS, table, "counter"}, every: 50]
      ]

      {:ok, pid1} = Server.start_link(opts)
      :ok = Server.update_sync(pid1, "a")
      # Let the periodic snapshot fire before killing.
      Process.sleep(100)
      :ok = Server.update_sync(pid1, "b")

      # Unlink first: start_link/1 links the test process to pid1, and an
      # untrappable :kill signal propagates across that link and would
      # crash this test process too, independent of the monitor below.
      Process.unlink(pid1)
      ref = Process.monitor(pid1)
      Process.exit(pid1, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid1, :killed}

      {:ok, pid2} = Server.start_link(opts)
      # "b" (written after the last periodic snapshot) is lost; "a" survives.
      assert_in_delta Server.estimate(pid2), 1.0, 0.5

      :ets.delete(table)
    end

    test "starts fresh (not crashing) when nothing has been snapshotted yet" do
      table = :"server_snapshot_missing_test_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      {:ok, pid} =
        Server.start_link(
          sketch: :hll,
          sketch_opts: [p: 10],
          snapshot: [to: {Storage.ETS, table, "never_saved"}, every: :infinity]
        )

      assert Server.estimate(pid) == 0.0
      :ets.delete(table)
    end

    test "windowed server survives a kill with the window intact" do
      table = :"server_snapshot_window_test_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      opts = [
        sketch: :hll,
        sketch_opts: [p: 10],
        window: [every: 60_000, keep: 5],
        snapshot: [to: {Storage.ETS, table, "windowed"}, every: :infinity]
      ]

      {:ok, pid1} = Server.start_link(opts)
      :ok = Server.update_sync(pid1, "a")
      :ok = GenServer.stop(pid1, :normal)

      {:ok, pid2} = Server.start_link(opts)
      assert %Window{} = Server.sketch(pid2)
      assert Server.estimate(pid2) > 0.0

      :ets.delete(table)
    end
  end

  describe "concurrent updates" do
    test "updates from N processes converge to the correct estimate" do
      {:ok, pid} = Server.start_link(sketch: :hll, sketch_opts: [p: 12])

      1..20
      |> Task.async_stream(fn n ->
        Enum.each(1..50, fn i -> Server.update_sync(pid, "user_#{n}_#{i}") end)
      end)
      |> Stream.run()

      assert_in_delta Server.estimate(pid), 1000.0, 1000 * 0.05
    end
  end

  describe "flush" do
    test "periodically returns the current sketch to the callback and resets" do
      test_pid = self()

      {:ok, pid} =
        Server.start_link(
          sketch: :hll,
          sketch_opts: [p: 10],
          flush: [
            interval: 50,
            callback: fn sketch -> send(test_pid, {:flushed, HLL.estimate(sketch)}) end
          ]
        )

      :ok = Server.update_sync(pid, "a")
      assert_receive {:flushed, estimate}, 200
      assert estimate > 0.0
      assert Server.estimate(pid) == 0.0
    end
  end
end
