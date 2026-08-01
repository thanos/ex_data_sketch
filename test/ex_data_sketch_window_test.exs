defmodule ExDataSketch.WindowTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Window

  alias ExDataSketch.{
    Bloom,
    CMS,
    Cuckoo,
    Errors,
    FilterChain,
    HLL,
    KLL,
    Telemetry,
    Window,
    XorFilter
  }

  describe "new/3" do
    test "builds an empty window for a mergeable family" do
      window = Window.new(HLL, [p: 10], every: 60_000, keep: 5)
      assert window.module == HLL
      assert window.sketch_opts == [p: 10]
      assert window.every == 60_000
      assert window.keep == 5
      assert window.slots == %{}
      assert is_function(window.time_fn, 0)
    end

    test "resolves a registry atom to its module" do
      window = Window.new(:hll, [p: 10], every: 60_000, keep: 5)
      assert window.module == HLL
    end

    test "accepts a mergeable membership filter" do
      window = Window.new(Bloom, [capacity: 100], every: 1000, keep: 3)
      assert window.module == Bloom
    end

    test "accepts a custom time_fn" do
      window = Window.new(HLL, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      assert window.time_fn.() == 0
    end

    test "rejects Cuckoo (no merge/2)" do
      assert_raise Errors.UnsupportedOperationError, ~r/Cuckoo/, fn ->
        Window.new(Cuckoo, [], every: 60_000, keep: 5)
      end
    end

    test "rejects XorFilter (no merge/2)" do
      assert_raise Errors.UnsupportedOperationError, ~r/XorFilter/, fn ->
        Window.new(XorFilter, [], every: 60_000, keep: 5)
      end
    end

    test "rejects FilterChain (no merge/2)" do
      assert_raise Errors.UnsupportedOperationError, ~r/FilterChain/, fn ->
        Window.new(FilterChain, [], every: 60_000, keep: 5)
      end
    end

    test "raises InvalidOptionError when :every is missing" do
      assert_raise Errors.InvalidOptionError, fn ->
        Window.new(HLL, [p: 10], keep: 5)
      end
    end

    test "raises InvalidOptionError when :keep is not a positive integer" do
      assert_raise Errors.InvalidOptionError, fn ->
        Window.new(HLL, [p: 10], every: 60_000, keep: 0)
      end
    end
  end

  describe "update/2 and update/3" do
    test "update/2 writes to the slot for the configured time_fn" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 5000 end)
      window = Window.update(window, "a")
      assert Window.slots(window) == [{5, Window.merged(window)}]
    end

    test "update/3 writes to the slot for the given explicit time" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      window = Window.update(window, "a", 5000)
      assert Enum.map(window.slots, fn {key, _} -> key end) == [5]
    end

    test "repeated updates within the same slot accumulate into one sketch" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      window = window |> Window.update("a", 500) |> Window.update("b", 999)
      assert map_size(window.slots) == 1
      assert HLL.estimate(window.slots[0]) >= 1.9
    end

    test "updates in different slots stay in separate slots" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      window = window |> Window.update("a", 500) |> Window.update("b", 1500)
      assert Map.keys(window.slots) |> Enum.sort() == [0, 1]
    end

    test "advancing time drops slots older than keep" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      window = Window.update(window, "a", 0)
      # now = 3999 -> current slot = 3, live range = 1..3, slot 0 expires
      window = Window.update(window, "b", 3999)
      assert Map.keys(window.slots) |> Enum.sort() == [3]
    end

    test "the current, still-filling slot counts toward keep" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 1)
      window = Window.update(window, "a", 0)
      window = Window.update(window, "b", 999)
      assert Map.keys(window.slots) == [0]

      window = Window.update(window, "c", 1000)
      assert Map.keys(window.slots) == [1]
    end
  end

  describe "update_many/2" do
    test "updates the current slot with every item at one point in time" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 0 end)
      window = Window.update_many(window, ["a", "b", "c"])
      assert map_size(window.slots) == 1
      assert_in_delta HLL.estimate(window.slots[0]), 3.0, 0.5
    end
  end

  describe "estimate/1" do
    test "returns 0.0 for an empty HLL window" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      assert Window.estimate(window) == 0.0
    end

    test "merges live slots before estimating" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 1500 end)
      window = window |> Window.update("a", 0) |> Window.update("b", 1500)
      assert_in_delta Window.estimate(window), 2.0, 0.5
    end

    test "excludes expired slots from the estimate" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 1, time_fn: fn -> 5000 end)
      window = Window.update(window, "a", 0)
      window = Window.update(window, "b", 5000)
      assert_in_delta Window.estimate(window), 1.0, 0.5
    end

    test "dispatches to count/1 for quantile families via KLL" do
      window = Window.new(KLL, [], every: 1000, keep: 3, time_fn: fn -> 0 end)
      window = Window.update(window, 1.0)
      assert Window.estimate(window) == 1
    end

    test "raises UnsupportedOperationError for CMS (no single-value estimate)" do
      window = Window.new(CMS, [], every: 1000, keep: 3)

      assert_raise Errors.UnsupportedOperationError, ~r/CMS/, fn ->
        Window.estimate(window)
      end
    end

    test "raises UnsupportedOperationError for a windowed membership filter" do
      window = Window.new(Bloom, [capacity: 100], every: 1000, keep: 3)

      assert_raise Errors.UnsupportedOperationError, ~r/Bloom/, fn ->
        Window.estimate(window)
      end
    end
  end

  describe "merged/1" do
    test "returns a fresh empty sketch when there are no live slots" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      merged = Window.merged(window)
      assert %HLL{} = merged
      assert HLL.estimate(merged) == 0.0
    end

    test "equals ExDataSketch.merge_many/1 of the live slot sketches" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 1500 end)
      window = window |> Window.update("a", 0) |> Window.update("b", 1500)

      expected = ExDataSketch.merge_many(Map.values(window.slots))
      assert HLL.estimate(Window.merged(window)) == HLL.estimate(expected)
    end
  end

  describe "tick/2" do
    test "drops expired slots without inserting an item" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 1)
      window = Window.update(window, "a", 0)
      window = Window.tick(window, 5000)
      assert window.slots == %{}
    end

    test "is a no-op when nothing has expired" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      window = Window.update(window, "a", 0)
      ticked = Window.tick(window, 500)
      assert ticked.slots == window.slots
    end
  end

  describe "slots/1" do
    test "returns live slots newest first" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 5, time_fn: fn -> 2999 end)

      window =
        window |> Window.update("a", 0) |> Window.update("b", 1500) |> Window.update("c", 2999)

      assert Enum.map(Window.slots(window), fn {key, _} -> key end) == [2, 1, 0]
    end

    test "returns an empty list for a fresh window" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      assert Window.slots(window) == []
    end
  end

  describe "size_bytes/1" do
    test "returns 0 for a fresh window" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      assert Window.size_bytes(window) == 0
    end

    test "sums size_bytes across every live slot" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 5, time_fn: fn -> 1500 end)
      window = window |> Window.update("a", 0) |> Window.update("b", 1500)

      expected =
        window
        |> Window.slots()
        |> Enum.map(fn {_key, sketch} -> HLL.size_bytes(sketch) end)
        |> Enum.sum()

      assert Window.size_bytes(window) == expected
      assert expected > 0
    end

    test "excludes expired slots" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 1, time_fn: fn -> 5000 end)
      window = Window.update(window, "a", 0)
      window = Window.update(window, "b", 5000)

      assert Window.size_bytes(window) == HLL.size_bytes(window.slots[5])
    end
  end

  describe "serialize/1 and deserialize/1" do
    test "round-trips module, sketch_opts, every, keep, and slot contents" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 1500 end)
      window = window |> Window.update("a", 0) |> Window.update("b", 1500)

      {:ok, restored} = window |> Window.serialize() |> Window.deserialize()

      assert restored.module == HLL
      assert restored.sketch_opts == [p: 10]
      assert restored.every == 1000
      assert restored.keep == 3
      assert Map.keys(restored.slots) |> Enum.sort() == [0, 1]
      assert HLL.estimate(restored.slots[0]) > 0.0
      assert HLL.estimate(restored.slots[1]) > 0.0
    end

    test "restores the default real-clock time_fn, not the original" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 0 end)
      {:ok, restored} = window |> Window.serialize() |> Window.deserialize()
      assert restored.time_fn.() != 0
      assert is_integer(restored.time_fn.())
    end

    test "drops expired slots at serialize time" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 1, time_fn: fn -> 5000 end)
      window = Window.update(window, "a", 0)
      window = Window.update(window, "b", 5000)

      {:ok, restored} = window |> Window.serialize() |> Window.deserialize()
      assert Map.keys(restored.slots) == [5]
    end

    test "round-trips an empty window" do
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      {:ok, restored} = window |> Window.serialize() |> Window.deserialize()
      assert restored.slots == %{}
    end

    test "round-trips a mergeable membership filter" do
      window = Window.new(Bloom, [capacity: 100], every: 1000, keep: 3, time_fn: fn -> 0 end)
      window = Window.update(window, "a")

      {:ok, restored} = window |> Window.serialize() |> Window.deserialize()
      assert Bloom.member?(restored.slots[0], "a")
    end

    test "returns a DeserializationError for garbage input" do
      assert {:error, %Errors.DeserializationError{}} = Window.deserialize(<<0, 1, 2>>)
    end

    test "returns a DeserializationError for a well-formed term that is not a Window envelope" do
      binary = :erlang.term_to_binary(%{not: :a_window})
      assert {:error, %Errors.DeserializationError{}} = Window.deserialize(binary)
    end

    test "returns an UnsupportedOperationError when the envelope names a non-mergeable module" do
      binary =
        :erlang.term_to_binary(%{
          module: Cuckoo,
          sketch_opts: [],
          every: 1000,
          keep: 3,
          slots: %{}
        })

      assert {:error, %Errors.UnsupportedOperationError{}} = Window.deserialize(binary)
    end

    test "returns a DeserializationError when a slot binary is corrupt" do
      binary =
        :erlang.term_to_binary(%{
          module: HLL,
          sketch_opts: [p: 10],
          every: 1000,
          keep: 3,
          slots: %{0 => <<0, 1, 2>>}
        })

      assert {:error, %Errors.DeserializationError{}} = Window.deserialize(binary)
    end
  end

  describe "[:ex_data_sketch, :window, :roll] telemetry" do
    test "emits when a slot is dropped, with slot_count, dropped_count, and oldest_age_ms" do
      test_pid = self()
      handler_id = "window-roll-test-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        Telemetry.event_name(:window, :roll),
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:roll, measurements, metadata})
        end,
        nil
      )

      window = Window.new(HLL, [p: 10], every: 1000, keep: 1)
      window = Window.update(window, "a", 0)
      Window.update(window, "b", 5000)

      assert_receive {:roll, measurements, metadata}
      assert measurements.dropped_count == 1
      assert measurements.slot_count == 1
      assert metadata.sketch_type == :hll
      assert metadata.oldest_age_ms == 0

      :telemetry.detach(handler_id)
    end

    test "does not emit when no slot is dropped" do
      test_pid = self()
      handler_id = "window-roll-test-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        Telemetry.event_name(:window, :roll),
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:roll, measurements, metadata})
        end,
        nil
      )

      window = Window.new(HLL, [p: 10], every: 1000, keep: 3)
      Window.update(window, "a", 0)

      refute_receive {:roll, _, _}, 50

      :telemetry.detach(handler_id)
    end

    test "estimate/1 and merged/1 do not emit :roll (transient read, no mutation)" do
      test_pid = self()
      handler_id = "window-roll-test-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        Telemetry.event_name(:window, :roll),
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:roll, measurements, metadata})
        end,
        nil
      )

      window = Window.new(HLL, [p: 10], every: 1000, keep: 1, time_fn: fn -> 5000 end)
      window = %{window | slots: %{0 => HLL.new(p: 10)}}

      Window.estimate(window)
      Window.merged(window)
      Window.slots(window)

      refute_receive {:roll, _, _}, 50

      :telemetry.detach(handler_id)
    end
  end
end
