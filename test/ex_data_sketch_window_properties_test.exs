defmodule ExDataSketch.WindowPropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.{HLL, Window}

  defp string_list(min_len, max_len, list_min, list_max) do
    StreamData.list_of(
      StreamData.string(:alphanumeric, min_length: min_len, max_length: max_len),
      length: list_min..list_max
    )
  end

  # Window's documented contract (see the moduledoc's "Clock" section) is
  # that explicit `now` values passed across calls are non-decreasing,
  # mirroring System.monotonic_time/1. Properties below sort generated
  # offsets before applying them so they stay within that contract.

  property "estimate is monotone under insertions within a single slot" do
    check all(items <- string_list(1, 10, 1, 30)) do
      window = Window.new(HLL, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)

      estimates =
        Enum.scan(items, window, fn item, w -> Window.update(w, item) end)
        |> Enum.map(&Window.estimate/1)

      estimates
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [earlier, later] -> assert later >= earlier end)
    end
  end

  property "window estimate never exceeds the all-time (unwindowed) estimate" do
    check all(
            items <- string_list(1, 10, 1, 30),
            slot_offsets <-
              StreamData.list_of(StreamData.integer(0..10), length: length(items))
          ) do
      sorted_offsets = Enum.sort(slot_offsets)
      last_now = Enum.max(slot_offsets, fn -> 0 end) * 1000

      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> last_now end)
      all_time = HLL.new(p: 10) |> HLL.update_many(items)

      final_window =
        Enum.zip(items, sorted_offsets)
        |> Enum.reduce(window, fn {item, offset}, w -> Window.update(w, item, offset * 1000) end)

      assert Window.estimate(final_window) <= HLL.estimate(all_time)
    end
  end

  property "expiry drops exactly the slots outside the live keep-range" do
    check all(
            slot_keys <-
              StreamData.list_of(StreamData.integer(0..20), min_length: 1, max_length: 15),
            keep <- StreamData.integer(1..5)
          ) do
      sorted_keys = Enum.sort(slot_keys)
      window = Window.new(HLL, [p: 10], every: 1000, keep: keep)

      window =
        Enum.reduce(sorted_keys, window, fn key, w -> Window.update(w, "x", key * 1000) end)

      current = Enum.max(slot_keys)
      oldest_live = current - keep + 1

      expected_live_keys =
        sorted_keys
        |> Enum.uniq()
        |> Enum.filter(&(&1 >= oldest_live and &1 <= current))
        |> Enum.sort()

      assert Map.keys(window.slots) |> Enum.sort() == expected_live_keys
    end
  end

  property "merged/1 equals ExDataSketch.merge_many/1 of the live slot sketches" do
    check all(
            items <- string_list(1, 10, 1, 20),
            slot_offsets <- StreamData.list_of(StreamData.integer(0..5), length: length(items))
          ) do
      sorted_offsets = Enum.sort(slot_offsets)
      window = Window.new(HLL, [p: 10], every: 1000, keep: 3, time_fn: fn -> 5000 end)

      window =
        Enum.zip(items, sorted_offsets)
        |> Enum.reduce(window, fn {item, offset}, w -> Window.update(w, item, offset * 1000) end)

      expected =
        case window |> Window.slots() |> Enum.map(&elem(&1, 1)) do
          [] -> HLL.new(p: 10)
          sketches -> ExDataSketch.merge_many(sketches)
        end

      assert HLL.estimate(Window.merged(window)) == HLL.estimate(expected)
    end
  end
end
