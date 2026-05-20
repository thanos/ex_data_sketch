defmodule ExDataSketch.BroadwayTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Broadway

  describe "accumulate/3" do
    test "builds HLL from message data" do
      messages = [%{data: "a"}, %{data: "b"}, %{data: "a"}]
      sketch = Broadway.accumulate(messages, ExDataSketch.HLL, p: 10)
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "builds HLL with custom key_fn" do
      messages = [%{user_id: "alice"}, %{user_id: "bob"}, %{user_id: "alice"}]

      sketch =
        Broadway.accumulate(messages, ExDataSketch.HLL, p: 10, key_fn: fn msg -> msg.user_id end)

      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "builds CMS from message data" do
      messages = [%{data: "page_a"}, %{data: "page_a"}, %{data: "page_b"}]
      sketch = Broadway.accumulate(messages, ExDataSketch.CMS, width: 64, depth: 3)
      assert ExDataSketch.CMS.estimate(sketch, "page_a") >= 2
    end

    test "empty messages produce empty sketch" do
      sketch = Broadway.accumulate([], ExDataSketch.HLL, p: 10)
      assert ExDataSketch.HLL.estimate(sketch) == 0.0
    end

    test "accumulate result matches from_enumerable" do
      messages = [%{data: "x"}, %{data: "y"}, %{data: "z"}]
      values = Enum.map(messages, fn msg -> msg.data end)

      from_broadway = Broadway.accumulate(messages, ExDataSketch.HLL, p: 12)
      from_direct = ExDataSketch.HLL.from_enumerable(values, p: 12)

      assert_in_delta ExDataSketch.HLL.estimate(from_broadway),
                      ExDataSketch.HLL.estimate(from_direct),
                      0.01
    end
  end

  describe "accumulate_into/3" do
    test "merges batch into existing sketch" do
      existing = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("existing")
      messages = [%{data: "a"}, %{data: "b"}]

      result = Broadway.accumulate_into(messages, existing)
      assert ExDataSketch.HLL.estimate(result) >= 3
    end

    test "preserves existing data" do
      existing = ExDataSketch.HLL.from_enumerable(["keep_me"], p: 10)
      messages = [%{data: "new_item"}]

      result = Broadway.accumulate_into(messages, existing)
      assert ExDataSketch.HLL.estimate(result) > 1
    end
  end

  describe "PeriodicAggregator" do
    test "starts and accepts merges" do
      {:ok, agg} =
        Broadway.PeriodicAggregator.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          flush_interval: :infinity
        )

      partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      :ok = Broadway.PeriodicAggregator.merge(agg, partial)

      assert Broadway.PeriodicAggregator.estimate(agg) > 0.0
    end

    test "flush returns accumulated sketch and resets" do
      {:ok, agg} =
        Broadway.PeriodicAggregator.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          flush_interval: :infinity
        )

      partial = ExDataSketch.HLL.from_enumerable(["a", "b", "c"], p: 10)
      :ok = Broadway.PeriodicAggregator.merge(agg, partial)

      flushed = Broadway.PeriodicAggregator.flush(agg)
      assert ExDataSketch.HLL.estimate(flushed) > 0.0

      assert Broadway.PeriodicAggregator.estimate(agg) == 0.0
    end

    test "get returns current without resetting" do
      {:ok, agg} =
        Broadway.PeriodicAggregator.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          flush_interval: :infinity
        )

      partial = ExDataSketch.HLL.from_enumerable(["a"], p: 10)
      :ok = Broadway.PeriodicAggregator.merge(agg, partial)

      current = Broadway.PeriodicAggregator.get(agg)
      assert ExDataSketch.HLL.estimate(current) > 0.0

      assert Broadway.PeriodicAggregator.estimate(agg) > 0.0
    end

    test "multiple merges accumulate correctly" do
      {:ok, agg} =
        Broadway.PeriodicAggregator.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          flush_interval: :infinity
        )

      partial1 = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      partial2 = ExDataSketch.HLL.from_enumerable(["c", "d"], p: 10)

      :ok = Broadway.PeriodicAggregator.merge(agg, partial1)
      :ok = Broadway.PeriodicAggregator.merge(agg, partial2)

      assert Broadway.PeriodicAggregator.estimate(agg) >= 3.0
    end

    test "supports named start" do
      {:ok, agg} =
        Broadway.PeriodicAggregator.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          flush_interval: :infinity,
          name: :test_agg_named
        )

      assert is_pid(agg)
      Broadway.PeriodicAggregator.get(:test_agg_named)
    after
      GenServer.stop(:test_agg_named)
    end
  end
end
