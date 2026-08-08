defmodule ExDataSketch.FlowTest do
  use ExUnit.Case, async: true

  describe "reduce/3" do
    test "produces partition-local sketches" do
      sketches =
        1..100
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 10)
        |> Enum.to_list()

      assert is_list(sketches)
      assert sketches != []

      Enum.each(sketches, fn sketch ->
        assert %ExDataSketch.HLL{} = sketch
      end)
    end

    test "partition-local sketches produce valid cardinality estimates" do
      sketches =
        1..100
        |> Stream.map(&to_string/1)
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 10)
        |> Enum.to_list()

      merged = ExDataSketch.HLL.merge_many(sketches)
      assert ExDataSketch.HLL.estimate(merged) > 0.0
    end
  end

  describe "merge/2" do
    test "merges partition-local sketches into single sketch" do
      final =
        1..100
        |> Stream.map(&to_string/1)
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 10)
        |> ExDataSketch.Flow.merge(ExDataSketch.HLL)

      assert %ExDataSketch.HLL{} = final
      assert ExDataSketch.HLL.estimate(final) > 0.0
    end

    test "merged result approximates single-pass result" do
      items = Enum.map(1..500, &to_string/1)

      single_pass = ExDataSketch.HLL.from_enumerable(items, p: 12)

      flow_result =
        items
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 12)
        |> ExDataSketch.Flow.merge(ExDataSketch.HLL)

      single_est = ExDataSketch.HLL.estimate(single_pass)
      flow_est = ExDataSketch.HLL.estimate(flow_result)

      rel_error = abs(single_est - flow_est) / max(1.0, single_est)
      assert rel_error < 0.10
    end

    test "CMS merge produces valid frequency estimates" do
      items = Enum.map(1..200, fn i -> "item_#{rem(i, 20)}" end)

      final =
        items
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> ExDataSketch.Flow.reduce(ExDataSketch.CMS, width: 128, depth: 3)
        |> ExDataSketch.Flow.merge(ExDataSketch.CMS)

      assert %ExDataSketch.CMS{} = final
      assert ExDataSketch.CMS.estimate(final, "item_1") >= 1
    end
  end

  describe "into/3" do
    test "collects flow items into a sketch" do
      sketch =
        1..100
        |> Flow.from_enumerable()
        |> ExDataSketch.Flow.into(ExDataSketch.HLL, p: 10)

      assert %ExDataSketch.HLL{} = sketch
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "into result matches from_enumerable" do
      items = Enum.map(1..200, &to_string/1)

      flow_result =
        items
        |> Flow.from_enumerable()
        |> ExDataSketch.Flow.into(ExDataSketch.HLL, p: 12)

      direct_result = ExDataSketch.HLL.from_enumerable(items, p: 12)

      assert_in_delta ExDataSketch.HLL.estimate(flow_result),
                      ExDataSketch.HLL.estimate(direct_result),
                      0.01
    end
  end
end
