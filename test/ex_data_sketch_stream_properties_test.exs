defmodule ExDataSketch.StreamPropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Stream, as: S

  defp string_list(min_len, max_len, list_min, list_max) do
    StreamData.list_of(
      StreamData.string(:alphanumeric, min_length: min_len, max_length: max_len),
      length: list_min..list_max
    )
  end

  property "HLL: Stream.hll/2 matches HLL.from_enumerable/2" do
    check all(items <- string_list(1, 10, 10, 200)) do
      stream_sketch = S.hll(items, p: 12)
      direct_sketch = ExDataSketch.HLL.from_enumerable(items, p: 12)

      stream_est = ExDataSketch.HLL.estimate(stream_sketch)
      direct_est = ExDataSketch.HLL.estimate(direct_sketch)

      assert_in_delta stream_est, direct_est, 0.01
    end
  end

  property "HLL: reduce_into/3 matches HLL.from_enumerable/2" do
    check all(items <- string_list(1, 10, 10, 200)) do
      reduce_sketch = S.reduce_into(items, ExDataSketch.HLL, p: 12)
      direct_sketch = ExDataSketch.HLL.from_enumerable(items, p: 12)

      assert_in_delta ExDataSketch.HLL.estimate(reduce_sketch),
                      ExDataSketch.HLL.estimate(direct_sketch),
                      0.01
    end
  end

  property "HLL: reduce_partitioned/3 approximates from_enumerable/2" do
    check all(items <- string_list(1, 10, 50, 500)) do
      single = ExDataSketch.HLL.from_enumerable(items, p: 12)
      partitioned = S.reduce_partitioned(items, ExDataSketch.HLL, partitions: 4, p: 12)

      single_est = ExDataSketch.HLL.estimate(single)
      part_est = ExDataSketch.HLL.estimate(partitioned)

      rel_error = abs(single_est - part_est) / max(1.0, single_est)
      assert rel_error < 0.10
    end
  end

  property "HLL: Collectable matches from_enumerable/2" do
    check all(items <- string_list(1, 10, 10, 200)) do
      collectable_sketch = Enum.into(items, ExDataSketch.HLL.new(p: 12))
      direct_sketch = ExDataSketch.HLL.from_enumerable(items, p: 12)

      assert_in_delta ExDataSketch.HLL.estimate(collectable_sketch),
                      ExDataSketch.HLL.estimate(direct_sketch),
                      0.01
    end
  end

  property "CMS: Stream.cms/2 matches CMS.from_enumerable/2 for counts" do
    check all(items <- string_list(1, 5, 20, 200)) do
      stream_sketch = S.cms(items, width: 128, depth: 3)
      direct_sketch = ExDataSketch.CMS.from_enumerable(items, width: 128, depth: 3)

      test_item = Enum.random(items)

      assert ExDataSketch.CMS.estimate(stream_sketch, test_item) ==
               ExDataSketch.CMS.estimate(direct_sketch, test_item)
    end
  end

  property "CMS: Collectable matches from_enumerable/2 for counts" do
    check all(items <- string_list(1, 5, 20, 200)) do
      collectable_sketch = Enum.into(items, ExDataSketch.CMS.new(width: 128, depth: 3))
      direct_sketch = ExDataSketch.CMS.from_enumerable(items, width: 128, depth: 3)

      test_item = Enum.random(items)

      assert ExDataSketch.CMS.estimate(collectable_sketch, test_item) ==
               ExDataSketch.CMS.estimate(direct_sketch, test_item)
    end
  end

  property "Bloom: Collectable matches from_enumerable/2 for membership" do
    check all(items <- string_list(1, 10, 10, 100)) do
      cap = max(length(items) * 2, 10)
      collectable_sketch = Enum.into(items, ExDataSketch.Bloom.new(capacity: cap))
      direct_sketch = ExDataSketch.Bloom.from_enumerable(items, capacity: cap)

      test_item = Enum.random(items)

      assert ExDataSketch.Bloom.member?(collectable_sketch, test_item) ==
               ExDataSketch.Bloom.member?(direct_sketch, test_item)
    end
  end

  property "HLL: Collectable with pre-populated sketch accumulates" do
    check all(items <- string_list(1, 10, 10, 100)) do
      pre = ExDataSketch.HLL.new(p: 12) |> ExDataSketch.HLL.update("pre_populated")
      collected = Enum.into(items, pre)

      pre_est = ExDataSketch.HLL.estimate(pre)
      collected_est = ExDataSketch.HLL.estimate(collected)

      assert collected_est > pre_est
    end
  end

  property "HLL: merge associativity holds across partitioned streams" do
    check all(items <- string_list(1, 10, 50, 300)) do
      third = div(length(items), 3)
      {a_items, rest} = Enum.split(items, third)
      {b_items, c_items} = Enum.split(rest, third)

      s1 = ExDataSketch.HLL.from_enumerable(a_items, p: 12)
      s2 = ExDataSketch.HLL.from_enumerable(b_items, p: 12)
      s3 = ExDataSketch.HLL.from_enumerable(c_items, p: 12)

      left = ExDataSketch.HLL.merge(ExDataSketch.HLL.merge(s1, s2), s3)
      right = ExDataSketch.HLL.merge(s1, ExDataSketch.HLL.merge(s2, s3))

      assert_in_delta ExDataSketch.HLL.estimate(left), ExDataSketch.HLL.estimate(right), 0.01
    end
  end

  property "CMS: reduce_partitioned/3 produces valid estimates" do
    check all(items <- string_list(1, 5, 30, 200)) do
      partitioned =
        S.reduce_partitioned(items, ExDataSketch.CMS, partitions: 3, width: 128, depth: 3)

      test_item = Enum.random(items)
      assert ExDataSketch.CMS.estimate(partitioned, test_item) >= 1
    end
  end
end
