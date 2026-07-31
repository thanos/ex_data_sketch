defmodule ExDataSketch.SerializationStabilityTest do
  @moduledoc """
  Phase 6: Serialization stability round-trip properties.

  Every sketch type must round-trip through serialize/deserialize with
  the estimate (or membership) preserved exactly or within floating-point
  tolerance.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.{Bloom, CMS, HLL, Theta, ULL}

  defp string_list(min_len, max_len, list_min, list_max) do
    StreamData.list_of(
      StreamData.string(:alphanumeric, min_length: min_len, max_length: max_len),
      length: list_min..list_max
    )
  end

  describe "HLL serialization round-trip" do
    property "v2 serialize/deserialize preserves estimate" do
      check all(items <- string_list(1, 10, 10, 200)) do
        sketch = HLL.from_enumerable(items, p: 10)
        bin = HLL.serialize(sketch)
        {:ok, restored} = HLL.deserialize(bin)
        assert_in_delta HLL.estimate(restored), HLL.estimate(sketch), 0.01
      end
    end

    property "v1 serialize/deserialize round-trips for :phash2" do
      check all(items <- string_list(1, 10, 10, 100)) do
        sketch = HLL.from_enumerable(items, p: 10, hash_strategy: :phash2)
        v1_bin = HLL.serialize(sketch, format: :v1)
        {:ok, restored} = HLL.deserialize(v1_bin)
        assert_in_delta HLL.estimate(restored), HLL.estimate(sketch), 0.01
      end
    end
  end

  describe "ULL serialization round-trip" do
    property "serialize/deserialize preserves estimate" do
      check all(items <- string_list(1, 10, 10, 200)) do
        sketch = ULL.from_enumerable(items, p: 10)
        bin = ULL.serialize(sketch)
        {:ok, restored} = ULL.deserialize(bin)
        assert_in_delta ULL.estimate(restored), ULL.estimate(sketch), 0.01
      end
    end
  end

  describe "CMS serialization round-trip" do
    property "serialize/deserialize preserves counts" do
      check all(items <- string_list(1, 5, 10, 100)) do
        sketch = CMS.from_enumerable(items, width: 128, depth: 5)
        bin = CMS.serialize(sketch)
        {:ok, restored} = CMS.deserialize(bin)
        test_item = Enum.random(items)
        assert CMS.estimate(restored, test_item) == CMS.estimate(sketch, test_item)
      end
    end
  end

  describe "Theta serialization round-trip" do
    property "serialize/deserialize preserves estimate" do
      check all(items <- string_list(1, 10, 10, 200)) do
        sketch = Theta.from_enumerable(items, delta: 0.01)
        bin = Theta.serialize(sketch)
        {:ok, restored} = Theta.deserialize(bin)
        assert_in_delta Theta.estimate(restored), Theta.estimate(sketch), 0.01
      end
    end
  end

  describe "Bloom serialization round-trip" do
    property "serialize/deserialize preserves membership" do
      check all(items <- string_list(1, 10, 5, 50)) do
        sketch = Bloom.from_enumerable(items, capacity: 100)
        bin = Bloom.serialize(sketch)
        {:ok, restored} = Bloom.deserialize(bin)
        test_item = Enum.random(items)
        assert Bloom.member?(restored, test_item) == Bloom.member?(sketch, test_item)
      end
    end
  end

  describe "Cross-version v1/v2 compatibility" do
    property "v2 round-trip preserves v1-decoded sketch state" do
      check all(items <- string_list(1, 10, 10, 100)) do
        sketch = HLL.from_enumerable(items, p: 10, hash_strategy: :phash2)
        v1_bin = HLL.serialize(sketch, format: :v1)
        {:ok, from_v1} = HLL.deserialize(v1_bin)
        v2_bin = HLL.serialize(from_v1)
        {:ok, from_v2} = HLL.deserialize(v2_bin)
        assert_in_delta HLL.estimate(from_v2), HLL.estimate(sketch), 0.01
      end
    end
  end
end
