defmodule ExDataSketch.FrequentItemsPropertiesTest do
  @moduledoc """
  StreamData property tests for FrequentItems (SpaceSaving) sketch.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.FrequentItems

  @max_runs 50

  defp k_gen do
    member_of([4, 8, 16, 32])
  end

  defp binary_key_gen do
    binary(min_length: 1, max_length: 8)
  end

  defp dataset_gen(max_length) do
    list_of(binary_key_gen(), max_length: max_length)
  end

  defp nonempty_dataset_gen(max_length) do
    list_of(binary_key_gen(), min_length: 1, max_length: max_length)
  end

  describe "FrequentItems properties" do
    property "determinism: identical inputs produce identical serialization" do
      check all(
              k <- k_gen(),
              items <- nonempty_dataset_gen(100),
              max_runs: @max_runs
            ) do
        s1 = FrequentItems.from_enumerable(items, k: k)
        s2 = FrequentItems.from_enumerable(items, k: k)
        assert FrequentItems.serialize(s1) == FrequentItems.serialize(s2)
      end
    end

    property "commutativity: merge(a,b) == merge(b,a)" do
      check all(
              k <- k_gen(),
              items_a <- nonempty_dataset_gen(50),
              items_b <- nonempty_dataset_gen(50),
              max_runs: @max_runs
            ) do
        sa = FrequentItems.from_enumerable(items_a, k: k)
        sb = FrequentItems.from_enumerable(items_b, k: k)

        ab = FrequentItems.merge(sa, sb)
        ba = FrequentItems.merge(sb, sa)

        assert FrequentItems.serialize(ab) == FrequentItems.serialize(ba)
      end
    end

    property "associativity: merge(merge(a,b),c) preserves count like merge(a,merge(b,c))" do
      check all(
              k <- k_gen(),
              items_a <- nonempty_dataset_gen(30),
              items_b <- nonempty_dataset_gen(30),
              items_c <- nonempty_dataset_gen(30),
              max_runs: @max_runs
            ) do
        sa = FrequentItems.from_enumerable(items_a, k: k)
        sb = FrequentItems.from_enumerable(items_b, k: k)
        sc = FrequentItems.from_enumerable(items_c, k: k)

        left = FrequentItems.merge(FrequentItems.merge(sa, sb), sc)
        right = FrequentItems.merge(sa, FrequentItems.merge(sb, sc))

        # Count is always exactly additive regardless of grouping
        assert FrequentItems.count(left) == FrequentItems.count(right)
        # Entry count bounded by k
        assert FrequentItems.entry_count(left) <= k
        assert FrequentItems.entry_count(right) <= k
      end
    end

    property "identity: merge(empty, s) == s" do
      check all(
              k <- k_gen(),
              items <- nonempty_dataset_gen(100),
              max_runs: @max_runs
            ) do
        sketch = FrequentItems.from_enumerable(items, k: k)
        empty = FrequentItems.new(k: k)

        merged_right = FrequentItems.merge(sketch, empty)
        merged_left = FrequentItems.merge(empty, sketch)

        assert FrequentItems.serialize(merged_right) == FrequentItems.serialize(sketch)
        assert FrequentItems.serialize(merged_left) == FrequentItems.serialize(sketch)
      end
    end

    property "size invariant: entry_count <= k" do
      check all(
              k <- k_gen(),
              items <- dataset_gen(200),
              max_runs: @max_runs
            ) do
        sketch = FrequentItems.from_enumerable(items, k: k)
        assert FrequentItems.entry_count(sketch) <= k
      end
    end

    property "count conservation: count(sketch) == length(dataset)" do
      check all(
              k <- k_gen(),
              items <- dataset_gen(200),
              max_runs: @max_runs
            ) do
        sketch = FrequentItems.from_enumerable(items, k: k)
        assert FrequentItems.count(sketch) == length(items)
      end
    end

    property "serialization stability: serialize(deserialize(serialize(s))) == serialize(s)" do
      check all(
              k <- k_gen(),
              items <- nonempty_dataset_gen(100),
              max_runs: @max_runs
            ) do
        sketch = FrequentItems.from_enumerable(items, k: k)
        bin1 = FrequentItems.serialize(sketch)
        {:ok, restored} = FrequentItems.deserialize(bin1)
        bin2 = FrequentItems.serialize(restored)
        assert bin1 == bin2
      end
    end
  end
end
