defmodule ExDataSketch.BloomTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Bloom

  # Deterministic test data
  @items_100 Enum.map(0..99, &"bloom_item_#{&1}")

  describe "new/1" do
    test "default options" do
      bloom = Bloom.new()
      assert bloom.opts[:capacity] == 10_000
      assert bloom.opts[:false_positive_rate] == 0.01
      assert bloom.opts[:seed] == 0
      assert bloom.opts[:bit_count] > 0
      assert bloom.opts[:hash_count] > 0
      assert is_binary(bloom.state)
    end

    test "custom capacity and FPR" do
      bloom = Bloom.new(capacity: 1000, false_positive_rate: 0.001)
      assert bloom.opts[:capacity] == 1000
      assert bloom.opts[:false_positive_rate] == 0.001
      assert bloom.opts[:bit_count] > 0
      assert bloom.opts[:hash_count] > 0
    end

    test "struct fields" do
      bloom = Bloom.new()
      assert %Bloom{state: _, opts: _, backend: _} = bloom
      assert bloom.backend == ExDataSketch.Backend.Pure
    end

    test "invalid capacity raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(capacity: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(capacity: -1)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(capacity: "abc")
      end
    end

    test "capacity exceeding u32 raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/u32/, fn ->
        Bloom.new(capacity: 0xFFFFFFFF + 1)
      end
    end

    test "capacity that overflows bit_count u32 raises" do
      # 500M items at 1% FPR requires ~4.8 billion bits, exceeding u32
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/exceeds the u32 maximum/, fn ->
        Bloom.new(capacity: 500_000_000, false_positive_rate: 0.01)
      end
    end

    test "invalid FPR raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(false_positive_rate: 0.0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(false_positive_rate: 1.0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(false_positive_rate: -0.1)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Bloom.new(false_positive_rate: 1.5)
      end
    end

    test "seed option is stored" do
      bloom = Bloom.new(seed: 42)
      assert bloom.opts[:seed] == 42
    end
  end

  describe "put/2 and member?/2" do
    test "inserted item is a member" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("hello")
      assert Bloom.member?(bloom, "hello")
    end

    test "non-inserted item is not a member" do
      bloom = Bloom.new(capacity: 100)
      refute Bloom.member?(bloom, "hello")
    end

    test "put is idempotent" do
      bloom =
        Bloom.new(capacity: 100)
        |> Bloom.put("hello")
        |> Bloom.put("hello")

      assert Bloom.member?(bloom, "hello")
    end

    test "various types work" do
      bloom =
        Bloom.new(capacity: 100)
        |> Bloom.put("string")
        |> Bloom.put(42)
        |> Bloom.put(:atom)
        |> Bloom.put({:tuple, 1})

      assert Bloom.member?(bloom, "string")
      assert Bloom.member?(bloom, 42)
      assert Bloom.member?(bloom, :atom)
      assert Bloom.member?(bloom, {:tuple, 1})
    end
  end

  describe "put_many/2" do
    test "all items become members" do
      bloom = Bloom.new(capacity: 1000) |> Bloom.put_many(@items_100)
      assert Enum.all?(@items_100, &Bloom.member?(bloom, &1))
    end

    test "empty list is no-op" do
      bloom = Bloom.new(capacity: 100)
      bloom2 = Bloom.put_many(bloom, [])
      assert Bloom.serialize(bloom) == Bloom.serialize(bloom2)
    end

    test "put_many equivalence with sequential put" do
      items = ["a", "b", "c", "d", "e"]
      a = Bloom.new(capacity: 100) |> Bloom.put_many(items)
      b = Enum.reduce(items, Bloom.new(capacity: 100), &Bloom.put(&2, &1))
      assert Bloom.serialize(a) == Bloom.serialize(b)
    end
  end

  describe "merge/2" do
    test "union semantics" do
      a = Bloom.new(capacity: 100) |> Bloom.put("x")
      b = Bloom.new(capacity: 100) |> Bloom.put("y")
      merged = Bloom.merge(a, b)
      assert Bloom.member?(merged, "x")
      assert Bloom.member?(merged, "y")
    end

    test "merge with empty is identity" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put_many(["a", "b", "c"])
      empty = Bloom.new(capacity: 100)
      assert Bloom.serialize(Bloom.merge(bloom, empty)) == Bloom.serialize(bloom)
    end

    test "merge commutativity" do
      a = Bloom.new(capacity: 100) |> Bloom.put_many(["a", "b"])
      b = Bloom.new(capacity: 100) |> Bloom.put_many(["c", "d"])
      assert Bloom.serialize(Bloom.merge(a, b)) == Bloom.serialize(Bloom.merge(b, a))
    end

    test "merge associativity" do
      a = Bloom.new(capacity: 100) |> Bloom.put("a")
      b = Bloom.new(capacity: 100) |> Bloom.put("b")
      c = Bloom.new(capacity: 100) |> Bloom.put("c")

      left = Bloom.merge(Bloom.merge(a, b), c)
      right = Bloom.merge(a, Bloom.merge(b, c))
      assert Bloom.serialize(left) == Bloom.serialize(right)
    end

    test "incompatible bit_count raises" do
      a = Bloom.new(capacity: 100)
      b = Bloom.new(capacity: 1000)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/bit_count/, fn ->
        Bloom.merge(a, b)
      end
    end

    test "incompatible seed raises" do
      a = Bloom.new(capacity: 100, seed: 0)
      b = Bloom.new(capacity: 100, seed: 99)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/seed/, fn ->
        Bloom.merge(a, b)
      end
    end

    test "merge_many" do
      filters =
        Enum.map(0..4, fn i ->
          Bloom.new(capacity: 100) |> Bloom.put("item_#{i}")
        end)

      merged = Bloom.merge_many(filters)
      assert Enum.all?(0..4, &Bloom.member?(merged, "item_#{&1}"))
    end
  end

  describe "serialize/deserialize" do
    test "round-trip" do
      bloom = Bloom.new(capacity: 1000) |> Bloom.put_many(@items_100)
      bin = Bloom.serialize(bloom)
      assert {:ok, recovered} = Bloom.deserialize(bin)
      assert Bloom.serialize(recovered) == bin
      assert Enum.all?(@items_100, &Bloom.member?(recovered, &1))
    end

    test "EXSK envelope has correct sketch ID" do
      bloom = Bloom.new(capacity: 100)
      <<"EXSK", _version::8, sketch_id::8, _rest::binary>> = Bloom.serialize(bloom)
      assert sketch_id == 7
    end

    test "BLM1 magic in state binary" do
      bloom = Bloom.new(capacity: 100)
      <<"BLM1", _rest::binary>> = bloom.state
    end

    test "wrong sketch ID returns error" do
      bloom = Bloom.new(capacity: 100)
      bin = Bloom.serialize(bloom)
      # Corrupt sketch ID byte (offset 5)
      <<prefix::binary-size(5), _id::8, rest::binary>> = bin
      corrupted = <<prefix::binary, 1::8, rest::binary>>
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} = Bloom.deserialize(corrupted)
    end

    test "invalid binary returns error" do
      assert {:error, _} = Bloom.deserialize(<<"invalid">>)
    end

    test "truncated binary returns error" do
      assert {:error, _} = Bloom.deserialize(<<1, 2>>)
    end
  end

  describe "introspection" do
    test "capacity returns configured value" do
      assert Bloom.new(capacity: 5000) |> Bloom.capacity() == 5000
    end

    test "error_rate returns configured FPR" do
      assert Bloom.new(false_positive_rate: 0.05) |> Bloom.error_rate() == 0.05
    end

    test "count is 0 for empty filter" do
      assert Bloom.new(capacity: 100) |> Bloom.count() == 0
    end

    test "count is positive after insertions" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("hello")
      assert Bloom.count(bloom) > 0
    end

    test "size_bytes matches expected" do
      bloom = Bloom.new(capacity: 100)
      bit_count = bloom.opts[:bit_count]
      expected_bitset_bytes = div(bit_count + 7, 8)
      # 40 byte header + bitset
      assert Bloom.size_bytes(bloom) == 40 + expected_bitset_bytes
    end
  end

  describe "convenience functions" do
    test "from_enumerable" do
      bloom = Bloom.from_enumerable(["a", "b", "c"], capacity: 100)
      assert Bloom.member?(bloom, "a")
      assert Bloom.member?(bloom, "b")
      assert Bloom.member?(bloom, "c")
    end

    test "reducer" do
      reducer = Bloom.reducer()
      bloom = Enum.reduce(["a", "b"], Bloom.new(capacity: 100), reducer)
      assert Bloom.member?(bloom, "a")
    end

    test "merger" do
      merger = Bloom.merger()
      a = Bloom.new(capacity: 100) |> Bloom.put("x")
      b = Bloom.new(capacity: 100) |> Bloom.put("y")
      merged = merger.(a, b)
      assert Bloom.member?(merged, "x")
      assert Bloom.member?(merged, "y")
    end
  end

  describe "facade dispatch" do
    test "ExDataSketch.update_many dispatches to Bloom.put_many" do
      bloom = Bloom.new(capacity: 100)
      updated = ExDataSketch.update_many(bloom, ["a", "b"])
      assert Bloom.member?(updated, "a")
    end
  end

  describe "Codec integration" do
    test "sketch_id_bloom is 7" do
      assert ExDataSketch.Codec.sketch_id_bloom() == 7
    end
  end

  describe "statistical validation" do
    test "false positive rate is within 2x of target" do
      capacity = 10_000
      fpr = 0.01

      bloom =
        Bloom.new(capacity: capacity, false_positive_rate: fpr)
        |> Bloom.put_many(1..capacity)

      # Test 100,000 non-inserted items
      test_items = (capacity + 1)..(capacity + 100_000)
      false_positives = Enum.count(test_items, &Bloom.member?(bloom, &1))
      observed_fpr = false_positives / 100_000

      assert observed_fpr < fpr * 2.0,
             "Observed FPR #{observed_fpr} exceeds 2x target #{fpr}"
    end

    test "empty filter has zero false positives" do
      bloom = Bloom.new(capacity: 1000)
      assert Enum.all?(1..10_000, fn i -> not Bloom.member?(bloom, i) end)
    end
  end

  describe "property tests" do
    property "no false negatives" do
      check all(items <- list_of(binary(), min_length: 1, max_length: 50)) do
        bloom = Bloom.new(capacity: 1000) |> Bloom.put_many(items)
        assert Enum.all?(items, &Bloom.member?(bloom, &1))
      end
    end

    property "merge commutativity" do
      check all(
              items_a <- list_of(binary(), max_length: 20),
              items_b <- list_of(binary(), max_length: 20)
            ) do
        a = Bloom.new(capacity: 1000) |> Bloom.put_many(items_a)
        b = Bloom.new(capacity: 1000) |> Bloom.put_many(items_b)
        assert Bloom.serialize(Bloom.merge(a, b)) == Bloom.serialize(Bloom.merge(b, a))
      end
    end

    property "merge identity" do
      check all(items <- list_of(binary(), max_length: 20)) do
        bloom = Bloom.new(capacity: 1000) |> Bloom.put_many(items)
        empty = Bloom.new(capacity: 1000)
        assert Bloom.serialize(Bloom.merge(bloom, empty)) == Bloom.serialize(bloom)
      end
    end

    property "serialization round-trip" do
      check all(items <- list_of(binary(), max_length: 20)) do
        bloom = Bloom.new(capacity: 1000) |> Bloom.put_many(items)
        bin = Bloom.serialize(bloom)
        {:ok, recovered} = Bloom.deserialize(bin)
        assert Bloom.serialize(recovered) == bin
      end
    end

    property "count monotonicity" do
      check all(
              items_a <- list_of(binary(), min_length: 1, max_length: 20),
              items_b <- list_of(binary(), min_length: 1, max_length: 20)
            ) do
        bloom_a = Bloom.new(capacity: 1000) |> Bloom.put_many(items_a)
        bloom_ab = Bloom.put_many(bloom_a, items_b)
        assert Bloom.count(bloom_ab) >= Bloom.count(bloom_a)
      end
    end
  end
end
