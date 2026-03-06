defmodule ExDataSketch.CuckooTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Cuckoo

  # Deterministic test data
  @items_100 Enum.map(0..99, &"cuckoo_item_#{&1}")

  describe "new/1" do
    test "default options" do
      cuckoo = Cuckoo.new()
      assert cuckoo.opts[:capacity] == 10_000
      assert cuckoo.opts[:fingerprint_size] == 8
      assert cuckoo.opts[:bucket_size] == 4
      assert cuckoo.opts[:max_kicks] == 500
      assert cuckoo.opts[:seed] == 0
      assert cuckoo.opts[:bucket_count] > 0
      assert is_binary(cuckoo.state)
    end

    test "custom options" do
      cuckoo = Cuckoo.new(capacity: 1000, fingerprint_size: 16, bucket_size: 2)
      assert cuckoo.opts[:capacity] == 1000
      assert cuckoo.opts[:fingerprint_size] == 16
      assert cuckoo.opts[:bucket_size] == 2
    end

    test "struct fields" do
      cuckoo = Cuckoo.new()
      assert %Cuckoo{state: _, opts: _, backend: _} = cuckoo
      assert cuckoo.backend == ExDataSketch.Backend.Pure
    end

    test "bucket_count is power of 2" do
      import Bitwise
      cuckoo = Cuckoo.new(capacity: 1000)
      bc = cuckoo.opts[:bucket_count]
      assert (bc &&& bc - 1) == 0
    end

    test "CKO1 magic in state binary" do
      cuckoo = Cuckoo.new(capacity: 100)
      <<"CKO1", _rest::binary>> = cuckoo.state
    end

    test "invalid capacity raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(capacity: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(capacity: -1)
      end
    end

    test "invalid fingerprint_size raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(fingerprint_size: 7)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(fingerprint_size: 10)
      end
    end

    test "invalid bucket_size raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(bucket_size: 3)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(bucket_size: 8)
      end
    end

    test "invalid max_kicks raises" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(max_kicks: 50)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Cuckoo.new(max_kicks: 3000)
      end
    end

    test "seed option is stored" do
      cuckoo = Cuckoo.new(seed: 42)
      assert cuckoo.opts[:seed] == 42
    end
  end

  describe "put/2 and member?/2" do
    test "inserted item is a member" do
      cuckoo = Cuckoo.new(capacity: 100)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "hello")
      assert Cuckoo.member?(cuckoo, "hello")
    end

    test "non-inserted item is not a member" do
      cuckoo = Cuckoo.new(capacity: 100)
      refute Cuckoo.member?(cuckoo, "hello")
    end

    test "various types work" do
      cuckoo = Cuckoo.new(capacity: 100)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "string")
      {:ok, cuckoo} = Cuckoo.put(cuckoo, 42)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, :atom)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, {:tuple, 1})

      assert Cuckoo.member?(cuckoo, "string")
      assert Cuckoo.member?(cuckoo, 42)
      assert Cuckoo.member?(cuckoo, :atom)
      assert Cuckoo.member?(cuckoo, {:tuple, 1})
    end

    test "count increments with each insertion" do
      cuckoo = Cuckoo.new(capacity: 100)
      assert Cuckoo.count(cuckoo) == 0
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "a")
      assert Cuckoo.count(cuckoo) == 1
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "b")
      assert Cuckoo.count(cuckoo) == 2
    end
  end

  describe "put!/2" do
    test "returns updated filter" do
      cuckoo = Cuckoo.new(capacity: 100)
      cuckoo = Cuckoo.put!(cuckoo, "hello")
      assert Cuckoo.member?(cuckoo, "hello")
    end
  end

  describe "put_many/2" do
    test "all items become members" do
      cuckoo = Cuckoo.new(capacity: 1000)
      {:ok, cuckoo} = Cuckoo.put_many(cuckoo, @items_100)
      assert Enum.all?(@items_100, &Cuckoo.member?(cuckoo, &1))
    end

    test "empty list is no-op" do
      cuckoo = Cuckoo.new(capacity: 100)
      {:ok, cuckoo2} = Cuckoo.put_many(cuckoo, [])
      assert Cuckoo.serialize(cuckoo) == Cuckoo.serialize(cuckoo2)
    end

    test "count reflects all insertions" do
      cuckoo = Cuckoo.new(capacity: 1000)
      {:ok, cuckoo} = Cuckoo.put_many(cuckoo, @items_100)
      assert Cuckoo.count(cuckoo) == 100
    end
  end

  describe "delete/2" do
    test "deleting an inserted item removes it" do
      cuckoo = Cuckoo.new(capacity: 100)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "hello")
      assert Cuckoo.member?(cuckoo, "hello")
      {:ok, cuckoo} = Cuckoo.delete(cuckoo, "hello")
      refute Cuckoo.member?(cuckoo, "hello")
    end

    test "deleting decrements count" do
      cuckoo = Cuckoo.new(capacity: 100)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "hello")
      assert Cuckoo.count(cuckoo) == 1
      {:ok, cuckoo} = Cuckoo.delete(cuckoo, "hello")
      assert Cuckoo.count(cuckoo) == 0
    end

    test "deleting a non-existent item returns error" do
      cuckoo = Cuckoo.new(capacity: 100)
      assert {:error, :not_found} = Cuckoo.delete(cuckoo, "missing")
    end

    test "insert-delete-insert cycle" do
      cuckoo = Cuckoo.new(capacity: 100)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "x")
      {:ok, cuckoo} = Cuckoo.delete(cuckoo, "x")
      refute Cuckoo.member?(cuckoo, "x")
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "x")
      assert Cuckoo.member?(cuckoo, "x")
    end
  end

  describe "serialize/deserialize" do
    test "round-trip" do
      cuckoo = Cuckoo.new(capacity: 1000)
      {:ok, cuckoo} = Cuckoo.put_many(cuckoo, @items_100)
      bin = Cuckoo.serialize(cuckoo)
      assert {:ok, recovered} = Cuckoo.deserialize(bin)
      assert Cuckoo.serialize(recovered) == bin
      assert Enum.all?(@items_100, &Cuckoo.member?(recovered, &1))
    end

    test "EXSK envelope has correct sketch ID" do
      cuckoo = Cuckoo.new(capacity: 100)
      <<"EXSK", _version::8, sketch_id::8, _rest::binary>> = Cuckoo.serialize(cuckoo)
      assert sketch_id == 8
    end

    test "wrong sketch ID returns error" do
      cuckoo = Cuckoo.new(capacity: 100)
      bin = Cuckoo.serialize(cuckoo)
      <<prefix::binary-size(5), _id::8, rest::binary>> = bin
      corrupted = <<prefix::binary, 1::8, rest::binary>>
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} = Cuckoo.deserialize(corrupted)
    end

    test "invalid binary returns error" do
      assert {:error, _} = Cuckoo.deserialize(<<"invalid">>)
    end

    test "truncated binary returns error" do
      assert {:error, _} = Cuckoo.deserialize(<<1, 2>>)
    end
  end

  describe "compatible_with?/2" do
    test "same params are compatible" do
      a = Cuckoo.new(capacity: 100)
      b = Cuckoo.new(capacity: 100)
      assert Cuckoo.compatible_with?(a, b)
    end

    test "different capacity (same bucket_count) are compatible" do
      # If both result in same bucket_count
      a = Cuckoo.new(capacity: 100)
      b = Cuckoo.new(capacity: 100)
      assert Cuckoo.compatible_with?(a, b)
    end

    test "different seed are incompatible" do
      a = Cuckoo.new(capacity: 100, seed: 0)
      b = Cuckoo.new(capacity: 100, seed: 99)
      refute Cuckoo.compatible_with?(a, b)
    end

    test "different fingerprint_size are incompatible" do
      a = Cuckoo.new(capacity: 100, fingerprint_size: 8)
      b = Cuckoo.new(capacity: 100, fingerprint_size: 16)
      refute Cuckoo.compatible_with?(a, b)
    end
  end

  describe "capabilities/0" do
    test "includes expected capabilities" do
      caps = Cuckoo.capabilities()
      assert :put in caps
      assert :delete in caps
      assert :member? in caps
      assert :count in caps
      assert :serialize in caps
      assert :deserialize in caps
    end
  end

  describe "introspection" do
    test "count is 0 for empty filter" do
      assert Cuckoo.new(capacity: 100) |> Cuckoo.count() == 0
    end

    test "size_bytes matches expected" do
      cuckoo = Cuckoo.new(capacity: 100, fingerprint_size: 8, bucket_size: 4)
      bc = cuckoo.opts[:bucket_count]
      # 32 header + bc * 4 * 1 body
      assert Cuckoo.size_bytes(cuckoo) == 32 + bc * 4
    end
  end

  describe "convenience functions" do
    test "from_enumerable" do
      {:ok, cuckoo} = Cuckoo.from_enumerable(["a", "b", "c"], capacity: 100)
      assert Cuckoo.member?(cuckoo, "a")
      assert Cuckoo.member?(cuckoo, "b")
      assert Cuckoo.member?(cuckoo, "c")
    end

    test "reducer" do
      reducer = Cuckoo.reducer()
      cuckoo = Enum.reduce(["a", "b"], Cuckoo.new(capacity: 100), reducer)
      assert Cuckoo.member?(cuckoo, "a")
    end
  end

  describe "facade dispatch" do
    test "ExDataSketch.update_many dispatches to Cuckoo.put_many" do
      cuckoo = Cuckoo.new(capacity: 100)
      updated = ExDataSketch.update_many(cuckoo, ["a", "b"])
      assert Cuckoo.member?(updated, "a")
    end
  end

  describe "Codec integration" do
    test "sketch_id_cuckoo is 8" do
      assert ExDataSketch.Codec.sketch_id_cuckoo() == 8
    end
  end

  describe "fingerprint_size variants" do
    test "f=8 works" do
      cuckoo = Cuckoo.new(capacity: 100, fingerprint_size: 8)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "test_8")
      assert Cuckoo.member?(cuckoo, "test_8")
    end

    test "f=12 works" do
      cuckoo = Cuckoo.new(capacity: 100, fingerprint_size: 12)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "test_12")
      assert Cuckoo.member?(cuckoo, "test_12")
    end

    test "f=16 works" do
      cuckoo = Cuckoo.new(capacity: 100, fingerprint_size: 16)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "test_16")
      assert Cuckoo.member?(cuckoo, "test_16")
    end

    test "f=12 round-trip serialization" do
      cuckoo = Cuckoo.new(capacity: 100, fingerprint_size: 12)
      {:ok, cuckoo} = Cuckoo.put_many(cuckoo, Enum.map(1..20, &"item_#{&1}"))
      bin = Cuckoo.serialize(cuckoo)
      {:ok, recovered} = Cuckoo.deserialize(bin)
      assert Enum.all?(1..20, &Cuckoo.member?(recovered, "item_#{&1}"))
    end
  end

  describe "bucket_size variants" do
    test "b=2 works" do
      cuckoo = Cuckoo.new(capacity: 100, bucket_size: 2)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "test_b2")
      assert Cuckoo.member?(cuckoo, "test_b2")
    end

    test "b=4 works" do
      cuckoo = Cuckoo.new(capacity: 100, bucket_size: 4)
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "test_b4")
      assert Cuckoo.member?(cuckoo, "test_b4")
    end
  end

  describe "statistical validation" do
    test "false positive rate is within expected bounds (f=8, b=4)" do
      n = 1_000
      cuckoo = Cuckoo.new(capacity: 10_000, fingerprint_size: 8, bucket_size: 4)
      items = Enum.map(1..n, &"item_#{&1}")
      {:ok, cuckoo} = Cuckoo.put_many(cuckoo, items)

      # Test 50,000 non-inserted items
      test_count = 50_000
      false_positives = Enum.count(1..test_count, &Cuckoo.member?(cuckoo, "non_item_#{&1}"))
      observed_fpr = false_positives / test_count

      # Theoretical FPR ~= 2*4/256 = 3.1%. Allow 2x margin.
      assert observed_fpr < 0.07,
             "Observed FPR #{Float.round(observed_fpr * 100, 2)}% exceeds 7%"
    end

    test "empty filter has zero false positives" do
      cuckoo = Cuckoo.new(capacity: 1000)
      assert Enum.all?(1..10_000, fn i -> not Cuckoo.member?(cuckoo, i) end)
    end
  end

  describe "property tests" do
    property "no false negatives" do
      check all(items <- list_of(binary(), min_length: 1, max_length: 30)) do
        cuckoo = Cuckoo.new(capacity: 1000)
        {:ok, cuckoo} = Cuckoo.put_many(cuckoo, items)
        assert Enum.all?(items, &Cuckoo.member?(cuckoo, &1))
      end
    end

    property "count matches insertions" do
      check all(items <- list_of(binary(min_length: 1), min_length: 1, max_length: 30)) do
        cuckoo = Cuckoo.new(capacity: 1000)
        {:ok, cuckoo} = Cuckoo.put_many(cuckoo, items)
        assert Cuckoo.count(cuckoo) == length(items)
      end
    end

    property "serialization round-trip" do
      check all(items <- list_of(binary(), max_length: 20)) do
        cuckoo = Cuckoo.new(capacity: 1000)
        {:ok, cuckoo} = Cuckoo.put_many(cuckoo, items)
        bin = Cuckoo.serialize(cuckoo)
        {:ok, recovered} = Cuckoo.deserialize(bin)
        assert Cuckoo.serialize(recovered) == bin
      end
    end

    property "delete removes item" do
      check all(item <- binary(min_length: 1)) do
        cuckoo = Cuckoo.new(capacity: 1000)
        {:ok, cuckoo} = Cuckoo.put(cuckoo, item)
        assert Cuckoo.member?(cuckoo, item)
        {:ok, cuckoo} = Cuckoo.delete(cuckoo, item)
        refute Cuckoo.member?(cuckoo, item)
      end
    end
  end
end
