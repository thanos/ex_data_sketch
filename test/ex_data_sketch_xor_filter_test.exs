defmodule ExDataSketch.XorFilterTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.{Codec, XorFilter}

  # -------------------------------------------------------
  # build/2
  # -------------------------------------------------------

  describe "build/2" do
    test "builds successfully from a list" do
      assert {:ok, %XorFilter{}} = XorFilter.build(["a", "b", "c"])
    end

    test "builds from a range" do
      assert {:ok, %XorFilter{}} = XorFilter.build(1..50)
    end

    test "builds from an empty list" do
      assert {:ok, filter} = XorFilter.build([])
      assert XorFilter.count(filter) == 0
    end

    test "builds from a single item" do
      assert {:ok, filter} = XorFilter.build(["only"])
      assert XorFilter.count(filter) == 1
      assert XorFilter.member?(filter, "only")
    end

    test "deduplicates items" do
      assert {:ok, filter} = XorFilter.build(["a", "a", "b", "b", "b"])
      assert XorFilter.count(filter) == 2
    end

    test "supports fingerprint_bits: 16 (Xor16)" do
      assert {:ok, filter} = XorFilter.build(1..100, fingerprint_bits: 16)
      assert XorFilter.member?(filter, 1)
      assert XorFilter.member?(filter, 100)
    end

    test "raises on invalid fingerprint_bits" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        XorFilter.build(["a"], fingerprint_bits: 4)
      end
    end
  end

  # -------------------------------------------------------
  # member?/2
  # -------------------------------------------------------

  describe "member?/2" do
    test "all built items are members (no false negatives)" do
      items = Enum.map(1..200, &"item_#{&1}")
      {:ok, filter} = XorFilter.build(items)

      for item <- items do
        assert XorFilter.member?(filter, item),
               "expected #{item} to be a member"
      end
    end

    test "non-built items are mostly not members" do
      {:ok, filter} = XorFilter.build(Enum.map(1..500, &"in_#{&1}"))

      false_positives =
        Enum.count(1..1000, fn i -> XorFilter.member?(filter, "out_#{i}") end)

      # With 8-bit fingerprints, FPR ~0.39%. 1000 probes -> expect < 20
      assert false_positives < 20,
             "expected < 20 false positives, got #{false_positives}"
    end

    test "empty filter has no members" do
      {:ok, filter} = XorFilter.build([])
      refute XorFilter.member?(filter, "anything")
    end
  end

  # -------------------------------------------------------
  # count/1
  # -------------------------------------------------------

  describe "count/1" do
    test "returns number of unique items built from" do
      {:ok, filter} = XorFilter.build(["a", "b", "c"])
      assert XorFilter.count(filter) == 3
    end

    test "returns 0 for empty filter" do
      {:ok, filter} = XorFilter.build([])
      assert XorFilter.count(filter) == 0
    end
  end

  # -------------------------------------------------------
  # serialize/1 and deserialize/1
  # -------------------------------------------------------

  describe "serialize/deserialize" do
    test "round-trip preserves membership" do
      items = Enum.map(1..50, &"item_#{&1}")
      {:ok, filter} = XorFilter.build(items)

      binary = XorFilter.serialize(filter)
      {:ok, recovered} = XorFilter.deserialize(binary)

      for item <- items do
        assert XorFilter.member?(recovered, item)
      end

      assert XorFilter.count(recovered) == XorFilter.count(filter)
    end

    test "EXSK format starts with magic bytes" do
      {:ok, filter} = XorFilter.build(["a"])
      binary = XorFilter.serialize(filter)
      assert <<"EXSK", _::binary>> = binary
    end

    test "XOR1 state starts with magic bytes" do
      {:ok, filter} = XorFilter.build(["a"])
      assert <<"XOR1", _::binary>> = filter.state
    end

    test "sketch_id is 11" do
      assert Codec.sketch_id_xor() == 11
    end

    test "EXSK contains sketch_id 11" do
      {:ok, filter} = XorFilter.build(["a"])
      binary = XorFilter.serialize(filter)
      <<"EXSK", _version::8, sketch_id::8, _::binary>> = binary
      assert sketch_id == 11
    end

    test "deserialize rejects wrong sketch_id" do
      {:ok, filter} = XorFilter.build(["a"])
      binary = XorFilter.serialize(filter)
      # Corrupt the sketch_id byte (offset 5)
      <<prefix::binary-size(5), _id::8, rest::binary>> = binary
      corrupted = <<prefix::binary, 99::8, rest::binary>>
      assert {:error, _} = XorFilter.deserialize(corrupted)
    end

    test "deserialize rejects invalid magic" do
      assert {:error, _} = XorFilter.deserialize(<<"BAAD", 1, 11, 0::32, 0::32>>)
    end

    test "Xor16 round-trip" do
      items = Enum.map(1..30, &"item_#{&1}")
      {:ok, filter} = XorFilter.build(items, fingerprint_bits: 16)

      binary = XorFilter.serialize(filter)
      {:ok, recovered} = XorFilter.deserialize(binary)

      for item <- items do
        assert XorFilter.member?(recovered, item)
      end
    end
  end

  # -------------------------------------------------------
  # compatible_with?/2
  # -------------------------------------------------------

  describe "compatible_with?/2" do
    test "same parameters are compatible" do
      {:ok, a} = XorFilter.build(["a"], fingerprint_bits: 8, seed: 0)
      {:ok, b} = XorFilter.build(["b"], fingerprint_bits: 8, seed: 0)
      assert XorFilter.compatible_with?(a, b)
    end

    test "different fingerprint_bits are incompatible" do
      {:ok, a} = XorFilter.build(["a"], fingerprint_bits: 8)
      {:ok, b} = XorFilter.build(["b"], fingerprint_bits: 16)
      refute XorFilter.compatible_with?(a, b)
    end

    test "different seeds are incompatible" do
      {:ok, a} = XorFilter.build(["a"], seed: 0)
      {:ok, b} = XorFilter.build(["b"], seed: 42)
      refute XorFilter.compatible_with?(a, b)
    end
  end

  # -------------------------------------------------------
  # capabilities/0
  # -------------------------------------------------------

  describe "capabilities/0" do
    test "returns expected set" do
      expected =
        MapSet.new([:build, :member?, :count, :serialize, :deserialize, :compatible_with?])

      assert XorFilter.capabilities() == expected
    end

    test "does not include put, delete, or merge" do
      caps = XorFilter.capabilities()
      refute MapSet.member?(caps, :put)
      refute MapSet.member?(caps, :delete)
      refute MapSet.member?(caps, :merge)
    end
  end

  # -------------------------------------------------------
  # size_bytes/1
  # -------------------------------------------------------

  describe "size_bytes/1" do
    test "returns positive value" do
      {:ok, filter} = XorFilter.build(1..10)
      assert XorFilter.size_bytes(filter) > 0
    end

    test "Xor16 is larger than Xor8 for same data" do
      items = Enum.to_list(1..100)
      {:ok, f8} = XorFilter.build(items, fingerprint_bits: 8)
      {:ok, f16} = XorFilter.build(items, fingerprint_bits: 16)
      assert XorFilter.size_bytes(f16) > XorFilter.size_bytes(f8)
    end
  end

  # -------------------------------------------------------
  # No put/delete/merge (compile-time check)
  # -------------------------------------------------------

  describe "API surface" do
    test "put/2 is not defined" do
      refute function_exported?(XorFilter, :put, 2)
    end

    test "delete/2 is not defined" do
      refute function_exported?(XorFilter, :delete, 2)
    end

    test "merge/2 is not defined" do
      refute function_exported?(XorFilter, :merge, 2)
    end

    test "new/1 is not defined" do
      refute function_exported?(XorFilter, :new, 1)
    end
  end

  # -------------------------------------------------------
  # FPR bounds
  # -------------------------------------------------------

  describe "FPR bounds" do
    test "Xor8 FPR is bounded at ~1%" do
      items = Enum.map(1..1000, &"member_#{&1}")
      {:ok, filter} = XorFilter.build(items)

      probe_count = 10_000

      fp_count =
        Enum.count(1..probe_count, fn i ->
          XorFilter.member?(filter, "non_member_#{i}")
        end)

      fpr = fp_count / probe_count
      # Theoretical FPR ~0.39%, allow up to 1%
      assert fpr < 0.01, "Xor8 FPR #{Float.round(fpr * 100, 2)}% exceeds 1%"
    end

    test "Xor16 has lower FPR than Xor8" do
      items = Enum.map(1..500, &"member_#{&1}")
      {:ok, f16} = XorFilter.build(items, fingerprint_bits: 16)

      probe_count = 10_000

      fp_count =
        Enum.count(1..probe_count, fn i ->
          XorFilter.member?(f16, "non_member_#{i}")
        end)

      fpr = fp_count / probe_count
      # Xor16 FPR should be ~0.0015%, well under 0.1%
      assert fpr < 0.001, "Xor16 FPR #{Float.round(fpr * 100, 4)}% exceeds 0.1%"
    end
  end

  # -------------------------------------------------------
  # Determinism
  # -------------------------------------------------------

  describe "determinism" do
    test "same items and seed produce identical serialization" do
      items = ["apple", "banana", "cherry"]
      {:ok, f1} = XorFilter.build(items, seed: 42)
      {:ok, f2} = XorFilter.build(items, seed: 42)

      assert XorFilter.serialize(f1) == XorFilter.serialize(f2)
    end

    test "same items in different order produce identical serialization" do
      items_a = ["cherry", "apple", "banana"]
      items_b = ["banana", "cherry", "apple"]
      {:ok, f1} = XorFilter.build(items_a, seed: 7)
      {:ok, f2} = XorFilter.build(items_b, seed: 7)

      assert XorFilter.serialize(f1) == XorFilter.serialize(f2)
    end
  end

  # -------------------------------------------------------
  # Property-based tests
  # -------------------------------------------------------

  describe "properties" do
    property "no false negatives" do
      check all(
              items <-
                uniq_list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 100)
            ) do
        {:ok, filter} = XorFilter.build(items)

        for item <- items do
          assert XorFilter.member?(filter, item)
        end
      end
    end

    property "count equals number of unique items" do
      check all(items <- list_of(integer(), min_length: 1, max_length: 200)) do
        {:ok, filter} = XorFilter.build(items)
        assert XorFilter.count(filter) == length(Enum.uniq(items))
      end
    end

    property "serialization round-trip stability" do
      check all(
              items <-
                uniq_list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 50)
            ) do
        {:ok, filter} = XorFilter.build(items)
        bin1 = XorFilter.serialize(filter)
        {:ok, recovered} = XorFilter.deserialize(bin1)
        bin2 = XorFilter.serialize(recovered)
        assert bin1 == bin2
      end
    end

    property "deterministic regardless of input order" do
      check all(
              items <-
                uniq_list_of(string(:alphanumeric, min_length: 1), min_length: 2, max_length: 50)
            ) do
        shuffled = Enum.shuffle(items)
        {:ok, f1} = XorFilter.build(items, seed: 0)
        {:ok, f2} = XorFilter.build(shuffled, seed: 0)
        assert XorFilter.serialize(f1) == XorFilter.serialize(f2)
      end
    end
  end
end
