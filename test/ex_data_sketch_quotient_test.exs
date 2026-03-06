defmodule ExDataSketch.QuotientTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Quotient

  # ============================================================
  # new/1
  # ============================================================

  describe "new/1" do
    test "creates filter with default options" do
      qf = Quotient.new()
      assert qf.opts[:q] == 16
      assert qf.opts[:r] == 8
      assert qf.opts[:seed] == 0
      assert qf.backend == ExDataSketch.Backend.Pure
    end

    test "accepts custom q and r" do
      qf = Quotient.new(q: 10, r: 12)
      assert qf.opts[:q] == 10
      assert qf.opts[:r] == 12
    end

    test "validates q range" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Quotient.new(q: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Quotient.new(q: 29)
      end
    end

    test "validates r range" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Quotient.new(r: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Quotient.new(r: 33)
      end
    end

    test "validates q + r <= 64" do
      # q=28 + r=32 = 60, valid
      qf = Quotient.new(q: 28, r: 32)
      assert qf.opts[:q] == 28

      # r > 32 is invalid (r range is 1..32)
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Quotient.new(q: 28, r: 37)
      end
    end

    test "slot_count is 2^q" do
      qf = Quotient.new(q: 10, r: 8)
      assert qf.opts[:slot_count] == 1024
    end
  end

  # ============================================================
  # put/2 and member?/2
  # ============================================================

  describe "put/2 and member?/2" do
    test "inserted item is a member" do
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put("hello")
      assert Quotient.member?(qf, "hello")
    end

    test "non-inserted item is not a member" do
      qf = Quotient.new(q: 10, r: 8)
      refute Quotient.member?(qf, "hello")
    end

    test "multiple inserts all become members" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put("a")
        |> Quotient.put("b")
        |> Quotient.put("c")

      assert Quotient.member?(qf, "a")
      assert Quotient.member?(qf, "b")
      assert Quotient.member?(qf, "c")
    end

    test "duplicate insert is idempotent" do
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put("x") |> Quotient.put("x")
      assert Quotient.member?(qf, "x")
      assert Quotient.count(qf) == 1
    end

    test "integer items" do
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put(42)
      assert Quotient.member?(qf, 42)
      refute Quotient.member?(qf, 43)
    end

    test "various term types" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put(:atom_val)
        |> Quotient.put({1, 2, 3})
        |> Quotient.put([1, 2, 3])

      assert Quotient.member?(qf, :atom_val)
      assert Quotient.member?(qf, {1, 2, 3})
      assert Quotient.member?(qf, [1, 2, 3])
    end
  end

  # ============================================================
  # put_many/2
  # ============================================================

  describe "put_many/2" do
    test "inserts multiple items" do
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put_many(["a", "b", "c"])
      assert Quotient.member?(qf, "a")
      assert Quotient.member?(qf, "b")
      assert Quotient.member?(qf, "c")
    end

    test "empty list is a no-op" do
      qf = Quotient.new(q: 10, r: 8)
      qf2 = Quotient.put_many(qf, [])
      assert Quotient.count(qf2) == 0
    end

    test "count reflects insertions" do
      items = Enum.map(1..50, &"item_#{&1}")
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put_many(items)
      # Count may be slightly less than 50 due to fingerprint collisions
      assert Quotient.count(qf) >= 45
      assert Quotient.count(qf) <= 50
    end
  end

  # ============================================================
  # delete/2
  # ============================================================

  describe "delete/2" do
    test "deleting an inserted item removes it" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put("hello")
        |> Quotient.delete("hello")

      refute Quotient.member?(qf, "hello")
      assert Quotient.count(qf) == 0
    end

    test "deleting a non-member is a no-op" do
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put("a")
      qf2 = Quotient.delete(qf, "b")
      assert Quotient.count(qf2) == 1
      assert Quotient.member?(qf2, "a")
    end

    test "safe deletion does not affect other items" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put("a")
        |> Quotient.put("b")
        |> Quotient.put("c")
        |> Quotient.delete("b")

      assert Quotient.member?(qf, "a")
      refute Quotient.member?(qf, "b")
      assert Quotient.member?(qf, "c")
      assert Quotient.count(qf) == 2
    end
  end

  # ============================================================
  # count/1
  # ============================================================

  describe "count/1" do
    test "empty filter has count 0" do
      assert Quotient.count(Quotient.new(q: 10, r: 8)) == 0
    end

    test "count increases with insertions" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put("a")
        |> Quotient.put("b")

      assert Quotient.count(qf) == 2
    end

    test "count decreases with deletions" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put("a")
        |> Quotient.put("b")
        |> Quotient.delete("a")

      assert Quotient.count(qf) == 1
    end
  end

  # ============================================================
  # merge/2 and merge_many/1
  # ============================================================

  describe "merge/2" do
    test "merged filter contains items from both" do
      a = Quotient.new(q: 10, r: 8) |> Quotient.put("x")
      b = Quotient.new(q: 10, r: 8) |> Quotient.put("y")
      merged = Quotient.merge(a, b)

      assert Quotient.member?(merged, "x")
      assert Quotient.member?(merged, "y")
    end

    test "merge with empty filter returns same membership" do
      a = Quotient.new(q: 10, r: 8) |> Quotient.put("a") |> Quotient.put("b")
      empty = Quotient.new(q: 10, r: 8)
      merged = Quotient.merge(a, empty)

      assert Quotient.member?(merged, "a")
      assert Quotient.member?(merged, "b")
      assert Quotient.count(merged) == 2
    end

    test "merge raises on incompatible q" do
      a = Quotient.new(q: 10, r: 8)
      b = Quotient.new(q: 12, r: 8)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        Quotient.merge(a, b)
      end
    end

    test "merge raises on incompatible r" do
      a = Quotient.new(q: 10, r: 8)
      b = Quotient.new(q: 10, r: 12)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        Quotient.merge(a, b)
      end
    end

    test "merge raises on incompatible seed" do
      a = Quotient.new(q: 10, r: 8, seed: 1)
      b = Quotient.new(q: 10, r: 8, seed: 2)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        Quotient.merge(a, b)
      end
    end
  end

  describe "merge_many/1" do
    test "merges multiple filters" do
      filters =
        Enum.map(1..3, fn i ->
          Quotient.new(q: 10, r: 8) |> Quotient.put("item_#{i}")
        end)

      merged = Quotient.merge_many(filters)

      Enum.each(1..3, fn i ->
        assert Quotient.member?(merged, "item_#{i}")
      end)
    end
  end

  # ============================================================
  # serialize/1 and deserialize/1
  # ============================================================

  describe "serialize/1 and deserialize/1" do
    test "round-trip preserves membership" do
      qf =
        Quotient.new(q: 10, r: 8)
        |> Quotient.put("hello")
        |> Quotient.put("world")

      binary = Quotient.serialize(qf)
      assert {:ok, recovered} = Quotient.deserialize(binary)

      assert Quotient.member?(recovered, "hello")
      assert Quotient.member?(recovered, "world")
      refute Quotient.member?(recovered, "not_inserted")
    end

    test "EXSK envelope format" do
      qf = Quotient.new(q: 10, r: 8)
      binary = Quotient.serialize(qf)
      assert <<"EXSK", _rest::binary>> = binary
    end

    test "preserves count" do
      qf = Quotient.new(q: 10, r: 8) |> Quotient.put_many(~w(a b c))
      binary = Quotient.serialize(qf)
      {:ok, recovered} = Quotient.deserialize(binary)
      assert Quotient.count(recovered) == 3
    end

    test "rejects invalid binary" do
      assert {:error, _} = Quotient.deserialize(<<"BAAD", 1, 1, 0::32, 0::32>>)
    end

    test "rejects wrong sketch ID" do
      # Encode with sketch ID 1 (HLL) instead of 9
      params = <<10::unsigned-8, 8::unsigned-8, 0::unsigned-little-32, 0::unsigned-8>>
      state = Quotient.new(q: 10, r: 8).state

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_hll(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, _} = Quotient.deserialize(binary)
    end
  end

  # ============================================================
  # compatible_with?/2
  # ============================================================

  describe "compatible_with?/2" do
    test "same params are compatible" do
      a = Quotient.new(q: 10, r: 8)
      b = Quotient.new(q: 10, r: 8)
      assert Quotient.compatible_with?(a, b)
    end

    test "different q not compatible" do
      a = Quotient.new(q: 10, r: 8)
      b = Quotient.new(q: 12, r: 8)
      refute Quotient.compatible_with?(a, b)
    end

    test "different r not compatible" do
      a = Quotient.new(q: 10, r: 8)
      b = Quotient.new(q: 10, r: 12)
      refute Quotient.compatible_with?(a, b)
    end

    test "different seed not compatible" do
      a = Quotient.new(q: 10, r: 8, seed: 1)
      b = Quotient.new(q: 10, r: 8, seed: 2)
      refute Quotient.compatible_with?(a, b)
    end
  end

  # ============================================================
  # capabilities/0
  # ============================================================

  describe "capabilities/0" do
    test "includes expected operations" do
      caps = Quotient.capabilities()
      assert :new in caps
      assert :put in caps
      assert :member? in caps
      assert :delete in caps
      assert :merge in caps
      assert :serialize in caps
      assert :deserialize in caps
    end
  end

  # ============================================================
  # Introspection
  # ============================================================

  describe "introspection" do
    test "size_bytes returns positive value" do
      qf = Quotient.new(q: 10, r: 8)
      assert Quotient.size_bytes(qf) > 0
    end
  end

  # ============================================================
  # Convenience functions
  # ============================================================

  describe "from_enumerable/2" do
    test "builds filter from list" do
      qf = Quotient.from_enumerable(~w(a b c), q: 10, r: 8)
      assert Quotient.member?(qf, "a")
      assert Quotient.member?(qf, "b")
      assert Quotient.member?(qf, "c")
      assert Quotient.count(qf) == 3
    end
  end

  describe "reducer/0" do
    test "returns a function for Enum.reduce" do
      reducer = Quotient.reducer()
      qf = Enum.reduce(~w(x y z), Quotient.new(q: 10, r: 8), reducer)
      assert Quotient.member?(qf, "x")
      assert Quotient.member?(qf, "y")
      assert Quotient.member?(qf, "z")
    end
  end

  describe "merger/1" do
    test "returns a function for merging" do
      merger = Quotient.merger()
      a = Quotient.new(q: 10, r: 8) |> Quotient.put("a")
      b = Quotient.new(q: 10, r: 8) |> Quotient.put("b")
      merged = merger.(a, b)
      assert Quotient.member?(merged, "a")
      assert Quotient.member?(merged, "b")
    end
  end

  # ============================================================
  # Facade dispatch
  # ============================================================

  describe "facade dispatch" do
    test "ExDataSketch.update_many/2 dispatches to Quotient" do
      qf = Quotient.new(q: 10, r: 8)
      updated = ExDataSketch.update_many(qf, ~w(a b c))
      assert Quotient.member?(updated, "a")
      assert Quotient.member?(updated, "b")
      assert Quotient.member?(updated, "c")
    end
  end

  # ============================================================
  # Codec integration
  # ============================================================

  describe "codec integration" do
    test "sketch_id_quotient is 9" do
      assert ExDataSketch.Codec.sketch_id_quotient() == 9
    end
  end

  # ============================================================
  # Statistical FPR validation
  # ============================================================

  describe "false positive rate" do
    test "FPR is within expected bounds for r=8" do
      # With r=8, theoretical FPR is ~1/2^8 = ~0.39%
      # We allow generous margin for small sample sizes
      n_items = 200
      n_test = 5000

      qf =
        Quotient.new(q: 12, r: 8)
        |> Quotient.put_many(Enum.map(1..n_items, &"inserted_#{&1}"))

      false_positives =
        Enum.count((n_items + 1)..(n_items + n_test), fn i ->
          Quotient.member?(qf, "test_#{i}")
        end)

      fpr = false_positives / n_test
      # Theoretical ~0.39%, allow up to 5% for statistical variation
      assert fpr < 0.05, "FPR #{Float.round(fpr * 100, 2)}% exceeds 5% threshold"
    end
  end

  # ============================================================
  # Parameter variants
  # ============================================================

  describe "parameter variants" do
    test "small q (4 bits, 16 slots)" do
      qf = Quotient.new(q: 4, r: 8) |> Quotient.put("a") |> Quotient.put("b")
      assert Quotient.member?(qf, "a")
      assert Quotient.member?(qf, "b")
    end

    test "larger r (16 bits)" do
      qf = Quotient.new(q: 8, r: 16) |> Quotient.put("hello")
      assert Quotient.member?(qf, "hello")
    end

    test "custom seed" do
      qf = Quotient.new(q: 10, r: 8, seed: 12_345) |> Quotient.put("test")
      assert Quotient.member?(qf, "test")
    end
  end

  # ============================================================
  # Property tests
  # ============================================================

  describe "property tests" do
    property "no false negatives" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 1,
                  max_length: 50
                )
            ) do
        qf = Quotient.new(q: 10, r: 8) |> Quotient.put_many(items)

        Enum.each(items, fn item ->
          assert Quotient.member?(qf, item),
                 "False negative for #{inspect(item)}"
        end)
      end
    end

    property "count does not exceed unique items" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 1,
                  max_length: 50
                )
            ) do
        qf = Quotient.new(q: 10, r: 8) |> Quotient.put_many(items)
        unique_count = items |> Enum.uniq() |> length()
        # Count may be less than unique items due to hash collisions
        # (different items that produce the same quotient+remainder fingerprint)
        assert Quotient.count(qf) <= unique_count
        assert Quotient.count(qf) > 0
      end
    end

    property "serialize/deserialize round-trip" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 0,
                  max_length: 30
                )
            ) do
        qf = Quotient.new(q: 8, r: 8) |> Quotient.put_many(items)
        binary = Quotient.serialize(qf)
        assert {:ok, recovered} = Quotient.deserialize(binary)

        Enum.each(items, fn item ->
          assert Quotient.member?(recovered, item)
        end)

        assert Quotient.count(recovered) == Quotient.count(qf)
      end
    end

    property "delete removes items" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 1,
                  max_length: 20
                ),
              to_delete <- member_of(items)
            ) do
        qf = Quotient.new(q: 10, r: 8) |> Quotient.put_many(items)
        qf = Quotient.delete(qf, to_delete)

        # Only assert removal if the item appeared exactly once
        if Enum.count(items, &(&1 == to_delete)) == 1 do
          refute Quotient.member?(qf, to_delete)
        end
      end
    end
  end
end
