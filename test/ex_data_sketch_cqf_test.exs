defmodule ExDataSketch.CQFTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.CQF

  # ============================================================
  # new/1
  # ============================================================

  describe "new/1" do
    test "creates with default parameters" do
      cqf = CQF.new()
      assert cqf.opts[:q] == 16
      assert cqf.opts[:r] == 8
      assert cqf.opts[:seed] == 0
    end

    test "creates with custom parameters" do
      cqf = CQF.new(q: 12, r: 10, seed: 42)
      assert cqf.opts[:q] == 12
      assert cqf.opts[:r] == 10
      assert cqf.opts[:seed] == 42
    end

    test "slot_count is 2^q" do
      cqf = CQF.new(q: 10, r: 8)
      assert cqf.opts[:slot_count] == 1024
    end

    test "validates q range" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn -> CQF.new(q: 0) end
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn -> CQF.new(q: 29) end
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn -> CQF.new(q: -1) end
    end

    test "validates r range" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn -> CQF.new(r: 0) end
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn -> CQF.new(r: 33) end
    end

    test "validates q + r <= 64" do
      # q=33 exceeds q range (1..28), caught by validate_q! first
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn -> CQF.new(q: 33, r: 32) end
    end

    test "state is a binary" do
      cqf = CQF.new(q: 8, r: 8)
      assert is_binary(cqf.state)
    end

    test "state starts with CQF1 magic" do
      cqf = CQF.new(q: 8, r: 8)
      assert <<"CQF1", _::binary>> = cqf.state
    end
  end

  # ============================================================
  # put/2 and member?/2
  # ============================================================

  describe "put/2 and member?/2" do
    test "inserted item is a member" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put("hello")
      assert CQF.member?(cqf, "hello")
    end

    test "non-inserted item is not a member" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put("hello")
      refute CQF.member?(cqf, "world")
    end

    test "multiple distinct items" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("b")
        |> CQF.put("c")

      assert CQF.member?(cqf, "a")
      assert CQF.member?(cqf, "b")
      assert CQF.member?(cqf, "c")
      refute CQF.member?(cqf, "d")
    end

    test "duplicate inserts maintain membership" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.put("x")
        |> CQF.put("x")

      assert CQF.member?(cqf, "x")
    end

    test "integer items" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put(42)
      assert CQF.member?(cqf, 42)
      refute CQF.member?(cqf, 43)
    end

    test "various term types" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("string")
        |> CQF.put(123)
        |> CQF.put(:atom)
        |> CQF.put({:tuple, 1})

      assert CQF.member?(cqf, "string")
      assert CQF.member?(cqf, 123)
      assert CQF.member?(cqf, :atom)
      assert CQF.member?(cqf, {:tuple, 1})
    end
  end

  # ============================================================
  # estimate_count/2
  # ============================================================

  describe "estimate_count/2" do
    test "returns 0 for empty filter" do
      cqf = CQF.new(q: 10, r: 8)
      assert CQF.estimate_count(cqf, "x") == 0
    end

    test "returns 1 for single insert" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put("x")
      assert CQF.estimate_count(cqf, "x") == 1
    end

    test "increments with duplicate inserts" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.put("x")

      assert CQF.estimate_count(cqf, "x") == 2
    end

    test "count=3 with three inserts" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.put("x")
        |> CQF.put("x")

      assert CQF.estimate_count(cqf, "x") == 3
    end

    test "high multiplicity" do
      n = 10
      cqf = Enum.reduce(1..n, CQF.new(q: 10, r: 8), fn _i, acc -> CQF.put(acc, "x") end)
      assert CQF.estimate_count(cqf, "x") == n
    end

    test "different items have independent counts" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("a")
        |> CQF.put("a")
        |> CQF.put("b")
        |> CQF.put("b")

      assert CQF.estimate_count(cqf, "a") == 3
      assert CQF.estimate_count(cqf, "b") == 2
      assert CQF.estimate_count(cqf, "c") == 0
    end

    test "returns 0 for non-member" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put("a")
      assert CQF.estimate_count(cqf, "b") == 0
    end
  end

  # ============================================================
  # put_many/2
  # ============================================================

  describe "put_many/2" do
    test "inserts multiple items" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(["a", "b", "c"])
      assert CQF.member?(cqf, "a")
      assert CQF.member?(cqf, "b")
      assert CQF.member?(cqf, "c")
    end

    test "empty list is no-op" do
      cqf = CQF.new(q: 10, r: 8)
      cqf2 = CQF.put_many(cqf, [])
      assert CQF.count(cqf) == CQF.count(cqf2)
    end

    test "duplicates in list increment counts" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(["x", "x", "x", "y", "y"])
      assert CQF.estimate_count(cqf, "x") == 3
      assert CQF.estimate_count(cqf, "y") == 2
    end

    test "equivalent to sequential puts" do
      items = ["a", "b", "a", "c", "b", "a"]

      cqf_many = CQF.new(q: 10, r: 8) |> CQF.put_many(items)
      cqf_seq = Enum.reduce(items, CQF.new(q: 10, r: 8), &CQF.put(&2, &1))

      assert CQF.count(cqf_many) == CQF.count(cqf_seq)
      assert CQF.estimate_count(cqf_many, "a") == CQF.estimate_count(cqf_seq, "a")
      assert CQF.estimate_count(cqf_many, "b") == CQF.estimate_count(cqf_seq, "b")
      assert CQF.estimate_count(cqf_many, "c") == CQF.estimate_count(cqf_seq, "c")
    end
  end

  # ============================================================
  # delete/2
  # ============================================================

  describe "delete/2" do
    test "decrements count" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.put("x")
        |> CQF.put("x")
        |> CQF.delete("x")

      assert CQF.estimate_count(cqf, "x") == 2
    end

    test "removes item when count reaches 0" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.delete("x")

      refute CQF.member?(cqf, "x")
      assert CQF.estimate_count(cqf, "x") == 0
    end

    test "non-member delete is no-op" do
      cqf = CQF.new(q: 10, r: 8)
      cqf2 = CQF.delete(cqf, "nonexistent")
      assert CQF.count(cqf) == CQF.count(cqf2)
    end

    test "does not affect other items" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("b")
        |> CQF.delete("a")

      refute CQF.member?(cqf, "a")
      assert CQF.member?(cqf, "b")
    end

    test "delete from count=2" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.put("x")
        |> CQF.delete("x")

      assert CQF.estimate_count(cqf, "x") == 1
      assert CQF.member?(cqf, "x")
    end
  end

  # ============================================================
  # count/1
  # ============================================================

  describe "count/1" do
    test "empty filter has count 0" do
      assert CQF.count(CQF.new(q: 10, r: 8)) == 0
    end

    test "count increases with inserts" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("b")

      assert CQF.count(cqf) == 2
    end

    test "count tracks multiplicities" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("a")
        |> CQF.put("b")

      assert CQF.count(cqf) == 3
    end

    test "count decreases with deletes" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("a")
        |> CQF.put("b")
        |> CQF.delete("a")

      assert CQF.count(cqf) == 2
    end
  end

  # ============================================================
  # merge/2
  # ============================================================

  describe "merge/2" do
    test "merged filter contains items from both" do
      a = CQF.new(q: 10, r: 8) |> CQF.put("x")
      b = CQF.new(q: 10, r: 8) |> CQF.put("y")
      merged = CQF.merge(a, b)

      assert CQF.member?(merged, "x")
      assert CQF.member?(merged, "y")
    end

    test "merge sums counts (multiset union)" do
      a = CQF.new(q: 10, r: 8) |> CQF.put("x") |> CQF.put("x")
      b = CQF.new(q: 10, r: 8) |> CQF.put("x") |> CQF.put("x") |> CQF.put("x")
      merged = CQF.merge(a, b)

      assert CQF.estimate_count(merged, "x") == 5
    end

    test "merge with empty filter" do
      a = CQF.new(q: 10, r: 8) |> CQF.put("x")
      b = CQF.new(q: 10, r: 8)
      merged = CQF.merge(a, b)

      assert CQF.member?(merged, "x")
      assert CQF.count(merged) == CQF.count(a)
    end

    test "merge total count is sum" do
      a = CQF.new(q: 10, r: 8) |> CQF.put("a") |> CQF.put("b")
      b = CQF.new(q: 10, r: 8) |> CQF.put("c") |> CQF.put("a")
      merged = CQF.merge(a, b)

      assert CQF.count(merged) == CQF.count(a) + CQF.count(b)
    end

    test "incompatible q raises error" do
      a = CQF.new(q: 10, r: 8)
      b = CQF.new(q: 12, r: 8)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        CQF.merge(a, b)
      end
    end

    test "incompatible r raises error" do
      a = CQF.new(q: 10, r: 8)
      b = CQF.new(q: 10, r: 12)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        CQF.merge(a, b)
      end
    end

    test "incompatible seed raises error" do
      a = CQF.new(q: 10, r: 8, seed: 1)
      b = CQF.new(q: 10, r: 8, seed: 2)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        CQF.merge(a, b)
      end
    end
  end

  # ============================================================
  # merge_many/1
  # ============================================================

  describe "merge_many/1" do
    test "merges multiple filters" do
      filters =
        Enum.map(1..3, fn i ->
          CQF.new(q: 10, r: 8) |> CQF.put("item_#{i}")
        end)

      merged = CQF.merge_many(filters)

      Enum.each(1..3, fn i ->
        assert CQF.member?(merged, "item_#{i}")
      end)
    end
  end

  # ============================================================
  # serialize/deserialize
  # ============================================================

  describe "serialize/deserialize" do
    test "round-trip preserves membership" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("a")
        |> CQF.put("b")

      binary = CQF.serialize(cqf)
      {:ok, recovered} = CQF.deserialize(binary)

      assert CQF.member?(recovered, "a")
      assert CQF.member?(recovered, "b")
      refute CQF.member?(recovered, "c")
    end

    test "round-trip preserves counts" do
      cqf =
        CQF.new(q: 10, r: 8)
        |> CQF.put("x")
        |> CQF.put("x")
        |> CQF.put("x")

      binary = CQF.serialize(cqf)
      {:ok, recovered} = CQF.deserialize(binary)

      assert CQF.estimate_count(recovered, "x") == 3
      assert CQF.count(recovered) == 3
    end

    test "EXSK format" do
      cqf = CQF.new(q: 10, r: 8)
      binary = CQF.serialize(cqf)
      assert <<"EXSK", _rest::binary>> = binary
    end

    test "count preservation" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(~w(a b c a b a))
      {:ok, recovered} = CQF.deserialize(CQF.serialize(cqf))
      assert CQF.count(recovered) == CQF.count(cqf)
    end

    test "invalid binary returns error" do
      assert {:error, _} = CQF.deserialize(<<0, 0, 0>>)
    end

    test "wrong sketch ID returns error" do
      # Serialize a Quotient filter and try to deserialize as CQF
      qf = ExDataSketch.Quotient.new(q: 10, r: 8)
      binary = ExDataSketch.Quotient.serialize(qf)
      assert {:error, _} = CQF.deserialize(binary)
    end
  end

  # ============================================================
  # compatible_with?/2
  # ============================================================

  describe "compatible_with?/2" do
    test "same parameters are compatible" do
      a = CQF.new(q: 10, r: 8)
      b = CQF.new(q: 10, r: 8)
      assert CQF.compatible_with?(a, b)
    end

    test "different q is incompatible" do
      a = CQF.new(q: 10, r: 8)
      b = CQF.new(q: 12, r: 8)
      refute CQF.compatible_with?(a, b)
    end

    test "different r is incompatible" do
      a = CQF.new(q: 10, r: 8)
      b = CQF.new(q: 10, r: 12)
      refute CQF.compatible_with?(a, b)
    end

    test "different seed is incompatible" do
      a = CQF.new(q: 10, r: 8, seed: 1)
      b = CQF.new(q: 10, r: 8, seed: 2)
      refute CQF.compatible_with?(a, b)
    end
  end

  # ============================================================
  # capabilities/0
  # ============================================================

  describe "capabilities/0" do
    test "includes expected operations" do
      caps = CQF.capabilities()

      expected = [
        :new,
        :put,
        :put_many,
        :member?,
        :estimate_count,
        :delete,
        :merge,
        :merge_many,
        :count,
        :serialize,
        :deserialize,
        :compatible_with?
      ]

      Enum.each(expected, fn op ->
        assert op in caps, "expected #{op} in capabilities"
      end)
    end
  end

  # ============================================================
  # introspection
  # ============================================================

  describe "introspection" do
    test "size_bytes is positive" do
      cqf = CQF.new(q: 10, r: 8)
      assert CQF.size_bytes(cqf) > 0
    end
  end

  # ============================================================
  # from_enumerable/2
  # ============================================================

  describe "from_enumerable/2" do
    test "builds from list" do
      cqf = CQF.from_enumerable(["a", "b", "c"], q: 10, r: 8)
      assert CQF.member?(cqf, "a")
      assert CQF.member?(cqf, "b")
      assert CQF.member?(cqf, "c")
    end
  end

  # ============================================================
  # reducer/0
  # ============================================================

  describe "reducer/0" do
    test "works with Enum.reduce" do
      reducer = CQF.reducer()
      cqf = Enum.reduce(["a", "b"], CQF.new(q: 10, r: 8), reducer)
      assert CQF.member?(cqf, "a")
      assert CQF.member?(cqf, "b")
    end
  end

  # ============================================================
  # merger/1
  # ============================================================

  describe "merger/1" do
    test "merging function" do
      merger = CQF.merger()
      a = CQF.new(q: 10, r: 8) |> CQF.put("x")
      b = CQF.new(q: 10, r: 8) |> CQF.put("y")
      merged = merger.(a, b)
      assert CQF.member?(merged, "x")
      assert CQF.member?(merged, "y")
    end
  end

  # ============================================================
  # facade dispatch
  # ============================================================

  describe "facade dispatch" do
    test "ExDataSketch.update_many/2 dispatches correctly" do
      cqf = CQF.new(q: 10, r: 8)
      updated = ExDataSketch.update_many(cqf, ["a", "b", "c"])
      assert CQF.member?(updated, "a")
      assert CQF.member?(updated, "b")
      assert CQF.member?(updated, "c")
    end
  end

  # ============================================================
  # codec integration
  # ============================================================

  describe "codec integration" do
    test "sketch_id_cqf is 10" do
      assert ExDataSketch.Codec.sketch_id_cqf() == 10
    end
  end

  # ============================================================
  # false positive rate
  # ============================================================

  describe "false positive rate" do
    test "r=8 gives bounded FPR" do
      n_items = 200
      n_test = 5000

      cqf =
        Enum.reduce(1..n_items, CQF.new(q: 10, r: 8), fn i, acc ->
          CQF.put(acc, "item_#{i}")
        end)

      false_positives =
        Enum.count(1..n_test, fn i ->
          CQF.member?(cqf, "test_#{i + n_items}")
        end)

      fpr = false_positives / n_test
      # r=8 -> theoretical FPR ~0.39%, allow margin
      assert fpr < 0.02, "FPR #{Float.round(fpr * 100, 2)}% exceeds 2% threshold"
    end
  end

  # ============================================================
  # parameter variants
  # ============================================================

  describe "parameter variants" do
    test "small q" do
      cqf = CQF.new(q: 4, r: 8) |> CQF.put("hello")
      assert CQF.member?(cqf, "hello")
    end

    test "larger r" do
      cqf = CQF.new(q: 10, r: 16) |> CQF.put("hello")
      assert CQF.member?(cqf, "hello")
      assert CQF.estimate_count(cqf, "hello") == 1
    end

    test "custom seed" do
      cqf = CQF.new(q: 10, r: 8, seed: 12_345) |> CQF.put("hello")
      assert CQF.member?(cqf, "hello")
    end
  end

  # ============================================================
  # property tests
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
        cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(items)

        Enum.each(items, fn item ->
          assert CQF.member?(cqf, item)
        end)
      end
    end

    property "count equals total insertions" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 1,
                  max_length: 50
                )
            ) do
        cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(items)
        assert CQF.count(cqf) == length(items)
      end
    end

    property "serialize round-trip" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 0,
                  max_length: 30
                )
            ) do
        cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(items)
        {:ok, recovered} = CQF.deserialize(CQF.serialize(cqf))
        assert CQF.count(recovered) == CQF.count(cqf)

        Enum.each(Enum.uniq(items), fn item ->
          assert CQF.member?(recovered, item)
        end)
      end
    end

    property "delete removes one occurrence" do
      check all(
              item <- string(:alphanumeric, min_length: 1, max_length: 10),
              n <- integer(2..10)
            ) do
        cqf = Enum.reduce(1..n, CQF.new(q: 10, r: 8), fn _i, acc -> CQF.put(acc, item) end)
        cqf = CQF.delete(cqf, item)
        assert CQF.estimate_count(cqf, item) == n - 1
        assert CQF.count(cqf) == n - 1
      end
    end

    property "merge commutativity" do
      check all(
              items_a <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 0,
                  max_length: 20
                ),
              items_b <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 0,
                  max_length: 20
                )
            ) do
        a = CQF.new(q: 10, r: 8) |> CQF.put_many(items_a)
        b = CQF.new(q: 10, r: 8) |> CQF.put_many(items_b)

        ab = CQF.merge(a, b)
        ba = CQF.merge(b, a)

        assert CQF.count(ab) == CQF.count(ba)

        all_items = Enum.uniq(items_a ++ items_b)

        Enum.each(all_items, fn item ->
          assert CQF.estimate_count(ab, item) == CQF.estimate_count(ba, item)
        end)
      end
    end

    property "serialization stability" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1, max_length: 10),
                  min_length: 0,
                  max_length: 20
                )
            ) do
        cqf = CQF.new(q: 10, r: 8) |> CQF.put_many(items)
        bin1 = CQF.serialize(cqf)
        {:ok, recovered} = CQF.deserialize(bin1)
        bin2 = CQF.serialize(recovered)
        assert bin1 == bin2
      end
    end
  end
end
