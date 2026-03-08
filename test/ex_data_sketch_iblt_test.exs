defmodule ExDataSketch.IBLTTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.IBLT

  # -- new/1 --

  describe "new/1" do
    test "creates with defaults" do
      iblt = IBLT.new()
      assert iblt.opts[:cell_count] == 1000
      assert iblt.opts[:hash_count] == 3
      assert iblt.opts[:seed] == 0
    end

    test "creates with custom params" do
      iblt = IBLT.new(cell_count: 500, hash_count: 4, seed: 42)
      assert iblt.opts[:cell_count] == 500
      assert iblt.opts[:hash_count] == 4
      assert iblt.opts[:seed] == 42
    end

    test "validates cell_count range" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        IBLT.new(cell_count: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        IBLT.new(cell_count: -1)
      end
    end

    test "validates hash_count range" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        IBLT.new(hash_count: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        IBLT.new(hash_count: 11)
      end
    end

    test "state starts with IBL1 magic" do
      iblt = IBLT.new()
      assert <<"IBL1", _rest::binary>> = iblt.state
    end
  end

  # -- put/2 + member?/2 --

  describe "put/2 and member?/2" do
    test "inserted item is a member" do
      iblt = IBLT.new() |> IBLT.put("hello")
      assert IBLT.member?(iblt, "hello")
    end

    test "non-inserted item is not a member" do
      iblt = IBLT.new() |> IBLT.put("hello")
      refute IBLT.member?(iblt, "world")
    end

    test "multiple items" do
      iblt = IBLT.new()
      iblt = iblt |> IBLT.put("a") |> IBLT.put("b") |> IBLT.put("c")
      assert IBLT.member?(iblt, "a")
      assert IBLT.member?(iblt, "b")
      assert IBLT.member?(iblt, "c")
      refute IBLT.member?(iblt, "d")
    end
  end

  # -- put/3 (KV mode) --

  describe "put/3 (KV mode)" do
    test "key is a member after KV insert" do
      iblt = IBLT.new() |> IBLT.put("key1", "value1")
      assert IBLT.member?(iblt, "key1")
    end
  end

  # -- put_many/2 --

  describe "put_many/2" do
    test "all items are members" do
      items = Enum.map(1..20, &"item_#{&1}")
      iblt = IBLT.new() |> IBLT.put_many(items)

      Enum.each(items, fn item ->
        assert IBLT.member?(iblt, item), "expected #{item} to be a member"
      end)
    end
  end

  # -- delete/2 --

  describe "delete/2" do
    test "deleted item is no longer a member" do
      iblt = IBLT.new() |> IBLT.put("hello")
      assert IBLT.member?(iblt, "hello")

      iblt = IBLT.delete(iblt, "hello")
      refute IBLT.member?(iblt, "hello")
    end

    test "put + delete roundtrip restores empty state" do
      empty = IBLT.new(cell_count: 100)
      iblt = empty |> IBLT.put("x") |> IBLT.delete("x")

      # Item count should be 0
      assert IBLT.count(iblt) == 0

      # The binary bodies should be identical (all zeros)
      <<_h1::binary-size(24), body1::binary>> = empty.state
      <<_h2::binary-size(24), body2::binary>> = iblt.state
      assert body1 == body2
    end
  end

  # -- delete/3 (KV mode) --

  describe "delete/3 (KV mode)" do
    test "KV delete removes the key" do
      iblt = IBLT.new() |> IBLT.put("k", "v")
      assert IBLT.member?(iblt, "k")

      iblt = IBLT.delete(iblt, "k", "v")
      refute IBLT.member?(iblt, "k")
    end
  end

  # -- count/1 --

  describe "count/1" do
    test "starts at 0" do
      assert IBLT.count(IBLT.new()) == 0
    end

    test "increases with put" do
      iblt = IBLT.new() |> IBLT.put("a") |> IBLT.put("b")
      assert IBLT.count(iblt) == 2
    end

    test "decreases with delete" do
      iblt = IBLT.new() |> IBLT.put("a") |> IBLT.put("b") |> IBLT.delete("a")
      assert IBLT.count(iblt) == 1
    end
  end

  # -- subtract/2 --

  describe "subtract/2" do
    test "subtract produces valid IBLT" do
      a = IBLT.new() |> IBLT.put("x") |> IBLT.put("shared")
      b = IBLT.new() |> IBLT.put("y") |> IBLT.put("shared")

      diff = IBLT.subtract(a, b)
      assert is_binary(diff.state)
    end

    test "subtract + list_entries recovers symmetric difference" do
      a = IBLT.new(cell_count: 200) |> IBLT.put("only_a") |> IBLT.put("shared")
      b = IBLT.new(cell_count: 200) |> IBLT.put("only_b") |> IBLT.put("shared")

      diff = IBLT.subtract(a, b)
      result = IBLT.list_entries(diff)

      assert {:ok, entries} = result
      total = length(entries.positive) + length(entries.negative)
      assert total == 2
    end

    test "raises on incompatible IBLTs" do
      a = IBLT.new(cell_count: 100)
      b = IBLT.new(cell_count: 200)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        IBLT.subtract(a, b)
      end
    end
  end

  # -- list_entries/1 --

  describe "list_entries/1" do
    test "succeeds for small sets" do
      iblt = IBLT.new(cell_count: 200) |> IBLT.put("a") |> IBLT.put("b")
      assert {:ok, entries} = IBLT.list_entries(iblt)
      assert length(entries.positive) == 2
      assert entries.negative == []
    end

    test "returns decode_failed when overfull" do
      # Insert many items into a small IBLT to overflow it
      iblt = IBLT.new(cell_count: 10, hash_count: 3)
      iblt = Enum.reduce(1..100, iblt, fn i, acc -> IBLT.put(acc, "item_#{i}") end)
      assert {:error, :decode_failed} = IBLT.list_entries(iblt)
    end
  end

  # -- merge/2 --

  describe "merge/2" do
    test "merged IBLT contains union of items" do
      a = IBLT.new() |> IBLT.put("x")
      b = IBLT.new() |> IBLT.put("y")
      merged = IBLT.merge(a, b)

      assert IBLT.member?(merged, "x")
      assert IBLT.member?(merged, "y")
    end

    test "count reflects both sides" do
      a = IBLT.new() |> IBLT.put("x")
      b = IBLT.new() |> IBLT.put("y") |> IBLT.put("z")
      merged = IBLT.merge(a, b)
      assert IBLT.count(merged) == 3
    end

    test "raises on incompatible hash_count" do
      a = IBLT.new(hash_count: 3)
      b = IBLT.new(hash_count: 4)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        IBLT.merge(a, b)
      end
    end
  end

  # -- merge_many/1 --

  describe "merge_many/1" do
    test "merges multiple IBLTs" do
      iblts =
        Enum.map(1..3, fn i ->
          IBLT.new() |> IBLT.put("item_#{i}")
        end)

      merged = IBLT.merge_many(iblts)
      assert IBLT.member?(merged, "item_1")
      assert IBLT.member?(merged, "item_2")
      assert IBLT.member?(merged, "item_3")
    end
  end

  # -- serialize/deserialize --

  describe "serialize/1 and deserialize/1" do
    test "round-trip preserves state and membership" do
      iblt = IBLT.new() |> IBLT.put("test") |> IBLT.put("data")
      binary = IBLT.serialize(iblt)
      {:ok, recovered} = IBLT.deserialize(binary)

      assert IBLT.member?(recovered, "test")
      assert IBLT.member?(recovered, "data")
      assert IBLT.count(recovered) == 2
    end

    test "EXSK format starts with EXSK magic" do
      binary = IBLT.serialize(IBLT.new())
      assert <<"EXSK", _rest::binary>> = binary
    end

    test "sketch_id is 12" do
      binary = IBLT.serialize(IBLT.new())
      <<"EXSK", _version::8, sketch_id::8, _rest::binary>> = binary
      assert sketch_id == 12
    end

    test "rejects wrong sketch_id" do
      iblt = IBLT.new()
      binary = IBLT.serialize(iblt)
      # Corrupt the sketch_id byte (offset 5)
      <<prefix::binary-size(5), _sid::8, rest::binary>> = binary
      corrupted = <<prefix::binary, 99::8, rest::binary>>

      assert {:error, _} = IBLT.deserialize(corrupted)
    end
  end

  # -- compatible_with?/2 --

  describe "compatible_with?/2" do
    test "same params are compatible" do
      a = IBLT.new(cell_count: 100, hash_count: 3, seed: 0)
      b = IBLT.new(cell_count: 100, hash_count: 3, seed: 0)
      assert IBLT.compatible_with?(a, b)
    end

    test "different cell_count is incompatible" do
      a = IBLT.new(cell_count: 100)
      b = IBLT.new(cell_count: 200)
      refute IBLT.compatible_with?(a, b)
    end

    test "different hash_count is incompatible" do
      a = IBLT.new(hash_count: 3)
      b = IBLT.new(hash_count: 4)
      refute IBLT.compatible_with?(a, b)
    end

    test "different seed is incompatible" do
      a = IBLT.new(seed: 0)
      b = IBLT.new(seed: 42)
      refute IBLT.compatible_with?(a, b)
    end
  end

  # -- capabilities/0 --

  describe "capabilities/0" do
    test "returns expected capabilities" do
      caps = IBLT.capabilities()
      assert MapSet.member?(caps, :new)
      assert MapSet.member?(caps, :put)
      assert MapSet.member?(caps, :member?)
      assert MapSet.member?(caps, :delete)
      assert MapSet.member?(caps, :subtract)
      assert MapSet.member?(caps, :list_entries)
      assert MapSet.member?(caps, :merge)
      assert MapSet.member?(caps, :serialize)
      assert MapSet.member?(caps, :deserialize)
    end
  end

  # -- size_bytes/1 --

  describe "size_bytes/1" do
    test "positive size" do
      assert IBLT.size_bytes(IBLT.new()) > 0
    end

    test "scales with cell_count" do
      small = IBLT.size_bytes(IBLT.new(cell_count: 100))
      large = IBLT.size_bytes(IBLT.new(cell_count: 1000))
      assert large > small
    end

    test "default is 24 header + 1000 * 24 = 24024" do
      assert IBLT.size_bytes(IBLT.new()) == 24 + 1000 * 24
    end
  end

  # -- from_enumerable/2 --

  describe "from_enumerable/2" do
    test "builds from enumerable" do
      iblt = IBLT.from_enumerable(["a", "b", "c"])
      assert IBLT.member?(iblt, "a")
      assert IBLT.member?(iblt, "b")
      assert IBLT.member?(iblt, "c")
      assert IBLT.count(iblt) == 3
    end
  end

  # -- reducer/0 --

  describe "reducer/0" do
    test "works with Enum.reduce" do
      reducer = IBLT.reducer()
      iblt = Enum.reduce(["x", "y"], IBLT.new(), reducer)
      assert IBLT.member?(iblt, "x")
      assert IBLT.member?(iblt, "y")
    end
  end

  # -- merger/1 --

  describe "merger/1" do
    test "works for merging IBLTs" do
      merger_fn = IBLT.merger()

      iblts =
        Enum.map(["a", "b"], fn item ->
          IBLT.new() |> IBLT.put(item)
        end)

      merged = Enum.reduce(iblts, merger_fn)
      assert IBLT.member?(merged, "a")
      assert IBLT.member?(merged, "b")
    end
  end

  # -- update_many dispatch --

  describe "ExDataSketch.update_many/2" do
    test "dispatches to IBLT.put_many" do
      iblt = IBLT.new()
      iblt = ExDataSketch.update_many(iblt, ["a", "b"])
      assert IBLT.member?(iblt, "a")
      assert IBLT.member?(iblt, "b")
    end
  end

  # -- KV mode list_entries --

  describe "KV mode list_entries" do
    test "recovers value hashes" do
      iblt = IBLT.new(cell_count: 200) |> IBLT.put("key1", "val1")
      {:ok, entries} = IBLT.list_entries(iblt)
      assert length(entries.positive) == 1

      [{_key_hash, value_hash}] = entries.positive
      # Value hash should be non-zero since we used a real value
      assert value_hash != 0
    end
  end

  # -- Property tests --

  describe "property: no false negatives" do
    property "all inserted items are members" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 50)
            ) do
        iblt = IBLT.from_enumerable(items)

        Enum.each(items, fn item ->
          assert IBLT.member?(iblt, item), "false negative for #{inspect(item)}"
        end)
      end
    end
  end

  describe "property: put + delete identity" do
    property "put then delete returns member? == false" do
      check all(item <- string(:alphanumeric, min_length: 1)) do
        iblt = IBLT.new() |> IBLT.put(item) |> IBLT.delete(item)
        refute IBLT.member?(iblt, item)
      end
    end
  end

  describe "property: count accuracy" do
    property "count equals total number of insertions" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
            ) do
        iblt = IBLT.from_enumerable(items)
        # count tracks total insertions (not unique)
        assert IBLT.count(iblt) == length(items)
      end
    end
  end

  describe "property: serialization round-trip" do
    property "serialize then deserialize preserves membership" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
            ) do
        iblt = IBLT.from_enumerable(items)
        binary = IBLT.serialize(iblt)
        {:ok, recovered} = IBLT.deserialize(binary)

        Enum.each(items, fn item ->
          assert IBLT.member?(recovered, item)
        end)
      end
    end
  end
end
