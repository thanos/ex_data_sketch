defmodule ExDataSketch.FrequentItemsTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.FrequentItems

  # Deterministic dataset: "a"x100 + "b"x60 + "c"x30 + "d"x10 + "u1".."u50" each once = 250
  @values_v1 List.duplicate("a", 100) ++
               List.duplicate("b", 60) ++
               List.duplicate("c", 30) ++
               List.duplicate("d", 10) ++
               Enum.map(1..50, fn i -> "u#{i}" end)

  describe "new/1" do
    test "creates sketch with default options" do
      sketch = FrequentItems.new()
      assert %FrequentItems{} = sketch
      assert sketch.opts[:k] == 10
      assert sketch.opts[:key_encoding] == :binary
      assert sketch.opts[:flags] == 0
      assert is_binary(sketch.state)
      assert sketch.backend == ExDataSketch.Backend.Pure
    end

    test "creates sketch with custom k" do
      sketch = FrequentItems.new(k: 5)
      assert sketch.opts[:k] == 5
    end

    test "creates sketch with integer key encoding" do
      sketch = FrequentItems.new(key_encoding: :int)
      assert sketch.opts[:key_encoding] == :int
      assert sketch.opts[:flags] == 1
    end

    test "creates sketch with term external key encoding" do
      sketch = FrequentItems.new(key_encoding: {:term, :external})
      assert sketch.opts[:key_encoding] == {:term, :external}
      assert sketch.opts[:flags] == 2
    end

    test "raises on invalid k" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        FrequentItems.new(k: 0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        FrequentItems.new(k: -1)
      end
    end

    test "raises on non-integer k" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        FrequentItems.new(k: 1.5)
      end
    end

    test "raises on invalid key_encoding" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/key_encoding/, fn ->
        FrequentItems.new(key_encoding: :invalid)
      end
    end
  end

  describe "struct fields" do
    test "has state, opts, and backend fields" do
      sketch = FrequentItems.new(k: 5)
      assert Map.has_key?(sketch, :state)
      assert Map.has_key?(sketch, :opts)
      assert Map.has_key?(sketch, :backend)
    end
  end

  describe "empty sketch" do
    test "count is 0" do
      assert FrequentItems.count(FrequentItems.new(k: 5)) == 0
    end

    test "entry_count is 0" do
      assert FrequentItems.entry_count(FrequentItems.new(k: 5)) == 0
    end

    test "top_k is empty list" do
      assert FrequentItems.top_k(FrequentItems.new(k: 5)) == []
    end
  end

  describe "single item" do
    test "count is 1 after one update" do
      sketch = FrequentItems.new(k: 5) |> FrequentItems.update("hello")
      assert FrequentItems.count(sketch) == 1
    end

    test "entry_count is 1 after one update" do
      sketch = FrequentItems.new(k: 5) |> FrequentItems.update("hello")
      assert FrequentItems.entry_count(sketch) == 1
    end

    test "estimate returns count=1 for tracked item" do
      sketch = FrequentItems.new(k: 5) |> FrequentItems.update("hello")
      assert {:ok, est} = FrequentItems.estimate(sketch, "hello")
      assert est.estimate == 1
      assert est.error == 0
      assert est.lower == 1
      assert est.upper == 1
    end

    test "estimate returns :not_tracked for absent item" do
      sketch = FrequentItems.new(k: 5) |> FrequentItems.update("hello")
      assert {:error, :not_tracked} = FrequentItems.estimate(sketch, "missing")
    end
  end

  describe "deterministic dataset @values_v1" do
    test "k=5: top_k contains 'a' and 'b', entry_count <= 5, count=250" do
      sketch = FrequentItems.from_enumerable(@values_v1, k: 5)
      assert FrequentItems.count(sketch) == 250
      assert FrequentItems.entry_count(sketch) <= 5

      top = FrequentItems.top_k(sketch)
      items = Enum.map(top, & &1.item)
      assert "a" in items
      assert "b" in items
    end

    test "k=10: top_k contains 'a', 'b', 'c', 'd', entry_count <= 10" do
      sketch = FrequentItems.from_enumerable(@values_v1, k: 10)
      assert FrequentItems.count(sketch) == 250
      assert FrequentItems.entry_count(sketch) <= 10

      top = FrequentItems.top_k(sketch)
      items = Enum.map(top, & &1.item)
      assert "a" in items
      assert "b" in items
      assert "c" in items
      assert "d" in items
    end

    test "top_k is sorted by estimate descending, key ascending on ties" do
      sketch = FrequentItems.from_enumerable(@values_v1, k: 10)
      top = FrequentItems.top_k(sketch)
      assert hd(top).item == "a"

      # Verify sort: descending by estimate, ascending by key on ties
      pairs = Enum.map(top, fn e -> {-e.estimate, e.item} end)
      assert pairs == Enum.sort(pairs)
    end

    test "top_k with limit returns at most limit entries" do
      sketch = FrequentItems.from_enumerable(@values_v1, k: 10)
      assert length(FrequentItems.top_k(sketch, limit: 2)) <= 2
    end
  end

  describe "update_many batch equivalence" do
    test "pre-aggregated matches sequential for same inputs" do
      items = ["x", "y", "x", "z", "y", "x"]

      sequential =
        Enum.reduce(items, FrequentItems.new(k: 5), fn item, sketch ->
          FrequentItems.update(sketch, item)
        end)

      batch = FrequentItems.new(k: 5) |> FrequentItems.update_many(items)

      assert FrequentItems.count(sequential) == FrequentItems.count(batch)
      assert FrequentItems.entry_count(sequential) == FrequentItems.entry_count(batch)

      # Same top_k results
      assert FrequentItems.top_k(sequential) == FrequentItems.top_k(batch)
    end
  end

  describe "merge" do
    test "count additivity" do
      a = FrequentItems.from_enumerable(["x", "x", "y"], k: 5)
      b = FrequentItems.from_enumerable(["y", "z"], k: 5)
      merged = FrequentItems.merge(a, b)
      assert FrequentItems.count(merged) == 5
    end

    test "identity: merge with empty" do
      sketch = FrequentItems.from_enumerable(["a", "b", "a"], k: 5)
      empty = FrequentItems.new(k: 5)

      merged_right = FrequentItems.merge(sketch, empty)
      merged_left = FrequentItems.merge(empty, sketch)

      assert FrequentItems.count(merged_right) == FrequentItems.count(sketch)
      assert FrequentItems.count(merged_left) == FrequentItems.count(sketch)
      assert FrequentItems.serialize(merged_right) == FrequentItems.serialize(sketch)
      assert FrequentItems.serialize(merged_left) == FrequentItems.serialize(sketch)
    end

    test "commutativity" do
      a = FrequentItems.from_enumerable(["x", "x", "y"], k: 5)
      b = FrequentItems.from_enumerable(["y", "z", "z"], k: 5)

      ab = FrequentItems.merge(a, b)
      ba = FrequentItems.merge(b, a)

      assert FrequentItems.serialize(ab) == FrequentItems.serialize(ba)
    end

    test "merge_many" do
      sketches =
        Enum.map(1..4, fn i ->
          FrequentItems.from_enumerable([to_string(i)], k: 5)
        end)

      merged = FrequentItems.merge_many(sketches)
      assert FrequentItems.count(merged) == 4
      assert FrequentItems.entry_count(merged) == 4
    end

    test "key_encoding mismatch raises IncompatibleSketchesError" do
      a = FrequentItems.new(k: 5, key_encoding: :binary)
      b = FrequentItems.new(k: 5, key_encoding: :int)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/key_encoding mismatch/, fn ->
        FrequentItems.merge(a, b)
      end
    end

    test "k mismatch raises IncompatibleSketchesError" do
      a = FrequentItems.new(k: 5)
      b = FrequentItems.new(k: 10)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/k mismatch/, fn ->
        FrequentItems.merge(a, b)
      end
    end
  end

  describe "serialize/deserialize" do
    test "round-trip preserves sketch" do
      sketch = FrequentItems.from_enumerable(["a", "b", "a", "c"], k: 5)
      binary = FrequentItems.serialize(sketch)

      assert {:ok, restored} = FrequentItems.deserialize(binary)
      assert FrequentItems.count(restored) == FrequentItems.count(sketch)
      assert FrequentItems.entry_count(restored) == FrequentItems.entry_count(sketch)
      assert FrequentItems.top_k(restored) == FrequentItems.top_k(sketch)
    end

    test "serialized binary starts with EXSK magic" do
      sketch = FrequentItems.new(k: 5)
      binary = FrequentItems.serialize(sketch)
      assert <<"EXSK", _rest::binary>> = binary
    end

    test "rejects invalid binary" do
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} =
               FrequentItems.deserialize(<<"invalid">>)
    end

    test "rejects wrong sketch ID" do
      bin = ExDataSketch.Codec.encode(1, 1, <<10::unsigned-little-32, 0::unsigned-8>>, <<>>)

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               FrequentItems.deserialize(bin)

      assert msg =~ "expected FrequentItems sketch ID (6)"
    end

    test "rejects invalid params binary" do
      bin = ExDataSketch.Codec.encode(6, 1, <<>>, <<>>)

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               FrequentItems.deserialize(bin)

      assert msg =~ "invalid FrequentItems params binary"
    end
  end

  describe "integer keys" do
    test "encode/decode round-trip" do
      sketch =
        FrequentItems.new(k: 5, key_encoding: :int)
        |> FrequentItems.update_many([1, 2, 3, 1, 2, 1])

      assert FrequentItems.count(sketch) == 6
      assert FrequentItems.entry_count(sketch) == 3

      {:ok, est} = FrequentItems.estimate(sketch, 1)
      assert est.estimate == 3

      top = FrequentItems.top_k(sketch)
      assert hd(top).item == 1
    end

    test "serialize/deserialize preserves integer keys" do
      sketch =
        FrequentItems.new(k: 5, key_encoding: :int)
        |> FrequentItems.update_many([10, 20, 10])

      binary = FrequentItems.serialize(sketch)
      {:ok, restored} = FrequentItems.deserialize(binary)

      {:ok, est} = FrequentItems.estimate(restored, 10)
      assert est.estimate == 2
    end
  end

  describe "entry_count <= k invariant" do
    test "never exceeds k" do
      sketch = FrequentItems.from_enumerable(Enum.map(1..100, &to_string/1), k: 5)
      assert FrequentItems.entry_count(sketch) <= 5
    end
  end

  describe "FI1 binary format" do
    test "magic bytes" do
      sketch = FrequentItems.new(k: 5)
      <<"FI1\0", _rest::binary>> = sketch.state
    end

    test "header is 32 bytes for empty sketch" do
      sketch = FrequentItems.new(k: 5)
      assert byte_size(sketch.state) == 32
    end
  end

  describe "codec" do
    test "sketch_id_fi returns 6" do
      assert ExDataSketch.Codec.sketch_id_fi() == 6
    end
  end

  describe "convenience functions" do
    test "reducer returns a 2-arity function" do
      assert is_function(FrequentItems.reducer(), 2)
    end

    test "merger returns a 2-arity function" do
      assert is_function(FrequentItems.merger(), 2)
    end

    test "from_enumerable builds sketch" do
      sketch = FrequentItems.from_enumerable(["a", "b", "a"], k: 5)
      assert FrequentItems.count(sketch) == 3
    end

    test "reducer works with Enum.reduce" do
      sketch =
        Enum.reduce(["x", "y", "x"], FrequentItems.new(k: 5), FrequentItems.reducer())

      assert FrequentItems.count(sketch) == 3
    end

    test "merger works with Enum.reduce" do
      sketches =
        Enum.map(["a", "b", "c"], fn item ->
          FrequentItems.from_enumerable([item], k: 5)
        end)

      merged = Enum.reduce(sketches, FrequentItems.merger())
      assert FrequentItems.count(merged) == 3
    end
  end

  describe "facade" do
    test "ExDataSketch.update_many/2 dispatches to FrequentItems" do
      sketch = FrequentItems.new(k: 5)
      updated = ExDataSketch.update_many(sketch, ["a", "b", "c"])
      assert FrequentItems.count(updated) == 3
    end
  end

  describe "frequent/2" do
    test "returns items above threshold" do
      sketch = FrequentItems.from_enumerable(List.duplicate("a", 100) ++ ["b"], k: 10)
      frequent = FrequentItems.frequent(sketch, 50)
      items = Enum.map(frequent, & &1.item)
      assert "a" in items
      refute "b" in items
    end

    test "returns empty list when nothing above threshold" do
      sketch = FrequentItems.from_enumerable(["a", "b", "c"], k: 10)
      assert FrequentItems.frequent(sketch, 100) == []
    end
  end
end
