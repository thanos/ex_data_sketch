defmodule ExDataSketch.MisraGriesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.MisraGries

  describe "option validation" do
    test "k defaults to 10" do
      sketch = MisraGries.new()
      assert sketch.opts[:k] == 10
    end

    test "key_encoding defaults to :binary" do
      sketch = MisraGries.new()
      assert sketch.opts[:key_encoding] == :binary
    end

    test "k must be >= 1" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        MisraGries.new(k: 0)
      end
    end
  end

  describe "empty sketch" do
    test "count is 0" do
      assert MisraGries.count(MisraGries.new()) == 0
    end

    test "entry_count is 0" do
      assert MisraGries.entry_count(MisraGries.new()) == 0
    end

    test "estimate returns 0 for any item" do
      assert MisraGries.estimate(MisraGries.new(), "hello") == 0
    end

    test "top_k returns empty list" do
      assert MisraGries.top_k(MisraGries.new(), 5) == []
    end

    test "size_bytes is positive" do
      assert MisraGries.size_bytes(MisraGries.new()) > 0
    end
  end

  describe "single item" do
    test "count is 1" do
      sketch = MisraGries.new() |> MisraGries.update("hello")
      assert MisraGries.count(sketch) == 1
    end

    test "entry_count is 1" do
      sketch = MisraGries.new() |> MisraGries.update("hello")
      assert MisraGries.entry_count(sketch) == 1
    end

    test "estimate is 1" do
      sketch = MisraGries.new() |> MisraGries.update("hello")
      assert MisraGries.estimate(sketch, "hello") == 1
    end
  end

  describe "deterministic guarantee" do
    test "items with frequency > n/k are always tracked" do
      k = 5
      heavy_count = 100
      light_items = Enum.map(1..20, fn i -> "light_#{i}" end)

      items =
        List.duplicate("heavy", heavy_count) ++
          Enum.flat_map(light_items, fn item -> List.duplicate(item, 2) end)

      sketch = MisraGries.from_enumerable(items, k: k)
      n = MisraGries.count(sketch)

      # heavy appears 100 times out of 140 total, 100 > 140/5 = 28
      assert heavy_count > n / k
      assert MisraGries.estimate(sketch, "heavy") > 0
    end

    test "heavy hitters always appear in top_k" do
      k = 3

      items =
        List.duplicate("a", 50) ++
          List.duplicate("b", 30) ++
          Enum.map(1..20, fn i -> "rare_#{i}" end)

      sketch = MisraGries.from_enumerable(items, k: k)
      top = MisraGries.top_k(sketch, k)
      top_items = Enum.map(top, &elem(&1, 0))
      assert "a" in top_items
    end
  end

  describe "update_many" do
    test "batch and sequential give same count" do
      items = ["a", "b", "a", "c", "a", "b"]

      batch = MisraGries.new(k: 5) |> MisraGries.update_many(items)

      sequential =
        Enum.reduce(items, MisraGries.new(k: 5), fn item, s ->
          MisraGries.update(s, item)
        end)

      assert MisraGries.count(batch) == MisraGries.count(sequential)
    end

    test "empty update_many is no-op" do
      sketch = MisraGries.new() |> MisraGries.update("x")
      before = MisraGries.serialize(sketch)
      after_empty = MisraGries.serialize(MisraGries.update_many(sketch, []))
      assert before == after_empty
    end
  end

  describe "top_k" do
    test "returns entries sorted by count descending" do
      sketch =
        MisraGries.new(k: 10)
        |> MisraGries.update_many(
          List.duplicate("a", 10) ++ List.duplicate("b", 5) ++ List.duplicate("c", 1)
        )

      top = MisraGries.top_k(sketch, 3)
      counts = Enum.map(top, &elem(&1, 1))
      assert counts == Enum.sort(counts, :desc)
    end

    test "respects limit" do
      sketch =
        MisraGries.new(k: 10)
        |> MisraGries.update_many(["a", "b", "c", "d", "e"])

      assert length(MisraGries.top_k(sketch, 2)) <= 2
    end
  end

  describe "frequent/2" do
    test "returns items above threshold" do
      items =
        List.duplicate("heavy", 100) ++
          List.duplicate("medium", 20) ++
          List.duplicate("light", 5)

      sketch = MisraGries.from_enumerable(items, k: 10)
      frequent = MisraGries.frequent(sketch, 0.5)

      frequent_items = Enum.map(frequent, &elem(&1, 0))
      assert "heavy" in frequent_items
      refute "light" in frequent_items
    end
  end

  describe "merge" do
    test "merge preserves count" do
      a = MisraGries.new() |> MisraGries.update_many(["a", "b", "c"])
      b = MisraGries.new() |> MisraGries.update_many(["d", "e"])
      merged = MisraGries.merge(a, b)
      assert MisraGries.count(merged) == 5
    end

    test "merge is commutative for count" do
      a = MisraGries.new() |> MisraGries.update_many(["a", "b"])
      b = MisraGries.new() |> MisraGries.update_many(["c", "d"])
      assert MisraGries.count(MisraGries.merge(a, b)) == MisraGries.count(MisraGries.merge(b, a))
    end

    test "merge with empty is identity for count" do
      a = MisraGries.new() |> MisraGries.update_many(["a", "a", "b"])
      empty = MisraGries.new()
      merged = MisraGries.merge(a, empty)
      assert MisraGries.count(merged) == MisraGries.count(a)
    end

    test "merge_many works" do
      sketches =
        Enum.map(1..3, fn _ ->
          MisraGries.new() |> MisraGries.update("x")
        end)

      merged = MisraGries.merge_many(sketches)
      assert MisraGries.count(merged) == 3
    end

    test "merge k mismatch raises IncompatibleSketchesError" do
      a = MisraGries.new(k: 5) |> MisraGries.update("a")
      b = MisraGries.new(k: 10) |> MisraGries.update("b")

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/k mismatch/, fn ->
        MisraGries.merge(a, b)
      end
    end
  end

  describe "serialize/deserialize round-trip" do
    test "empty sketch round-trips" do
      sketch = MisraGries.new(k: 10)
      binary = MisraGries.serialize(sketch)
      assert {:ok, restored} = MisraGries.deserialize(binary)
      assert MisraGries.count(restored) == 0
      assert restored.opts[:k] == 10
    end

    test "populated sketch round-trips" do
      sketch = MisraGries.from_enumerable(["a", "a", "b", "c"], k: 5)
      binary = MisraGries.serialize(sketch)
      assert {:ok, restored} = MisraGries.deserialize(binary)
      assert MisraGries.count(restored) == MisraGries.count(sketch)
      assert MisraGries.estimate(restored, "a") == MisraGries.estimate(sketch, "a")
    end

    test "EXSK header is correct" do
      sketch = MisraGries.new()
      binary = MisraGries.serialize(sketch)
      <<"EXSK", 1, 14, _rest::binary>> = binary
    end

    test "deserialize rejects wrong sketch ID" do
      params = <<10::unsigned-little-32, 0::unsigned-8>>
      state = <<0>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_kll(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               MisraGries.deserialize(binary)

      assert msg =~ "expected MisraGries sketch ID (14)"
    end

    test "deserialize rejects invalid binary" do
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} =
               MisraGries.deserialize(<<"invalid">>)
    end
  end

  describe "key encoding" do
    test "integer keys" do
      sketch = MisraGries.new(key_encoding: :int)
      sketch = MisraGries.update_many(sketch, [1, 2, 1, 1])
      assert MisraGries.count(sketch) == 4
      assert MisraGries.estimate(sketch, 1) == 3

      [{top_item, top_count} | _] = MisraGries.top_k(sketch, 1)
      assert top_item == 1
      assert top_count == 3
    end

    test "term keys" do
      sketch = MisraGries.new(key_encoding: {:term, :external})
      sketch = MisraGries.update_many(sketch, [{:user, 1}, {:user, 2}, {:user, 1}])
      assert MisraGries.count(sketch) == 3
      assert MisraGries.estimate(sketch, {:user, 1}) == 2
    end

    test "key encoding round-trips through serialization" do
      sketch = MisraGries.new(k: 5, key_encoding: :int)
      sketch = MisraGries.update_many(sketch, [10, 20, 10])
      binary = MisraGries.serialize(sketch)
      assert {:ok, restored} = MisraGries.deserialize(binary)
      assert restored.opts[:key_encoding] == :int
    end
  end

  describe "convenience functions" do
    test "from_enumerable builds sketch" do
      sketch = MisraGries.from_enumerable(["a", "b", "a"], k: 5)
      assert MisraGries.count(sketch) == 3
    end

    test "reducer works with Enum.reduce" do
      sketch = MisraGries.new()
      result = Enum.reduce(["a", "b", "c"], sketch, MisraGries.reducer())
      assert MisraGries.count(result) == 3
    end

    test "merger works" do
      sketches =
        Enum.map(1..3, fn _ ->
          MisraGries.new() |> MisraGries.update("x")
        end)

      merged = Enum.reduce(sketches, MisraGries.merger())
      assert MisraGries.count(merged) == 3
    end
  end

  describe "MG01 binary format" do
    test "magic bytes are MG01" do
      sketch = MisraGries.new()
      <<"MG01", _rest::binary>> = sketch.state
    end

    test "state grows with entries" do
      empty = MisraGries.new()
      one_item = MisraGries.new() |> MisraGries.update("hello")
      assert byte_size(one_item.state) > byte_size(empty.state)
    end
  end

  describe "codec" do
    test "sketch_id_mg is 14" do
      assert ExDataSketch.Codec.sketch_id_mg() == 14
    end
  end

  describe "ExDataSketch.update_many integration" do
    test "dispatches to MisraGries" do
      sketch = MisraGries.new() |> ExDataSketch.update_many(["a", "b", "c"])
      assert MisraGries.count(sketch) == 3
    end
  end

  describe "property tests" do
    property "count equals total number of insertions" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 100)
            ) do
        sketch = MisraGries.from_enumerable(items, k: 10)
        assert MisraGries.count(sketch) == length(items)
      end
    end

    property "deterministic guarantee: freq > n/k always tracked" do
      check all(
              k <- integer(2..10),
              heavy_count <- integer(50..100),
              light_count <- integer(1..20)
            ) do
        light_items = Enum.map(1..light_count, fn i -> "light_#{i}" end)

        items =
          List.duplicate("heavy", heavy_count) ++
            Enum.flat_map(light_items, fn item -> [item] end)

        sketch = MisraGries.from_enumerable(items, k: k)
        n = MisraGries.count(sketch)

        if heavy_count > n / k do
          assert MisraGries.estimate(sketch, "heavy") > 0
        end
      end
    end
  end
end
