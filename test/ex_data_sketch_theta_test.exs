defmodule ExDataSketch.ThetaTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  doctest ExDataSketch.Theta

  alias ExDataSketch.Errors.{DeserializationError, IncompatibleSketchesError, InvalidOptionError}
  alias ExDataSketch.Theta

  # -- Construction --

  describe "new/1" do
    test "creates sketch with default k=4096" do
      sketch = Theta.new()
      assert sketch.opts == [k: 4096]
      assert sketch.backend == ExDataSketch.Backend.Pure
    end

    test "creates sketch with custom k" do
      for k <- [16, 32, 64, 128, 256, 512, 1024, 2048, 4096] do
        sketch = Theta.new(k: k)
        assert sketch.opts == [k: k]
      end
    end

    test "empty sketch has correct binary size (17 bytes)" do
      sketch = Theta.new(k: 1024)
      assert byte_size(sketch.state) == 17
    end

    test "binary has correct header fields" do
      sketch = Theta.new(k: 1024)

      <<version::unsigned-8, k::unsigned-little-32, theta::unsigned-little-64,
        count::unsigned-little-32>> = sketch.state

      assert version == 1
      assert k == 1024
      assert theta == 0xFFFFFFFFFFFFFFFF
      assert count == 0
    end

    test "validates k must be power of 2" do
      assert_raise InvalidOptionError, ~r/power of 2/, fn ->
        Theta.new(k: 100)
      end
    end

    test "validates k minimum" do
      assert_raise InvalidOptionError, ~r/k must be/, fn ->
        Theta.new(k: 8)
      end
    end

    test "validates k maximum" do
      assert_raise InvalidOptionError, ~r/k must be/, fn ->
        Theta.new(k: 1 <<< 27)
      end
    end

    test "validates k type" do
      assert_raise InvalidOptionError, ~r/k must be/, fn ->
        Theta.new(k: "4096")
      end
    end
  end

  # -- Update --

  describe "update/2" do
    test "single update changes state" do
      sketch = Theta.new(k: 1024) |> Theta.update("hello")
      assert byte_size(sketch.state) > 17
    end

    test "same item is deduplicated (idempotent)" do
      sketch1 = Theta.new(k: 1024) |> Theta.update("hello")
      sketch2 = Theta.new(k: 1024) |> Theta.update("hello") |> Theta.update("hello")
      assert sketch1.state == sketch2.state
    end

    test "different items produce different states" do
      sketch1 = Theta.new(k: 1024) |> Theta.update("a")
      sketch2 = Theta.new(k: 1024) |> Theta.update("b")
      assert sketch1.state != sketch2.state
    end

    test "entries are stored sorted" do
      sketch = Theta.new(k: 1024) |> Theta.update("z") |> Theta.update("a") |> Theta.update("m")

      <<_header::binary-size(17), entries_bin::binary>> = sketch.state
      entries = decode_entries(entries_bin)
      assert entries == Enum.sort(entries)
    end
  end

  # -- Update Many --

  describe "update_many/2" do
    test "batch update produces same result as sequential updates" do
      items = ["a", "b", "c", "d", "e"]
      sequential = Enum.reduce(items, Theta.new(k: 1024), &Theta.update(&2, &1))
      batch = Theta.new(k: 1024) |> Theta.update_many(items)
      assert sequential.state == batch.state
    end

    test "empty list is a no-op" do
      sketch = Theta.new(k: 1024)
      assert Theta.update_many(sketch, []).state == sketch.state
    end
  end

  # -- Estimate --

  describe "estimate/1" do
    test "empty sketch estimates 0.0" do
      assert Theta.new(k: 1024) |> Theta.estimate() == 0.0
    end

    test "single item estimates approximately 1.0" do
      estimate = Theta.new(k: 4096) |> Theta.update("x") |> Theta.estimate()
      assert_in_delta estimate, 1.0, 0.01
    end

    test "100 items within error bounds (k=4096)" do
      items = for i <- 0..99, do: "item_#{i}"
      estimate = Theta.from_enumerable(items, k: 4096) |> Theta.estimate()
      # Exact mode (100 < k=4096), so estimate should be exactly 100.0
      assert estimate == 100.0
    end

    test "1000 items within error bounds (k=4096)" do
      items = for i <- 0..999, do: "item_#{i}"
      estimate = Theta.from_enumerable(items, k: 4096) |> Theta.estimate()
      # Exact mode (1000 < k=4096)
      assert estimate == 1000.0
    end

    test "10_000 items within error bounds (k=4096)" do
      items = for i <- 0..9999, do: "item_#{i}"
      estimate = Theta.from_enumerable(items, k: 4096) |> Theta.estimate()
      # Estimation mode (10000 > k=4096)
      assert_in_delta estimate, 10_000.0, 10_000 * 0.1
    end

    test "monotonicity: estimate never decreases when adding items" do
      sketch = Theta.new(k: 1024)

      Enum.reduce(1..50, {sketch, 0.0}, fn i, {s, prev_est} ->
        s = Theta.update(s, "item_#{i}")
        est = Theta.estimate(s)
        assert est >= prev_est, "estimate decreased from #{prev_est} to #{est} after item #{i}"
        {s, est}
      end)
    end
  end

  # -- Compaction --

  describe "compact/1" do
    test "compacting an empty sketch returns empty" do
      sketch = Theta.new(k: 16) |> Theta.compact()
      assert Theta.estimate(sketch) == 0.0
    end

    test "compacting preserves entries below theta" do
      sketch = Theta.new(k: 1024) |> Theta.update("a") |> Theta.update("b") |> Theta.compact()
      assert Theta.estimate(sketch) > 0.0
    end

    test "compaction triggers when count exceeds k" do
      # Use small k to force compaction
      items = for i <- 0..99, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 16)

      <<_v::unsigned-8, _k::unsigned-little-32, theta::unsigned-little-64,
        count::unsigned-little-32, _entries::binary>> = sketch.state

      assert count <= 16
      assert theta < 0xFFFFFFFFFFFFFFFF
    end

    test "entries are sorted after compact" do
      items = for i <- 0..99, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 16) |> Theta.compact()

      <<_header::binary-size(17), entries_bin::binary>> = sketch.state
      entries = decode_entries(entries_bin)
      assert entries == Enum.sort(entries)
    end
  end

  # -- Merge --

  describe "merge/2" do
    test "merging two empty sketches produces empty sketch" do
      a = Theta.new(k: 1024)
      b = Theta.new(k: 1024)
      merged = Theta.merge(a, b)
      assert Theta.estimate(merged) == 0.0
    end

    test "merge is commutative" do
      a = Theta.from_enumerable(["x", "y"], k: 1024)
      b = Theta.from_enumerable(["y", "z"], k: 1024)
      assert Theta.merge(a, b).state == Theta.merge(b, a).state
    end

    test "merge is associative" do
      a = Theta.from_enumerable(["a", "b"], k: 1024)
      b = Theta.from_enumerable(["c", "d"], k: 1024)
      c = Theta.from_enumerable(["e", "f"], k: 1024)

      ab_c = Theta.merge(Theta.merge(a, b), c)
      a_bc = Theta.merge(a, Theta.merge(b, c))
      assert ab_c.state == a_bc.state
    end

    test "self-merge is idempotent" do
      sketch = Theta.from_enumerable(["a", "b", "c"], k: 1024)
      merged = Theta.merge(sketch, sketch)
      assert merged.state == sketch.state
    end

    test "merge with empty sketch preserves state" do
      sketch = Theta.from_enumerable(["a", "b"], k: 1024)
      empty = Theta.new(k: 1024)
      assert Theta.merge(sketch, empty).state == sketch.state
      assert Theta.merge(empty, sketch).state == sketch.state
    end

    test "merge with estimation mode" do
      # Force both into estimation mode by exceeding k
      items_a = for i <- 0..99, do: "a_#{i}"
      items_b = for i <- 0..99, do: "b_#{i}"
      a = Theta.from_enumerable(items_a, k: 16)
      b = Theta.from_enumerable(items_b, k: 16)
      merged = Theta.merge(a, b)
      assert Theta.estimate(merged) > 0.0
    end

    test "raises on k mismatch" do
      a = Theta.new(k: 1024)
      b = Theta.new(k: 2048)

      assert_raise IncompatibleSketchesError, ~r/k mismatch/, fn ->
        Theta.merge(a, b)
      end
    end
  end

  # -- Serialization (EXSK) --

  describe "serialize/deserialize" do
    test "round-trip preserves state and opts" do
      sketch = Theta.from_enumerable(["a", "b", "c"], k: 1024)
      binary = Theta.serialize(sketch)
      assert {:ok, restored} = Theta.deserialize(binary)
      assert restored.state == sketch.state
      assert restored.opts == sketch.opts
    end

    test "round-trip preserves estimate" do
      sketch = Theta.from_enumerable(for(i <- 1..100, do: i), k: 4096)
      binary = Theta.serialize(sketch)
      {:ok, restored} = Theta.deserialize(binary)
      assert Theta.estimate(restored) == Theta.estimate(sketch)
    end

    test "rejects invalid binary" do
      assert {:error, %DeserializationError{}} = Theta.deserialize(<<"invalid">>)
    end

    test "rejects wrong sketch ID" do
      bin = ExDataSketch.Codec.encode(1, 1, <<0, 0, 0, 0>>, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = Theta.deserialize(bin)
      assert msg =~ "expected Theta sketch ID (3)"
    end

    test "rejects invalid params binary" do
      bin = ExDataSketch.Codec.encode(3, 1, <<>>, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = Theta.deserialize(bin)
      assert msg =~ "invalid Theta params"
    end
  end

  # -- DataSketches Interop --

  describe "serialize_datasketches/deserialize_datasketches" do
    test "round-trip preserves estimate for empty sketch" do
      sketch = Theta.new(k: 1024)
      binary = Theta.serialize_datasketches(sketch)
      assert {:ok, restored} = Theta.deserialize_datasketches(binary)
      assert Theta.estimate(restored) == 0.0
    end

    test "round-trip preserves estimate for single item" do
      sketch = Theta.new(k: 1024) |> Theta.update("hello")
      binary = Theta.serialize_datasketches(sketch)
      assert {:ok, restored} = Theta.deserialize_datasketches(binary)
      assert Theta.estimate(restored) == Theta.estimate(sketch)
    end

    test "round-trip preserves estimate for exact mode" do
      items = for i <- 0..49, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 1024)
      binary = Theta.serialize_datasketches(sketch)
      assert {:ok, restored} = Theta.deserialize_datasketches(binary)
      assert Theta.estimate(restored) == Theta.estimate(sketch)
    end

    test "round-trip preserves estimate for estimation mode" do
      items = for i <- 0..99, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 16)
      binary = Theta.serialize_datasketches(sketch)
      assert {:ok, restored} = Theta.deserialize_datasketches(binary)
      assert Theta.estimate(restored) == Theta.estimate(sketch)
    end

    test "empty sketch produces 8-byte binary" do
      sketch = Theta.new(k: 1024)
      binary = Theta.serialize_datasketches(sketch)
      assert byte_size(binary) == 8
    end

    test "single item produces 16-byte binary" do
      sketch = Theta.new(k: 1024) |> Theta.update("hello")
      binary = Theta.serialize_datasketches(sketch)
      assert byte_size(binary) == 16
    end

    test "exact mode produces correct size" do
      items = for i <- 0..9, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 1024)
      binary = Theta.serialize_datasketches(sketch)
      # 2 preamble longs (16 bytes) + 10 entries (80 bytes) = 96
      assert byte_size(binary) == 16 + 10 * 8
    end

    test "estimation mode produces correct size" do
      items = for i <- 0..99, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 16)

      <<_v::unsigned-8, _k::unsigned-little-32, _theta::unsigned-little-64,
        count::unsigned-little-32, _::binary>> = sketch.state

      binary = Theta.serialize_datasketches(sketch)
      # 3 preamble longs (24 bytes) + count entries
      assert byte_size(binary) == 24 + count * 8
    end

    test "preamble has correct serial version and family ID" do
      sketch = Theta.new(k: 1024) |> Theta.update("x")
      binary = Theta.serialize_datasketches(sketch)
      <<_pre::unsigned-8, ser_ver::unsigned-8, fam_id::unsigned-8, _rest::binary>> = binary
      assert ser_ver == 3
      assert fam_id == 3
    end

    test "rejects invalid binary" do
      assert {:error, %DeserializationError{}} = Theta.deserialize_datasketches(<<1, 2>>)
    end

    test "rejects wrong serial version" do
      # Craft a minimal valid-looking but wrong version preamble
      binary =
        <<1::unsigned-8, 2::unsigned-8, 3::unsigned-8, 10::unsigned-8, 0::unsigned-8,
          0x0E::unsigned-8, 0::unsigned-little-16>>

      assert {:error, %DeserializationError{message: msg}} =
               Theta.deserialize_datasketches(binary, seed: nil)

      assert msg =~ "serial version"
    end
  end

  # -- Integration --

  describe "from_enumerable/2" do
    test "builds sketch from enumerable" do
      sketch = Theta.from_enumerable(["a", "b", "c"], k: 1024)
      assert Theta.estimate(sketch) > 0.0
    end

    test "works with ranges" do
      sketch = Theta.from_enumerable(1..100, k: 4096)
      assert Theta.estimate(sketch) == 100.0
    end
  end

  describe "merge_many/1" do
    test "raises Enum.EmptyError on empty list" do
      assert_raise Enum.EmptyError, fn ->
        Theta.merge_many([])
      end
    end

    test "returns single sketch unchanged" do
      sketch = Theta.new(k: 1024)
      assert Theta.merge_many([sketch]) == sketch
    end

    test "merges multiple sketches" do
      sketches =
        for i <- 1..5, do: Theta.from_enumerable(((i - 1) * 20 + 1)..(i * 20), k: 4096)

      merged = Theta.merge_many(sketches)
      assert Theta.estimate(merged) == 100.0
    end
  end

  describe "reducer/0" do
    test "returns a 2-arity function" do
      assert is_function(Theta.reducer(), 2)
    end

    test "works with Enum.reduce" do
      sketch = Enum.reduce(["a", "b", "c"], Theta.new(k: 1024), Theta.reducer())
      assert Theta.estimate(sketch) > 0.0
    end
  end

  describe "merger/0" do
    test "returns a 2-arity function" do
      assert is_function(Theta.merger(), 2)
    end

    test "works for merging sketches" do
      a = Theta.from_enumerable(["x"], k: 1024)
      b = Theta.from_enumerable(["y"], k: 1024)
      merged = Theta.merger().(a, b)
      assert Theta.estimate(merged) > 0.0
    end
  end

  describe "size_bytes/1" do
    test "returns 17 for empty sketch" do
      assert Theta.new(k: 1024) |> Theta.size_bytes() == 17
    end

    test "increases after adding items" do
      sketch = Theta.new(k: 1024) |> Theta.update("x")
      assert Theta.size_bytes(sketch) == 17 + 8
    end
  end

  # -- Property Tests --

  describe "properties" do
    property "merge commutativity" do
      check all(
              items_a <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20),
              items_b <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
            ) do
        a = Theta.from_enumerable(items_a, k: 64)
        b = Theta.from_enumerable(items_b, k: 64)
        assert Theta.merge(a, b).state == Theta.merge(b, a).state
      end
    end

    property "merge associativity" do
      check all(
              items_a <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 10),
              items_b <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 10),
              items_c <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 10)
            ) do
        a = Theta.from_enumerable(items_a, k: 64)
        b = Theta.from_enumerable(items_b, k: 64)
        c = Theta.from_enumerable(items_c, k: 64)
        ab_c = Theta.merge(Theta.merge(a, b), c)
        a_bc = Theta.merge(a, Theta.merge(b, c))
        assert ab_c.state == a_bc.state
      end
    end

    property "monotonicity: estimate grows or stays with more items" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 2, max_length: 30)
            ) do
        {half_a, half_b} = Enum.split(items, div(length(items), 2))
        sketch_a = Theta.from_enumerable(half_a, k: 64)
        sketch_full = Theta.update_many(sketch_a, half_b)
        assert Theta.estimate(sketch_full) >= Theta.estimate(sketch_a)
      end
    end

    property "DataSketches round-trip preserves entries" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
            ) do
        sketch = Theta.from_enumerable(items, k: 64)
        binary = Theta.serialize_datasketches(sketch)
        assert {:ok, restored} = Theta.deserialize_datasketches(binary)
        assert Theta.estimate(restored) == Theta.estimate(sketch)
      end
    end
  end

  describe "struct" do
    test "has expected fields" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}
      assert Map.has_key?(sketch, :state)
      assert Map.has_key?(sketch, :opts)
      assert Map.has_key?(sketch, :backend)
    end
  end

  # -- Helpers --

  defp decode_entries(<<>>), do: []

  defp decode_entries(binary) do
    for <<val::unsigned-little-64 <- binary>>, do: val
  end
end
