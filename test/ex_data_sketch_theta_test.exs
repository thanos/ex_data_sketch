defmodule ExDataSketch.ThetaTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  doctest ExDataSketch.Theta

  alias ExDataSketch.Backend
  alias ExDataSketch.Errors.{DeserializationError, IncompatibleSketchesError, InvalidOptionError}
  alias ExDataSketch.Theta

  # Backends to test against
  @backends [Backend.Pure] ++
              if(Backend.Rust.available?(),
                do: [Backend.Rust],
                else: []
              )

  # -- Construction (not parameterized — tests public API/validation) --

  describe "new/1" do
    test "creates sketch with default k=4096" do
      sketch = Theta.new()
      assert sketch.opts == [k: 4096]
      assert sketch.backend == Backend.Pure
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

  # -- Parameterized backend tests --

  for backend <- @backends do
    @backend backend
    backend_name = backend |> Module.split() |> List.last()

    describe "update/2 [#{backend_name}]" do
      test "single update changes state" do
        sketch = Theta.new(k: 1024, backend: @backend) |> Theta.update("hello")
        assert byte_size(sketch.state) > 17
      end

      test "same item is deduplicated (idempotent)" do
        sketch1 = Theta.new(k: 1024, backend: @backend) |> Theta.update("hello")

        sketch2 =
          Theta.new(k: 1024, backend: @backend) |> Theta.update("hello") |> Theta.update("hello")

        assert sketch1.state == sketch2.state
      end

      test "different items produce different states" do
        sketch1 = Theta.new(k: 1024, backend: @backend) |> Theta.update("a")
        sketch2 = Theta.new(k: 1024, backend: @backend) |> Theta.update("b")
        assert sketch1.state != sketch2.state
      end

      test "entries are stored sorted" do
        sketch =
          Theta.new(k: 1024, backend: @backend)
          |> Theta.update("z")
          |> Theta.update("a")
          |> Theta.update("m")

        <<_header::binary-size(17), entries_bin::binary>> = sketch.state
        entries = decode_entries(entries_bin)
        assert entries == Enum.sort(entries)
      end
    end

    describe "update_many/2 [#{backend_name}]" do
      test "batch update produces same result as sequential updates" do
        items = ["a", "b", "c", "d", "e"]

        sequential =
          Enum.reduce(items, Theta.new(k: 1024, backend: @backend), &Theta.update(&2, &1))

        batch = Theta.new(k: 1024, backend: @backend) |> Theta.update_many(items)
        assert sequential.state == batch.state
      end

      test "empty list is a no-op" do
        sketch = Theta.new(k: 1024, backend: @backend)
        assert Theta.update_many(sketch, []).state == sketch.state
      end
    end

    describe "estimate/1 [#{backend_name}]" do
      test "empty sketch estimates 0.0" do
        assert Theta.new(k: 1024, backend: @backend) |> Theta.estimate() == 0.0
      end

      test "single item estimates approximately 1.0" do
        estimate = Theta.new(k: 4096, backend: @backend) |> Theta.update("x") |> Theta.estimate()
        assert_in_delta estimate, 1.0, 0.01
      end

      test "100 items within error bounds (k=4096)" do
        items = for i <- 0..99, do: "item_#{i}"
        estimate = Theta.from_enumerable(items, k: 4096, backend: @backend) |> Theta.estimate()
        # Exact mode (100 < k=4096), so estimate should be exactly 100.0
        assert estimate == 100.0
      end

      test "1000 items within error bounds (k=4096)" do
        items = for i <- 0..999, do: "item_#{i}"
        estimate = Theta.from_enumerable(items, k: 4096, backend: @backend) |> Theta.estimate()
        # Exact mode (1000 < k=4096)
        assert estimate == 1000.0
      end

      test "10_000 items within error bounds (k=4096)" do
        items = for i <- 0..9999, do: "item_#{i}"
        estimate = Theta.from_enumerable(items, k: 4096, backend: @backend) |> Theta.estimate()
        # Estimation mode (10000 > k=4096)
        assert_in_delta estimate, 10_000.0, 10_000 * 0.1
      end

      test "monotonicity: estimate never decreases when adding items" do
        sketch = Theta.new(k: 1024, backend: @backend)

        Enum.reduce(1..50, {sketch, 0.0}, fn i, {s, prev_est} ->
          s = Theta.update(s, "item_#{i}")
          est = Theta.estimate(s)
          assert est >= prev_est, "estimate decreased from #{prev_est} to #{est} after item #{i}"
          {s, est}
        end)
      end
    end

    describe "compact/1 [#{backend_name}]" do
      test "compacting an empty sketch returns empty" do
        sketch = Theta.new(k: 16, backend: @backend) |> Theta.compact()
        assert Theta.estimate(sketch) == 0.0
      end

      test "compacting preserves entries below theta" do
        sketch =
          Theta.new(k: 1024, backend: @backend)
          |> Theta.update("a")
          |> Theta.update("b")
          |> Theta.compact()

        assert Theta.estimate(sketch) > 0.0
      end

      test "compaction triggers when count exceeds k" do
        # Use small k to force compaction
        items = for i <- 0..99, do: "item_#{i}"
        sketch = Theta.from_enumerable(items, k: 16, backend: @backend)

        <<_v::unsigned-8, _k::unsigned-little-32, theta::unsigned-little-64,
          count::unsigned-little-32, _entries::binary>> = sketch.state

        assert count <= 16
        assert theta < 0xFFFFFFFFFFFFFFFF
      end

      test "entries are sorted after compact" do
        items = for i <- 0..99, do: "item_#{i}"
        sketch = Theta.from_enumerable(items, k: 16, backend: @backend) |> Theta.compact()

        <<_header::binary-size(17), entries_bin::binary>> = sketch.state
        entries = decode_entries(entries_bin)
        assert entries == Enum.sort(entries)
      end
    end

    describe "merge/2 [#{backend_name}]" do
      test "merging two empty sketches produces empty sketch" do
        a = Theta.new(k: 1024, backend: @backend)
        b = Theta.new(k: 1024, backend: @backend)
        merged = Theta.merge(a, b)
        assert Theta.estimate(merged) == 0.0
      end

      test "merge is commutative" do
        a = Theta.from_enumerable(["x", "y"], k: 1024, backend: @backend)
        b = Theta.from_enumerable(["y", "z"], k: 1024, backend: @backend)
        assert Theta.merge(a, b).state == Theta.merge(b, a).state
      end

      test "merge is associative" do
        a = Theta.from_enumerable(["a", "b"], k: 1024, backend: @backend)
        b = Theta.from_enumerable(["c", "d"], k: 1024, backend: @backend)
        c = Theta.from_enumerable(["e", "f"], k: 1024, backend: @backend)

        ab_c = Theta.merge(Theta.merge(a, b), c)
        a_bc = Theta.merge(a, Theta.merge(b, c))
        assert ab_c.state == a_bc.state
      end

      test "self-merge is idempotent" do
        sketch = Theta.from_enumerable(["a", "b", "c"], k: 1024, backend: @backend)
        merged = Theta.merge(sketch, sketch)
        assert merged.state == sketch.state
      end

      test "merge with empty sketch preserves state" do
        sketch = Theta.from_enumerable(["a", "b"], k: 1024, backend: @backend)
        empty = Theta.new(k: 1024, backend: @backend)
        assert Theta.merge(sketch, empty).state == sketch.state
        assert Theta.merge(empty, sketch).state == sketch.state
      end

      test "merge with estimation mode" do
        # Force both into estimation mode by exceeding k
        items_a = for i <- 0..99, do: "a_#{i}"
        items_b = for i <- 0..99, do: "b_#{i}"
        a = Theta.from_enumerable(items_a, k: 16, backend: @backend)
        b = Theta.from_enumerable(items_b, k: 16, backend: @backend)
        merged = Theta.merge(a, b)
        assert Theta.estimate(merged) > 0.0
      end

      test "raises on k mismatch" do
        a = Theta.new(k: 1024, backend: @backend)
        b = Theta.new(k: 2048, backend: @backend)

        assert_raise IncompatibleSketchesError, ~r/k mismatch/, fn ->
          Theta.merge(a, b)
        end
      end
    end

    describe "properties [#{backend_name}]" do
      property "merge commutativity" do
        check all(
                items_a <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20),
                items_b <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
              ) do
          a = Theta.from_enumerable(items_a, k: 64, backend: @backend)
          b = Theta.from_enumerable(items_b, k: 64, backend: @backend)
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
          a = Theta.from_enumerable(items_a, k: 64, backend: @backend)
          b = Theta.from_enumerable(items_b, k: 64, backend: @backend)
          c = Theta.from_enumerable(items_c, k: 64, backend: @backend)
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
          sketch_a = Theta.from_enumerable(half_a, k: 64, backend: @backend)
          sketch_full = Theta.update_many(sketch_a, half_b)
          assert Theta.estimate(sketch_full) >= Theta.estimate(sketch_a)
        end
      end
    end
  end

  # -- Non-parameterized tests --

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

    test "rejects non-power-of-2 k in params" do
      # k=100 is in range 16..2^26 but not a power of 2
      params = <<100::unsigned-little-32>>
      bin = ExDataSketch.Codec.encode(3, 1, params, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = Theta.deserialize(bin)
      assert msg =~ "power of 2"
    end
  end

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

    test "rejects truncated multi-entry preamble" do
      # Valid 8-byte preamble header indicating 2 preamble longs, but no count bytes
      binary =
        <<2::unsigned-8, 3::unsigned-8, 3::unsigned-8, 10::unsigned-8, 0::unsigned-8,
          0x1A::unsigned-8, 0::unsigned-little-16>>

      assert {:error, %DeserializationError{message: msg}} =
               Theta.deserialize_datasketches(binary, seed: nil)

      assert msg =~ "truncated"
    end

    test "rejects truncated estimation mode preamble (missing theta)" do
      # 3 preamble longs indicated, count present, but theta field missing
      binary =
        <<3::unsigned-8, 3::unsigned-8, 3::unsigned-8, 10::unsigned-8, 0::unsigned-8,
          0x1A::unsigned-8, 0::unsigned-little-16, 5::unsigned-little-32, 0::unsigned-little-32>>

      assert {:error, %DeserializationError{message: msg}} =
               Theta.deserialize_datasketches(binary, seed: nil)

      assert msg =~ "truncated"
    end

    test "rejects invalid preamble longs > 3" do
      # pre_longs = 4 is not a valid CompactSketch preamble
      binary =
        <<4::unsigned-8, 3::unsigned-8, 3::unsigned-8, 10::unsigned-8, 0::unsigned-8,
          0x1A::unsigned-8, 0::unsigned-little-16, 5::unsigned-little-32, 0::unsigned-little-32,
          0::unsigned-little-64, 0::unsigned-little-64>>

      assert {:error, %DeserializationError{message: msg}} =
               Theta.deserialize_datasketches(binary, seed: nil)

      assert msg =~ "invalid preamble longs"
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

    test "rejects lgNomLongs out of valid range" do
      # lg_nom = 3 means k=8, below minimum k=16 (lg_nom=4)
      binary =
        <<1::unsigned-8, 3::unsigned-8, 3::unsigned-8, 3::unsigned-8, 0::unsigned-8,
          0x0E::unsigned-8, 0::unsigned-little-16>>

      assert {:error, %DeserializationError{message: msg}} =
               Theta.deserialize_datasketches(binary, seed: nil)

      assert msg =~ "lgNomLongs"

      # lg_nom = 27 means k=2^27, above maximum k=2^26
      binary_high =
        <<1::unsigned-8, 3::unsigned-8, 3::unsigned-8, 27::unsigned-8, 0::unsigned-8,
          0x0E::unsigned-8, 0::unsigned-little-16>>

      assert {:error, %DeserializationError{message: msg_high}} =
               Theta.deserialize_datasketches(binary_high, seed: nil)

      assert msg_high =~ "lgNomLongs"
    end

    test "theta_from_components deduplicates entries" do
      backend = Backend.Pure
      max_theta = 0xFFFFFFFFFFFFFFFF
      # Duplicate entries should be deduplicated
      state = backend.theta_from_components(16, max_theta, [100, 200, 100, 300, 200])

      <<1, _k::unsigned-little-32, _theta::unsigned-little-64, count::unsigned-little-32,
        _::binary>> = state

      assert count == 3
    end

    test "theta_from_components filters entries >= theta" do
      backend = Backend.Pure
      # Only entries strictly below theta should be kept
      state = backend.theta_from_components(16, 500, [100, 200, 500, 600, 300])

      <<1, _k::unsigned-little-32, _theta::unsigned-little-64, count::unsigned-little-32,
        _::binary>> = state

      assert count == 3
    end

    test "theta_from_components compacts when entries exceed k" do
      backend = Backend.Pure
      max_theta = 0xFFFFFFFFFFFFFFFF
      # k=16, provide 20 entries — should compact to 16
      entries = Enum.to_list(1..20)
      state = backend.theta_from_components(16, max_theta, entries)

      <<1, _k::unsigned-little-32, theta::unsigned-little-64, count::unsigned-little-32,
        _::binary>> = state

      assert count == 16
      # theta should be set to the 17th element (first excluded)
      assert theta == 17
    end
  end

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

  describe "struct" do
    test "has expected fields" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}
      assert Map.has_key?(sketch, :state)
      assert Map.has_key?(sketch, :opts)
      assert Map.has_key?(sketch, :backend)
    end
  end

  # -- DataSketches property (not parameterized — Pure-only format) --

  describe "DataSketches properties" do
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

  # -- Cross-backend parity tests --

  if Backend.Rust.available?() do
    describe "Pure vs Rust parity" do
      test "update_many produces identical state" do
        items = for i <- 0..999, do: "item_#{i}"
        pure = Theta.from_enumerable(items, k: 4096, backend: Backend.Pure)
        rust = Theta.from_enumerable(items, k: 4096, backend: Backend.Rust)
        assert pure.state == rust.state
      end

      test "merge produces identical state" do
        items_a = for i <- 0..499, do: "a_#{i}"
        items_b = for i <- 0..499, do: "b_#{i}"
        pure_a = Theta.from_enumerable(items_a, k: 4096, backend: Backend.Pure)
        pure_b = Theta.from_enumerable(items_b, k: 4096, backend: Backend.Pure)
        rust_a = Theta.from_enumerable(items_a, k: 4096, backend: Backend.Rust)
        rust_b = Theta.from_enumerable(items_b, k: 4096, backend: Backend.Rust)
        assert Theta.merge(pure_a, pure_b).state == Theta.merge(rust_a, rust_b).state
      end

      test "estimate is identical" do
        items = for i <- 0..9999, do: "item_#{i}"
        pure = Theta.from_enumerable(items, k: 4096, backend: Backend.Pure)
        rust = Theta.from_enumerable(items, k: 4096, backend: Backend.Rust)
        assert Theta.estimate(pure) == Theta.estimate(rust)
      end

      test "serialization produces identical binary" do
        items = for i <- 0..99, do: "item_#{i}"
        pure = Theta.from_enumerable(items, k: 4096, backend: Backend.Pure)
        rust = Theta.from_enumerable(items, k: 4096, backend: Backend.Rust)
        assert Theta.serialize(pure) == Theta.serialize(rust)
      end

      test "merge in estimation mode produces identical state" do
        items_a = for i <- 0..499, do: "a_#{i}"
        items_b = for i <- 0..499, do: "b_#{i}"
        pure_a = Theta.from_enumerable(items_a, k: 64, backend: Backend.Pure)
        pure_b = Theta.from_enumerable(items_b, k: 64, backend: Backend.Pure)
        rust_a = Theta.from_enumerable(items_a, k: 64, backend: Backend.Rust)
        rust_b = Theta.from_enumerable(items_b, k: 64, backend: Backend.Rust)
        assert Theta.merge(pure_a, pure_b).state == Theta.merge(rust_a, rust_b).state
      end
    end
  end

  # -- Helpers --

  defp decode_entries(<<>>), do: []

  defp decode_entries(binary) do
    for <<val::unsigned-little-64 <- binary>>, do: val
  end
end
