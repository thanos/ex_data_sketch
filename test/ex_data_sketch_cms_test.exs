defmodule ExDataSketch.CMSTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  doctest ExDataSketch.CMS

  alias ExDataSketch.Backend
  alias ExDataSketch.CMS
  alias ExDataSketch.Errors.{DeserializationError, IncompatibleSketchesError, InvalidOptionError}

  # Backends to test against
  @backends [Backend.Pure] ++
              if(Backend.Rust.available?(),
                do: [Backend.Rust],
                else: []
              )

  # -- Construction (not parameterized — tests public API/validation) --

  describe "new/1" do
    test "creates sketch with default options" do
      sketch = CMS.new()
      assert sketch.opts[:width] == 2048
      assert sketch.opts[:depth] == 5
      assert sketch.opts[:counter_width] == 32
      assert sketch.opts[:hash_strategy] in [:phash2, :xxhash3]
      assert sketch.backend == Backend.Pure
    end

    test "creates sketch with custom options" do
      sketch = CMS.new(width: 1024, depth: 3, counter_width: 64)
      assert sketch.opts[:width] == 1024
      assert sketch.opts[:depth] == 3
      assert sketch.opts[:counter_width] == 64
    end

    test "binary has correct size" do
      sketch = CMS.new(width: 100, depth: 3, counter_width: 32)
      # 9 header + 100*3*4 = 1209
      assert byte_size(sketch.state) == 1209
    end

    test "binary has correct size for 64-bit counters" do
      sketch = CMS.new(width: 100, depth: 3, counter_width: 64)
      # 9 header + 100*3*8 = 2409
      assert byte_size(sketch.state) == 2409
    end

    test "binary has correct header fields" do
      sketch = CMS.new(width: 512, depth: 3, counter_width: 32)

      <<version::unsigned-8, width::unsigned-little-32, depth::unsigned-little-16, cw::unsigned-8,
        reserved::unsigned-8, _counters::binary>> = sketch.state

      assert version == 1
      assert width == 512
      assert depth == 3
      assert cw == 32
      assert reserved == 0
    end

    test "counters are initially all zero" do
      sketch = CMS.new(width: 10, depth: 2, counter_width: 32)
      <<_header::binary-size(9), counters::binary>> = sketch.state
      assert counters == :binary.copy(<<0>>, 10 * 2 * 4)
    end

    test "validates width must be positive" do
      assert_raise InvalidOptionError, ~r/width must be a positive integer/, fn ->
        CMS.new(width: 0)
      end
    end

    test "validates width type" do
      assert_raise InvalidOptionError, ~r/width must be a positive integer/, fn ->
        CMS.new(width: "2048")
      end
    end

    test "validates depth must be positive" do
      assert_raise InvalidOptionError, ~r/depth must be a positive integer/, fn ->
        CMS.new(depth: 0)
      end
    end

    test "validates counter_width must be 32 or 64" do
      assert_raise InvalidOptionError, ~r/counter_width must be 32 or 64/, fn ->
        CMS.new(counter_width: 16)
      end
    end
  end

  # -- Parameterized backend tests --

  for backend <- @backends do
    @backend backend
    backend_name = backend |> Module.split() |> List.last()

    describe "update/2 [#{backend_name}]" do
      test "empty sketch estimates 0" do
        sketch = CMS.new(backend: @backend)
        assert CMS.estimate(sketch, "hello") == 0
      end

      test "single item has exact count" do
        sketch = CMS.new(backend: @backend) |> CMS.update("hello")
        assert CMS.estimate(sketch, "hello") == 1
      end

      test "update with custom increment" do
        sketch = CMS.new(backend: @backend) |> CMS.update("hello", 5)
        assert CMS.estimate(sketch, "hello") == 5
      end

      test "multiple updates accumulate" do
        sketch =
          CMS.new(backend: @backend) |> CMS.update("a") |> CMS.update("a") |> CMS.update("a")

        assert CMS.estimate(sketch, "a") == 3
      end

      test "known frequencies" do
        sketch =
          CMS.new(backend: @backend)
          |> CMS.update("a", 10)
          |> CMS.update("b", 3)
          |> CMS.update("c", 7)

        # Estimates >= true count (no undercount guarantee)
        assert CMS.estimate(sketch, "a") >= 10
        assert CMS.estimate(sketch, "b") >= 3
        assert CMS.estimate(sketch, "c") >= 7
      end
    end

    describe "update_many/2 [#{backend_name}]" do
      test "batch update produces same result as sequential updates" do
        items = ["a", "b", "c", "d", "e"]

        sequential =
          Enum.reduce(items, CMS.new(backend: @backend), fn item, s -> CMS.update(s, item) end)

        batch = CMS.new(backend: @backend) |> CMS.update_many(items)
        assert sequential.state == batch.state
      end

      test "accepts {item, increment} tuples" do
        sketch = CMS.new(backend: @backend) |> CMS.update_many([{"a", 5}, {"b", 3}])
        assert CMS.estimate(sketch, "a") >= 5
        assert CMS.estimate(sketch, "b") >= 3
      end

      test "empty list is a no-op" do
        sketch = CMS.new(backend: @backend)
        assert CMS.update_many(sketch, []).state == sketch.state
      end
    end

    describe "no-undercount [#{backend_name}]" do
      test "estimate is always >= true count" do
        items = List.duplicate("x", 100) ++ List.duplicate("y", 50) ++ List.duplicate("z", 1)
        sketch = CMS.from_enumerable(items, backend: @backend)
        assert CMS.estimate(sketch, "x") >= 100
        assert CMS.estimate(sketch, "y") >= 50
        assert CMS.estimate(sketch, "z") >= 1
      end
    end

    describe "merge/2 [#{backend_name}]" do
      test "merge is commutative" do
        a = CMS.from_enumerable(["x", "y"], backend: @backend)
        b = CMS.from_enumerable(["y", "z"], backend: @backend)
        assert CMS.merge(a, b).state == CMS.merge(b, a).state
      end

      test "merge adds counts" do
        a = CMS.new(backend: @backend) |> CMS.update("x", 3)
        b = CMS.new(backend: @backend) |> CMS.update("x", 5)
        merged = CMS.merge(a, b)
        assert CMS.estimate(merged, "x") >= 8
      end

      test "merge with empty preserves counts" do
        sketch = CMS.new(backend: @backend) |> CMS.update("x", 5)
        empty = CMS.new(backend: @backend)
        assert CMS.estimate(CMS.merge(sketch, empty), "x") >= 5
        assert CMS.estimate(CMS.merge(empty, sketch), "x") >= 5
      end

      test "merge is associative" do
        a = CMS.from_enumerable(["a", "b"], backend: @backend)
        b = CMS.from_enumerable(["c", "d"], backend: @backend)
        c = CMS.from_enumerable(["e", "f"], backend: @backend)

        ab_c = CMS.merge(CMS.merge(a, b), c)
        a_bc = CMS.merge(a, CMS.merge(b, c))
        assert ab_c.state == a_bc.state
      end

      test "raises on parameter mismatch" do
        a = CMS.new(width: 1024, backend: @backend)
        b = CMS.new(width: 2048, backend: @backend)

        assert_raise IncompatibleSketchesError, ~r/CMS parameter mismatch/, fn ->
          CMS.merge(a, b)
        end
      end
    end

    describe "saturating overflow [#{backend_name}]" do
      test "32-bit counter saturates at 2^32-1" do
        max32 = (1 <<< 32) - 1
        sketch = CMS.new(width: 10, depth: 1, counter_width: 32, backend: @backend)
        sketch = CMS.update(sketch, "x", max32)
        sketch = CMS.update(sketch, "x", 1)
        assert CMS.estimate(sketch, "x") == max32
      end

      test "64-bit counter saturates at 2^64-1" do
        max64 = (1 <<< 64) - 1
        sketch = CMS.new(width: 10, depth: 1, counter_width: 64, backend: @backend)
        sketch = CMS.update(sketch, "x", max64)
        sketch = CMS.update(sketch, "x", 1)
        assert CMS.estimate(sketch, "x") == max64
      end
    end

    describe "properties [#{backend_name}]" do
      property "merge commutativity" do
        check all(
                items_a <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 10),
                items_b <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 10)
              ) do
          a = CMS.from_enumerable(items_a, width: 256, depth: 3, backend: @backend)
          b = CMS.from_enumerable(items_b, width: 256, depth: 3, backend: @backend)
          assert CMS.merge(a, b).state == CMS.merge(b, a).state
        end
      end

      property "estimates are non-negative" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20),
                query <- string(:alphanumeric, min_length: 1)
              ) do
          sketch = CMS.from_enumerable(items, width: 256, depth: 3, backend: @backend)
          assert CMS.estimate(sketch, query) >= 0
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
          a = CMS.from_enumerable(items_a, width: 256, depth: 3, backend: @backend)
          b = CMS.from_enumerable(items_b, width: 256, depth: 3, backend: @backend)
          c = CMS.from_enumerable(items_c, width: 256, depth: 3, backend: @backend)
          assert CMS.merge(CMS.merge(a, b), c).state == CMS.merge(a, CMS.merge(b, c)).state
        end
      end

      property "no-undercount: estimate >= true frequency" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 30)
              ) do
          sketch = CMS.from_enumerable(items, width: 256, depth: 3, backend: @backend)
          freqs = Enum.frequencies(items)

          Enum.each(freqs, fn {item, count} ->
            assert CMS.estimate(sketch, item) >= count
          end)
        end
      end

      property "self-merge is idempotent on estimates" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 15),
                query <- string(:alphanumeric, min_length: 1)
              ) do
          sketch = CMS.from_enumerable(items, width: 256, depth: 3, backend: @backend)
          merged = CMS.merge(sketch, sketch)
          # Merging with self doubles all counters, so estimate doubles
          assert CMS.estimate(merged, query) == 2 * CMS.estimate(sketch, query)
        end
      end

      property "serialize/deserialize round-trip preserves state" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
              ) do
          sketch = CMS.from_enumerable(items, width: 256, depth: 3, backend: @backend)
          binary = CMS.serialize(sketch)
          assert {:ok, restored} = CMS.deserialize(binary)
          assert restored.state == sketch.state
          assert restored.opts == sketch.opts
        end
      end
    end
  end

  # -- Non-parameterized tests --

  describe "serialize/deserialize" do
    test "round-trip preserves state and opts" do
      sketch = CMS.from_enumerable(["a", "b", "c"])
      binary = CMS.serialize(sketch)
      assert {:ok, restored} = CMS.deserialize(binary)
      assert restored.state == sketch.state
      assert restored.opts == sketch.opts
    end

    test "round-trip preserves estimates" do
      sketch = CMS.from_enumerable(["a", "a", "b"])
      binary = CMS.serialize(sketch)
      {:ok, restored} = CMS.deserialize(binary)
      assert CMS.estimate(restored, "a") == CMS.estimate(sketch, "a")
      assert CMS.estimate(restored, "b") == CMS.estimate(sketch, "b")
    end

    test "rejects invalid binary" do
      assert {:error, %DeserializationError{}} = CMS.deserialize(<<"invalid">>)
    end

    test "rejects wrong sketch ID" do
      params = <<2048::unsigned-little-32, 5::unsigned-little-16, 32::unsigned-8>>
      bin = ExDataSketch.Codec.encode(1, 1, params, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = CMS.deserialize(bin)
      assert msg =~ "expected CMS sketch ID (2)"
    end

    test "rejects invalid params binary" do
      bin = ExDataSketch.Codec.encode(2, 1, <<>>, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = CMS.deserialize(bin)
      assert msg =~ "invalid CMS params"
    end
  end

  describe "serialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise ExDataSketch.Errors.NotImplementedError, ~r/serialize_datasketches/, fn ->
        CMS.serialize_datasketches(%CMS{state: <<>>, opts: [], backend: nil})
      end
    end
  end

  describe "deserialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise ExDataSketch.Errors.NotImplementedError, ~r/deserialize_datasketches/, fn ->
        CMS.deserialize_datasketches(<<>>)
      end
    end
  end

  describe "from_enumerable/2" do
    test "builds sketch from enumerable" do
      sketch = CMS.from_enumerable(["a", "b", "a"])
      assert CMS.estimate(sketch, "a") == 2
      assert CMS.estimate(sketch, "b") == 1
    end
  end

  describe "merge_many/1" do
    test "raises Enum.EmptyError on empty list" do
      assert_raise Enum.EmptyError, fn ->
        CMS.merge_many([])
      end
    end

    test "returns single sketch unchanged" do
      sketch = CMS.new()
      assert CMS.merge_many([sketch]) == sketch
    end

    test "merges multiple sketches" do
      sketches = for _ <- 1..3, do: CMS.new() |> CMS.update("x", 2)
      merged = CMS.merge_many(sketches)
      assert CMS.estimate(merged, "x") >= 6
    end
  end

  describe "reducer/0" do
    test "returns a 2-arity function" do
      assert is_function(CMS.reducer(), 2)
    end

    test "works with Enum.reduce" do
      sketch = Enum.reduce(["a", "b", "a"], CMS.new(), CMS.reducer())
      assert CMS.estimate(sketch, "a") == 2
    end
  end

  describe "merger/0" do
    test "returns a 2-arity function" do
      assert is_function(CMS.merger(), 2)
    end

    test "works for merging sketches" do
      a = CMS.from_enumerable(["x"])
      b = CMS.from_enumerable(["x"])
      merged = CMS.merger().(a, b)
      assert CMS.estimate(merged, "x") >= 2
    end
  end

  describe "size_bytes/1" do
    test "returns correct size" do
      sketch = CMS.new(width: 100, depth: 3, counter_width: 32)
      assert CMS.size_bytes(sketch) == 9 + 100 * 3 * 4
    end
  end

  describe "struct" do
    test "has expected fields" do
      sketch = %CMS{state: <<>>, opts: [], backend: nil}
      assert Map.has_key?(sketch, :state)
      assert Map.has_key?(sketch, :opts)
      assert Map.has_key?(sketch, :backend)
    end
  end

  # -- Cross-backend parity tests --

  if Backend.Rust.available?() do
    describe "Pure vs Rust parity" do
      test "update_many produces identical state" do
        items = for i <- 0..999, do: "item_#{i}"
        pure = CMS.from_enumerable(items, backend: Backend.Pure)
        rust = CMS.from_enumerable(items, backend: Backend.Rust)
        assert pure.state == rust.state
      end

      test "merge produces identical state" do
        items_a = for i <- 0..499, do: "a_#{i}"
        items_b = for i <- 0..499, do: "b_#{i}"
        pure_a = CMS.from_enumerable(items_a, backend: Backend.Pure)
        pure_b = CMS.from_enumerable(items_b, backend: Backend.Pure)
        rust_a = CMS.from_enumerable(items_a, backend: Backend.Rust)
        rust_b = CMS.from_enumerable(items_b, backend: Backend.Rust)
        assert CMS.merge(pure_a, pure_b).state == CMS.merge(rust_a, rust_b).state
      end

      test "estimate is identical" do
        items = for i <- 0..99, do: "item_#{i}"
        pure = CMS.from_enumerable(items, backend: Backend.Pure)
        rust = CMS.from_enumerable(items, backend: Backend.Rust)

        for i <- 0..99 do
          assert CMS.estimate(pure, "item_#{i}") == CMS.estimate(rust, "item_#{i}")
        end
      end

      test "serialization produces identical binary" do
        items = for i <- 0..99, do: "item_#{i}"
        pure = CMS.from_enumerable(items, backend: Backend.Pure)
        rust = CMS.from_enumerable(items, backend: Backend.Rust)
        assert CMS.serialize(pure) == CMS.serialize(rust)
      end

      property "update_many produces identical state for random inputs" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 0, max_length: 30)
              ) do
          pure = CMS.from_enumerable(items, width: 256, depth: 3, backend: Backend.Pure)
          rust = CMS.from_enumerable(items, width: 256, depth: 3, backend: Backend.Rust)
          assert pure.state == rust.state
        end
      end

      property "merge produces identical state for random inputs" do
        check all(
                items_a <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 0, max_length: 15),
                items_b <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 0, max_length: 15)
              ) do
          pure_a = CMS.from_enumerable(items_a, width: 256, depth: 3, backend: Backend.Pure)
          pure_b = CMS.from_enumerable(items_b, width: 256, depth: 3, backend: Backend.Pure)
          rust_a = CMS.from_enumerable(items_a, width: 256, depth: 3, backend: Backend.Rust)
          rust_b = CMS.from_enumerable(items_b, width: 256, depth: 3, backend: Backend.Rust)
          assert CMS.merge(pure_a, pure_b).state == CMS.merge(rust_a, rust_b).state
        end
      end

      property "estimate is identical for random inputs" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 30),
                query <- member_of(items)
              ) do
          pure = CMS.from_enumerable(items, width: 256, depth: 3, backend: Backend.Pure)
          rust = CMS.from_enumerable(items, width: 256, depth: 3, backend: Backend.Rust)
          assert CMS.estimate(pure, query) == CMS.estimate(rust, query)
        end
      end
    end
  end
end
