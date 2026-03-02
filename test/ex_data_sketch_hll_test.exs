defmodule ExDataSketch.HLLTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  doctest ExDataSketch.HLL

  alias ExDataSketch.Backend
  alias ExDataSketch.Errors.{DeserializationError, IncompatibleSketchesError, InvalidOptionError}
  alias ExDataSketch.HLL

  # Backends to test against
  @backends [Backend.Pure] ++
              if(Backend.Rust.available?(),
                do: [Backend.Rust],
                else: []
              )

  # -- Construction (not parameterized — tests public API/validation) --

  describe "new/1" do
    test "creates sketch with default p=14" do
      sketch = HLL.new()
      assert sketch.opts == [p: 14]
      assert sketch.backend == Backend.Pure
    end

    test "creates sketch with custom p" do
      for p <- 4..16 do
        sketch = HLL.new(p: p)
        assert sketch.opts == [p: p]
      end
    end

    test "binary has correct size" do
      for p <- [4, 10, 14, 16] do
        sketch = HLL.new(p: p)
        expected_size = 4 + (1 <<< p)
        assert byte_size(sketch.state) == expected_size
      end
    end

    test "binary has correct header fields" do
      sketch = HLL.new(p: 12)

      <<version::unsigned-8, p::unsigned-8, flags::unsigned-little-16, _registers::binary>> =
        sketch.state

      assert version == 1
      assert p == 12
      assert flags == 0
    end

    test "registers are initially all zero" do
      sketch = HLL.new(p: 10)
      <<_header::binary-size(4), registers::binary>> = sketch.state
      assert registers == :binary.copy(<<0>>, 1024)
    end

    test "validates p minimum" do
      assert_raise InvalidOptionError, ~r/p must be/, fn ->
        HLL.new(p: 3)
      end
    end

    test "validates p maximum" do
      assert_raise InvalidOptionError, ~r/p must be/, fn ->
        HLL.new(p: 17)
      end
    end

    test "validates p type" do
      assert_raise InvalidOptionError, ~r/p must be/, fn ->
        HLL.new(p: "14")
      end
    end
  end

  # -- Parameterized backend tests --

  for backend <- @backends do
    @backend backend
    backend_name = backend |> Module.split() |> List.last()

    describe "update/2 [#{backend_name}]" do
      test "single update changes a register" do
        sketch = HLL.new(p: 10, backend: @backend) |> HLL.update("hello")
        <<_header::binary-size(4), registers::binary>> = sketch.state
        assert registers != :binary.copy(<<0>>, 1024)
      end

      test "same item is idempotent" do
        sketch1 = HLL.new(p: 10, backend: @backend) |> HLL.update("hello")
        sketch2 = HLL.new(p: 10, backend: @backend) |> HLL.update("hello") |> HLL.update("hello")
        assert sketch1.state == sketch2.state
      end

      test "different items produce different states" do
        sketch1 = HLL.new(p: 10, backend: @backend) |> HLL.update("a")
        sketch2 = HLL.new(p: 10, backend: @backend) |> HLL.update("b")
        assert sketch1.state != sketch2.state
      end
    end

    describe "update_many/2 [#{backend_name}]" do
      test "batch update matches sequential" do
        items = ["a", "b", "c", "d", "e"]
        sequential = Enum.reduce(items, HLL.new(p: 10, backend: @backend), &HLL.update(&2, &1))
        batch = HLL.new(p: 10, backend: @backend) |> HLL.update_many(items)
        assert sequential.state == batch.state
      end

      test "empty list is a no-op" do
        sketch = HLL.new(p: 10, backend: @backend)
        assert HLL.update_many(sketch, []).state == sketch.state
      end
    end

    describe "estimate/1 [#{backend_name}]" do
      test "empty sketch estimates 0.0" do
        assert HLL.new(p: 10, backend: @backend) |> HLL.estimate() == 0.0
      end

      test "single item estimates approximately 1.0" do
        estimate = HLL.new(p: 14, backend: @backend) |> HLL.update("x") |> HLL.estimate()
        assert_in_delta estimate, 1.0, 0.1
      end

      test "100 items within error bounds (p=14)" do
        items = for i <- 0..99, do: "item_#{i}"
        estimate = HLL.from_enumerable(items, p: 14, backend: @backend) |> HLL.estimate()
        assert_in_delta estimate, 100.0, 100 * 0.05
      end

      test "1000 items within error bounds (p=14)" do
        items = for i <- 0..999, do: "item_#{i}"
        estimate = HLL.from_enumerable(items, p: 14, backend: @backend) |> HLL.estimate()
        assert_in_delta estimate, 1000.0, 1000 * 0.05
      end

      test "10_000 items within error bounds (p=14)" do
        items = for i <- 0..9999, do: "item_#{i}"
        estimate = HLL.from_enumerable(items, p: 14, backend: @backend) |> HLL.estimate()
        assert_in_delta estimate, 10_000.0, 10_000 * 0.05
      end

      test "monotonicity: estimate never decreases" do
        sketch = HLL.new(p: 10, backend: @backend)

        Enum.reduce(1..50, {sketch, 0.0}, fn i, {s, prev_est} ->
          s = HLL.update(s, "item_#{i}")
          est = HLL.estimate(s)
          assert est >= prev_est
          {s, est}
        end)
      end
    end

    describe "merge/2 [#{backend_name}]" do
      test "merging two empty sketches produces empty sketch" do
        a = HLL.new(p: 10, backend: @backend)
        b = HLL.new(p: 10, backend: @backend)
        assert HLL.estimate(HLL.merge(a, b)) == 0.0
      end

      test "merge is commutative" do
        a = HLL.from_enumerable(["x", "y"], p: 10, backend: @backend)
        b = HLL.from_enumerable(["y", "z"], p: 10, backend: @backend)
        assert HLL.merge(a, b).state == HLL.merge(b, a).state
      end

      test "merge is associative" do
        a = HLL.from_enumerable(["a", "b"], p: 10, backend: @backend)
        b = HLL.from_enumerable(["c", "d"], p: 10, backend: @backend)
        c = HLL.from_enumerable(["e", "f"], p: 10, backend: @backend)
        assert HLL.merge(HLL.merge(a, b), c).state == HLL.merge(a, HLL.merge(b, c)).state
      end

      test "self-merge is idempotent" do
        sketch = HLL.from_enumerable(["a", "b", "c"], p: 10, backend: @backend)
        assert HLL.merge(sketch, sketch).state == sketch.state
      end

      test "merge with empty preserves state" do
        sketch = HLL.from_enumerable(["a", "b"], p: 10, backend: @backend)
        empty = HLL.new(p: 10, backend: @backend)
        assert HLL.merge(sketch, empty).state == sketch.state
        assert HLL.merge(empty, sketch).state == sketch.state
      end

      test "raises on precision mismatch" do
        a = HLL.new(p: 10, backend: @backend)
        b = HLL.new(p: 12, backend: @backend)

        assert_raise IncompatibleSketchesError, ~r/precision mismatch/, fn ->
          HLL.merge(a, b)
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
          a = HLL.from_enumerable(items_a, p: 10, backend: @backend)
          b = HLL.from_enumerable(items_b, p: 10, backend: @backend)
          assert HLL.merge(a, b).state == HLL.merge(b, a).state
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
          a = HLL.from_enumerable(items_a, p: 10, backend: @backend)
          b = HLL.from_enumerable(items_b, p: 10, backend: @backend)
          c = HLL.from_enumerable(items_c, p: 10, backend: @backend)
          assert HLL.merge(HLL.merge(a, b), c).state == HLL.merge(a, HLL.merge(b, c)).state
        end
      end

      property "monotonicity" do
        check all(
                items <-
                  list_of(string(:alphanumeric, min_length: 1), min_length: 2, max_length: 30)
              ) do
          {half_a, half_b} = Enum.split(items, div(length(items), 2))
          sketch_a = HLL.from_enumerable(half_a, p: 10, backend: @backend)
          sketch_full = HLL.update_many(sketch_a, half_b)
          assert HLL.estimate(sketch_full) >= HLL.estimate(sketch_a)
        end
      end
    end
  end

  # -- Non-parameterized tests --

  describe "serialize/deserialize" do
    test "round-trip preserves state and opts" do
      sketch = HLL.from_enumerable(["a", "b", "c"], p: 12)
      binary = HLL.serialize(sketch)
      assert {:ok, restored} = HLL.deserialize(binary)
      assert restored.state == sketch.state
      assert restored.opts == sketch.opts
    end

    test "round-trip preserves estimate" do
      sketch = HLL.from_enumerable(for(i <- 1..100, do: i), p: 14)
      binary = HLL.serialize(sketch)
      {:ok, restored} = HLL.deserialize(binary)
      assert HLL.estimate(restored) == HLL.estimate(sketch)
    end

    test "rejects invalid binary" do
      assert {:error, %DeserializationError{}} = HLL.deserialize(<<"invalid">>)
    end

    test "rejects wrong sketch ID" do
      bin = ExDataSketch.Codec.encode(2, 1, <<14>>, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = HLL.deserialize(bin)
      assert msg =~ "expected HLL sketch ID (1)"
    end

    test "rejects invalid p in params" do
      bin = ExDataSketch.Codec.encode(1, 1, <<3>>, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = HLL.deserialize(bin)
      assert msg =~ "invalid HLL precision"
    end

    test "rejects invalid params binary" do
      bin = ExDataSketch.Codec.encode(1, 1, <<>>, <<0, 0>>)
      assert {:error, %DeserializationError{message: msg}} = HLL.deserialize(bin)
      assert msg =~ "invalid HLL params"
    end
  end

  describe "serialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise ExDataSketch.Errors.NotImplementedError, ~r/serialize_datasketches/, fn ->
        HLL.serialize_datasketches(%HLL{state: <<>>, opts: [p: 14], backend: nil})
      end
    end
  end

  describe "deserialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise ExDataSketch.Errors.NotImplementedError, ~r/deserialize_datasketches/, fn ->
        HLL.deserialize_datasketches(<<>>)
      end
    end
  end

  describe "from_enumerable/2" do
    test "builds sketch from enumerable" do
      sketch = HLL.from_enumerable(["a", "b", "c"], p: 10)
      assert HLL.estimate(sketch) > 0.0
    end

    test "works with ranges" do
      sketch = HLL.from_enumerable(1..100, p: 10)
      assert_in_delta HLL.estimate(sketch), 100.0, 100 * 0.05
    end
  end

  describe "merge_many/1" do
    test "raises Enum.EmptyError on empty list" do
      assert_raise Enum.EmptyError, fn -> HLL.merge_many([]) end
    end

    test "returns single sketch unchanged" do
      sketch = HLL.new(p: 10)
      assert HLL.merge_many([sketch]) == sketch
    end

    test "merges multiple sketches" do
      sketches = for i <- 1..5, do: HLL.from_enumerable(((i - 1) * 20 + 1)..(i * 20), p: 10)
      merged = HLL.merge_many(sketches)
      assert_in_delta HLL.estimate(merged), 100.0, 100 * 0.1
    end
  end

  describe "reducer/0" do
    test "returns a 2-arity function" do
      assert is_function(HLL.reducer(), 2)
    end

    test "works with Enum.reduce" do
      sketch = Enum.reduce(["a", "b", "c"], HLL.new(p: 10), HLL.reducer())
      assert HLL.estimate(sketch) > 0.0
    end
  end

  describe "merger/0" do
    test "returns a 2-arity function" do
      assert is_function(HLL.merger(), 2)
    end

    test "works for merging sketches" do
      a = HLL.from_enumerable(["x"], p: 10)
      b = HLL.from_enumerable(["y"], p: 10)
      merged = HLL.merger().(a, b)
      assert HLL.estimate(merged) > 0.0
    end
  end

  describe "size_bytes/1" do
    test "returns correct size for various p values" do
      for p <- [4, 10, 14, 16] do
        sketch = HLL.new(p: p)
        assert HLL.size_bytes(sketch) == 4 + (1 <<< p)
      end
    end
  end

  describe "struct" do
    test "has expected fields" do
      sketch = %HLL{state: <<>>, opts: [], backend: nil}
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
        pure = HLL.from_enumerable(items, p: 14, backend: Backend.Pure)
        rust = HLL.from_enumerable(items, p: 14, backend: Backend.Rust)
        assert pure.state == rust.state
      end

      test "merge produces identical state" do
        items_a = for i <- 0..499, do: "a_#{i}"
        items_b = for i <- 0..499, do: "b_#{i}"
        pure_a = HLL.from_enumerable(items_a, p: 14, backend: Backend.Pure)
        pure_b = HLL.from_enumerable(items_b, p: 14, backend: Backend.Pure)
        rust_a = HLL.from_enumerable(items_a, p: 14, backend: Backend.Rust)
        rust_b = HLL.from_enumerable(items_b, p: 14, backend: Backend.Rust)
        assert HLL.merge(pure_a, pure_b).state == HLL.merge(rust_a, rust_b).state
      end

      test "estimate is identical" do
        items = for i <- 0..9999, do: "item_#{i}"
        pure = HLL.from_enumerable(items, p: 14, backend: Backend.Pure)
        rust = HLL.from_enumerable(items, p: 14, backend: Backend.Rust)
        assert_in_delta HLL.estimate(pure), HLL.estimate(rust), 1.0e-9
      end

      test "serialization produces identical binary" do
        items = for i <- 0..99, do: "item_#{i}"
        pure = HLL.from_enumerable(items, p: 14, backend: Backend.Pure)
        rust = HLL.from_enumerable(items, p: 14, backend: Backend.Rust)
        assert HLL.serialize(pure) == HLL.serialize(rust)
      end
    end
  end
end
