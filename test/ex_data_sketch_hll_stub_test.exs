defmodule ExDataSketch.HLLStubTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch.HLL

  alias ExDataSketch.Errors.{DeserializationError, InvalidOptionError, NotImplementedError}
  alias ExDataSketch.HLL

  describe "new/1" do
    test "raises NotImplementedError (stub)" do
      assert_raise NotImplementedError, ~r/hll_new/, fn ->
        HLL.new()
      end
    end

    test "raises NotImplementedError with custom p" do
      assert_raise NotImplementedError, ~r/hll_new/, fn ->
        HLL.new(p: 10)
      end
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

    test "accepts valid p range boundaries" do
      for p <- [4, 16] do
        assert_raise NotImplementedError, ~r/hll_new/, fn ->
          HLL.new(p: p)
        end
      end
    end
  end

  describe "deserialize/1" do
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

    test "successfully deserializes valid binary" do
      # Valid EXSK with HLL sketch ID, p=14
      state = <<0, 0, 0>>
      bin = ExDataSketch.Codec.encode(1, 1, <<14>>, state)
      assert {:ok, sketch} = HLL.deserialize(bin)
      assert %HLL{} = sketch
      assert sketch.opts == [p: 14]
      assert sketch.state == state
      assert sketch.backend == ExDataSketch.Backend.Pure
    end
  end

  describe "serialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise NotImplementedError, ~r/serialize_datasketches/, fn ->
        HLL.serialize_datasketches(%HLL{state: <<>>, opts: [p: 14], backend: nil})
      end
    end
  end

  describe "deserialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise NotImplementedError, ~r/deserialize_datasketches/, fn ->
        HLL.deserialize_datasketches(<<>>)
      end
    end
  end

  describe "from_enumerable/2" do
    test "raises NotImplementedError (delegates to new)" do
      assert_raise NotImplementedError, ~r/hll_new/, fn ->
        HLL.from_enumerable(["a", "b", "c"])
      end
    end

    test "raises NotImplementedError with custom opts" do
      assert_raise NotImplementedError, ~r/hll_new/, fn ->
        HLL.from_enumerable(["a", "b"], p: 10)
      end
    end
  end

  describe "merge_many/1" do
    test "raises Enum.EmptyError on empty list" do
      assert_raise Enum.EmptyError, fn ->
        HLL.merge_many([])
      end
    end

    test "returns single sketch unchanged" do
      sketch = %HLL{state: <<0, 1, 2>>, opts: [p: 14], backend: ExDataSketch.Backend.Pure}
      assert HLL.merge_many([sketch]) == sketch
    end

    test "raises NotImplementedError when merging multiple (stub backend)" do
      sketch = %HLL{state: <<0>>, opts: [p: 14], backend: ExDataSketch.Backend.Pure}

      assert_raise NotImplementedError, ~r/hll_merge/, fn ->
        HLL.merge_many([sketch, sketch])
      end
    end
  end

  describe "reducer/1" do
    test "returns a 2-arity function" do
      fun = HLL.reducer()
      assert is_function(fun, 2)
    end

    test "returned function calls update/2" do
      sketch = %HLL{state: <<0>>, opts: [p: 14], backend: ExDataSketch.Backend.Pure}

      fun = HLL.reducer()

      assert_raise NotImplementedError, ~r/hll_update/, fn ->
        fun.("item", sketch)
      end
    end
  end

  describe "merger/1" do
    test "returns a 2-arity function" do
      fun = HLL.merger()
      assert is_function(fun, 2)
    end

    test "returned function calls merge/2" do
      sketch = %HLL{state: <<0>>, opts: [p: 14], backend: ExDataSketch.Backend.Pure}

      fun = HLL.merger()

      assert_raise NotImplementedError, ~r/hll_merge/, fn ->
        fun.(sketch, sketch)
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
end
