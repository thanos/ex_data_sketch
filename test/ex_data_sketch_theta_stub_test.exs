defmodule ExDataSketch.ThetaStubTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch.Theta

  alias ExDataSketch.Errors.{DeserializationError, NotImplementedError}
  alias ExDataSketch.Theta

  describe "new/1" do
    test "raises NotImplementedError (stub)" do
      assert_raise NotImplementedError, ~r/Theta.new is not yet implemented/, fn ->
        Theta.new()
      end
    end

    test "raises NotImplementedError with options" do
      assert_raise NotImplementedError, ~r/Theta.new is not yet implemented/, fn ->
        Theta.new(k: 8192)
      end
    end
  end

  describe "update/2" do
    test "raises NotImplementedError (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/Theta.update is not yet implemented/, fn ->
        Theta.update(sketch, "item")
      end
    end
  end

  describe "compact/1" do
    test "raises NotImplementedError (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/Theta.compact is not yet implemented/, fn ->
        Theta.compact(sketch)
      end
    end
  end

  describe "estimate/1" do
    test "raises NotImplementedError (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/Theta.estimate is not yet implemented/, fn ->
        Theta.estimate(sketch)
      end
    end
  end

  describe "merge/2" do
    test "raises NotImplementedError (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/Theta.merge is not yet implemented/, fn ->
        Theta.merge(sketch, sketch)
      end
    end
  end

  describe "serialize/1" do
    test "raises NotImplementedError (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/Theta.serialize is not yet implemented/, fn ->
        Theta.serialize(sketch)
      end
    end
  end

  describe "deserialize/1" do
    test "rejects invalid binary" do
      assert {:error, %DeserializationError{}} = Theta.deserialize(<<"invalid">>)
    end

    test "rejects wrong sketch ID" do
      bin = ExDataSketch.Codec.encode(1, 1, <<>>, <<>>)
      assert {:error, %DeserializationError{message: msg}} = Theta.deserialize(bin)
      assert msg =~ "expected Theta sketch ID (3)"
    end

    test "raises NotImplementedError for correct sketch ID (stub)" do
      bin = ExDataSketch.Codec.encode(3, 1, <<>>, <<>>)

      assert_raise NotImplementedError, ~r/Theta.deserialize is not yet implemented/, fn ->
        Theta.deserialize(bin)
      end
    end
  end

  describe "serialize_datasketches/1" do
    test "raises NotImplementedError (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/serialize_datasketches/, fn ->
        Theta.serialize_datasketches(sketch)
      end
    end
  end

  describe "deserialize_datasketches/1" do
    test "raises NotImplementedError (stub)" do
      assert_raise NotImplementedError, ~r/deserialize_datasketches/, fn ->
        Theta.deserialize_datasketches(<<>>)
      end
    end
  end

  describe "from_enumerable/2" do
    test "raises NotImplementedError (delegates to new)" do
      assert_raise NotImplementedError, ~r/Theta.new is not yet implemented/, fn ->
        Theta.from_enumerable(["a", "b", "c"])
      end
    end

    test "raises NotImplementedError with custom opts" do
      assert_raise NotImplementedError, ~r/Theta.new is not yet implemented/, fn ->
        Theta.from_enumerable(["a", "b"], k: 8192)
      end
    end
  end

  describe "merge_many/1" do
    test "raises Enum.EmptyError on empty list" do
      assert_raise Enum.EmptyError, fn ->
        Theta.merge_many([])
      end
    end

    test "returns single sketch unchanged" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}
      assert Theta.merge_many([sketch]) == sketch
    end

    test "raises NotImplementedError when merging multiple (stub)" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}

      assert_raise NotImplementedError, ~r/Theta.merge is not yet implemented/, fn ->
        Theta.merge_many([sketch, sketch])
      end
    end
  end

  describe "reducer/1" do
    test "returns a 2-arity function" do
      fun = Theta.reducer()
      assert is_function(fun, 2)
    end

    test "returned function calls update/2" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}
      fun = Theta.reducer()

      assert_raise NotImplementedError, ~r/Theta.update is not yet implemented/, fn ->
        fun.("item", sketch)
      end
    end
  end

  describe "merger/1" do
    test "returns a 2-arity function" do
      fun = Theta.merger()
      assert is_function(fun, 2)
    end

    test "returned function calls merge/2" do
      sketch = %Theta{state: <<>>, opts: [], backend: nil}
      fun = Theta.merger()

      assert_raise NotImplementedError, ~r/Theta.merge is not yet implemented/, fn ->
        fun.(sketch, sketch)
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
end
