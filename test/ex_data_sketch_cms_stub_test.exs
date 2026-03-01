defmodule ExDataSketch.CMSStubTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch.CMS

  alias ExDataSketch.Errors.{DeserializationError, InvalidOption, NotImplementedError}
  alias ExDataSketch.CMS

  describe "new/1" do
    test "raises NotImplementedError (stub)" do
      assert_raise NotImplementedError, ~r/cms_new/, fn ->
        CMS.new()
      end
    end

    test "raises NotImplementedError with custom options" do
      assert_raise NotImplementedError, ~r/cms_new/, fn ->
        CMS.new(width: 1024, depth: 3)
      end
    end

    test "validates width must be positive" do
      assert_raise InvalidOption, ~r/width must be a positive integer/, fn ->
        CMS.new(width: 0)
      end
    end

    test "validates width type" do
      assert_raise InvalidOption, ~r/width must be a positive integer/, fn ->
        CMS.new(width: "2048")
      end
    end

    test "validates depth must be positive" do
      assert_raise InvalidOption, ~r/depth must be a positive integer/, fn ->
        CMS.new(depth: 0)
      end
    end

    test "validates counter_width must be 32 or 64" do
      assert_raise InvalidOption, ~r/counter_width must be 32 or 64/, fn ->
        CMS.new(counter_width: 16)
      end
    end

    test "accepts counter_width 32" do
      assert_raise NotImplementedError, ~r/cms_new/, fn ->
        CMS.new(counter_width: 32)
      end
    end

    test "accepts counter_width 64" do
      assert_raise NotImplementedError, ~r/cms_new/, fn ->
        CMS.new(counter_width: 64)
      end
    end
  end

  describe "deserialize/1" do
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

    test "successfully deserializes valid binary" do
      params = <<2048::unsigned-little-32, 5::unsigned-little-16, 32::unsigned-8>>
      state = <<0, 0, 0>>
      bin = ExDataSketch.Codec.encode(2, 1, params, state)
      assert {:ok, sketch} = CMS.deserialize(bin)
      assert %CMS{} = sketch
      assert sketch.opts == [width: 2048, depth: 5, counter_width: 32]
      assert sketch.state == state
    end
  end

  describe "serialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise NotImplementedError, ~r/serialize_datasketches/, fn ->
        CMS.serialize_datasketches(%CMS{state: <<>>, opts: [], backend: nil})
      end
    end
  end

  describe "deserialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise NotImplementedError, ~r/deserialize_datasketches/, fn ->
        CMS.deserialize_datasketches(<<>>)
      end
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
end
