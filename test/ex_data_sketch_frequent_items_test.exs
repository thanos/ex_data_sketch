defmodule ExDataSketch.FrequentItemsTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.FrequentItems

  describe "new/1 option validation" do
    test "raises on invalid k (zero)" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        FrequentItems.new(k: 0)
      end
    end

    test "raises on invalid k (negative)" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        FrequentItems.new(k: -1)
      end
    end

    test "raises on invalid key_encoding" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/key_encoding/, fn ->
        FrequentItems.new(key_encoding: :invalid)
      end
    end

    test "raises on non-integer k" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        FrequentItems.new(k: 1.5)
      end
    end
  end

  describe "new/1 backend stub" do
    test "raises RuntimeError from fi_new stub" do
      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        FrequentItems.new(k: 5)
      end
    end

    test "default options pass validation before hitting stub" do
      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        FrequentItems.new()
      end
    end

    test "integer key encoding passes validation before hitting stub" do
      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        FrequentItems.new(key_encoding: :int)
      end
    end

    test "term external key encoding passes validation before hitting stub" do
      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        FrequentItems.new(key_encoding: {:term, :external})
      end
    end
  end

  describe "codec" do
    test "sketch_id_fi returns 6" do
      assert ExDataSketch.Codec.sketch_id_fi() == 6
    end
  end

  describe "deserialize/1" do
    test "rejects invalid binary" do
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} =
               FrequentItems.deserialize(<<"invalid">>)
    end

    test "rejects wrong sketch ID" do
      # Encode with sketch_id=1 (HLL), should fail for FrequentItems
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

  describe "convenience functions" do
    test "reducer returns a 2-arity function" do
      assert is_function(FrequentItems.reducer(), 2)
    end

    test "merger returns a 2-arity function" do
      assert is_function(FrequentItems.merger(), 2)
    end
  end
end
