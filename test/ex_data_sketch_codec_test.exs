defmodule ExDataSketch.CodecTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  doctest ExDataSketch.Codec

  alias ExDataSketch.Codec
  alias ExDataSketch.Errors.DeserializationError

  describe "constants" do
    test "magic is EXSK" do
      assert Codec.magic() == "EXSK"
    end

    test "version is 1" do
      assert Codec.version() == 1
    end

    test "sketch IDs are distinct" do
      ids = [Codec.sketch_id_hll(), Codec.sketch_id_cms(), Codec.sketch_id_theta()]
      assert length(Enum.uniq(ids)) == 3
    end

    test "sketch ID values" do
      assert Codec.sketch_id_hll() == 1
      assert Codec.sketch_id_cms() == 2
      assert Codec.sketch_id_theta() == 3
    end
  end

  describe "encode/4" do
    test "produces binary with magic header" do
      bin = Codec.encode(1, 1, <<>>, <<>>)
      assert <<"EXSK", _rest::binary>> = bin
    end

    test "encodes version and sketch ID" do
      bin = Codec.encode(2, 1, <<>>, <<>>)
      <<"EXSK", version, sketch_id, _rest::binary>> = bin
      assert version == 1
      assert sketch_id == 2
    end

    test "correct total size with empty params and state" do
      bin = Codec.encode(1, 1, <<>>, <<>>)
      # 4 magic + 1 version + 1 sketch_id + 4 params_len + 0 params + 4 state_len + 0 state
      assert byte_size(bin) == 14
    end

    test "correct total size with non-empty params and state" do
      params = <<14>>
      state = <<0, 0, 0, 0, 0>>
      bin = Codec.encode(1, 1, params, state)
      # 4 + 1 + 1 + 4 + 1 + 4 + 5 = 20
      assert byte_size(bin) == 20
    end
  end

  describe "decode/1" do
    test "round-trips with encode" do
      params = <<14, 0, 1>>
      state = :crypto.strong_rand_bytes(100)
      bin = Codec.encode(1, 1, params, state)

      assert {:ok, decoded} = Codec.decode(bin)
      assert decoded.version == 1
      assert decoded.sketch_id == 1
      assert decoded.params == params
      assert decoded.state == state
    end

    test "round-trips empty params and state" do
      bin = Codec.encode(3, 1, <<>>, <<>>)
      assert {:ok, decoded} = Codec.decode(bin)
      assert decoded.sketch_id == 3
      assert decoded.params == <<>>
      assert decoded.state == <<>>
    end

    test "rejects invalid magic" do
      bad = <<"BAAD", 1, 1, 0::unsigned-little-32, 0::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(bad)
      assert msg =~ "invalid magic bytes"
    end

    test "rejects binary too short" do
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(<<1, 2>>)
      assert msg =~ "too short"
    end

    test "rejects unsupported version" do
      bin = <<"EXSK", 99, 1, 0::unsigned-little-32, 0::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(bin)
      assert msg =~ "unsupported version"
    end

    test "rejects trailing bytes" do
      bin = Codec.encode(1, 1, <<>>, <<>>) <> <<0xFF>>
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(bin)
      assert msg =~ "trailing bytes"
    end

    test "rejects truncated params" do
      # Claims 10 bytes of params but provides 0
      bin = <<"EXSK", 1, 1, 10::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(bin)
      assert msg =~ "params segment shorter"
    end

    test "rejects truncated state" do
      # Correct params (0 bytes), but claims 10 bytes of state and provides 0
      bin = <<"EXSK", 1, 1, 0::unsigned-little-32, 10::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(bin)
      assert msg =~ "state segment shorter"
    end

    test "rejects truncated header after sketch ID" do
      bin = <<"EXSK", 1, 1, 0>>
      assert {:error, %DeserializationError{message: msg}} = Codec.decode(bin)
      assert msg =~ "truncated header"
    end

    test "header fields encoded correctly" do
      params = <<42>>
      state = <<1, 2, 3>>
      bin = Codec.encode(2, 1, params, state)

      # Manually parse the binary to verify layout
      <<"EXSK", version::unsigned-8, sketch_id::unsigned-8, params_len::unsigned-little-32,
        p::binary-size(1), state_len::unsigned-little-32, s::binary-size(3)>> = bin

      assert version == 1
      assert sketch_id == 2
      assert params_len == 1
      assert p == <<42>>
      assert state_len == 3
      assert s == <<1, 2, 3>>
    end
  end

  describe "property: encode/decode round-trip" do
    property "any sketch_id, params, and state round-trip" do
      check all(
              sketch_id <- integer(1..3),
              params <- binary(min_length: 0, max_length: 100),
              state <- binary(min_length: 0, max_length: 1000)
            ) do
        bin = Codec.encode(sketch_id, 1, params, state)
        assert {:ok, decoded} = Codec.decode(bin)
        assert decoded.version == 1
        assert decoded.sketch_id == sketch_id
        assert decoded.params == params
        assert decoded.state == state
      end
    end
  end
end
