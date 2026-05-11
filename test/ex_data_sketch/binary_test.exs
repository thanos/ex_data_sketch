defmodule ExDataSketch.BinaryTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.{Binary, Codec}
  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.Hash.Metadata

  doctest ExDataSketch.Binary

  describe "magic/0" do
    test "is EXSK" do
      assert Binary.magic() == "EXSK"
    end
  end

  describe "peek_version/1" do
    test "returns {:ok, 2} for a v2 frame" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Binary.encode(meta, <<>>)
      assert {:ok, 2} = Binary.peek_version(bin)
    end

    test "returns {:ok, 1} for a v1 frame" do
      bin = Codec.encode(1, 1, <<>>, <<>>)
      assert {:ok, 1} = Binary.peek_version(bin)
    end

    test "rejects future version" do
      bin = <<"EXSK", 9, 0::8>>
      assert {:error, %DeserializationError{message: msg}} = Binary.peek_version(bin)
      assert msg =~ "unsupported EXSK frame version 9"
    end

    test "rejects bad magic and short binaries" do
      assert {:error, %DeserializationError{}} = Binary.peek_version("BAAD" <> <<0>>)
      assert {:error, %DeserializationError{}} = Binary.peek_version(<<>>)
    end
  end

  describe "encode/3 + decode/1 round-trip (v2)" do
    test "small payload" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      payload = Binary.build_payload(<<14>>, <<0, 1, 2, 3>>)
      bin = Binary.encode(meta, payload)

      assert {:ok, decoded} = Binary.decode(bin)
      assert decoded.version == 2
      assert decoded.sketch_id == 1
      assert decoded.family_version == 1
      assert decoded.metadata.algorithm == :xxhash3
      assert decoded.params == <<14>>
      assert decoded.state == <<0, 1, 2, 3>>
    end

    property "round-trip identity over arbitrary params + state" do
      check all(
              algo <- StreamData.member_of([:phash2, :xxhash3, :murmur3]),
              seed <- StreamData.integer(0..0xFFFFFFFFFFFFFFFF),
              family <- StreamData.integer(0..255),
              params <- StreamData.binary(max_length: 32),
              state <- StreamData.binary(max_length: 128),
              max_runs: 100
            ) do
        meta = Metadata.new(algo, seed, family, 1, :pure)
        payload = Binary.build_payload(params, state)
        bin = Binary.encode(meta, payload)

        assert {:ok, decoded} = Binary.decode(bin)
        assert decoded.sketch_id == family
        assert decoded.metadata.algorithm == algo
        assert decoded.metadata.seed == seed
        assert decoded.params == params
        assert decoded.state == state
      end
    end
  end

  describe "decode/1 (v1 backward compatibility)" do
    test "decodes legacy v1 frames produced by Codec.encode/4" do
      v1 = Codec.encode(1, 1, <<14, 1>>, <<0, 0, 0, 0>>)
      assert {:ok, decoded} = Binary.decode(v1)
      assert decoded.version == 1
      assert decoded.metadata == nil
      assert decoded.sketch_id == 1
      assert decoded.family_version == 0
      assert decoded.params == <<14, 1>>
      assert decoded.state == <<0, 0, 0, 0>>
    end

    test "decodes v1 frames for every sketch ID" do
      for id <- 1..15 do
        v1 = Codec.encode(id, 1, <<id>>, <<0xAA, 0xBB>>)
        assert {:ok, decoded} = Binary.decode(v1)
        assert decoded.version == 1
        assert decoded.sketch_id == id
      end
    end
  end

  describe "decode/1 error paths" do
    test "rejects non-binary input" do
      assert {:error, %DeserializationError{}} = Binary.decode(:not_a_binary)
    end

    test "rejects bad magic" do
      assert {:error, %DeserializationError{}} = Binary.decode("BAAD" <> <<0, 0, 0, 0>>)
    end

    test "rejects future version" do
      assert {:error, %DeserializationError{}} = Binary.decode(<<"EXSK", 9, 0::8>>)
    end

    test "rejects v2 frame with CRC mismatch" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Binary.encode(meta, Binary.build_payload(<<>>, <<>>))
      <<body::binary-size(byte_size(bin) - 4), crc::unsigned-little-32>> = bin
      bad = <<body::binary, crc + 1::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Binary.decode(bad)
      assert msg =~ "checksum mismatch"
    end

    test "rejects v2 frame with truncated metadata extension" do
      assert {:error, %DeserializationError{}} = Binary.decode(<<"EXSK", 2, 0, 0, 0, 0::16>>)
    end

    test "rejects v1 frame with truncated params" do
      assert {:error, %DeserializationError{}} =
               Binary.decode(<<"EXSK", 1, 1, 100::unsigned-little-32, 1, 2>>)
    end
  end

  describe "build_payload/2" do
    test "encodes params length as u32 LE prefix" do
      assert Binary.build_payload(<<>>, <<>>) == <<0, 0, 0, 0>>
      assert Binary.build_payload(<<1>>, <<>>) == <<1, 0, 0, 0, 1>>
      assert Binary.build_payload(<<1, 2>>, <<3, 4, 5>>) == <<2, 0, 0, 0, 1, 2, 3, 4, 5>>
    end
  end

  describe "metadata_from_opts/3" do
    test "maps common hash strategy values" do
      assert Binary.metadata_from_opts(1, 1, []).algorithm == :phash2
      assert Binary.metadata_from_opts(1, 1, hash_strategy: :xxhash3).algorithm == :xxhash3
      assert Binary.metadata_from_opts(1, 1, hash_strategy: :murmur3).algorithm == :murmur3
      assert Binary.metadata_from_opts(1, 1, hash_strategy: :custom).algorithm == :custom
    end

    test "propagates seed" do
      assert Binary.metadata_from_opts(1, 1, seed: 42).seed == 42
    end

    test "captures sketch_family and family_version" do
      meta = Binary.metadata_from_opts(7, 3, [])
      assert meta.sketch_family == 7
      assert meta.sketch_family_version == 3
    end
  end
end
