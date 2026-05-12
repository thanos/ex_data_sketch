defmodule ExDataSketch.Binary.HeaderTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bitwise

  alias ExDataSketch.Binary.Header
  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.Hash.Metadata

  doctest ExDataSketch.Binary.Header

  describe "encode/3" do
    test "produces a v2 frame starting with magic + version" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<>>)
      assert <<"EXSK", 2, _rest::binary>> = bin
      # 4 magic + 6 fixed header bytes + 16 metadata + 4 payload_size + 0 payload + 4 CRC
      assert byte_size(bin) == 34
    end

    test "embeds the metadata's sketch_family and family_version" do
      meta = Metadata.new(:murmur3, 42, 7, 3, :pure)
      bin = Header.encode(meta, <<1, 2, 3>>)
      <<"EXSK", 2, family, family_v, flags, _hsize::16, _rest::binary>> = bin
      assert family == 7
      assert family_v == 3
      assert flags == 0
    end

    test "rejects non-zero flags as ArgumentError (v2 reserved)" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      # Flags is u8, so legal range is 0..255, but only 0 is meaningful in v2.
      # We don't reject non-zero flags at encode time — they're informational.
      # We DO reject invalid u8 values.
      assert_raise ArgumentError, fn -> Header.encode(meta, <<>>, flags: -1) end
      assert_raise ArgumentError, fn -> Header.encode(meta, <<>>, flags: 256) end
    end

    test "rejects metadata whose encoded size pushes header_size beyond u16" do
      # header_size = 10 + (16 + ext_size) + 4. For overflow we need ext_size > 65505.
      # The Metadata layer caps ext_size to u16 (65,535), so this can only fail at
      # encode-time when 65,506 <= ext_size <= 65,535. Use 65,535 to stress it.
      meta = %Metadata{
        block_version: 1,
        algorithm: :xxhash3,
        seed: 0,
        sketch_family: 1,
        sketch_family_version: 1,
        backend: :rust,
        flags: 0,
        extension: :binary.copy(<<0>>, 65_535)
      }

      assert_raise ArgumentError, ~r/header_size exceeds u16/, fn ->
        Header.encode(meta, <<>>)
      end
    end
  end

  describe "encode/3 + decode/1 round-trip" do
    test "small payload" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<1, 2, 3>>)
      assert {:ok, frame} = Header.decode(bin)
      assert frame.serialization_version == 2
      assert frame.sketch_family == 1
      assert frame.family_version == 1
      assert frame.flags == 0
      assert frame.metadata.algorithm == :xxhash3
      assert frame.payload == <<1, 2, 3>>
    end

    test "empty payload" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<>>)
      assert {:ok, frame} = Header.decode(bin)
      assert frame.payload == <<>>
    end

    test "round-trip preserves arbitrary metadata extension" do
      ext = :binary.copy(<<0xAB>>, 100)

      meta = %Metadata{
        block_version: 1,
        algorithm: :xxhash3,
        seed: 0xDEADBEEFCAFEBABE,
        sketch_family: 5,
        sketch_family_version: 7,
        backend: :rust,
        flags: 0,
        extension: ext
      }

      bin = Header.encode(meta, "payload-bytes")
      assert {:ok, frame} = Header.decode(bin)
      assert frame.metadata.extension == ext
      assert frame.metadata.seed == 0xDEADBEEFCAFEBABE
      assert frame.payload == "payload-bytes"
    end

    property "round-trip identity over arbitrary metadata + payloads" do
      check all(
              algo <- StreamData.member_of([:phash2, :xxhash3, :murmur3, :custom]),
              seed <- StreamData.integer(0..0xFFFFFFFFFFFFFFFF),
              family <- StreamData.integer(0..255),
              family_v <- StreamData.integer(0..255),
              backend <- StreamData.member_of([:unspecified, :pure, :rust]),
              payload <- StreamData.binary(max_length: 256),
              max_runs: 100
            ) do
        meta = Metadata.new(algo, seed, family, family_v, backend)
        bin = Header.encode(meta, payload)
        assert {:ok, frame} = Header.decode(bin)
        assert frame.serialization_version == 2
        assert frame.sketch_family == family
        assert frame.family_version == family_v
        assert frame.metadata.algorithm == algo
        assert frame.metadata.seed == seed
        assert frame.payload == payload
      end
    end
  end

  describe "decode/1 error paths" do
    test "rejects empty binary" do
      assert {:error, %DeserializationError{}} = Header.decode(<<>>)
    end

    test "rejects binary shorter than minimum frame" do
      assert {:error, %DeserializationError{}} = Header.decode(<<1, 2, 3>>)
    end

    test "rejects wrong magic" do
      payload = :binary.copy(<<0>>, 40)

      assert {:error, %DeserializationError{message: msg}} =
               Header.decode(<<"BAAD", payload::binary>>)

      assert msg =~ "invalid magic bytes"
    end

    test "rejects wrong version" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<>>)
      <<"EXSK", _v, rest::binary>> = bin
      bad = <<"EXSK", 99, rest::binary>>
      assert {:error, %DeserializationError{message: msg}} = Header.decode(bad)
      assert msg =~ "unsupported EXSK frame version 99"
    end

    test "detects CRC mismatch (corrupted payload)" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<10, 20, 30, 40>>)
      # Flip a payload byte (not the magic, not the CRC).
      flip_pos = byte_size(bin) - 5
      <<before::binary-size(^flip_pos), b, after_::binary>> = bin
      corrupted = <<before::binary, bxor(b, 0xFF), after_::binary>>
      assert {:error, %DeserializationError{message: msg}} = Header.decode(corrupted)
      assert msg =~ "checksum mismatch"
    end

    test "detects CRC mismatch when only the CRC trailer is flipped" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<10, 20, 30>>)
      <<body::binary-size(byte_size(bin) - 4), crc::unsigned-little-32>> = bin
      bad = <<body::binary, bxor(crc, 1)::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Header.decode(bad)
      assert msg =~ "checksum mismatch"
    end

    test "rejects truncated payload" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<10, 20, 30>>)
      truncated = binary_part(bin, 0, byte_size(bin) - 2)
      assert {:error, %DeserializationError{}} = Header.decode(truncated)
    end

    test "rejects trailing bytes after declared payload" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<10, 20, 30>>)
      bloated = bin <> <<99, 100>>
      assert {:error, %DeserializationError{message: msg}} = Header.decode(bloated)
      assert msg =~ "trailing bytes"
    end

    test "rejects non-zero reserved flags" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<1, 2, 3>>, flags: 255)
      assert {:error, %DeserializationError{message: msg}} = Header.decode(bin)
      assert msg =~ "unsupported EXSK v2 flags"
    end
  end

  describe "fuzz / corruption resistance" do
    test "random bit-flips at random positions are detected or rejected, never crash" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, :crypto.strong_rand_bytes(64))

      for _ <- 1..200 do
        pos = :rand.uniform(byte_size(bin)) - 1
        mask = 1 <<< (:rand.uniform(8) - 1)
        <<head::binary-size(^pos), b, tail::binary>> = bin
        corrupted = <<head::binary, bxor(b, mask), tail::binary>>

        # The decoder may succeed only if the bit-flip happens to land in a
        # "don't care" region (e.g., a metadata extension byte that's still
        # checksum-consistent). With our v2 layout, every byte preceding the
        # CRC is checksum-covered, so any flip in [0, crc_off) is detected.
        # A flip inside the CRC trailer itself is also detected as a mismatch
        # (against the recomputed CRC over the unchanged prefix).
        case Header.decode(corrupted) do
          {:ok, _frame} ->
            # Allowed only when the corrupted byte equals the original after
            # the mask (i.e., the random mask landed on an already-set bit
            # and effectively cancelled out -- impossible with non-zero XOR).
            flunk(
              "corruption survived: pos=#{pos} mask=#{Integer.to_string(mask, 2)} bin=#{inspect(bin, limit: 8)}"
            )

          {:error, %DeserializationError{}} ->
            :ok
        end
      end
    end

    property "random binaries never crash the decoder" do
      check all(bin <- StreamData.binary(max_length: 200), max_runs: 200) do
        case Header.decode(bin) do
          {:ok, _} -> :ok
          {:error, %DeserializationError{}} -> :ok
        end
      end
    end
  end
end
