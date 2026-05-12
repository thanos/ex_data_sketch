defmodule ExDataSketch.Hash.MetadataTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.Hash.Metadata

  doctest ExDataSketch.Hash.Metadata

  describe "new/5" do
    test "constructs a struct with the given fields" do
      m = Metadata.new(:xxhash3, 42, 1, 2, :rust)
      assert m.algorithm == :xxhash3
      assert m.seed == 42
      assert m.sketch_family == 1
      assert m.sketch_family_version == 2
      assert m.backend == :rust
      assert m.block_version == Metadata.block_version()
      assert m.flags == 0
      assert m.extension == <<>>
    end

    test "rejects unknown algorithm" do
      assert_raise ArgumentError, fn -> Metadata.new(:unknown_algo, 0, 1, 1, :pure) end
    end

    test "rejects unknown backend" do
      assert_raise ArgumentError, fn -> Metadata.new(:xxhash3, 0, 1, 1, :unknown) end
    end
  end

  describe "encode/1 + decode/1 round-trip" do
    test "all known algorithm/backend combinations" do
      algorithms = [:phash2, :xxhash3, :murmur3, :custom]
      backends = [:unspecified, :pure, :rust]

      for algo <- algorithms, backend <- backends do
        meta = Metadata.new(algo, 9001, 7, 3, backend)
        bin = Metadata.encode(meta)
        assert byte_size(bin) == 16
        assert {:ok, decoded, <<>>} = Metadata.decode(bin)
        assert decoded == %{meta | block_version: Metadata.block_version()}
      end
    end

    test "leaves trailing bytes untouched" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Metadata.encode(meta)
      trailing = <<1, 2, 3, 4, 5>>
      assert {:ok, decoded, ^trailing} = Metadata.decode(bin <> trailing)
      assert decoded.algorithm == :xxhash3
    end

    test "preserves seed across full u64 range" do
      seeds = [
        0,
        1,
        0xFFFF,
        0xFFFFFFFF,
        0x1234567890ABCDEF,
        0xFFFFFFFFFFFFFFFF
      ]

      for s <- seeds do
        meta = Metadata.new(:xxhash3, s, 1, 1, :rust)
        bin = Metadata.encode(meta)
        assert {:ok, decoded, <<>>} = Metadata.decode(bin)
        assert decoded.seed == s
      end
    end

    property "round-trip is the identity over arbitrary fields" do
      check all(
              algo <- StreamData.member_of([:phash2, :xxhash3, :murmur3, :custom]),
              seed <- StreamData.integer(0..0xFFFFFFFFFFFFFFFF),
              family <- StreamData.integer(0..255),
              family_v <- StreamData.integer(0..255),
              backend <- StreamData.member_of([:unspecified, :pure, :rust]),
              max_runs: 200
            ) do
        meta = Metadata.new(algo, seed, family, family_v, backend)
        bin = Metadata.encode(meta)
        assert {:ok, decoded, <<>>} = Metadata.decode(bin)
        assert decoded.algorithm == algo
        assert decoded.seed == seed
        assert decoded.sketch_family == family
        assert decoded.sketch_family_version == family_v
        assert decoded.backend == backend
      end
    end
  end

  describe "forward-compatibility (extension bytes)" do
    test "decodes a block with non-empty extension and round-trips bytes" do
      meta = %Metadata{
        block_version: 1,
        algorithm: :xxhash3,
        seed: 0,
        sketch_family: 1,
        sketch_family_version: 1,
        backend: :rust,
        flags: 0,
        extension: <<0xDE, 0xAD, 0xBE, 0xEF>>
      }

      bin = Metadata.encode(meta)
      assert byte_size(bin) == 16 + 4
      assert {:ok, decoded, <<>>} = Metadata.decode(bin)
      assert decoded.extension == <<0xDE, 0xAD, 0xBE, 0xEF>>
      # Re-encoding must be the identity.
      assert Metadata.encode(decoded) == bin
    end

    test "rejects extension > 65535 bytes" do
      big_ext = :binary.copy(<<0>>, 70_000)

      meta = %Metadata{
        block_version: 1,
        algorithm: :xxhash3,
        seed: 0,
        sketch_family: 1,
        sketch_family_version: 1,
        backend: :rust,
        flags: 0,
        extension: big_ext
      }

      assert_raise ArgumentError, ~r/extension is too large/, fn ->
        Metadata.encode(meta)
      end
    end
  end

  describe "decode/1 error paths" do
    test "rejects future block_version" do
      bin = <<
        9::unsigned-8,
        1::unsigned-8,
        0::unsigned-little-64,
        1::unsigned-8,
        1::unsigned-8,
        2::unsigned-8,
        0::unsigned-8,
        0::unsigned-little-16
      >>

      assert {:error, %DeserializationError{} = err} = Metadata.decode(bin)
      assert err.message =~ "unsupported metadata block_version 9"
    end

    test "rejects unknown algorithm byte" do
      bin = <<
        1::unsigned-8,
        99::unsigned-8,
        0::unsigned-little-64,
        1::unsigned-8,
        1::unsigned-8,
        2::unsigned-8,
        0::unsigned-8,
        0::unsigned-little-16
      >>

      assert {:error, %DeserializationError{} = err} = Metadata.decode(bin)
      assert err.message =~ "unknown hash algorithm byte"
    end

    test "rejects unknown backend byte" do
      bin = <<
        1::unsigned-8,
        1::unsigned-8,
        0::unsigned-little-64,
        1::unsigned-8,
        1::unsigned-8,
        99::unsigned-8,
        0::unsigned-8,
        0::unsigned-little-16
      >>

      assert {:error, %DeserializationError{} = err} = Metadata.decode(bin)
      assert err.message =~ "unknown backend byte"
    end

    test "rejects truncated header" do
      assert {:error, %DeserializationError{} = err} = Metadata.decode(<<1, 2, 3>>)
      assert err.message =~ "truncated"
    end

    test "rejects truncated extension" do
      # Declares 100 extension bytes but provides only 3.
      bin = <<
        1::unsigned-8,
        1::unsigned-8,
        0::unsigned-little-64,
        1::unsigned-8,
        1::unsigned-8,
        2::unsigned-8,
        0::unsigned-8,
        100::unsigned-little-16,
        1,
        2,
        3
      >>

      assert {:error, %DeserializationError{} = err} = Metadata.decode(bin)
      assert err.message =~ "extension truncated"
    end
  end

  describe "byte mapping helpers" do
    test "algorithm_to_byte and back are inverse" do
      for algo <- [:phash2, :xxhash3, :murmur3, :custom] do
        b = Metadata.algorithm_to_byte(algo)
        assert {:ok, ^algo} = Metadata.algorithm_from_byte(b)
      end
    end

    test "backend_to_byte and back are inverse" do
      for b <- [:unspecified, :pure, :rust] do
        byte = Metadata.backend_to_byte(b)
        assert {:ok, ^b} = Metadata.backend_from_byte(byte)
      end
    end

    test "unknown wire bytes are reported as errors" do
      assert {:error, _} = Metadata.algorithm_from_byte(7)
      assert {:error, _} = Metadata.backend_from_byte(7)
    end
  end
end
