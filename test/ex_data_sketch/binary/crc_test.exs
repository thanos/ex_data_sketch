defmodule ExDataSketch.Binary.CRCTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Binary.CRC

  doctest ExDataSketch.Binary.CRC

  describe "CRC32C correctness" do
    test "empty binary -> 0" do
      assert CRC.crc32c(<<>>) == 0
      assert CRC.pure_crc32c(<<>>) == 0
    end

    test "matches the standard CRC32C check vector ('123456789' -> 0xE3069283)" do
      {vec, expected} = CRC.check_vector()
      assert CRC.crc32c(vec) == expected
      assert CRC.pure_crc32c(vec) == expected
    end

    test "matches additional cross-language reference values" do
      # Cross-referenced against Python `crc32c.crc32c(...)`.
      vectors = [
        {"", 0x00000000},
        {"hello", 0x9A71BB4C},
        {"123456789", 0xE3069283},
        {<<0>>, 0x527D5351},
        {<<0, 0, 0, 0>>, 0x48674BC7}
      ]

      for {input, expected} <- vectors do
        assert CRC.crc32c(input) == expected, "Rust drift on #{inspect(input)}"
        assert CRC.pure_crc32c(input) == expected, "Pure drift on #{inspect(input)}"
      end
    end

    test "single-bit change produces a different CRC" do
      a = "the quick brown fox jumps over the lazy dog"
      b = "the quick brown fox jumps over the lazy dof"
      assert CRC.crc32c(a) != CRC.crc32c(b)
    end

    test "output fits in u32" do
      for bin <- ["", "a", "abc", :crypto.strong_rand_bytes(64), :crypto.strong_rand_bytes(1024)] do
        crc = CRC.crc32c(bin)
        assert is_integer(crc) and crc >= 0 and crc <= 0xFFFFFFFF
      end
    end
  end

  describe "Pure vs Rust parity" do
    @describetag :rust_nif

    test "byte-identical output across a deterministic corpus" do
      cases = [
        <<>>,
        "a",
        "abc",
        "123456789",
        :binary.copy(<<0xAB>>, 17),
        :binary.copy(<<0x55>>, 256),
        :crypto.strong_rand_bytes(1024)
      ]

      for bin <- cases do
        assert CRC.crc32c(bin) == CRC.pure_crc32c(bin),
               "Pure vs Rust diverged for input of #{byte_size(bin)} bytes"
      end
    end

    property "Pure and Rust agree on random inputs" do
      check all(bin <- StreamData.binary(max_length: 4096), max_runs: 200) do
        assert CRC.crc32c(bin) == CRC.pure_crc32c(bin)
      end
    end
  end
end
