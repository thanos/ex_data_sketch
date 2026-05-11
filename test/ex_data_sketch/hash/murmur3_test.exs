defmodule ExDataSketch.Hash.Murmur3Test do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Hash.Murmur3

  doctest ExDataSketch.Hash.Murmur3

  describe "id/0 and available?/0" do
    test "id is :murmur3" do
      assert Murmur3.id() == :murmur3
    end

    test "is always available (pure fallback bundled)" do
      assert Murmur3.available?() == true
    end
  end

  describe "hash/2 (pure correctness)" do
    test "empty binary, seed 0 matches reference vector" do
      # Reference: org.apache.datasketches.hash.MurmurHash3.hash(new byte[0], 0L)
      # high 64 bits of the 128-bit output.
      assert Murmur3.pure_hash(<<>>, 0) == 0
    end

    test "deterministic across calls" do
      assert Murmur3.hash("hello", 0) == Murmur3.hash("hello", 0)
      assert Murmur3.hash("hello", 42) == Murmur3.hash("hello", 42)
    end

    test "different seeds produce different hashes" do
      assert Murmur3.hash("hello", 0) != Murmur3.hash("hello", 1)
      assert Murmur3.hash("hello", 1) != Murmur3.hash("hello", 2)
    end

    test "different inputs produce different hashes" do
      assert Murmur3.hash("hello", 0) != Murmur3.hash("world", 0)
    end

    test "output is in 0..2^64-1" do
      h = Murmur3.hash("anything", 0)
      assert is_integer(h)
      assert h >= 0
      assert h <= 0xFFFFFFFFFFFFFFFF
    end

    test "handles inputs of every length from 0 to 32" do
      for n <- 0..32 do
        bin = :binary.copy(<<0xAB>>, n)
        h = Murmur3.hash(bin, 0)
        assert is_integer(h) and h >= 0 and h <= 0xFFFFFFFFFFFFFFFF
      end
    end
  end

  describe "hash128/2" do
    test "returns a 128-bit pair" do
      {h1, h2} = Murmur3.hash128("hello", 0)
      assert is_integer(h1) and h1 >= 0 and h1 <= 0xFFFFFFFFFFFFFFFF
      assert is_integer(h2) and h2 >= 0 and h2 <= 0xFFFFFFFFFFFFFFFF
    end

    test "high 64 bits match hash/2" do
      seeds = [0, 1, 42, 9001, 0xDEADBEEF]
      inputs = ["", "a", "abc", :crypto.strong_rand_bytes(17), :crypto.strong_rand_bytes(64)]

      for s <- seeds, x <- inputs do
        {h1, _h2} = Murmur3.hash128(x, s)
        assert Murmur3.hash(x, s) == h1
      end
    end
  end

  describe "interop with existing DataSketches.Murmur3.seed_hash/1" do
    test "matches seed_hash for 8-byte little-endian seed encoding" do
      # The existing Murmur3 helper hashes the 8-byte LE of the seed with hash-seed 0
      # then takes the low 16 bits of the first u64 output word.
      for seed <- [0, 1, 42, 9001, 0xDEAD] do
        bin = <<seed::unsigned-little-64>>
        {h1, _h2} = Murmur3.hash128(bin, 0)
        expected = Bitwise.band(h1, 0xFFFF)
        assert ExDataSketch.DataSketches.Murmur3.seed_hash(seed) == expected
      end
    end
  end

  describe "Pure vs Rust parity" do
    @describetag :rust_nif

    test "byte-identical output across a deterministic corpus" do
      if ExDataSketch.Hash.nif_available?() do
        cases = [
          {"", 0},
          {"", 42},
          {"a", 0},
          {"hello", 0},
          {"hello", 1},
          {"hello, world!", 9001},
          {:binary.copy(<<0xAB>>, 15), 0},
          {:binary.copy(<<0xAB>>, 16), 0},
          {:binary.copy(<<0xAB>>, 17), 0},
          {:binary.copy(<<0xAB>>, 32), 0},
          {:binary.copy(<<0xAB>>, 33), 0},
          {:binary.copy(<<0xAB>>, 128), 0xDEADBEEF}
        ]

        for {bin, seed} <- cases do
          pure = Murmur3.pure_hash(bin, seed)
          via_nif = ExDataSketch.Nif.murmur3_x64_128_nif(bin, seed)

          assert pure == via_nif,
                 "Pure vs Rust diverged for input of #{byte_size(bin)} bytes, seed #{seed}"
        end
      end
    end

    property "Pure and Rust produce byte-identical output on random inputs" do
      if ExDataSketch.Hash.nif_available?() do
        check all(
                bin <- StreamData.binary(),
                seed <- StreamData.integer(0..0xFFFFFFFF),
                max_runs: 200
              ) do
          pure = Murmur3.pure_hash(bin, seed)
          via_nif = ExDataSketch.Nif.murmur3_x64_128_nif(bin, seed)
          assert pure == via_nif
        end
      end
    end

    property "Pure and Rust produce byte-identical full 128-bit output" do
      if ExDataSketch.Hash.nif_available?() do
        check all(
                bin <- StreamData.binary(),
                seed <- StreamData.integer(0..0xFFFFFFFF),
                max_runs: 200
              ) do
          pure = Murmur3.hash128(bin, seed)
          rust = ExDataSketch.Nif.murmur3_x64_128_full_nif(bin, seed)
          assert pure == rust
        end
      end
    end
  end

  describe "stability (cross-language regression guard)" do
    # Cross-referenced against Python `mmh3.hash64(..., signed=False)[0]` — the
    # canonical MurmurHash3_x64_128 high-64 result. These vectors lock the
    # algorithm and the Apache DataSketches high-64-bit convention. Drift in
    # either the Pure or Rust implementation will fail this test.
    test "matches canonical Python mmh3 reference" do
      vectors = [
        # {input, seed, expected_high_u64}
        {"", 0, 0x0000000000000000},
        {"a", 0, 0x85555565F6597889},
        {"abc", 0, 0xB4963F3F3FAD7867},
        {"hello", 0, 0xCBD8A7B341BD9B02},
        {"hello", 1, 0xA78DDFF5ADAE8D10}
      ]

      for {bin, seed, expected} <- vectors do
        assert Murmur3.pure_hash(bin, seed) == expected,
               "pure_hash drift for #{inspect(bin)}, seed #{seed}"

        assert Murmur3.hash(bin, seed) == expected,
               "hash/2 drift for #{inspect(bin)}, seed #{seed}"
      end
    end
  end
end
