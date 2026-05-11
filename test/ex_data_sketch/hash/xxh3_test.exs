defmodule ExDataSketch.Hash.XXH3Test do
  use ExUnit.Case, async: true

  alias ExDataSketch.Hash.XXH3

  doctest ExDataSketch.Hash.XXH3

  describe "id/0 and available?/0" do
    test "id is :xxhash3" do
      assert XXH3.id() == :xxhash3
    end

    test "available?/0 mirrors Hash.nif_available?/0" do
      assert XXH3.available?() == ExDataSketch.Hash.nif_available?()
    end
  end

  describe "hash/2" do
    @describetag :rust_nif
    test "deterministic across calls" do
      if XXH3.available?() do
        assert XXH3.hash("hello", 0) == XXH3.hash("hello", 0)
      end
    end

    test "different seeds produce different hashes" do
      if XXH3.available?() do
        assert XXH3.hash("hello", 0) != XXH3.hash("hello", 1)
      end
    end

    test "agrees with the legacy ExDataSketch.Hash.xxhash3_64 entry point" do
      if XXH3.available?() do
        for seed <- [0, 1, 42, 0xDEAD_BEEF] do
          assert XXH3.hash("hello", seed) == ExDataSketch.Hash.xxhash3_64("hello", seed)
        end
      end
    end
  end
end
