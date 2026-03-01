defmodule ExDataSketch.HashTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch.Hash

  alias ExDataSketch.Hash

  describe "hash64/2" do
    test "returns a non-negative integer" do
      result = Hash.hash64("hello")
      assert is_integer(result)
      assert result >= 0
    end

    test "is deterministic" do
      assert Hash.hash64("test") == Hash.hash64("test")
    end

    test "different inputs produce different hashes" do
      h1 = Hash.hash64("alice")
      h2 = Hash.hash64("bob")
      assert h1 != h2
    end

    test "produces 64-bit values" do
      result = Hash.hash64("something")
      assert result >= 0
      assert result <= 0xFFFFFFFFFFFFFFFF
    end

    test "different seeds produce different hashes" do
      h1 = Hash.hash64("item", seed: 0)
      h2 = Hash.hash64("item", seed: 42)
      assert h1 != h2
    end

    test "supports custom hash function" do
      custom_fn = fn _term -> 12_345 end
      assert Hash.hash64("anything", hash_fn: custom_fn) == 12_345
    end

    test "hashes different types" do
      h1 = Hash.hash64(:atom)
      h2 = Hash.hash64(42)
      h3 = Hash.hash64({1, 2})
      h4 = Hash.hash64([1, 2, 3])

      hashes = [h1, h2, h3, h4]
      assert length(Enum.uniq(hashes)) == 4
    end
  end

  describe "hash64_binary/2" do
    test "returns a non-negative integer" do
      result = Hash.hash64_binary(<<1, 2, 3>>)
      assert is_integer(result)
      assert result >= 0
    end

    test "is deterministic" do
      assert Hash.hash64_binary(<<"abc">>) == Hash.hash64_binary(<<"abc">>)
    end

    test "different binaries produce different hashes" do
      h1 = Hash.hash64_binary(<<1, 2, 3>>)
      h2 = Hash.hash64_binary(<<4, 5, 6>>)
      assert h1 != h2
    end

    test "supports custom hash function" do
      custom_fn = fn _binary -> 99_999 end
      assert Hash.hash64_binary(<<0>>, hash_fn: custom_fn) == 99_999
    end

    test "empty binary hashes to a value" do
      result = Hash.hash64_binary(<<>>)
      assert is_integer(result)
      assert result >= 0
    end
  end
end
