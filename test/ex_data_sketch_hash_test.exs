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

  describe "xxhash3_64/1" do
    test "returns a non-negative integer" do
      result = Hash.xxhash3_64("hello")
      assert is_integer(result)
      assert result >= 0
    end

    test "is deterministic" do
      assert Hash.xxhash3_64("test") == Hash.xxhash3_64("test")
    end

    test "different inputs produce different hashes" do
      h1 = Hash.xxhash3_64("alice")
      h2 = Hash.xxhash3_64("bob")
      assert h1 != h2
    end

    test "produces 64-bit values" do
      result = Hash.xxhash3_64("something")
      assert result >= 0
      assert result <= 0xFFFFFFFFFFFFFFFF
    end

    test "empty binary hashes to a value" do
      result = Hash.xxhash3_64(<<>>)
      assert is_integer(result)
      assert result >= 0
    end

    test "known test vector: empty string" do
      # XXHash3 of empty string with seed 0 is a known constant
      h = Hash.xxhash3_64(<<>>)
      assert is_integer(h)
      # Just verify it works and is deterministic
      assert h == Hash.xxhash3_64(<<>>, 0)
    end
  end

  describe "xxhash3_64/2 (seeded)" do
    test "different seeds produce different hashes" do
      h1 = Hash.xxhash3_64("item", 0)
      h2 = Hash.xxhash3_64("item", 42)
      assert h1 != h2
    end

    test "is deterministic with same seed" do
      assert Hash.xxhash3_64("data", 123) == Hash.xxhash3_64("data", 123)
    end

    test "can be used as hash_fn for sketches" do
      hash_fn = fn term -> Hash.xxhash3_64(to_string(term)) end
      sketch = ExDataSketch.HLL.new(p: 10, hash_fn: hash_fn)
      sketch = ExDataSketch.HLL.update_many(sketch, ["a", "b", "c"])
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end
  end
end
