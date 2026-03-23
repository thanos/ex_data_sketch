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

    test "empty string with default seed matches explicit seed 0" do
      h = Hash.xxhash3_64(<<>>)
      assert is_integer(h)
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

  describe "validate_merge_hash_compat!/3" do
    test "passes when both use same default strategy and seed" do
      opts = [p: 14, hash_strategy: :xxhash3]
      assert :ok == Hash.validate_merge_hash_compat!(opts, opts, "HLL")
    end

    test "passes when both use same strategy with explicit seed" do
      opts_a = [p: 14, hash_strategy: :xxhash3, seed: 42]
      opts_b = [p: 14, hash_strategy: :xxhash3, seed: 42]
      assert :ok == Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL")
    end

    test "passes when both omit seed (defaults to 0)" do
      opts_a = [p: 14, hash_strategy: :phash2]
      opts_b = [p: 14, hash_strategy: :phash2]
      assert :ok == Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL")
    end

    test "raises on strategy mismatch" do
      opts_a = [p: 14, hash_strategy: :xxhash3]
      opts_b = [p: 14, hash_strategy: :phash2]

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError,
                   ~r/hash strategy mismatch/,
                   fn -> Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL") end
    end

    test "raises on seed mismatch" do
      opts_a = [p: 14, hash_strategy: :xxhash3, seed: 1]
      opts_b = [p: 14, hash_strategy: :xxhash3, seed: 2]

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError,
                   ~r/seed mismatch/,
                   fn -> Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL") end
    end

    test "raises when either uses custom hash_fn" do
      opts_a = [p: 14, hash_strategy: :custom]
      opts_b = [p: 14, hash_strategy: :xxhash3]

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError,
                   ~r/custom :hash_fn/,
                   fn -> Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL") end
    end

    test "raises when both use custom hash_fn" do
      opts = [p: 14, hash_strategy: :custom]

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError,
                   ~r/custom :hash_fn/,
                   fn -> Hash.validate_merge_hash_compat!(opts, opts, "HLL") end
    end

    test "seed 0 matches missing seed" do
      opts_a = [p: 14, hash_strategy: :xxhash3, seed: 0]
      opts_b = [p: 14, hash_strategy: :xxhash3]
      assert :ok == Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL")
    end
  end
end
