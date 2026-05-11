defmodule ExDataSketch.Hash.RegistryTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Hash
  alias ExDataSketch.Hash.Murmur3

  doctest ExDataSketch.Hash,
    except: [:moduledoc]

  describe "supported_algorithms/0" do
    test "includes all expected algorithms" do
      algos = Hash.supported_algorithms()
      assert :phash2 in algos
      assert :xxhash3 in algos
      assert :murmur3 in algos
      assert :custom in algos
    end
  end

  describe "default_algorithm/0" do
    test "is one of the deterministic algorithms" do
      assert Hash.default_algorithm() in [:xxhash3, :phash2]
    end

    test "matches default_hash_strategy/0 (backward-compat shim)" do
      assert Hash.default_algorithm() == Hash.default_hash_strategy()
    end
  end

  describe "algorithm_info/1" do
    test ":xxhash3 descriptor" do
      info = Hash.algorithm_info(:xxhash3)
      assert info.id == :xxhash3
      assert info.output_bits == 64
      assert info.has_seed == true
      assert is_boolean(info.available?)
      assert info.stability == :stable
    end

    test ":murmur3 descriptor" do
      info = Hash.algorithm_info(:murmur3)
      assert info.id == :murmur3
      assert info.output_bits == 64
      assert info.has_seed == true
      # Murmur3 is always available because the pure implementation is bundled.
      assert info.available? == true
      assert info.stability == :stable
    end

    test ":phash2 descriptor" do
      info = Hash.algorithm_info(:phash2)
      assert info.id == :phash2
      assert info.output_bits == 64
      assert info.stability == :otp_dependent
      assert info.available? == true
    end

    test ":custom descriptor" do
      info = Hash.algorithm_info(:custom)
      assert info.id == :custom
      assert info.has_seed == false
      assert info.stability == :runtime_dependent
    end

    test "raises on unknown id" do
      assert_raise ArgumentError, fn -> Hash.algorithm_info(:not_a_hash) end
    end
  end

  describe "hash64/2 — strategy dispatch" do
    test ":murmur3 routes through the Murmur3 module for binary terms" do
      # Binary terms are hashed as-is (no term_to_binary wrap).
      h = Hash.hash64("hello", hash_strategy: :murmur3)
      assert h == Murmur3.hash("hello", 0)
    end

    test ":murmur3 wraps non-binary terms via term_to_binary" do
      # Non-binary terms cross :erlang.term_to_binary first.
      h = Hash.hash64({:a, 1}, hash_strategy: :murmur3)
      bin = :erlang.term_to_binary({:a, 1})
      assert h == Murmur3.hash(bin, 0)
    end

    test ":murmur3 with explicit seed" do
      h0 = Hash.hash64("hello", hash_strategy: :murmur3, seed: 0)
      h7 = Hash.hash64("hello", hash_strategy: :murmur3, seed: 7)
      assert h0 == Murmur3.hash("hello", 0)
      assert h7 == Murmur3.hash("hello", 7)
      assert h0 != h7
    end

    test ":phash2 still works (no regression)" do
      h = Hash.hash64("hello", hash_strategy: :phash2)
      assert is_integer(h) and h >= 0 and h <= 0xFFFFFFFFFFFFFFFF
    end
  end

  describe "hash64_binary/2 — strategy dispatch" do
    test ":murmur3 routes through the Murmur3 module" do
      bin = <<1, 2, 3, 4, 5>>
      h = Hash.hash64_binary(bin, hash_strategy: :murmur3, seed: 42)
      assert h == Murmur3.hash(bin, 42)
    end
  end
end
