defmodule ExDataSketch.HotPathTest do
  @moduledoc """
  Phase 3 hot-path correctness tests.

  Asserts that the new v0.8.0 `_raw_h_nif` family (algorithm-dispatched
  in-Rust hashing) is byte-identical to:

  - the legacy v0.7.1 `_raw_nif` family for `:xxhash3`;
  - the Pure-Elixir-hashed `update_many_nif` path for `:murmur3`.

  These tests do NOT cover the high-level sketch API (that's covered by the
  existing per-sketch test suites). They poke the NIFs directly to lock the
  parity contract.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Hash.Murmur3
  alias ExDataSketch.HLL
  alias ExDataSketch.Nif
  alias ExDataSketch.Theta
  alias ExDataSketch.ULL

  @moduletag :rust_nif

  @algo_xxh3 1
  @algo_murmur3 2

  describe "HLL raw_h NIF parity" do
    test "raw_h with XXH3 algorithm matches legacy raw NIF byte-for-byte" do
      sketch = HLL.new(p: 12, hash_strategy: :xxhash3)
      items = for i <- 1..1_000, do: "item_#{i}"

      legacy = Nif.hll_update_many_raw_nif(sketch.state, items, 12, 0)
      dispatched = Nif.hll_update_many_raw_h_nif(sketch.state, items, 12, 0, @algo_xxh3)
      assert legacy == dispatched
    end

    test "raw_h with Murmur3 produces the same state as Elixir-side Murmur3 + non-raw NIF" do
      sketch = HLL.new(p: 12, hash_strategy: :murmur3)
      items = for i <- 1..1_000, do: "item_#{i}"

      via_raw_h = Nif.hll_update_many_raw_h_nif(sketch.state, items, 12, 0, @algo_murmur3)

      hashes_bin =
        items
        |> Enum.map(fn item -> <<Murmur3.hash(item, 0)::unsigned-little-64>> end)
        |> IO.iodata_to_binary()

      via_elixir_hashed = Nif.hll_update_many_nif(sketch.state, hashes_bin, 12)
      assert via_raw_h == via_elixir_hashed
    end

    test "raw_h rejects unknown algorithm byte" do
      sketch = HLL.new(p: 12)

      assert {:error, msg} = Nif.hll_update_many_raw_h_nif(sketch.state, ["a"], 12, 0, 99)
      assert msg =~ "unsupported hash algorithm byte"
    end

    property "every algorithm × seed combination round-trips identically" do
      check all(
              bin_items <-
                StreamData.list_of(StreamData.binary(min_length: 1, max_length: 16),
                  min_length: 1,
                  max_length: 200
                ),
              seed <- StreamData.integer(0..0xFFFFFFFF),
              max_runs: 30
            ) do
        sketch = HLL.new(p: 10)

        out_x = Nif.hll_update_many_raw_h_nif(sketch.state, bin_items, 10, seed, @algo_xxh3)
        legacy = Nif.hll_update_many_raw_nif(sketch.state, bin_items, 10, seed)
        assert out_x == legacy

        out_m = Nif.hll_update_many_raw_h_nif(sketch.state, bin_items, 10, seed, @algo_murmur3)

        hashes_bin =
          bin_items
          |> Enum.map(fn item -> <<Murmur3.hash(item, seed)::unsigned-little-64>> end)
          |> IO.iodata_to_binary()

        elixir_hashed = Nif.hll_update_many_nif(sketch.state, hashes_bin, 10)
        assert out_m == elixir_hashed
      end
    end
  end

  describe "ULL raw_h NIF parity" do
    test "raw_h with XXH3 matches legacy raw" do
      sketch = ULL.new(p: 12, hash_strategy: :xxhash3)
      items = for i <- 1..500, do: "item_#{i}"

      legacy = Nif.ull_update_many_raw_nif(sketch.state, items, 12, 0)
      dispatched = Nif.ull_update_many_raw_h_nif(sketch.state, items, 12, 0, @algo_xxh3)
      assert legacy == dispatched
    end

    test "raw_h with Murmur3 matches Elixir-side Murmur3 + non-raw NIF" do
      sketch = ULL.new(p: 12, hash_strategy: :murmur3)
      items = for i <- 1..500, do: "item_#{i}"

      via_raw_h = Nif.ull_update_many_raw_h_nif(sketch.state, items, 12, 0, @algo_murmur3)

      hashes_bin =
        items
        |> Enum.map(fn item -> <<Murmur3.hash(item, 0)::unsigned-little-64>> end)
        |> IO.iodata_to_binary()

      via_elixir_hashed = Nif.ull_update_many_nif(sketch.state, hashes_bin, 12)
      assert via_raw_h == via_elixir_hashed
    end
  end

  describe "Theta raw_h NIF parity" do
    test "raw_h with XXH3 matches legacy raw" do
      sketch = Theta.new(k: 1024, hash_strategy: :xxhash3)
      items = for i <- 1..500, do: "item_#{i}"

      legacy = Nif.theta_update_many_raw_nif(sketch.state, items, 0)
      dispatched = Nif.theta_update_many_raw_h_nif(sketch.state, items, 0, @algo_xxh3)
      assert legacy == dispatched
    end

    test "raw_h with Murmur3 matches Elixir-side Murmur3 + non-raw NIF" do
      sketch = Theta.new(k: 1024, hash_strategy: :murmur3)
      items = for i <- 1..500, do: "item_#{i}"

      via_raw_h = Nif.theta_update_many_raw_h_nif(sketch.state, items, 0, @algo_murmur3)

      hashes_bin =
        items
        |> Enum.map(fn item -> <<Murmur3.hash(item, 0)::unsigned-little-64>> end)
        |> IO.iodata_to_binary()

      via_elixir_hashed = Nif.theta_update_many_nif(sketch.state, hashes_bin)
      assert via_raw_h == via_elixir_hashed
    end
  end

  describe "CMS raw_h NIF parity" do
    test "raw_h with XXH3 matches legacy raw" do
      sketch =
        ExDataSketch.CMS.new(width: 1024, depth: 4, counter_width: 32, hash_strategy: :xxhash3)

      items = for i <- 1..500, do: {"item_#{i}", 1}

      legacy = Nif.cms_update_many_raw_nif(sketch.state, items, 1024, 4, 32, 0)
      dispatched = Nif.cms_update_many_raw_h_nif(sketch.state, items, 1024, 4, 32, 0, @algo_xxh3)
      assert legacy == dispatched
    end

    test "raw_h with Murmur3 matches Elixir-side Murmur3 + non-raw NIF" do
      sketch =
        ExDataSketch.CMS.new(width: 1024, depth: 4, counter_width: 32, hash_strategy: :murmur3)

      items = for i <- 1..500, do: {"item_#{i}", 1}

      via_raw_h =
        Nif.cms_update_many_raw_h_nif(sketch.state, items, 1024, 4, 32, 0, @algo_murmur3)

      pairs_bin =
        items
        |> Enum.map(fn {bin, inc} ->
          <<Murmur3.hash(bin, 0)::unsigned-little-64, inc::unsigned-little-32>>
        end)
        |> IO.iodata_to_binary()

      via_elixir_hashed = Nif.cms_update_many_nif(sketch.state, pairs_bin, 1024, 4, 32)
      assert via_raw_h == via_elixir_hashed
    end
  end

  describe "high-level API: Murmur3 end-to-end" do
    test "HLL with Murmur3 produces a usable estimate" do
      items = for i <- 1..1_000, do: "item_#{i}"
      sketch = HLL.from_enumerable(items, p: 12, hash_strategy: :murmur3)
      assert sketch.opts[:hash_strategy] == :murmur3
      # 1000 items, p=12 → expect ~1000 ± 25% (HLL has ~1.6% RSE at p=12)
      assert_in_delta HLL.estimate(sketch), 1000.0, 250.0
    end

    test "ULL with Murmur3 produces a usable estimate" do
      items = for i <- 1..1_000, do: "item_#{i}"
      sketch = ULL.from_enumerable(items, p: 12, hash_strategy: :murmur3)
      assert sketch.opts[:hash_strategy] == :murmur3
      assert_in_delta ULL.estimate(sketch), 1000.0, 250.0
    end

    test "Theta with Murmur3 produces a usable estimate" do
      items = for i <- 1..1_000, do: "item_#{i}"
      sketch = Theta.from_enumerable(items, k: 4096, hash_strategy: :murmur3)
      assert sketch.opts[:hash_strategy] == :murmur3
      assert_in_delta Theta.estimate(sketch), 1000.0, 50.0
    end

    test "CMS with Murmur3 produces sane point estimates" do
      items = for i <- 1..500, do: "item_#{i}"

      sketch =
        ExDataSketch.CMS.from_enumerable(items,
          width: 2048,
          depth: 5,
          counter_width: 32,
          hash_strategy: :murmur3
        )

      assert sketch.opts[:hash_strategy] == :murmur3

      # Each item was inserted once; CMS over-estimates, so estimate ≥ 1.
      for i <- 1..500 do
        assert ExDataSketch.CMS.estimate(sketch, "item_#{i}") >= 1
      end
    end

    test "merge across different hash strategies is rejected" do
      items = for i <- 1..50, do: "item_#{i}"
      x = HLL.from_enumerable(items, p: 12, hash_strategy: :xxhash3)
      m = HLL.from_enumerable(items, p: 12, hash_strategy: :murmur3)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, fn ->
        HLL.merge(x, m)
      end
    end
  end
end
