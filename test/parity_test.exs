defmodule ExDataSketch.ParityTest do
  @moduledoc """
  Asserts byte-identical serialization and identical estimates between
  the Pure Elixir and Rust NIF backends for all sketch algorithms.

  Tagged :rust_nif — skipped when Rust backend is unavailable.
  """
  use ExUnit.Case, async: true

  alias ExDataSketch.Backend.{Pure, Rust}
  alias ExDataSketch.{CMS, DDSketch, FrequentItems, HLL, KLL, Theta}

  # Deterministic input strings
  @items_1000 Enum.map(0..999, &"parity_item_#{&1}")
  @items_a Enum.map(0..499, &"set_a_#{&1}")
  @items_b Enum.map(500..999, &"set_b_#{&1}")

  describe "HLL parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and estimate" do
      pure = HLL.new(backend: Pure) |> HLL.update_many(@items_1000)
      rust = HLL.new(backend: Rust) |> HLL.update_many(@items_1000)

      assert HLL.serialize(pure) == HLL.serialize(rust)
      assert_in_delta HLL.estimate(pure), HLL.estimate(rust), 1.0e-9
    end

    test "merge produces identical serialization and estimate" do
      pure_a = HLL.new(backend: Pure) |> HLL.update_many(@items_a)
      pure_b = HLL.new(backend: Pure) |> HLL.update_many(@items_b)
      pure_merged = HLL.merge(pure_a, pure_b)

      rust_a = HLL.new(backend: Rust) |> HLL.update_many(@items_a)
      rust_b = HLL.new(backend: Rust) |> HLL.update_many(@items_b)
      rust_merged = HLL.merge(rust_a, rust_b)

      assert HLL.serialize(pure_merged) == HLL.serialize(rust_merged)
      assert_in_delta HLL.estimate(pure_merged), HLL.estimate(rust_merged), 1.0e-9
    end
  end

  describe "CMS parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and point estimates" do
      pure = CMS.new(backend: Pure) |> CMS.update_many(@items_1000)
      rust = CMS.new(backend: Rust) |> CMS.update_many(@items_1000)

      assert CMS.serialize(pure) == CMS.serialize(rust)

      # Verify point estimates match for a sample of items
      for item <- Enum.take(@items_1000, 20) do
        assert CMS.estimate(pure, item) == CMS.estimate(rust, item),
               "CMS estimate mismatch for #{item}"
      end
    end

    test "merge produces identical serialization and point estimates" do
      pure_a = CMS.new(backend: Pure) |> CMS.update_many(@items_a)
      pure_b = CMS.new(backend: Pure) |> CMS.update_many(@items_b)
      pure_merged = CMS.merge(pure_a, pure_b)

      rust_a = CMS.new(backend: Rust) |> CMS.update_many(@items_a)
      rust_b = CMS.new(backend: Rust) |> CMS.update_many(@items_b)
      rust_merged = CMS.merge(rust_a, rust_b)

      assert CMS.serialize(pure_merged) == CMS.serialize(rust_merged)

      for item <- Enum.take(@items_a ++ @items_b, 20) do
        assert CMS.estimate(pure_merged, item) == CMS.estimate(rust_merged, item),
               "CMS merge estimate mismatch for #{item}"
      end
    end
  end

  describe "KLL parity" do
    @describetag :rust_nif

    @kll_items Enum.map(1..1000, &(&1 * 1.0))
    @kll_a Enum.map(1..500, &(&1 * 1.0))
    @kll_b Enum.map(501..1000, &(&1 * 1.0))

    test "update_many produces identical serialization and quantile estimates" do
      pure = KLL.new(k: 200, backend: Pure) |> KLL.update_many(@kll_items)
      rust = KLL.new(k: 200, backend: Rust) |> KLL.update_many(@kll_items)

      assert KLL.serialize(pure) == KLL.serialize(rust)
      assert KLL.count(pure) == KLL.count(rust)
      assert_in_delta KLL.quantile(pure, 0.5), KLL.quantile(rust, 0.5), 1.0e-9
    end

    test "successive update_many calls produce identical serialization" do
      pure =
        KLL.new(k: 200, backend: Pure)
        |> KLL.update_many(@kll_a)
        |> KLL.update_many(@kll_b)

      rust =
        KLL.new(k: 200, backend: Rust)
        |> KLL.update_many(@kll_a)
        |> KLL.update_many(@kll_b)

      assert KLL.serialize(pure) == KLL.serialize(rust)
      assert KLL.count(pure) == KLL.count(rust)
    end

    test "merge produces identical serialization and quantile estimates" do
      pure_a = KLL.new(k: 200, backend: Pure) |> KLL.update_many(@kll_a)
      pure_b = KLL.new(k: 200, backend: Pure) |> KLL.update_many(@kll_b)
      pure_merged = KLL.merge(pure_a, pure_b)

      rust_a = KLL.new(k: 200, backend: Rust) |> KLL.update_many(@kll_a)
      rust_b = KLL.new(k: 200, backend: Rust) |> KLL.update_many(@kll_b)
      rust_merged = KLL.merge(rust_a, rust_b)

      assert KLL.serialize(pure_merged) == KLL.serialize(rust_merged)
      assert KLL.count(pure_merged) == KLL.count(rust_merged)
      assert_in_delta KLL.quantile(pure_merged, 0.5), KLL.quantile(rust_merged, 0.5), 1.0e-9
    end
  end

  describe "DDSketch parity" do
    @describetag :rust_nif

    @dds_items Enum.map(1..1000, &(&1 * 1.0))
    @dds_a Enum.map(1..500, &(&1 * 1.0))
    @dds_b Enum.map(501..1000, &(&1 * 1.0))

    test "update_many produces identical serialization and quantile estimates" do
      pure = DDSketch.new(alpha: 0.01, backend: Pure) |> DDSketch.update_many(@dds_items)
      rust = DDSketch.new(alpha: 0.01, backend: Rust) |> DDSketch.update_many(@dds_items)

      assert DDSketch.serialize(pure) == DDSketch.serialize(rust)
      assert DDSketch.count(pure) == DDSketch.count(rust)
      assert_in_delta DDSketch.quantile(pure, 0.5), DDSketch.quantile(rust, 0.5), 1.0e-9
    end

    test "successive update_many calls produce identical serialization" do
      pure =
        DDSketch.new(alpha: 0.01, backend: Pure)
        |> DDSketch.update_many(@dds_a)
        |> DDSketch.update_many(@dds_b)

      rust =
        DDSketch.new(alpha: 0.01, backend: Rust)
        |> DDSketch.update_many(@dds_a)
        |> DDSketch.update_many(@dds_b)

      assert DDSketch.serialize(pure) == DDSketch.serialize(rust)
      assert DDSketch.count(pure) == DDSketch.count(rust)
    end

    test "merge produces identical serialization and quantile estimates" do
      pure_a = DDSketch.new(alpha: 0.01, backend: Pure) |> DDSketch.update_many(@dds_a)
      pure_b = DDSketch.new(alpha: 0.01, backend: Pure) |> DDSketch.update_many(@dds_b)
      pure_merged = DDSketch.merge(pure_a, pure_b)

      rust_a = DDSketch.new(alpha: 0.01, backend: Rust) |> DDSketch.update_many(@dds_a)
      rust_b = DDSketch.new(alpha: 0.01, backend: Rust) |> DDSketch.update_many(@dds_b)
      rust_merged = DDSketch.merge(rust_a, rust_b)

      assert DDSketch.serialize(pure_merged) == DDSketch.serialize(rust_merged)
      assert DDSketch.count(pure_merged) == DDSketch.count(rust_merged)

      assert_in_delta DDSketch.quantile(pure_merged, 0.5),
                      DDSketch.quantile(rust_merged, 0.5),
                      1.0e-9
    end
  end

  describe "FrequentItems parity" do
    @describetag :rust_nif

    @fi_items List.duplicate("a", 100) ++
                List.duplicate("b", 60) ++
                List.duplicate("c", 30) ++
                List.duplicate("d", 10) ++
                Enum.map(1..50, fn i -> "u#{i}" end)
    @fi_a List.duplicate("a", 50) ++ List.duplicate("b", 30) ++ Enum.map(1..20, &"x#{&1}")
    @fi_b List.duplicate("a", 50) ++ List.duplicate("b", 30) ++ Enum.map(21..40, &"x#{&1}")

    test "update_many produces identical serialization and estimates" do
      pure = FrequentItems.new(k: 10, backend: Pure) |> FrequentItems.update_many(@fi_items)
      rust = FrequentItems.new(k: 10, backend: Rust) |> FrequentItems.update_many(@fi_items)

      assert FrequentItems.serialize(pure) == FrequentItems.serialize(rust)
      assert FrequentItems.count(pure) == FrequentItems.count(rust)
      assert FrequentItems.entry_count(pure) == FrequentItems.entry_count(rust)
      assert FrequentItems.top_k(pure) == FrequentItems.top_k(rust)
    end

    test "successive update_many calls produce identical serialization" do
      pure =
        FrequentItems.new(k: 10, backend: Pure)
        |> FrequentItems.update_many(@fi_a)
        |> FrequentItems.update_many(@fi_b)

      rust =
        FrequentItems.new(k: 10, backend: Rust)
        |> FrequentItems.update_many(@fi_a)
        |> FrequentItems.update_many(@fi_b)

      assert FrequentItems.serialize(pure) == FrequentItems.serialize(rust)
      assert FrequentItems.count(pure) == FrequentItems.count(rust)
    end

    test "merge produces identical serialization and estimates" do
      pure_a = FrequentItems.new(k: 10, backend: Pure) |> FrequentItems.update_many(@fi_a)
      pure_b = FrequentItems.new(k: 10, backend: Pure) |> FrequentItems.update_many(@fi_b)
      pure_merged = FrequentItems.merge(pure_a, pure_b)

      rust_a = FrequentItems.new(k: 10, backend: Rust) |> FrequentItems.update_many(@fi_a)
      rust_b = FrequentItems.new(k: 10, backend: Rust) |> FrequentItems.update_many(@fi_b)
      rust_merged = FrequentItems.merge(rust_a, rust_b)

      assert FrequentItems.serialize(pure_merged) == FrequentItems.serialize(rust_merged)
      assert FrequentItems.count(pure_merged) == FrequentItems.count(rust_merged)
      assert FrequentItems.top_k(pure_merged) == FrequentItems.top_k(rust_merged)
    end
  end

  describe "Theta parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and estimate" do
      pure = Theta.new(backend: Pure) |> Theta.update_many(@items_1000)
      rust = Theta.new(backend: Rust) |> Theta.update_many(@items_1000)

      assert Theta.serialize(pure) == Theta.serialize(rust)
      assert_in_delta Theta.estimate(pure), Theta.estimate(rust), 1.0e-9
    end

    test "merge produces identical serialization and estimate" do
      pure_a = Theta.new(backend: Pure) |> Theta.update_many(@items_a)
      pure_b = Theta.new(backend: Pure) |> Theta.update_many(@items_b)
      pure_merged = Theta.merge(pure_a, pure_b)

      rust_a = Theta.new(backend: Rust) |> Theta.update_many(@items_a)
      rust_b = Theta.new(backend: Rust) |> Theta.update_many(@items_b)
      rust_merged = Theta.merge(rust_a, rust_b)

      assert Theta.serialize(pure_merged) == Theta.serialize(rust_merged)
      assert_in_delta Theta.estimate(pure_merged), Theta.estimate(rust_merged), 1.0e-9
    end

    test "compact produces identical serialization and estimate" do
      pure = Theta.new(backend: Pure) |> Theta.update_many(@items_1000) |> Theta.compact()
      rust = Theta.new(backend: Rust) |> Theta.update_many(@items_1000) |> Theta.compact()

      assert Theta.serialize(pure) == Theta.serialize(rust)
      assert_in_delta Theta.estimate(pure), Theta.estimate(rust), 1.0e-9
    end

    test "compact + merge produces identical serialization and estimate" do
      pure_a = Theta.new(backend: Pure) |> Theta.update_many(@items_a) |> Theta.compact()
      pure_b = Theta.new(backend: Pure) |> Theta.update_many(@items_b) |> Theta.compact()
      pure_merged = Theta.merge(pure_a, pure_b)

      rust_a = Theta.new(backend: Rust) |> Theta.update_many(@items_a) |> Theta.compact()
      rust_b = Theta.new(backend: Rust) |> Theta.update_many(@items_b) |> Theta.compact()
      rust_merged = Theta.merge(rust_a, rust_b)

      assert Theta.serialize(pure_merged) == Theta.serialize(rust_merged)
      assert_in_delta Theta.estimate(pure_merged), Theta.estimate(rust_merged), 1.0e-9
    end
  end
end
