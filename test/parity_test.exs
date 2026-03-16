defmodule ExDataSketch.ParityTest do
  @moduledoc """
  Asserts byte-identical serialization and identical estimates between
  the Pure Elixir and Rust NIF backends for all sketch algorithms.

  Tagged :rust_nif — skipped when Rust backend is unavailable.
  """
  use ExUnit.Case, async: true

  alias ExDataSketch.Backend.{Pure, Rust}

  alias ExDataSketch.{
    Bloom,
    CMS,
    CQF,
    Cuckoo,
    DDSketch,
    FrequentItems,
    HLL,
    IBLT,
    KLL,
    Quotient,
    Theta,
    ULL,
    XorFilter
  }

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

  describe "Bloom parity" do
    @describetag :rust_nif

    test "put_many produces identical serialization" do
      pure = Bloom.new(capacity: 1000, backend: Pure) |> Bloom.put_many(@items_1000)
      rust = Bloom.new(capacity: 1000, backend: Rust) |> Bloom.put_many(@items_1000)

      assert Bloom.serialize(pure) == Bloom.serialize(rust)
    end

    test "merge produces identical serialization" do
      pure_a = Bloom.new(capacity: 1000, backend: Pure) |> Bloom.put_many(@items_a)
      pure_b = Bloom.new(capacity: 1000, backend: Pure) |> Bloom.put_many(@items_b)
      pure_merged = Bloom.merge(pure_a, pure_b)

      rust_a = Bloom.new(capacity: 1000, backend: Rust) |> Bloom.put_many(@items_a)
      rust_b = Bloom.new(capacity: 1000, backend: Rust) |> Bloom.put_many(@items_b)
      rust_merged = Bloom.merge(rust_a, rust_b)

      assert Bloom.serialize(pure_merged) == Bloom.serialize(rust_merged)
    end

    test "member? returns identical results" do
      pure = Bloom.new(capacity: 1000, backend: Pure) |> Bloom.put_many(@items_a)
      rust = Bloom.new(capacity: 1000, backend: Rust) |> Bloom.put_many(@items_a)

      test_items = @items_a ++ @items_b

      for item <- test_items do
        assert Bloom.member?(pure, item) == Bloom.member?(rust, item),
               "Bloom member? parity mismatch for #{item}"
      end
    end
  end

  describe "Cuckoo parity" do
    @describetag :rust_nif

    @cuckoo_items Enum.map(0..199, &"cuckoo_item_#{&1}")
    @cuckoo_a Enum.map(0..99, &"cuckoo_a_#{&1}")
    @cuckoo_b Enum.map(100..199, &"cuckoo_b_#{&1}")

    test "put_many produces identical serialization" do
      {:ok, pure} = Cuckoo.new(capacity: 512, backend: Pure) |> Cuckoo.put_many(@cuckoo_items)
      {:ok, rust} = Cuckoo.new(capacity: 512, backend: Rust) |> Cuckoo.put_many(@cuckoo_items)

      assert Cuckoo.serialize(pure) == Cuckoo.serialize(rust)
    end

    test "member? returns identical results" do
      {:ok, pure} = Cuckoo.new(capacity: 512, backend: Pure) |> Cuckoo.put_many(@cuckoo_a)
      {:ok, rust} = Cuckoo.new(capacity: 512, backend: Rust) |> Cuckoo.put_many(@cuckoo_a)

      for item <- @cuckoo_a ++ @cuckoo_b do
        assert Cuckoo.member?(pure, item) == Cuckoo.member?(rust, item),
               "Cuckoo member? parity mismatch for #{item}"
      end
    end
  end

  describe "Quotient parity" do
    @describetag :rust_nif

    @qot_items Enum.map(0..99, &"qot_item_#{&1}")
    @qot_a Enum.map(0..49, &"qot_a_#{&1}")
    @qot_b Enum.map(50..99, &"qot_b_#{&1}")

    test "put_many produces identical serialization" do
      pure = Quotient.new(q: 8, r: 5, backend: Pure) |> Quotient.put_many(@qot_items)
      rust = Quotient.new(q: 8, r: 5, backend: Rust) |> Quotient.put_many(@qot_items)

      assert Quotient.serialize(pure) == Quotient.serialize(rust)
    end

    test "merge produces identical serialization" do
      pure_a = Quotient.new(q: 8, r: 5, backend: Pure) |> Quotient.put_many(@qot_a)
      pure_b = Quotient.new(q: 8, r: 5, backend: Pure) |> Quotient.put_many(@qot_b)
      pure_merged = Quotient.merge(pure_a, pure_b)

      rust_a = Quotient.new(q: 8, r: 5, backend: Rust) |> Quotient.put_many(@qot_a)
      rust_b = Quotient.new(q: 8, r: 5, backend: Rust) |> Quotient.put_many(@qot_b)
      rust_merged = Quotient.merge(rust_a, rust_b)

      assert Quotient.serialize(pure_merged) == Quotient.serialize(rust_merged)
    end

    test "member? returns identical results" do
      pure = Quotient.new(q: 8, r: 5, backend: Pure) |> Quotient.put_many(@qot_a)
      rust = Quotient.new(q: 8, r: 5, backend: Rust) |> Quotient.put_many(@qot_a)

      for item <- @qot_a ++ @qot_b do
        assert Quotient.member?(pure, item) == Quotient.member?(rust, item),
               "Quotient member? parity mismatch for #{item}"
      end
    end
  end

  describe "CQF parity" do
    @describetag :rust_nif

    @cqf_items Enum.map(0..99, &"cqf_item_#{&1}")
    @cqf_dupes @cqf_items ++ Enum.map(0..49, &"cqf_item_#{&1}")
    @cqf_a Enum.map(0..49, &"cqf_a_#{&1}")
    @cqf_b Enum.map(50..99, &"cqf_b_#{&1}")

    test "put_many with duplicates produces identical serialization" do
      pure = CQF.new(q: 8, r: 5, backend: Pure) |> CQF.put_many(@cqf_dupes)
      rust = CQF.new(q: 8, r: 5, backend: Rust) |> CQF.put_many(@cqf_dupes)

      assert CQF.serialize(pure) == CQF.serialize(rust)
      assert CQF.count(pure) == CQF.count(rust)
    end

    test "merge produces identical serialization" do
      pure_a = CQF.new(q: 8, r: 5, backend: Pure) |> CQF.put_many(@cqf_a)
      pure_b = CQF.new(q: 8, r: 5, backend: Pure) |> CQF.put_many(@cqf_b)
      pure_merged = CQF.merge(pure_a, pure_b)

      rust_a = CQF.new(q: 8, r: 5, backend: Rust) |> CQF.put_many(@cqf_a)
      rust_b = CQF.new(q: 8, r: 5, backend: Rust) |> CQF.put_many(@cqf_b)
      rust_merged = CQF.merge(rust_a, rust_b)

      assert CQF.serialize(pure_merged) == CQF.serialize(rust_merged)
      assert CQF.count(pure_merged) == CQF.count(rust_merged)
    end

    test "member? and estimate_count return identical results" do
      pure = CQF.new(q: 8, r: 5, backend: Pure) |> CQF.put_many(@cqf_dupes)
      rust = CQF.new(q: 8, r: 5, backend: Rust) |> CQF.put_many(@cqf_dupes)

      for item <- @cqf_items do
        assert CQF.member?(pure, item) == CQF.member?(rust, item),
               "CQF member? parity mismatch for #{item}"

        assert CQF.estimate_count(pure, item) == CQF.estimate_count(rust, item),
               "CQF estimate_count parity mismatch for #{item}"
      end
    end
  end

  describe "XorFilter parity" do
    @describetag :rust_nif

    @xor_items Enum.map(0..199, &"xor_item_#{&1}")
    @xor_non_members Enum.map(0..199, &"xor_absent_#{&1}")

    test "build produces identical serialization (xor8)" do
      {:ok, pure} = XorFilter.build(@xor_items, backend: Pure)
      {:ok, rust} = XorFilter.build(@xor_items, backend: Rust)

      assert XorFilter.serialize(pure) == XorFilter.serialize(rust)
      assert XorFilter.count(pure) == XorFilter.count(rust)
    end

    test "build produces identical serialization (xor16)" do
      {:ok, pure} = XorFilter.build(@xor_items, fingerprint_bits: 16, backend: Pure)
      {:ok, rust} = XorFilter.build(@xor_items, fingerprint_bits: 16, backend: Rust)

      assert XorFilter.serialize(pure) == XorFilter.serialize(rust)
      assert XorFilter.count(pure) == XorFilter.count(rust)
    end

    test "member? returns identical results for members and non-members (xor8)" do
      {:ok, pure} = XorFilter.build(@xor_items, backend: Pure)
      {:ok, rust} = XorFilter.build(@xor_items, backend: Rust)

      for item <- @xor_items do
        assert XorFilter.member?(pure, item) == true
        assert XorFilter.member?(rust, item) == true
      end

      for item <- @xor_non_members do
        assert XorFilter.member?(pure, item) == XorFilter.member?(rust, item),
               "XorFilter member? parity mismatch for non-member #{item}"
      end
    end

    test "member? returns identical results for members and non-members (xor16)" do
      {:ok, pure} = XorFilter.build(@xor_items, fingerprint_bits: 16, backend: Pure)
      {:ok, rust} = XorFilter.build(@xor_items, fingerprint_bits: 16, backend: Rust)

      for item <- @xor_items do
        assert XorFilter.member?(pure, item) == true
        assert XorFilter.member?(rust, item) == true
      end

      for item <- @xor_non_members do
        assert XorFilter.member?(pure, item) == XorFilter.member?(rust, item),
               "XorFilter member? parity mismatch for non-member #{item}"
      end
    end
  end

  describe "IBLT parity" do
    @describetag :rust_nif

    @iblt_items Enum.map(0..99, &"iblt_item_#{&1}")
    @iblt_a Enum.map(0..49, &"iblt_a_#{&1}")
    @iblt_b Enum.map(50..99, &"iblt_b_#{&1}")

    test "put_many produces identical serialization" do
      pure = IBLT.new(cell_count: 256, backend: Pure) |> IBLT.put_many(@iblt_items)
      rust = IBLT.new(cell_count: 256, backend: Rust) |> IBLT.put_many(@iblt_items)

      assert IBLT.serialize(pure) == IBLT.serialize(rust)
      assert IBLT.count(pure) == IBLT.count(rust)
    end

    test "merge produces identical serialization" do
      pure_a = IBLT.new(cell_count: 256, backend: Pure) |> IBLT.put_many(@iblt_a)
      pure_b = IBLT.new(cell_count: 256, backend: Pure) |> IBLT.put_many(@iblt_b)
      pure_merged = IBLT.merge(pure_a, pure_b)

      rust_a = IBLT.new(cell_count: 256, backend: Rust) |> IBLT.put_many(@iblt_a)
      rust_b = IBLT.new(cell_count: 256, backend: Rust) |> IBLT.put_many(@iblt_b)
      rust_merged = IBLT.merge(rust_a, rust_b)

      assert IBLT.serialize(pure_merged) == IBLT.serialize(rust_merged)
      assert IBLT.count(pure_merged) == IBLT.count(rust_merged)
    end

    test "member? returns identical results" do
      pure = IBLT.new(cell_count: 256, backend: Pure) |> IBLT.put_many(@iblt_a)
      rust = IBLT.new(cell_count: 256, backend: Rust) |> IBLT.put_many(@iblt_a)

      for item <- @iblt_a ++ @iblt_b do
        assert IBLT.member?(pure, item) == IBLT.member?(rust, item),
               "IBLT member? parity mismatch for #{item}"
      end
    end
  end

  describe "ULL parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and estimate" do
      pure = ULL.new(backend: Pure) |> ULL.update_many(@items_1000)
      rust = ULL.new(backend: Rust) |> ULL.update_many(@items_1000)

      assert ULL.serialize(pure) == ULL.serialize(rust)
      assert_in_delta ULL.estimate(pure), ULL.estimate(rust), 1.0e-9
    end

    test "merge produces identical serialization and estimate" do
      pure_a = ULL.new(backend: Pure) |> ULL.update_many(@items_a)
      pure_b = ULL.new(backend: Pure) |> ULL.update_many(@items_b)
      pure_merged = ULL.merge(pure_a, pure_b)

      rust_a = ULL.new(backend: Rust) |> ULL.update_many(@items_a)
      rust_b = ULL.new(backend: Rust) |> ULL.update_many(@items_b)
      rust_merged = ULL.merge(rust_a, rust_b)

      assert ULL.serialize(pure_merged) == ULL.serialize(rust_merged)
      assert_in_delta ULL.estimate(pure_merged), ULL.estimate(rust_merged), 1.0e-9
    end
  end
end
