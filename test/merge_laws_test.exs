defmodule ExDataSketch.MergeLawsTest do
  @moduledoc """
  StreamData property tests verifying algebraic merge laws for all sketch types.
  Fixed seeds and bounded sizes ensure determinism and fast execution.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.{CMS, DDSketch, FrequentItems, HLL, KLL, Theta}

  @max_runs 50
  @hll_opts [p: 10]
  @cms_opts [width: 256, depth: 4, counter_width: 32]
  @theta_opts [k: 1024]
  @kll_opts [k: 200]
  @dds_opts [alpha: 0.01]
  @fi_opts [k: 16]

  defp string_list(max_length \\ 20) do
    list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: max_length)
  end

  describe "HLL merge laws" do
    property "associativity" do
      check all(
              a <- string_list(10),
              b <- string_list(10),
              c <- string_list(10),
              max_runs: @max_runs
            ) do
        sa = HLL.from_enumerable(a, @hll_opts)
        sb = HLL.from_enumerable(b, @hll_opts)
        sc = HLL.from_enumerable(c, @hll_opts)

        left = HLL.merge(sa, HLL.merge(sb, sc))
        right = HLL.merge(HLL.merge(sa, sb), sc)

        assert left.state == right.state
      end
    end

    property "commutativity" do
      check all(
              a <- string_list(),
              b <- string_list(),
              max_runs: @max_runs
            ) do
        sa = HLL.from_enumerable(a, @hll_opts)
        sb = HLL.from_enumerable(b, @hll_opts)

        assert HLL.merge(sa, sb).state == HLL.merge(sb, sa).state
      end
    end

    property "identity: merge with empty" do
      check all(items <- string_list(), max_runs: @max_runs) do
        sketch = HLL.from_enumerable(items, @hll_opts)
        empty = HLL.new(@hll_opts)

        assert HLL.merge(sketch, empty).state == sketch.state
        assert HLL.merge(empty, sketch).state == sketch.state
      end
    end

    property "chunking equivalence" do
      check all(
              chunk_a <- string_list(15),
              chunk_b <- string_list(15),
              max_runs: @max_runs
            ) do
        all_items = chunk_a ++ chunk_b
        whole = HLL.from_enumerable(all_items, @hll_opts)

        merged =
          HLL.merge(
            HLL.from_enumerable(chunk_a, @hll_opts),
            HLL.from_enumerable(chunk_b, @hll_opts)
          )

        whole_est = HLL.estimate(whole)
        merged_est = HLL.estimate(merged)

        # Relative error should be very small for identical inputs
        assert_in_delta(whole_est, merged_est, whole_est * 0.01 + 0.001)
      end
    end
  end

  describe "CMS merge laws" do
    property "associativity" do
      check all(
              a <- string_list(10),
              b <- string_list(10),
              c <- string_list(10),
              max_runs: @max_runs
            ) do
        sa = CMS.from_enumerable(a, @cms_opts)
        sb = CMS.from_enumerable(b, @cms_opts)
        sc = CMS.from_enumerable(c, @cms_opts)

        left = CMS.merge(sa, CMS.merge(sb, sc))
        right = CMS.merge(CMS.merge(sa, sb), sc)

        assert left.state == right.state
      end
    end

    property "commutativity" do
      check all(
              a <- string_list(),
              b <- string_list(),
              max_runs: @max_runs
            ) do
        sa = CMS.from_enumerable(a, @cms_opts)
        sb = CMS.from_enumerable(b, @cms_opts)

        assert CMS.merge(sa, sb).state == CMS.merge(sb, sa).state
      end
    end

    property "identity: merge with empty" do
      check all(items <- string_list(), max_runs: @max_runs) do
        sketch = CMS.from_enumerable(items, @cms_opts)
        empty = CMS.new(@cms_opts)

        assert CMS.merge(sketch, empty).state == sketch.state
        assert CMS.merge(empty, sketch).state == sketch.state
      end
    end

    property "monotonicity: adding more never decreases estimate" do
      check all(
              item <- string(:alphanumeric, min_length: 1),
              n <- integer(1..20),
              max_runs: @max_runs
            ) do
        sketch = CMS.new(@cms_opts)

        {_final, estimates} =
          Enum.reduce(1..n, {sketch, []}, fn _, {s, acc} ->
            s = CMS.update(s, item)
            {s, [CMS.estimate(s, item) | acc]}
          end)

        # Estimates should be non-decreasing (reversed list is non-increasing)
        assert estimates == Enum.sort(estimates, :desc)
      end
    end

    property "chunking equivalence: merged chunks match whole" do
      check all(
              chunk_a <- string_list(10),
              chunk_b <- string_list(10),
              max_runs: @max_runs
            ) do
        all_items = chunk_a ++ chunk_b
        whole = CMS.from_enumerable(all_items, @cms_opts)

        merged =
          CMS.merge(
            CMS.from_enumerable(chunk_a, @cms_opts),
            CMS.from_enumerable(chunk_b, @cms_opts)
          )

        # Point estimates must be exactly equal for CMS
        for item <- Enum.uniq(all_items) do
          assert CMS.estimate(whole, item) == CMS.estimate(merged, item)
        end
      end
    end
  end

  describe "Theta merge laws" do
    property "associativity" do
      check all(
              a <- string_list(10),
              b <- string_list(10),
              c <- string_list(10),
              max_runs: @max_runs
            ) do
        sa = Theta.from_enumerable(a, @theta_opts)
        sb = Theta.from_enumerable(b, @theta_opts)
        sc = Theta.from_enumerable(c, @theta_opts)

        left = Theta.merge(sa, Theta.merge(sb, sc))
        right = Theta.merge(Theta.merge(sa, sb), sc)

        assert Theta.estimate(left) == Theta.estimate(right)
      end
    end

    property "commutativity" do
      check all(
              a <- string_list(),
              b <- string_list(),
              max_runs: @max_runs
            ) do
        sa = Theta.from_enumerable(a, @theta_opts)
        sb = Theta.from_enumerable(b, @theta_opts)

        assert Theta.estimate(Theta.merge(sa, sb)) == Theta.estimate(Theta.merge(sb, sa))
      end
    end

    property "identity: merge with empty" do
      check all(items <- string_list(), max_runs: @max_runs) do
        sketch = Theta.from_enumerable(items, @theta_opts)
        empty = Theta.new(@theta_opts)

        assert Theta.estimate(Theta.merge(sketch, empty)) == Theta.estimate(sketch)
        assert Theta.estimate(Theta.merge(empty, sketch)) == Theta.estimate(sketch)
      end
    end

    property "idempotency: merge(a, a) estimate == estimate(a)" do
      check all(items <- string_list(), max_runs: @max_runs) do
        sketch = Theta.from_enumerable(items, @theta_opts)

        assert Theta.estimate(Theta.merge(sketch, sketch)) == Theta.estimate(sketch)
      end
    end
  end

  describe "KLL merge laws" do
    defp float_list(max_length \\ 20) do
      list_of(float(min: -1_000.0, max: 1_000.0), min_length: 1, max_length: max_length)
    end

    property "associativity" do
      check all(
              a <- float_list(10),
              b <- float_list(10),
              c <- float_list(10),
              max_runs: @max_runs
            ) do
        sa = KLL.from_enumerable(a, @kll_opts)
        sb = KLL.from_enumerable(b, @kll_opts)
        sc = KLL.from_enumerable(c, @kll_opts)

        left = KLL.merge(sa, KLL.merge(sb, sc))
        right = KLL.merge(KLL.merge(sa, sb), sc)

        # With k=200 and at most 30 items, no compaction occurs —
        # quantile answers are exact and must match.
        assert KLL.count(left) == KLL.count(right)
        assert KLL.quantile(left, 0.5) == KLL.quantile(right, 0.5)
      end
    end

    property "commutativity" do
      check all(
              a <- float_list(),
              b <- float_list(),
              max_runs: @max_runs
            ) do
        sa = KLL.from_enumerable(a, @kll_opts)
        sb = KLL.from_enumerable(b, @kll_opts)

        left = KLL.merge(sa, sb)
        right = KLL.merge(sb, sa)

        # With k=200 and at most 40 items, no compaction occurs —
        # quantile answers are exact and must match.
        assert KLL.count(left) == KLL.count(right)
        assert KLL.quantile(left, 0.5) == KLL.quantile(right, 0.5)
      end
    end

    property "identity: merge with empty" do
      check all(items <- float_list(), max_runs: @max_runs) do
        sketch = KLL.from_enumerable(items, @kll_opts)
        empty = KLL.new(@kll_opts)

        merged_right = KLL.merge(sketch, empty)
        merged_left = KLL.merge(empty, sketch)

        assert KLL.count(merged_right) == KLL.count(sketch)
        assert KLL.count(merged_left) == KLL.count(sketch)

        if KLL.count(sketch) > 0 do
          assert_in_delta KLL.quantile(merged_right, 0.5), KLL.quantile(sketch, 0.5), 1.0e-9
          assert_in_delta KLL.quantile(merged_left, 0.5), KLL.quantile(sketch, 0.5), 1.0e-9
        end
      end
    end

    property "count additivity" do
      check all(
              a <- float_list(),
              b <- float_list(),
              max_runs: @max_runs
            ) do
        sa = KLL.from_enumerable(a, @kll_opts)
        sb = KLL.from_enumerable(b, @kll_opts)

        merged = KLL.merge(sa, sb)
        assert KLL.count(merged) == KLL.count(sa) + KLL.count(sb)
      end
    end

    property "min/max preservation" do
      check all(
              a <- float_list(),
              b <- float_list(),
              max_runs: @max_runs
            ) do
        sa = KLL.from_enumerable(a, @kll_opts)
        sb = KLL.from_enumerable(b, @kll_opts)

        merged = KLL.merge(sa, sb)
        assert KLL.min_value(merged) == min(KLL.min_value(sa), KLL.min_value(sb))
        assert KLL.max_value(merged) == max(KLL.max_value(sa), KLL.max_value(sb))
      end
    end
  end

  describe "DDSketch merge laws" do
    defp positive_float_list(max_length \\ 20) do
      list_of(float(min: 0.0, max: 1_000.0), min_length: 1, max_length: max_length)
    end

    property "commutativity" do
      check all(
              a <- positive_float_list(),
              b <- positive_float_list(),
              max_runs: @max_runs
            ) do
        sa = DDSketch.from_enumerable(a, @dds_opts)
        sb = DDSketch.from_enumerable(b, @dds_opts)

        left = DDSketch.merge(sa, sb)
        right = DDSketch.merge(sb, sa)

        assert DDSketch.serialize(left) == DDSketch.serialize(right)
      end
    end

    property "identity: merge with empty" do
      check all(items <- positive_float_list(), max_runs: @max_runs) do
        sketch = DDSketch.from_enumerable(items, @dds_opts)
        empty = DDSketch.new(@dds_opts)

        merged_right = DDSketch.merge(sketch, empty)
        merged_left = DDSketch.merge(empty, sketch)

        assert DDSketch.count(merged_right) == DDSketch.count(sketch)
        assert DDSketch.count(merged_left) == DDSketch.count(sketch)
        assert DDSketch.serialize(merged_right) == DDSketch.serialize(sketch)
        assert DDSketch.serialize(merged_left) == DDSketch.serialize(sketch)
      end
    end

    property "count additivity" do
      check all(
              a <- positive_float_list(),
              b <- positive_float_list(),
              max_runs: @max_runs
            ) do
        sa = DDSketch.from_enumerable(a, @dds_opts)
        sb = DDSketch.from_enumerable(b, @dds_opts)

        merged = DDSketch.merge(sa, sb)
        assert DDSketch.count(merged) == DDSketch.count(sa) + DDSketch.count(sb)
      end
    end

    property "min/max preservation" do
      check all(
              a <- positive_float_list(),
              b <- positive_float_list(),
              max_runs: @max_runs
            ) do
        sa = DDSketch.from_enumerable(a, @dds_opts)
        sb = DDSketch.from_enumerable(b, @dds_opts)

        merged = DDSketch.merge(sa, sb)
        assert DDSketch.min_value(merged) == min(DDSketch.min_value(sa), DDSketch.min_value(sb))
        assert DDSketch.max_value(merged) == max(DDSketch.max_value(sa), DDSketch.max_value(sb))
      end
    end
  end

  describe "FrequentItems merge laws" do
    property "commutativity" do
      check all(
              a <- string_list(),
              b <- string_list(),
              max_runs: @max_runs
            ) do
        sa = FrequentItems.from_enumerable(a, @fi_opts)
        sb = FrequentItems.from_enumerable(b, @fi_opts)

        assert FrequentItems.serialize(FrequentItems.merge(sa, sb)) ==
                 FrequentItems.serialize(FrequentItems.merge(sb, sa))
      end
    end

    property "identity: merge with empty" do
      check all(items <- string_list(), max_runs: @max_runs) do
        sketch = FrequentItems.from_enumerable(items, @fi_opts)
        empty = FrequentItems.new(@fi_opts)

        assert FrequentItems.serialize(FrequentItems.merge(sketch, empty)) ==
                 FrequentItems.serialize(sketch)

        assert FrequentItems.serialize(FrequentItems.merge(empty, sketch)) ==
                 FrequentItems.serialize(sketch)
      end
    end

    property "count conservation" do
      check all(
              a <- string_list(),
              b <- string_list(),
              max_runs: @max_runs
            ) do
        sa = FrequentItems.from_enumerable(a, @fi_opts)
        sb = FrequentItems.from_enumerable(b, @fi_opts)

        merged = FrequentItems.merge(sa, sb)

        assert FrequentItems.count(merged) ==
                 FrequentItems.count(sa) + FrequentItems.count(sb)
      end
    end
  end
end
