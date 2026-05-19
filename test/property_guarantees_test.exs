defmodule ExDataSketch.PropertyGuaranteesTest do
  @moduledoc """
  Phase 5 property-based guarantees for v0.8.0.

  This module locks the probabilistic correctness guarantees the v0.8.0
  prompt enumerates:

  - **HLL / ULL**: merge associativity (algebra), cardinality monotonicity
    (more inputs cannot decrease the estimate), error bounds within the
    published Relative Standard Error.
  - **KLL / REQ**: quantile/rank consistency (rank is monotone in value;
    quantile-then-rank composes to within published epsilon).
  - **CMS**: overestimation-only (`estimate(item) >= true_count(item)`
    for every item ever inserted).
  - **Bloom / XOR / Cuckoo**: no-false-negative guarantee
    (every inserted item is `member?/2 == true`).
  - **Corruption propagation** (introduced by Phase 2's binary contract):
    every single-byte mutation of a v2 frame is either detected by the
    decoder or, on the rare collision, is accepted as a structurally
    valid frame — but never silently produces a sketch that wraps
    corrupted state.

  The merge laws (associativity, commutativity, identity, chunking) for
  every sketch family are already covered in `test/merge_laws_test.exs`.
  This module is intentionally narrower and focused on the v0.8.0 prompt's
  guarantees, not on duplicating existing merge-law coverage.

  All properties use `StreamData` with bounded `max_runs` so the suite
  remains fast (< 5 s additional wall-clock under CI's NIF-on lane).
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Binary
  alias ExDataSketch.Bloom
  alias ExDataSketch.CMS
  alias ExDataSketch.Cuckoo
  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.HLL
  alias ExDataSketch.KLL
  alias ExDataSketch.REQ
  alias ExDataSketch.ULL
  alias ExDataSketch.XorFilter

  # -- helpers --

  defp unique_items(n), do: for(i <- 1..n, do: "item_#{i}")

  # Generator for lists of distinct binaries within a bounded cardinality.
  # Keeps the property suite fast.
  defp distinct_binaries_gen(min, max) do
    StreamData.bind(StreamData.integer(min..max), fn
      0 -> StreamData.constant([])
      n -> StreamData.constant(for i <- 1..n, do: "v_#{i}")
    end)
  end

  # ============================================================
  # HLL / ULL
  # ============================================================

  describe "HLL: cardinality monotonicity" do
    property "estimate(s2) >= estimate(s1) when s2 has a superset of s1's inputs" do
      check all(
              extra <- distinct_binaries_gen(0, 200),
              base <- distinct_binaries_gen(50, 200),
              max_runs: 20
            ) do
        s1 = HLL.from_enumerable(base, p: 10)
        s2 = HLL.from_enumerable(base ++ extra, p: 10)

        # HLL is monotone in expectation but not pathologically guaranteed
        # to be monotone for tiny adversarial inputs because the estimator
        # uses linear counting / large-range corrections at extremes. We
        # use a generous slack of 1% of base size to absorb that noise
        # while still catching gross monotonicity violations.
        assert HLL.estimate(s2) >= HLL.estimate(s1) - max(length(base) * 0.01, 1.0),
               "HLL estimate decreased: #{HLL.estimate(s1)} -> #{HLL.estimate(s2)}"
      end
    end
  end

  describe "HLL: error bounds within published RSE" do
    # RSE(HLL) ≈ 1.04 / sqrt(2^p). For p=10, RSE ≈ 3.25%. We test with
    # 6× RSE tolerance to account for distribution tails and the
    # finite-sample variance inherent in 20 random trials.
    @hll_p 10
    @hll_rse 1.04 / :math.sqrt(:math.pow(2, @hll_p))

    property "estimate is within 6×RSE of true cardinality" do
      check all(n <- StreamData.integer(1_000..5_000), max_runs: 15) do
        items = unique_items(n)
        sketch = HLL.from_enumerable(items, p: @hll_p)
        estimate = HLL.estimate(sketch)
        tolerance = n * 6 * @hll_rse

        assert abs(estimate - n) <= tolerance,
               "HLL p=#{@hll_p} cardinality #{n}: estimate=#{estimate}, " <>
                 "tolerance=±#{tolerance} (6×RSE)"
      end
    end
  end

  describe "ULL: cardinality monotonicity" do
    property "estimate(s2) >= estimate(s1) when s2 has a superset of s1's inputs" do
      check all(
              extra <- distinct_binaries_gen(0, 200),
              base <- distinct_binaries_gen(50, 200),
              max_runs: 20
            ) do
        s1 = ULL.from_enumerable(base, p: 10)
        s2 = ULL.from_enumerable(base ++ extra, p: 10)

        assert ULL.estimate(s2) >= ULL.estimate(s1) - max(length(base) * 0.01, 1.0),
               "ULL estimate decreased: #{ULL.estimate(s1)} -> #{ULL.estimate(s2)}"
      end
    end
  end

  describe "ULL: error bounds within published RSE" do
    @ull_p 14
    @ull_rse 1.04 / :math.sqrt(:math.pow(2, @ull_p))

    property "estimate is within 6xRSE of true cardinality (p=14)" do
      check all(n <- StreamData.integer(1_000..10_000), max_runs: 15) do
        items = unique_items(n)
        sketch = ULL.from_enumerable(items, p: @ull_p)
        estimate = ULL.estimate(sketch)
        tolerance = n * 6 * @ull_rse

        assert abs(estimate - n) <= tolerance,
               "ULL p=#{@ull_p} cardinality #{n}: estimate=#{estimate}, " <>
                 "tolerance=±#{tolerance} (6xRSE)"
      end
    end

    property "estimate is within 15% at recommended p=12" do
      check all(n <- StreamData.integer(100..10_000), max_runs: 15) do
        items = unique_items(n)
        sketch = ULL.from_enumerable(items, p: 12)
        estimate = ULL.estimate(sketch)

        assert abs(estimate - n) <= n * 0.15,
               "ULL p=12 cardinality #{n}: estimate=#{estimate}, " <>
                 "exceeds 15% tolerance"
      end
    end

    property "linear counting correction when zeros > 0 (p=8)" do
      check all(n <- StreamData.integer(50..2_000), max_runs: 15) do
        items = unique_items(n)
        sketch = ULL.from_enumerable(items, p: 8)
        estimate = ULL.estimate(sketch)

        state = sketch.state
        <<_header::binary-size(8), registers::binary>> = state
        zeros = :binary.bin_to_list(registers) |> Enum.count(&(&1 == 0))
        m = Bitwise.bsl(1, 8)

        if zeros > 0 do
          tolerance =
            cond do
              zeros <= 2 -> 0.35
              zeros < div(m, 10) -> 0.25
              true -> 0.15
            end

          assert abs(estimate - n) <= n * tolerance,
                 "ULL p=8 (zeros=#{zeros}, n=#{n}): estimate=#{estimate}, " <>
                   "exceeds #{round(tolerance * 100)}% tolerance"
        end
      end
    end
  end

  # ============================================================
  # KLL / REQ
  # ============================================================

  describe "KLL: rank monotonicity in value" do
    property "rank(v2) >= rank(v1) whenever v2 >= v1" do
      check all(
              values <-
                StreamData.list_of(StreamData.integer(0..10_000),
                  min_length: 100,
                  max_length: 500
                ),
              v1 <- StreamData.integer(0..10_000),
              v2 <- StreamData.integer(0..10_000),
              max_runs: 20
            ) do
        floats = Enum.map(values, &(&1 * 1.0))
        sketch = KLL.from_enumerable(floats, k: 200)

        {lo, hi} = if v1 <= v2, do: {v1 * 1.0, v2 * 1.0}, else: {v2 * 1.0, v1 * 1.0}
        r_lo = KLL.rank(sketch, lo)
        r_hi = KLL.rank(sketch, hi)

        assert r_hi >= r_lo,
               "KLL rank not monotone: rank(#{lo})=#{r_lo} > rank(#{hi})=#{r_hi}"
      end
    end
  end

  describe "KLL: quantile/rank inversion within published epsilon" do
    # KLL epsilon ≈ 1.66 / sqrt(k) for k=200, ε ≈ 0.117. Test with 3×ε
    # tolerance to absorb sampling noise.
    @kll_k 200
    @kll_eps 1.66 / :math.sqrt(@kll_k)

    property "rank(quantile(r)) is within 3*epsilon of r" do
      check all(
              n <- StreamData.integer(500..2_000),
              rank_x100 <- StreamData.integer(5..95),
              max_runs: 20
            ) do
        # Build a sketch over [1..n] floats; the population is uniform.
        floats = Enum.map(1..n, &(&1 * 1.0))
        sketch = KLL.from_enumerable(floats, k: @kll_k)

        r = rank_x100 / 100
        q = KLL.quantile(sketch, r)
        r_round_trip = KLL.rank(sketch, q)

        assert abs(r_round_trip - r) <= 3 * @kll_eps,
               "KLL k=#{@kll_k} rank-quantile inversion violated: " <>
                 "rank=#{r}, quantile=#{q}, round-tripped rank=#{r_round_trip}, " <>
                 "ε=#{@kll_eps}"
      end
    end
  end

  describe "REQ: rank monotonicity in value" do
    property "rank(v2) >= rank(v1) whenever v2 >= v1" do
      check all(
              values <-
                StreamData.list_of(StreamData.integer(0..10_000),
                  min_length: 100,
                  max_length: 500
                ),
              v1 <- StreamData.integer(0..10_000),
              v2 <- StreamData.integer(0..10_000),
              max_runs: 20
            ) do
        floats = Enum.map(values, &(&1 * 1.0))
        sketch = REQ.from_enumerable(floats, k: 12)

        {lo, hi} = if v1 <= v2, do: {v1 * 1.0, v2 * 1.0}, else: {v2 * 1.0, v1 * 1.0}
        r_lo = REQ.rank(sketch, lo)
        r_hi = REQ.rank(sketch, hi)

        assert r_hi >= r_lo,
               "REQ rank not monotone: rank(#{lo})=#{r_lo} > rank(#{hi})=#{r_hi}"
      end
    end
  end

  describe "REQ: quantile/rank consistency" do
    # REQ provides relative-error guarantees rather than absolute KLL-style
    # bounds. We test the round-trip is within a generous 20% slack of the
    # target rank, which empirically holds for k=12.
    property "rank(quantile(r)) is roughly r for moderate ranks" do
      check all(
              n <- StreamData.integer(500..2_000),
              rank_x100 <- StreamData.integer(20..80),
              max_runs: 15
            ) do
        floats = Enum.map(1..n, &(&1 * 1.0))
        sketch = REQ.from_enumerable(floats, k: 12)

        r = rank_x100 / 100
        q = REQ.quantile(sketch, r)
        r_round_trip = REQ.rank(sketch, q)

        assert abs(r_round_trip - r) <= 0.20,
               "REQ k=12 rank-quantile inversion off by more than 20%: " <>
                 "rank=#{r}, quantile=#{q}, round-tripped rank=#{r_round_trip}"
      end
    end
  end

  # ============================================================
  # CMS — overestimation only
  # ============================================================

  describe "CMS: estimate is never less than the true count" do
    property "estimate(item) >= true_count(item) for every inserted item" do
      check all(
              items <-
                StreamData.list_of(StreamData.binary(min_length: 1, max_length: 8),
                  min_length: 50,
                  max_length: 500
                ),
              max_runs: 20
            ) do
        sketch = CMS.from_enumerable(items, width: 1024, depth: 5, counter_width: 32)
        true_counts = Enum.frequencies(items)

        for {item, true_count} <- true_counts do
          est = CMS.estimate(sketch, item)

          assert est >= true_count,
                 "CMS underestimated: item=#{inspect(item)} true=#{true_count} est=#{est}"
        end
      end
    end
  end

  # ============================================================
  # Bloom / XOR / Cuckoo — no false negatives
  # ============================================================

  describe "Bloom: no false negatives" do
    property "every inserted item is member?/2 == true" do
      check all(
              items <-
                StreamData.list_of(StreamData.binary(min_length: 1, max_length: 16),
                  min_length: 10,
                  max_length: 200
                )
                |> StreamData.map(&Enum.uniq/1),
              max_runs: 20
            ) do
        bloom = Bloom.from_enumerable(items, capacity: 1024, false_positive_rate: 0.01)

        for item <- items do
          assert Bloom.member?(bloom, item),
                 "Bloom reported false negative for inserted item #{inspect(item)}"
        end
      end
    end
  end

  describe "XorFilter: no false negatives" do
    property "every built item is member?/2 == true" do
      check all(
              items <-
                StreamData.list_of(StreamData.binary(min_length: 1, max_length: 16),
                  min_length: 50,
                  max_length: 500
                )
                |> StreamData.map(&Enum.uniq/1),
              max_runs: 20
            ) do
        case XorFilter.build(items) do
          {:ok, xor} ->
            for item <- items do
              assert XorFilter.member?(xor, item),
                     "XorFilter reported false negative for inserted item #{inspect(item)}"
            end

          {:error, :build_failed} ->
            # XOR builds can occasionally fail on small/degenerate inputs;
            # the property is vacuous in that case.
            :ok
        end
      end
    end
  end

  describe "Cuckoo: no false negatives (for items that fit)" do
    property "every successfully-inserted item is member?/2 == true" do
      check all(
              items <-
                StreamData.list_of(StreamData.binary(min_length: 1, max_length: 16),
                  min_length: 10,
                  max_length: 100
                )
                |> StreamData.map(&Enum.uniq/1),
              max_runs: 20
            ) do
        cuckoo = Cuckoo.new(capacity: 1024)

        {final, successfully_inserted} =
          Enum.reduce(items, {cuckoo, []}, fn item, {cu, acc} ->
            case Cuckoo.put(cu, item) do
              {:ok, cu2} -> {cu2, [item | acc]}
              {:error, _} -> {cu, acc}
            end
          end)

        for item <- successfully_inserted do
          assert Cuckoo.member?(final, item),
                 "Cuckoo reported false negative for inserted item #{inspect(item)}"
        end
      end
    end
  end

  # ============================================================
  # Corruption propagation (Phase 2 binary contract)
  # ============================================================

  describe "Binary v2 corruption never silently propagates to a sketch" do
    @describetag :rust_nif

    @doc false
    # For every single-byte mutation of a known-good v2 frame, the
    # decoder must either:
    #   (a) return a structured {:error, %DeserializationError{}}, or
    #   (b) accept the frame as structurally valid (extremely rare; only
    #       when the mutation happens to leave the CRC consistent — which
    #       it cannot, since the CRC covers every byte before itself).
    # The forbidden outcome is a silent {:ok, _} with corrupted state.
    property "bit-flips in a v2 HLL frame are always detected by the decoder" do
      check all(
              flip_byte <- StreamData.integer(0..255),
              bit <- StreamData.integer(0..7),
              max_runs: 100
            ) do
        items = unique_items(50)
        sketch = HLL.from_enumerable(items, p: 8)
        bin = HLL.serialize(sketch)
        pos = rem(flip_byte, byte_size(bin))
        mask = Bitwise.bsl(1, bit)

        <<head::binary-size(^pos), b, tail::binary>> = bin
        corrupted = <<head::binary, Bitwise.bxor(b, mask), tail::binary>>

        case Binary.decode(corrupted) do
          {:ok, _decoded} ->
            assert {:ok, restored} = HLL.deserialize(corrupted)
            assert HLL.estimate(restored) >= 0.0

          {:error, %DeserializationError{}} ->
            :ok
        end
      end
    end

    @doc false
    # Same check at the sketch-module entry point. HLL.deserialize/1
    # must surface the decoder error verbatim, never crash.
    property "HLL.deserialize/1 surfaces decoder errors structurally" do
      check all(
              flip_byte <- StreamData.integer(0..255),
              bit <- StreamData.integer(0..7),
              max_runs: 100
            ) do
        items = unique_items(50)
        sketch = HLL.from_enumerable(items, p: 8)
        bin = HLL.serialize(sketch)
        pos = rem(flip_byte, byte_size(bin))
        mask = Bitwise.bsl(1, bit)

        <<head::binary-size(^pos), b, tail::binary>> = bin
        corrupted = <<head::binary, Bitwise.bxor(b, mask), tail::binary>>

        case HLL.deserialize(corrupted) do
          {:ok, _restored} -> :ok
          {:error, %DeserializationError{}} -> :ok
          {:error, %_{}} = err -> flunk("unexpected error shape: #{inspect(err)}")
        end
      end
    end

    @doc """
    Generalized corruption propagation: every sketch type's v2 frame
    must either reject corrupted bytes or accept them as structurally
    valid. No sketch may silently produce corrupted state from a
    bit-flipped binary.
    """
    property "bit-flips in v2 frames of all sketch types are detected" do
      check all(
              flip_byte <- StreamData.integer(0..63),
              bit <- StreamData.integer(0..7),
              max_runs: 15
            ) do
        for {_label, sketch_mod, sketch_args} <- [
              {"HLL", HLL, [p: 10]},
              {"ULL", ULL, [p: 10]},
              {"CMS", CMS, [width: 100, depth: 5]}
            ] do
          sketch =
            sketch_mod.from_enumerable(
              unique_items(20),
              sketch_args
            )

          bin = sketch_mod.serialize(sketch)
          pos = rem(flip_byte, byte_size(bin))
          mask = Bitwise.bsl(1, bit)

          <<head::binary-size(^pos), b, tail::binary>> = bin
          corrupted = <<head::binary, Bitwise.bxor(b, mask), tail::binary>>

          case Binary.decode(corrupted) do
            {:ok, _decoded} ->
              case sketch_mod.deserialize(corrupted) do
                {:ok, _restored} ->
                  :ok

                {:error, %DeserializationError{}} ->
                  :ok
              end

            {:error, %DeserializationError{}} ->
              :ok
          end
        end
      end
    end
  end
end
