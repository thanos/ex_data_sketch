defmodule ExDataSketch.Hash do
  @moduledoc """
  Stable 64-bit hash interface for ExDataSketch.

  All sketch algorithms require a deterministic hash function that maps
  arbitrary Elixir terms to 64-bit unsigned integers. This module provides
  that interface with automatic backend selection and a pure-Elixir fallback.

  ## Hash Properties

  - Output range: 0..2^64-1 (unsigned 64-bit integer).
  - Deterministic: same input always produces same output within the same
    runtime configuration.
  - Uniform distribution: output bits are well-distributed for sketch accuracy.

  ## Auto-detection

  When no custom `:hash_fn` is provided, `hash64/2` automatically selects the
  best available hash implementation:

  - **XXHash3 (NIF)**: When the Rust NIF is loaded, `hash64/2` uses XXHash3
    which produces native 64-bit hashes with zero Elixir-side overhead. XXHash3
    output is stable across platforms.

  - **phash2 + mix64 (pure)**: When the NIF is not available, `hash64/2` falls
    back to `:erlang.phash2/2` with a fixnum-safe 64-bit mixer. The mixer uses
    16-bit partial products to avoid bigint heap allocations while preserving
    full 64-bit output quality.

  The NIF availability check is performed once and cached in
  `:persistent_term` for zero-cost subsequent lookups.

  ## Pluggable Hash

  Pass `hash_fn: fn term -> non_neg_integer end` to override the default.
  The custom function must return values in 0..2^64-1.

  ## Stability

  `:erlang.phash2/2` output is not guaranteed stable across OTP major versions.
  XXHash3 output is stable across platforms. For cross-version stability, use
  the NIF build (XXHash3) or supply a custom `:hash_fn`.
  """

  import Bitwise

  @type hash64 :: non_neg_integer()
  @type hash_opt :: {:seed, non_neg_integer()} | {:hash_fn, (term() -> hash64())}
  @type opts :: [hash_opt()]

  @mask16 0xFFFF
  @mask32 0xFFFFFFFF
  @mask64 0xFFFFFFFFFFFFFFFF
  @max_u64 0xFFFFFFFFFFFFFFFF

  # Mixing constants as {hi, lo} 32-bit pairs for fixnum-safe multiplication
  @mix_c1_pair {0xBF58476D, 0x1CE4E5B9}
  @mix_c2_pair {0x94D049BB, 0x133111EB}

  # Golden ratio constant for initial combine step
  @golden_ratio 0x9E3779B9

  # Persistent term key for NIF availability cache
  @nif_key {__MODULE__, :nif_available}

  @doc """
  Returns whether the NIF is available for hashing.

  The result is computed once and cached in `:persistent_term`.
  """
  @spec nif_available?() :: boolean()
  def nif_available? do
    case :persistent_term.get(@nif_key, :unset) do
      :unset ->
        result = nif_loaded?()
        :persistent_term.put(@nif_key, result)
        result

      val ->
        val
    end
  end

  @doc """
  Returns the default hash strategy based on NIF availability.

  Returns `:xxhash3` when the NIF is loaded, `:phash2` otherwise.
  """
  @spec default_hash_strategy() :: :xxhash3 | :phash2
  def default_hash_strategy do
    if nif_available?(), do: :xxhash3, else: :phash2
  end

  @doc """
  Hashes an arbitrary Elixir term to a 64-bit unsigned integer.

  When no `:hash_fn` is provided, automatically uses XXHash3 via NIF if
  available, otherwise falls back to phash2 with fixnum-safe bit mixing.

  ## Options

  - `:seed` - seed value for the hash (default: 0). Combined with the base hash.
  - `:hash_fn` - custom hash function `(term -> 0..2^64-1)`. When provided,
    `:seed` is ignored and the function is called directly.

  ## Examples

      iex> h = ExDataSketch.Hash.hash64("hello")
      iex> is_integer(h) and h >= 0
      true

      iex> ExDataSketch.Hash.hash64("hello") == ExDataSketch.Hash.hash64("hello")
      true

      iex> ExDataSketch.Hash.hash64("hello") != ExDataSketch.Hash.hash64("world")
      true

      iex> ExDataSketch.Hash.hash64("test", seed: 42) != ExDataSketch.Hash.hash64("test", seed: 0)
      true

  """
  @spec hash64(term(), opts()) :: hash64()
  def hash64(term, opts \\ []) do
    case Keyword.get(opts, :hash_fn) do
      nil ->
        seed = Keyword.get(opts, :seed, 0)
        strategy = Keyword.get(opts, :hash_strategy)
        hash64_default(term, seed, strategy)

      hash_fn when is_function(hash_fn, 1) ->
        hash_fn.(term)
    end
  end

  defp hash64_default(term, seed, :phash2) do
    mix64(:erlang.phash2(term, 1 <<< 32), seed &&& @mask64)
  end

  defp hash64_default(term, seed, :xxhash3) do
    if nif_available?() do
      bin = if is_binary(term), do: term, else: :erlang.term_to_binary(term)
      ExDataSketch.Nif.xxhash3_64_seeded_nif(bin, seed &&& @mask64)
    else
      raise ArgumentError,
        "hash_strategy :xxhash3 requires the Rust NIF but it is not available"
    end
  end

  defp hash64_default(term, seed, _auto) do
    if nif_available?() do
      bin = if is_binary(term), do: term, else: :erlang.term_to_binary(term)
      ExDataSketch.Nif.xxhash3_64_seeded_nif(bin, seed &&& @mask64)
    else
      mix64(:erlang.phash2(term, 1 <<< 32), seed &&& @mask64)
    end
  end

  @doc """
  Hashes a raw binary to a 64-bit unsigned integer.

  Operates directly on binary bytes without term encoding overhead.
  Useful when the input is already binary data (e.g., from external sources).

  When no `:hash_fn` is provided, automatically uses XXHash3 via NIF if
  available, otherwise falls back to phash2 with fixnum-safe bit mixing.

  ## Options

  Same as `hash64/2`.

  ## Examples

      iex> h = ExDataSketch.Hash.hash64_binary(<<1, 2, 3>>)
      iex> is_integer(h) and h >= 0
      true

      iex> ExDataSketch.Hash.hash64_binary(<<"abc">>) == ExDataSketch.Hash.hash64_binary(<<"abc">>)
      true

  """
  @spec hash64_binary(binary(), opts()) :: hash64()
  def hash64_binary(binary, opts \\ []) when is_binary(binary) do
    case Keyword.get(opts, :hash_fn) do
      nil ->
        seed = Keyword.get(opts, :seed, 0)
        strategy = Keyword.get(opts, :hash_strategy)
        hash64_binary_default(binary, seed, strategy)

      hash_fn when is_function(hash_fn, 1) ->
        hash_fn.(binary)
    end
  end

  defp hash64_binary_default(binary, seed, :phash2) do
    mix64(:erlang.phash2(binary, 1 <<< 32), seed &&& @mask64)
  end

  defp hash64_binary_default(binary, seed, :xxhash3) do
    if nif_available?() do
      ExDataSketch.Nif.xxhash3_64_seeded_nif(binary, seed &&& @mask64)
    else
      raise ArgumentError,
        "hash_strategy :xxhash3 requires the Rust NIF but it is not available"
    end
  end

  defp hash64_binary_default(binary, seed, _auto) do
    if nif_available?() do
      ExDataSketch.Nif.xxhash3_64_seeded_nif(binary, seed &&& @mask64)
    else
      mix64(:erlang.phash2(binary, 1 <<< 32), seed &&& @mask64)
    end
  end

  @doc """
  Hashes a binary using XXHash3 (64-bit) via Rust NIF.

  Returns a deterministic 64-bit hash that is stable across platforms and
  versions when the Rust NIF is available. Falls back to the phash2-based
  hash if the NIF is not loaded; the fallback is NOT stable across OTP
  major versions (see module docs).

  This function operates on raw binary data. For Elixir terms, convert to
  binary first (e.g., using `:erlang.term_to_binary/1` or `to_string/1`).

  ## Examples

      iex> h = ExDataSketch.Hash.xxhash3_64("hello")
      iex> is_integer(h) and h >= 0
      true

      iex> ExDataSketch.Hash.xxhash3_64("hello") == ExDataSketch.Hash.xxhash3_64("hello")
      true

  """
  @spec xxhash3_64(binary()) :: hash64()
  def xxhash3_64(data) when is_binary(data) do
    xxhash3_64(data, 0)
  end

  @doc """
  Hashes a binary using XXHash3 (64-bit) with a seed via Rust NIF.

  Falls back to the phash2-based hash if the NIF is not available.

  ## Examples

      iex> h = ExDataSketch.Hash.xxhash3_64("hello", 42)
      iex> is_integer(h) and h >= 0
      true

      iex> ExDataSketch.Hash.xxhash3_64("hello", 0) != ExDataSketch.Hash.xxhash3_64("hello", 42)
      true

  """
  @spec xxhash3_64(binary(), non_neg_integer()) :: hash64()
  def xxhash3_64(data, seed) when is_binary(data) and is_integer(seed) and seed >= 0 do
    clamped = seed &&& @max_u64
    ExDataSketch.Nif.xxhash3_64_seeded_nif(data, clamped)
  rescue
    ErlangError ->
      # Fallback to phash2-based hash when NIF is not loaded
      mix64(:erlang.phash2(data, 1 <<< 32), seed &&& @max_u64)
  end

  # -- Fixnum-safe mix64 --
  #
  # Extends a 32-bit base hash to 64 bits using Murmur3-style finalization.
  # All intermediate arithmetic uses {hi32, lo32} pair representation with
  # 16-bit partial products, keeping every value under 35 bits (well within
  # BEAM's 60-bit fixnum limit). The only bigint allocation is the final
  # hi <<< 32 ||| lo return value.
  @spec mix64(non_neg_integer(), non_neg_integer()) :: hash64()
  defp mix64(base32, seed) do
    seed_hi = seed >>> 32 &&& @mask32
    seed_lo = seed &&& @mask32

    # Full 64-bit product: base32 * golden_ratio
    {prod_hi, prod_lo} = mul32_full(base32, @golden_ratio)

    # Add seed to the product (with carry propagation)
    raw_lo = prod_lo + seed_lo
    b_lo = raw_lo &&& @mask32
    carry = raw_lo >>> 32
    b_hi = prod_hi + seed_hi + carry &&& @mask32

    # combined = (bxor(base32, seed) <<< 32 ||| (base32 * golden + seed))
    # The <<< 32 puts bxor(base32, seed_lo) into the high word (seed_hi drops
    # out after masking to 64 bits).
    a_hi = bxor(base32, seed_lo)
    combined = {a_hi ||| b_hi, b_lo}

    # Murmur3 64-bit finalization
    v = combined |> xor_rshift(30) |> mul64_pair(@mix_c1_pair)
    v = v |> xor_rshift(27) |> mul64_pair(@mix_c2_pair)
    v = xor_rshift(v, 31)

    from_pair(v)
  end

  # Reassemble {hi32, lo32} to a single 64-bit integer.
  defp from_pair({hi, lo}), do: hi <<< 32 ||| lo

  # XOR a {hi, lo} pair with itself right-shifted by n bits.
  defp xor_rshift({hi, lo}, n) when n < 32 do
    shifted_hi = hi >>> n
    shifted_lo = (hi <<< (32 - n) &&& @mask32) ||| lo >>> n
    {bxor(hi, shifted_hi), bxor(lo, shifted_lo)}
  end

  # Full 64-bit product of two 32-bit values using 16-bit schoolbook multiply.
  # Returns {hi32, lo32}. All intermediates stay under 35 bits (fixnum).
  defp mul32_full(a, b) do
    a1 = a >>> 16
    a0 = a &&& @mask16
    b1 = b >>> 16
    b0 = b &&& @mask16

    col0 = a0 * b0
    r0 = col0 &&& @mask16
    carry0 = col0 >>> 16

    col1 = a1 * b0 + a0 * b1 + carry0
    r1 = col1 &&& @mask16
    carry1 = col1 >>> 16

    col2 = a1 * b1 + carry1
    r2 = col2 &&& @mask16
    r3 = col2 >>> 16

    {r2 ||| r3 <<< 16, r0 ||| r1 <<< 16}
  end

  # 64x64 multiply mod 2^64 using 16-bit schoolbook partial products.
  # Each 16x16 product is at most ~30 bits; column sums reach at most ~35 bits.
  # All intermediates are fixnums on 64-bit BEAM.
  defp mul64_pair({a_hi, a_lo}, {b_hi, b_lo}) do
    a3 = a_hi >>> 16
    a2 = a_hi &&& @mask16
    a1 = a_lo >>> 16
    a0 = a_lo &&& @mask16

    b3 = b_hi >>> 16
    b2 = b_hi &&& @mask16
    b1 = b_lo >>> 16
    b0 = b_lo &&& @mask16

    # Column 0 (bits 0-15)
    col0 = a0 * b0
    r0 = col0 &&& @mask16
    carry0 = col0 >>> 16

    # Column 1 (bits 16-31)
    col1 = a1 * b0 + a0 * b1 + carry0
    r1 = col1 &&& @mask16
    carry1 = col1 >>> 16

    # Column 2 (bits 32-47)
    col2 = a2 * b0 + a1 * b1 + a0 * b2 + carry1
    r2 = col2 &&& @mask16
    carry2 = col2 >>> 16

    # Column 3 (bits 48-63)
    col3 = a3 * b0 + a2 * b1 + a1 * b2 + a0 * b3 + carry2
    r3 = col3 &&& @mask16

    {r2 ||| r3 <<< 16, r0 ||| r1 <<< 16}
  end

  @doc """
  Validates that two sets of sketch options have compatible hashing configuration.

  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if:
  - Either sketch uses a custom `:hash_fn` (closures cannot be compared)
  - Hash strategies differ (e.g. `:xxhash3` vs `:phash2`)
  - Seeds differ (default is 0)
  """
  @spec validate_merge_hash_compat!(Keyword.t(), Keyword.t(), String.t()) :: :ok
  def validate_merge_hash_compat!(opts_a, opts_b, sketch_type) do
    strategy_a = Keyword.get(opts_a, :hash_strategy)
    strategy_b = Keyword.get(opts_b, :hash_strategy)

    if strategy_a == :custom or strategy_b == :custom do
      raise ExDataSketch.Errors.IncompatibleSketchesError,
        reason:
          "#{sketch_type} merge is not supported with custom :hash_fn (cannot verify hash compatibility)"
    end

    if strategy_a != strategy_b do
      raise ExDataSketch.Errors.IncompatibleSketchesError,
        reason: "#{sketch_type} hash strategy mismatch: #{strategy_a} vs #{strategy_b}"
    end

    seed_a = Keyword.get(opts_a, :seed, 0)
    seed_b = Keyword.get(opts_b, :seed, 0)

    if seed_a != seed_b do
      raise ExDataSketch.Errors.IncompatibleSketchesError,
        reason: "#{sketch_type} seed mismatch: #{seed_a} vs #{seed_b}"
    end

    :ok
  end

  defp nif_loaded? do
    Code.ensure_loaded?(ExDataSketch.Nif) and ExDataSketch.Nif.nif_loaded() == :ok
  rescue
    _ -> false
  end
end
