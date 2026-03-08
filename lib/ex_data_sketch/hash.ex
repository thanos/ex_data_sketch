defmodule ExDataSketch.Hash do
  @moduledoc """
  Stable 64-bit hash interface for ExDataSketch.

  All sketch algorithms require a deterministic hash function that maps
  arbitrary Elixir terms to 64-bit unsigned integers. This module provides
  that interface with a pure-Elixir default implementation.

  ## Hash Properties

  - Output range: 0..2^64-1 (unsigned 64-bit integer).
  - Deterministic: same input always produces same output within the same
    BEAM version.
  - Uniform distribution: output bits are well-distributed for sketch accuracy.

  ## Default Implementation

  The default pure-Elixir hash uses `:erlang.phash2/2` as a base, then applies
  additional bit mixing to extend to 64 bits. This approach is fast and
  deterministic within a BEAM instance.

  Note: `:erlang.phash2/2` is not guaranteed stable across OTP major versions.
  For cross-version stability, a custom hash function can be supplied via the
  `:hash_fn` option.

  ## XXHash3

  When the Rust NIF is available, `xxhash3_64/1` and `xxhash3_64/2` provide
  XXHash3 hashing which is faster and has better distribution than the
  phash2-based default. XXHash3 is opt-in for backwards compatibility with
  existing serialized data:

      HLL.new(p: 14, hash_fn: &ExDataSketch.Hash.xxhash3_64/1)

  ## Pluggable Hash

  Pass `hash_fn: fn term -> non_neg_integer end` to override the default.
  The custom function must return values in 0..2^64-1.
  """

  import Bitwise

  @type hash64 :: non_neg_integer()
  @type hash_opt :: {:seed, non_neg_integer()} | {:hash_fn, (term() -> hash64())}
  @type opts :: [hash_opt()]

  # Mixing constants (Murmur-style finalization)
  @mix_c1 0xBF58476D1CE4E5B9
  @mix_c2 0x94D049BB133111EB
  @mask64 0xFFFFFFFFFFFFFFFF

  @doc """
  Hashes an arbitrary Elixir term to a 64-bit unsigned integer.

  Uses `:erlang.phash2/2` as the base hash, then mixes the bits to produce
  a full 64-bit output.

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
        base = :erlang.phash2(term, 1 <<< 32)
        mix64(base, seed)

      hash_fn when is_function(hash_fn, 1) ->
        hash_fn.(term)
    end
  end

  @doc """
  Hashes a raw binary to a 64-bit unsigned integer.

  Operates directly on binary bytes without term encoding overhead.
  Useful when the input is already binary data (e.g., from external sources).

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
        base = :erlang.phash2(binary, 1 <<< 32)
        mix64(base, seed)

      hash_fn when is_function(hash_fn, 1) ->
        hash_fn.(binary)
    end
  end

  @doc """
  Hashes a binary using XXHash3 (64-bit) via Rust NIF.

  Returns a deterministic 64-bit hash that is stable across platforms and
  versions. Falls back to the phash2-based hash if the NIF is not available.

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
  def xxhash3_64(data, seed) when is_binary(data) and is_integer(seed) do
    ExDataSketch.Nif.xxhash3_64_seeded_nif(data, seed)
  rescue
    _ ->
      # Fallback to phash2-based hash
      mix64(:erlang.phash2(data, 1 <<< 32), seed)
  end

  # Extends a 32-bit base hash to 64 bits using bit mixing.
  # Combines the base hash with a seed, then applies Murmur3-style finalization.
  @spec mix64(non_neg_integer(), non_neg_integer()) :: hash64()
  defp mix64(base32, seed) do
    # Combine base with seed to form a 64-bit starting value
    combined =
      (bxor(base32, seed) <<< 32 ||| base32 * 0x9E3779B9 + seed) &&& @mask64

    # Murmur3 64-bit finalization mix
    v = bxor(combined, combined >>> 30) * @mix_c1 &&& @mask64
    v = bxor(v, v >>> 27) * @mix_c2 &&& @mask64
    bxor(v, v >>> 31) &&& @mask64
  end
end
