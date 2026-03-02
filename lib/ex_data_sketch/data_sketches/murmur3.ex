defmodule ExDataSketch.DataSketches.Murmur3 do
  @moduledoc """
  Minimal MurmurHash3_x64_128 implementation for DataSketches seed hash computation.

  This module implements only the subset of MurmurHash3 needed to compute the
  16-bit seed hash used by Apache DataSketches for compatibility verification.
  The seed hash identifies which hash function/seed was used to create a sketch,
  preventing merges between incompatible sketches.

  ## Seed Hash Computation

  The seed hash is computed as:
  1. Hash the 8-byte little-endian encoding of the seed using MurmurHash3_x64_128
     with hash seed 0.
  2. Take the lower 16 bits of the first 64-bit output word.

  For the default DataSketches seed of 9001, this produces a fixed constant.
  """

  import Bitwise

  @mask64 0xFFFFFFFFFFFFFFFF
  @c1 0x87C37B91114253D5
  @c2 0x4CF5AD432745937F

  @doc """
  Computes the DataSketches seed hash for a given seed value.

  Returns a 16-bit unsigned integer matching the value produced by
  `org.apache.datasketches.common.Util.computeSeedHash(seed)` in Java.
  The computation hashes the seed as a little-endian u64 using MurmurHash3_x64_128
  with hash seed 0, then takes the lower 16 bits of the first output word.

  ## Examples

      iex> h = ExDataSketch.DataSketches.Murmur3.seed_hash(9001)
      iex> is_integer(h) and h >= 0 and h <= 0xFFFF
      true

  """
  @spec seed_hash(non_neg_integer()) :: non_neg_integer()
  def seed_hash(seed) when is_integer(seed) and seed >= 0 do
    {h1, _h2} = murmurhash3_x64_128(<<seed::unsigned-little-64>>, 0)
    h1 &&& 0xFFFF
  end

  # MurmurHash3_x64_128 for an 8-byte input with the given hash seed.
  # This is the minimal implementation needed for seed hash computation.
  @spec murmurhash3_x64_128(binary(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()}
  defp murmurhash3_x64_128(<<k1::unsigned-little-64>>, seed) do
    h1 = seed &&& @mask64
    h2 = seed &&& @mask64
    len = 8

    # No full 16-byte blocks for 8-byte input.
    # Process tail (8 bytes = case 8 in the switch).
    k1 = k1 * @c1 &&& @mask64
    k1 = rotl64(k1, 31)
    k1 = k1 * @c2 &&& @mask64
    h1 = bxor(h1, k1) &&& @mask64

    # Finalization
    h1 = bxor(h1, len) &&& @mask64
    h2 = bxor(h2, len) &&& @mask64

    h1 = h1 + h2 &&& @mask64
    h2 = h2 + h1 &&& @mask64

    h1 = fmix64(h1)
    h2 = fmix64(h2)

    h1 = h1 + h2 &&& @mask64
    h2 = h2 + h1 &&& @mask64

    {h1, h2}
  end

  defp rotl64(val, n) do
    (val <<< n ||| val >>> (64 - n)) &&& @mask64
  end

  defp fmix64(k) do
    k = bxor(k, k >>> 33) * 0xFF51AFD7ED558CCD &&& @mask64
    k = bxor(k, k >>> 33) * 0xC4CEB9FE1A85EC53 &&& @mask64
    bxor(k, k >>> 33) &&& @mask64
  end
end
