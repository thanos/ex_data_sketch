defmodule ExDataSketch.Hash.Murmur3 do
  @moduledoc """
  MurmurHash3_x64_128 (64-bit output).

  This module implements the full MurmurHash3_x64_128 algorithm and returns
  the high 64 bits of the 128-bit output. This matches the convention used
  by Apache DataSketches when a 64-bit hash is needed.

  Murmur3 is **not the default** for ExDataSketch (XXHash3 is faster and the
  default), but it is provided for interoperability with the Apache DataSketches
  ecosystem and as a portable BEAM-side reference implementation.

  ## Properties

  | Property            | Value                                          |
  |---------------------|------------------------------------------------|
  | Output bits         | 64 (high half of x64_128)                      |
  | Seedable            | Yes (`u32`)                                    |
  | Cross-platform      | Yes                                            |
  | Cross-OTP stable    | Yes — pure Elixir, no `:erlang.phash2`         |
  | Rust acceleration   | Yes (internal NIF, see `hash/2`)               |

  When the Rust NIF is available, `hash/2` dispatches to it. When it is not,
  the pure Elixir implementation is used. Both implementations are
  **byte-identical** on every input (verified by parity tests).

  ## Examples

      iex> ExDataSketch.Hash.Murmur3.hash("hello") == ExDataSketch.Hash.Murmur3.hash("hello")
      true

      iex> ExDataSketch.Hash.Murmur3.hash("hello", 0) != ExDataSketch.Hash.Murmur3.hash("hello", 1)
      true

      iex> ExDataSketch.Hash.Murmur3.hash("", 0) >= 0
      true

  ## Reference

  - Austin Appleby's reference implementation: <https://github.com/aappleby/smhasher>
  - Apache DataSketches Java: `org.apache.datasketches.hash.MurmurHash3`
  """

  import Bitwise

  alias ExDataSketch.Hash
  alias ExDataSketch.Nif

  @mask64 0xFFFFFFFFFFFFFFFF
  @mask32 0xFFFFFFFF

  @c1 0x87C37B91114253D5
  @c2 0x4CF5AD432745937F

  @doc """
  Returns the algorithm identifier `:murmur3`.

  ## Examples

      iex> ExDataSketch.Hash.Murmur3.id()
      :murmur3

  """
  @spec id() :: :murmur3
  def id, do: :murmur3

  @doc """
  Computes the high 64 bits of MurmurHash3_x64_128 over the given binary.

  The seed is the standard Murmur3 `u32` seed; values above `2^32 - 1`
  are masked to 32 bits.

  ## Examples

      iex> h = ExDataSketch.Hash.Murmur3.hash("hello", 0)
      iex> is_integer(h) and h >= 0 and h <= 0xFFFFFFFFFFFFFFFF
      true

  """
  @spec hash(binary(), non_neg_integer()) :: Hash.hash64()
  def hash(data, seed \\ 0) when is_binary(data) and is_integer(seed) and seed >= 0 do
    if Hash.nif_available?() do
      try do
        Nif.murmur3_x64_128_nif(data, seed &&& @mask32)
      rescue
        # Older NIF builds may not have the Murmur3 NIF compiled in yet.
        # Falling back to pure preserves correctness; the parity tests verify
        # the two implementations agree byte-for-byte when both are present.
        UndefinedFunctionError -> pure_hash(data, seed &&& @mask32)
        ErlangError -> pure_hash(data, seed &&& @mask32)
      end
    else
      pure_hash(data, seed &&& @mask32)
    end
  end

  @doc """
  Pure Elixir implementation of MurmurHash3_x64_128 returning the high 64 bits.

  This function never calls the NIF. It is used by the parity tests and as
  the fallback when the NIF is unavailable. Keep this function deterministic
  across all OTP versions — it must not depend on `:erlang.phash2/1,2`.

  ## Examples

      iex> ExDataSketch.Hash.Murmur3.pure_hash("hello", 0) == ExDataSketch.Hash.Murmur3.hash("hello", 0)
      true

  """
  @spec pure_hash(binary(), non_neg_integer()) :: Hash.hash64()
  def pure_hash(data, seed) when is_binary(data) and is_integer(seed) and seed >= 0 do
    {h1, _h2} = murmur3_x64_128(data, seed &&& @mask32)
    h1
  end

  @doc """
  Returns the full 128-bit MurmurHash3_x64_128 as a `{h1, h2}` pair.

  Provided for interop scenarios where the full 128-bit hash is required
  (e.g., computing Apache DataSketches seed_hash, or other Murmur3-based
  fingerprints).

  ## Examples

      iex> {h1, h2} = ExDataSketch.Hash.Murmur3.hash128("hello", 0)
      iex> is_integer(h1) and is_integer(h2)
      true

  """
  @spec hash128(binary(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()}
  def hash128(data, seed \\ 0) when is_binary(data) and is_integer(seed) and seed >= 0 do
    murmur3_x64_128(data, seed &&& @mask32)
  end

  @doc """
  Returns whether this algorithm is available in the current runtime.

  Murmur3 is always available because a pure-Elixir implementation is bundled.

  ## Examples

      iex> ExDataSketch.Hash.Murmur3.available?()
      true

  """
  @spec available?() :: true
  def available?, do: true

  # -- core algorithm --

  @spec murmur3_x64_128(binary(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()}
  defp murmur3_x64_128(data, seed) do
    len = byte_size(data)
    nblocks = div(len, 16)
    body_size = nblocks * 16
    <<body::binary-size(^body_size), tail::binary>> = data

    {h1, h2} = process_blocks(body, seed, seed)

    {h1, h2} = process_tail(tail, h1, h2)

    h1 = bxor(h1, len) &&& @mask64
    h2 = bxor(h2, len) &&& @mask64

    h1 = add64(h1, h2)
    h2 = add64(h2, h1)

    h1 = fmix64(h1)
    h2 = fmix64(h2)

    h1 = add64(h1, h2)
    h2 = add64(h2, h1)

    {h1, h2}
  end

  @spec process_blocks(binary(), non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()}
  defp process_blocks(<<>>, h1, h2), do: {h1, h2}

  defp process_blocks(
         <<k1::unsigned-little-64, k2::unsigned-little-64, rest::binary>>,
         h1,
         h2
       ) do
    k1 = mul64(k1, @c1)
    k1 = rotl64(k1, 31)
    k1 = mul64(k1, @c2)
    h1 = bxor(h1, k1) &&& @mask64

    h1 = rotl64(h1, 27)
    h1 = add64(h1, h2)
    h1 = add64(mul64(h1, 5), 0x52DCE729)

    k2 = mul64(k2, @c2)
    k2 = rotl64(k2, 33)
    k2 = mul64(k2, @c1)
    h2 = bxor(h2, k2) &&& @mask64

    h2 = rotl64(h2, 31)
    h2 = add64(h2, h1)
    h2 = add64(mul64(h2, 5), 0x38495AB5)

    process_blocks(rest, h1, h2)
  end

  # Tail processing: 1..15 remaining bytes. Mirrors the canonical reference
  # implementation's switch/fall-through. k1 is always mixed when any tail is
  # present; k2 is mixed only when there are 9..15 tail bytes.
  @spec process_tail(binary(), non_neg_integer(), non_neg_integer()) ::
          {non_neg_integer(), non_neg_integer()}
  defp process_tail(<<>>, h1, h2), do: {h1, h2}

  defp process_tail(tail, h1, h2) do
    tail_size = byte_size(tail)
    {k1, k2} = tail_keys(tail, tail_size)

    h2 =
      if tail_size > 8 do
        k2m = mul64(k2, @c2)
        k2m = rotl64(k2m, 33)
        k2m = mul64(k2m, @c1)
        bxor(h2, k2m) &&& @mask64
      else
        h2
      end

    k1m = mul64(k1, @c1)
    k1m = rotl64(k1m, 31)
    k1m = mul64(k1m, @c2)
    h1 = bxor(h1, k1m) &&& @mask64

    {h1, h2}
  end

  # Extract (k1, k2) little-endian from a tail of 1..15 bytes.
  defp tail_keys(tail, n) when n >= 9 and n <= 15 do
    <<k1::unsigned-little-64, rest::binary>> = tail
    k2 = bytes_to_u64_le(rest)
    {k1, k2}
  end

  defp tail_keys(tail, 8) do
    <<k1::unsigned-little-64>> = tail
    {k1, 0}
  end

  defp tail_keys(tail, n) when n >= 1 and n <= 7 do
    {bytes_to_u64_le(tail), 0}
  end

  # Convert a binary of length 1..8 to a little-endian u64 (zero-padded).
  defp bytes_to_u64_le(bin) do
    pad_bits = (8 - byte_size(bin)) * 8
    <<v::unsigned-little-64>> = <<bin::binary, 0::size(pad_bits)>>
    v
  end

  defp fmix64(k) do
    k = bxor(k, k >>> 33)
    k = mul64(k, 0xFF51AFD7ED558CCD)
    k = bxor(k, k >>> 33)
    k = mul64(k, 0xC4CEB9FE1A85EC53)
    bxor(k, k >>> 33) &&& @mask64
  end

  defp rotl64(val, n) do
    val = val &&& @mask64
    (val <<< n ||| val >>> (64 - n)) &&& @mask64
  end

  defp add64(a, b), do: a + b &&& @mask64

  # 64x64 multiply mod 2^64, fixnum-safe via 16-bit schoolbook columns.
  defp mul64(a, b) do
    a = a &&& @mask64
    b = b &&& @mask64

    a0 = a &&& 0xFFFF
    a1 = a >>> 16 &&& 0xFFFF
    a2 = a >>> 32 &&& 0xFFFF
    a3 = a >>> 48 &&& 0xFFFF

    b0 = b &&& 0xFFFF
    b1 = b >>> 16 &&& 0xFFFF
    b2 = b >>> 32 &&& 0xFFFF
    b3 = b >>> 48 &&& 0xFFFF

    col0 = a0 * b0
    r0 = col0 &&& 0xFFFF
    c0 = col0 >>> 16

    col1 = a1 * b0 + a0 * b1 + c0
    r1 = col1 &&& 0xFFFF
    c1 = col1 >>> 16

    col2 = a2 * b0 + a1 * b1 + a0 * b2 + c1
    r2 = col2 &&& 0xFFFF
    c2 = col2 >>> 16

    col3 = a3 * b0 + a2 * b1 + a1 * b2 + a0 * b3 + c2
    r3 = col3 &&& 0xFFFF

    (r0 ||| r1 <<< 16 ||| r2 <<< 32 ||| r3 <<< 48) &&& @mask64
  end
end
