defmodule ExDataSketch.DataSketches.CompactSketch do
  @moduledoc """
  Apache DataSketches CompactSketch binary codec for Theta sketches.

  This module encodes and decodes the CompactSketch binary format used by
  Apache DataSketches (Java, C++, Python) for cross-language interoperability.

  ## Hash Semantics

  ExDataSketch uses `ExDataSketch.Hash.hash64/1` (`:erlang.phash2` + Murmur
  finalization) while DataSketches uses MurmurHash3_x64_128. These hash
  functions are **not** cross-compatible — the same input string will produce
  different hash values. Interop works at the binary level: serialized sketches
  contain pre-computed hash values, so they can be deserialized and merged
  regardless of which hash function originally produced them.

  ## Seed Hash

  The seed hash is a 16-bit checksum derived from the hash function's seed.
  It prevents merging sketches that used different hash functions/seeds.
  The default seed is 9001, matching the DataSketches default. The seed hash
  is computed using MurmurHash3_x64_128 (see `ExDataSketch.DataSketches.Murmur3`).

  ## Supported Features

  - **Compact format only**: This codec reads and writes the compact, ordered
    representation. Non-compact (hash table) sketches are rejected.
  - **Little-endian only**: Big-endian sketches (flag bit 0 set) are rejected.
  - **All modes**: empty, single-item, exact, and estimation modes are supported.

  ## Binary Layout

  Variable-length preamble (1, 2, or 3 longs of 8 bytes):

  | Offset | Size | Field |
  |--------|------|-------|
  | 0 | 1 | Preamble longs (1, 2, or 3) |
  | 1 | 1 | Serial version (3) |
  | 2 | 1 | Family ID (3 = CompactSketch) |
  | 3 | 1 | lgNomLongs (log2 of k) |
  | 4 | 1 | lgArrLongs (0 for compact) |
  | 5 | 1 | Flags |
  | 6 | 2 | Seed hash (u16-le) |
  | 8 | 4 | Retained entry count (u32-le, preamble ≥ 2) |
  | 12 | 4 | Padding (preamble ≥ 2) |
  | 16 | 8 | Theta (u64-le, preamble == 3) |

  After preamble: entries as u64 little-endian values.
  """

  import Bitwise

  alias ExDataSketch.DataSketches.Murmur3
  alias ExDataSketch.Errors.DeserializationError

  @serial_version 3
  @family_id 3
  @max_theta 0xFFFFFFFFFFFFFFFF

  # Flag bits
  @flag_read_only 0x02
  @flag_empty 0x04
  @flag_compact 0x08
  @flag_ordered 0x10
  @flag_single_item 0x20

  @default_seed 9001

  @doc """
  Encodes a Theta sketch into the DataSketches CompactSketch binary format.

  ## Options

  - `:seed` - seed for seed hash computation (default: 9001)
  """
  @spec encode(ExDataSketch.Theta.t(), keyword()) :: binary()
  def encode(%ExDataSketch.Theta{state: state, opts: opts}, encode_opts \\ []) do
    seed = Keyword.get(encode_opts, :seed, @default_seed)
    seed_hash = Murmur3.seed_hash(seed)
    k = Keyword.fetch!(opts, :k)
    lg_k = trunc(:math.log2(k))

    <<1::unsigned-8, ^k::unsigned-little-32, theta::unsigned-little-64, count::unsigned-little-32,
      entries_bin::binary>> = state

    cond do
      # Empty mode: 1 preamble long, no entries
      count == 0 ->
        flags = @flag_empty ||| @flag_compact ||| @flag_read_only
        encode_preamble(1, lg_k, flags, seed_hash)

      # Single item mode: 1 preamble long + 1 entry
      count == 1 and theta == @max_theta ->
        flags = @flag_compact ||| @flag_read_only ||| @flag_single_item ||| @flag_ordered

        <<encode_preamble(1, lg_k, flags, seed_hash)::binary, entries_bin::binary>>

      # Exact mode: 2 preamble longs + entries (theta == max)
      theta == @max_theta ->
        flags = @flag_compact ||| @flag_read_only ||| @flag_ordered

        <<encode_preamble_with_count(2, lg_k, flags, seed_hash, count)::binary,
          entries_bin::binary>>

      # Estimation mode: 3 preamble longs + entries (theta < max)
      true ->
        flags = @flag_compact ||| @flag_read_only ||| @flag_ordered

        <<encode_preamble_with_theta(3, lg_k, flags, seed_hash, count, theta)::binary,
          entries_bin::binary>>
    end
  end

  @doc """
  Decodes a DataSketches CompactSketch binary into sketch components.

  Returns `{:ok, %{k: k, theta: theta, entries: [u64], seed_hash: u16}}`
  or `{:error, %DeserializationError{}}`.

  ## Options

  - `:seed` - expected seed for seed hash verification (default: 9001).
    Pass `nil` to skip seed hash verification.
  """
  @spec decode(binary(), keyword()) :: {:ok, map()} | {:error, Exception.t()}
  def decode(binary, opts \\ []) when is_binary(binary) do
    seed = Keyword.get(opts, :seed, @default_seed)

    with {:ok, preamble} <- parse_preamble(binary),
         :ok <- validate_serial_version(preamble.serial_version),
         :ok <- validate_family_id(preamble.family_id),
         :ok <- validate_compact(preamble.flags),
         :ok <- validate_endianness(preamble.flags),
         :ok <- maybe_validate_seed_hash(preamble.seed_hash, seed),
         {:ok, result} <- extract_data(binary, preamble) do
      {:ok, result}
    end
  end

  # -- Encoding helpers --

  defp encode_preamble(pre_longs, lg_k, flags, seed_hash) do
    <<pre_longs::unsigned-8, @serial_version::unsigned-8, @family_id::unsigned-8,
      lg_k::unsigned-8, 0::unsigned-8, flags::unsigned-8, seed_hash::unsigned-little-16>>
  end

  defp encode_preamble_with_count(pre_longs, lg_k, flags, seed_hash, count) do
    <<pre_longs::unsigned-8, @serial_version::unsigned-8, @family_id::unsigned-8,
      lg_k::unsigned-8, 0::unsigned-8, flags::unsigned-8, seed_hash::unsigned-little-16,
      count::unsigned-little-32, 0::unsigned-little-32>>
  end

  defp encode_preamble_with_theta(pre_longs, lg_k, flags, seed_hash, count, theta) do
    <<pre_longs::unsigned-8, @serial_version::unsigned-8, @family_id::unsigned-8,
      lg_k::unsigned-8, 0::unsigned-8, flags::unsigned-8, seed_hash::unsigned-little-16,
      count::unsigned-little-32, 0::unsigned-little-32, theta::unsigned-little-64>>
  end

  # -- Decoding helpers --

  defp parse_preamble(
         <<pre_longs::unsigned-8, ser_ver::unsigned-8, fam_id::unsigned-8, lg_nom::unsigned-8,
           _lg_arr::unsigned-8, flags::unsigned-8, seed_hash::unsigned-little-16, _rest::binary>>
       ) do
    {:ok,
     %{
       pre_longs: pre_longs,
       serial_version: ser_ver,
       family_id: fam_id,
       lg_nom_longs: lg_nom,
       flags: flags,
       seed_hash: seed_hash
     }}
  end

  defp parse_preamble(_) do
    {:error,
     DeserializationError.exception(reason: "binary too short for CompactSketch preamble")}
  end

  defp validate_serial_version(@serial_version), do: :ok

  defp validate_serial_version(v) do
    {:error,
     DeserializationError.exception(
       reason: "unsupported serial version #{v}, expected #{@serial_version}"
     )}
  end

  defp validate_family_id(@family_id), do: :ok

  defp validate_family_id(id) do
    {:error,
     DeserializationError.exception(reason: "unsupported family ID #{id}, expected #{@family_id}")}
  end

  defp validate_compact(flags) do
    if (flags &&& @flag_compact) != 0 do
      :ok
    else
      {:error,
       DeserializationError.exception(reason: "only compact format is supported, got non-compact")}
    end
  end

  defp validate_endianness(flags) do
    # Bit 0 (0x01) = BIG_ENDIAN in some versions; we reject it
    if (flags &&& 0x01) != 0 do
      {:error, DeserializationError.exception(reason: "big-endian sketches are not supported")}
    else
      :ok
    end
  end

  defp maybe_validate_seed_hash(_seed_hash, nil), do: :ok

  defp maybe_validate_seed_hash(actual, seed) do
    expected = Murmur3.seed_hash(seed)

    if actual == expected do
      :ok
    else
      {:error,
       DeserializationError.exception(
         reason: "seed hash mismatch: got #{actual}, expected #{expected} (seed=#{seed})"
       )}
    end
  end

  defp extract_data(
         binary,
         %{pre_longs: pre_longs, flags: flags, lg_nom_longs: lg_nom} = preamble
       ) do
    k = 1 <<< lg_nom
    is_empty = (flags &&& @flag_empty) != 0
    is_single = (flags &&& @flag_single_item) != 0

    cond do
      # Empty sketch
      is_empty ->
        {:ok, %{k: k, theta: @max_theta, entries: [], seed_hash: preamble.seed_hash}}

      # Single item (preamble = 1 long = 8 bytes, then 1 entry)
      is_single ->
        case binary do
          <<_preamble::binary-size(8), entry::unsigned-little-64>> ->
            {:ok, %{k: k, theta: @max_theta, entries: [entry], seed_hash: preamble.seed_hash}}

          <<_preamble::binary-size(8), entry::unsigned-little-64, _rest::binary>> ->
            {:ok, %{k: k, theta: @max_theta, entries: [entry], seed_hash: preamble.seed_hash}}

          _ ->
            {:error,
             DeserializationError.exception(reason: "truncated single-item CompactSketch")}
        end

      # Exact or estimation mode
      pre_longs >= 2 ->
        preamble_size = pre_longs * 8
        extract_multi_entry(binary, preamble_size, k, pre_longs, preamble)

      true ->
        {:error,
         DeserializationError.exception(
           reason: "invalid preamble longs #{pre_longs} for non-empty sketch"
         )}
    end
  end

  defp extract_multi_entry(binary, _preamble_size, k, pre_longs, preamble) do
    # Read count from bytes 8-11
    <<_first8::binary-size(8), count::unsigned-little-32, _pad::unsigned-little-32, rest::binary>> =
      binary

    # Read theta if 3 preamble longs
    {theta, entries_bin} =
      if pre_longs >= 3 do
        <<theta::unsigned-little-64, entries::binary>> = rest
        {theta, entries}
      else
        {_entries_only_rest, _} = {rest, nil}
        {@max_theta, rest}
      end

    # Skip any remaining preamble bytes beyond what we've consumed
    expected_entries_size = count * 8

    if byte_size(entries_bin) < expected_entries_size do
      {:error,
       DeserializationError.exception(
         reason:
           "truncated entries: expected #{count} entries (#{expected_entries_size} bytes), got #{byte_size(entries_bin)} bytes"
       )}
    else
      <<entries_data::binary-size(^expected_entries_size), _trailing::binary>> = entries_bin
      entries = decode_entries(entries_data)
      {:ok, %{k: k, theta: theta, entries: entries, seed_hash: preamble.seed_hash}}
    end
  end

  defp decode_entries(<<>>), do: []

  defp decode_entries(binary) do
    do_decode_entries(binary, []) |> :lists.reverse()
  end

  defp do_decode_entries(<<>>, acc), do: acc

  defp do_decode_entries(<<val::unsigned-little-64, rest::binary>>, acc) do
    do_decode_entries(rest, [val | acc])
  end
end
