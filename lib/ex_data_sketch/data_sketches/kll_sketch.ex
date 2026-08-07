defmodule ExDataSketch.DataSketches.KLLSketch do
  @moduledoc """
  Apache DataSketches KLL binary codec for `ExDataSketch.KLL`.

  This module encodes and decodes the compact `KllFloatsSketch`/
  `KllDoublesSketch` binary format used by Apache DataSketches (Java, C++,
  Python) for cross-language interoperability.

  ## Value Semantics

  Unlike `ExDataSketch.DataSketches.CompactSketch` (Theta), KLL does not
  hash its inputs -- it stores the raw numeric values directly. This means
  KLL interop is a full item-level round trip: a sketch built by Apache
  DataSketches and decoded here (or vice versa) answers `quantile/2`,
  `rank/2`, `min_value/1`, and `max_value/1` queries using the exact same
  retained values the other implementation would, with no hash-equality
  caveat to worry about.

  ## Float vs. Double

  Apache's on-disk format does **not** self-describe whether the sketch
  holds `float` (4-byte) or `double` (8-byte) items -- the caller must
  know in advance which variant they're working with, exactly as the
  Java API requires picking `KllFloatsSketch` or `KllDoublesSketch`
  explicitly. Pass `variant: :float` or `variant: :double` (default)
  accordingly; a wrong guess will usually (not always) surface as a
  `DeserializationError` because the item width shifts the expected
  binary layout.

  ## Supported Features

  - **Compact format only**: reads and writes the compact, read-only
    representation. The "updatable" structure (`SerVer == 3`) is rejected.
  - **Default `M` only**: Apache's minimum-level-capacity parameter `M`
    must be the default (8) -- the only value `ExDataSketch.KLL` itself
    ever produces or expects.
  - **`KLL_FLOATS_SKETCH`/`KLL_DOUBLES_SKETCH` family only**: the KLL
    `LONGS_SKETCH`/`ITEMS_SKETCH` variants are out of scope.
  - **All modes**: empty, single-item, and full (n > 1) are supported.

  ## Binary Layout

  Little-endian, native byte order for multi-byte fields.

  | Structure | SerVer | PreInts | Layout |
  |---|---|---|---|
  | Compact Empty | 1 | 2 | 8-byte preamble only |
  | Compact Single | 2 | 2 | 8-byte preamble + 1 item |
  | Compact Full (n > 1) | 1 | 5 | 20-byte preamble + levels array + min + max + items |

  Preamble bytes 0-7 (shared by all three structures):

      byte 0: PreambleInts   byte 1: SerVer   byte 2: FamilyID (15)
      byte 3: Flags          byte 4-5: K (u16)   byte 6: M (u8, must be 8)
      byte 7: unused

  Full-only, bytes 8-19: `N` (u64), `MinK` (u16), `NumLevels` (u8), unused
  byte. Then at byte 20: `LevelsArr` (`NumLevels` x signed i32 -- **not**
  `NumLevels + 1`; the top boundary is implied by the total binary length,
  since the compact form has no free space), followed by `MinItem`,
  `MaxItem`, then the items array itself, packed level-major (level 0
  first).
  """

  import Bitwise, only: [&&&: 2]

  alias ExDataSketch.Errors.DeserializationError

  @family_id 15
  @default_m 8
  @min_k 8
  @max_k 65_535

  @ser_ver_empty_full 1
  @ser_ver_single 2
  @ser_ver_updatable 3

  @pre_ints_empty_single 2
  @pre_ints_full 5

  @flag_empty 0x01
  @flag_single_item 0x04

  @default_variant :double

  @doc """
  Encodes an `ExDataSketch.KLL` sketch into the Apache DataSketches KLL
  compact binary format.

  ## Options

  - `:variant` - `:float` or `:double` (default: `:double`).
  """
  @spec encode(ExDataSketch.KLL.t(), keyword()) :: binary()
  def encode(%ExDataSketch.KLL{state: state, opts: opts}, encode_opts \\ []) do
    variant = Keyword.get(encode_opts, :variant, @default_variant)
    k = Keyword.fetch!(opts, :k)

    <<1::unsigned-8, ^k::unsigned-little-32, n::unsigned-little-64, min_bin::binary-size(8),
      max_bin::binary-size(8), num_levels::unsigned-8, rest::binary>> = state

    parity_bytes = div(num_levels + 7, 8)
    <<_compaction_bits::binary-size(^parity_bytes), rest2::binary>> = rest
    level_sizes_bytes = num_levels * 4
    <<level_sizes_bin::binary-size(^level_sizes_bytes), items_bin::binary>> = rest2
    level_sizes = decode_state_u32_list(level_sizes_bin)
    items = decode_state_f64_list(items_bin)

    cond do
      n == 0 ->
        encode_empty(k)

      n == 1 ->
        [value] = items
        encode_single(k, value, variant)

      true ->
        <<min_val::float-little-64>> = min_bin
        <<max_val::float-little-64>> = max_bin
        encode_full(k, n, min_val, max_val, level_sizes, items, variant)
    end
  end

  @doc """
  Decodes an Apache DataSketches KLL compact binary into sketch
  components.

  Returns `{:ok, %{k: k, n: n, min_val: v, max_val: v, levels: [[float()]]}}`
  (levels list-of-lists, level 0 first) or `{:error, %DeserializationError{}}`.

  ## Options

  - `:variant` - `:float` or `:double` (default: `:double`). Must match
    the variant the binary was originally produced as -- see "Float vs.
    Double" in the moduledoc.
  """
  @spec decode(binary(), keyword()) :: {:ok, map()} | {:error, Exception.t()}
  def decode(binary, opts \\ []) when is_binary(binary) do
    variant = Keyword.get(opts, :variant, @default_variant)

    with {:ok, preamble} <- parse_preamble(binary),
         :ok <- validate_family_id(preamble.family_id),
         :ok <- validate_m(preamble.m),
         :ok <- validate_k(preamble.k),
         {:ok, structure} <- validate_structure(preamble.pre_ints, preamble.ser_ver),
         :ok <- validate_flags(structure, preamble.flags),
         {:ok, result} <- extract_data(binary, preamble, structure, variant) do
      {:ok, result}
    end
  end

  # -- Encoding helpers --

  defp encode_empty(k) do
    <<@pre_ints_empty_single::unsigned-8, @ser_ver_empty_full::unsigned-8, @family_id::unsigned-8,
      @flag_empty::unsigned-8, k::unsigned-little-16, @default_m::unsigned-8, 0::unsigned-8>>
  end

  defp encode_single(k, value, variant) do
    <<@pre_ints_empty_single::unsigned-8, @ser_ver_single::unsigned-8, @family_id::unsigned-8,
      @flag_single_item::unsigned-8, k::unsigned-little-16, @default_m::unsigned-8, 0::unsigned-8,
      encode_item(value, variant)::binary>>
  end

  defp encode_full(k, n, min_val, max_val, level_sizes, items, variant) do
    total_retained = Enum.sum(level_sizes)
    num_levels = grow_num_levels_for_capacity(k, length(level_sizes), total_retained)
    padded_level_sizes = level_sizes ++ List.duplicate(0, num_levels - length(level_sizes))
    capacity = apache_compute_total_capacity(k, @default_m, num_levels)
    base = capacity - total_retained

    {boundaries, _final} =
      Enum.map_reduce(padded_level_sizes, base, fn size, offset -> {offset, offset + size} end)

    levels_bin =
      boundaries
      |> Enum.map(fn b -> <<b::signed-little-32>> end)
      |> IO.iodata_to_binary()

    items_bin =
      items
      |> Enum.map(&encode_item(&1, variant))
      |> IO.iodata_to_binary()

    preamble =
      <<@pre_ints_full::unsigned-8, @ser_ver_empty_full::unsigned-8, @family_id::unsigned-8,
        0::unsigned-8, k::unsigned-little-16, @default_m::unsigned-8, 0::unsigned-8,
        n::unsigned-little-64, k::unsigned-little-16, num_levels::unsigned-8, 0::unsigned-8>>

    <<preamble::binary, levels_bin::binary, encode_item(min_val, variant)::binary,
      encode_item(max_val, variant)::binary, items_bin::binary>>
  end

  defp encode_item(value, :double), do: <<value::float-little-64>>
  defp encode_item(value, :float), do: <<value::float-little-32>>

  # Apache's compact writer positions retained items at the *tail* of a
  # virtual buffer sized for the theoretical full (non-compact) capacity of
  # (k, m, num_levels) -- readers recompute that capacity from the preamble
  # fields alone (kll_helper::compute_total_capacity in datasketches-cpp),
  # not from the file's byte length, and derive levels[0] = capacity -
  # total_retained. Our own internal growth/compaction formula
  # (kll_level_capacity/3 in Backend.Pure) is unrelated and can retain more
  # items at a given num_levels than Apache's formula would allow there, so
  # we grow num_levels (padding with empty top levels) until Apache's own
  # formula has room for everything we're about to write.
  defp grow_num_levels_for_capacity(k, num_levels, total_retained) do
    if apache_compute_total_capacity(k, @default_m, num_levels) >= total_retained do
      num_levels
    else
      grow_num_levels_for_capacity(k, num_levels + 1, total_retained)
    end
  end

  defp apache_compute_total_capacity(k, m, num_levels) do
    0..(num_levels - 1)
    |> Enum.reduce(0, fn height, acc -> acc + apache_level_capacity(k, num_levels, height, m) end)
  end

  defp apache_level_capacity(k, num_levels, height, m) do
    depth = num_levels - height - 1
    max(m, apache_int_cap_aux(k, depth))
  end

  defp apache_int_cap_aux(k, depth) when depth <= 30, do: apache_int_cap_aux_aux(k, depth)

  defp apache_int_cap_aux(k, depth) do
    half = div(depth, 2)
    rest = depth - half
    tmp = apache_int_cap_aux_aux(k, half)
    apache_int_cap_aux_aux(tmp, rest)
  end

  defp apache_int_cap_aux_aux(k, depth) do
    twok = k * 2
    tmp = div(twok * Integer.pow(2, depth), Integer.pow(3, depth))
    div(tmp + 1, 2)
  end

  defp decode_state_u32_list(bin), do: decode_state_u32_list(bin, [])
  defp decode_state_u32_list(<<>>, acc), do: Enum.reverse(acc)

  defp decode_state_u32_list(<<v::unsigned-little-32, rest::binary>>, acc),
    do: decode_state_u32_list(rest, [v | acc])

  defp decode_state_f64_list(bin), do: decode_state_f64_list(bin, [])
  defp decode_state_f64_list(<<>>, acc), do: Enum.reverse(acc)

  defp decode_state_f64_list(<<v::float-little-64, rest::binary>>, acc),
    do: decode_state_f64_list(rest, [v | acc])

  # -- Decoding helpers --

  defp parse_preamble(
         <<pre_ints::unsigned-8, ser_ver::unsigned-8, family_id::unsigned-8, flags::unsigned-8,
           k::unsigned-little-16, m::unsigned-8, _unused::unsigned-8, _rest::binary>>
       ) do
    {:ok, %{pre_ints: pre_ints, ser_ver: ser_ver, family_id: family_id, flags: flags, k: k, m: m}}
  end

  defp parse_preamble(_) do
    {:error, DeserializationError.exception(reason: "binary too short for KLL preamble")}
  end

  defp validate_family_id(@family_id), do: :ok

  defp validate_family_id(id) do
    {:error,
     DeserializationError.exception(
       reason: "unsupported family ID #{id}, expected #{@family_id} (KLL)"
     )}
  end

  defp validate_m(@default_m), do: :ok

  defp validate_m(m) do
    {:error,
     DeserializationError.exception(
       reason: "unsupported M value #{m}, only the default M=#{@default_m} is supported"
     )}
  end

  defp validate_k(k) when k >= @min_k and k <= @max_k, do: :ok

  defp validate_k(k) do
    {:error,
     DeserializationError.exception(
       reason: "k value #{k} out of valid range #{@min_k}..#{@max_k}"
     )}
  end

  defp validate_structure(@pre_ints_empty_single, @ser_ver_empty_full), do: {:ok, :empty}
  defp validate_structure(@pre_ints_empty_single, @ser_ver_single), do: {:ok, :single}
  defp validate_structure(@pre_ints_full, @ser_ver_empty_full), do: {:ok, :full}

  defp validate_structure(pre_ints, @ser_ver_updatable) do
    {:error,
     DeserializationError.exception(
       reason:
         "updatable (non-compact) KLL structures are not supported, only compact serialized " <>
           "sketches (preInts=#{pre_ints}, serVer=#{@ser_ver_updatable})"
     )}
  end

  defp validate_structure(pre_ints, ser_ver) do
    {:error,
     DeserializationError.exception(
       reason:
         "invalid or unsupported preInts/serVer combination: preInts=#{pre_ints}, serVer=#{ser_ver}"
     )}
  end

  defp validate_flags(:empty, flags) do
    if (flags &&& @flag_empty) != 0 do
      :ok
    else
      {:error, DeserializationError.exception(reason: "EMPTY flag not set for empty structure")}
    end
  end

  defp validate_flags(:single, flags) do
    if (flags &&& @flag_single_item) != 0 do
      :ok
    else
      {:error,
       DeserializationError.exception(reason: "SINGLE_ITEM flag not set for single structure")}
    end
  end

  defp validate_flags(:full, _flags), do: :ok

  defp extract_data(_binary, preamble, :empty, _variant) do
    {:ok, %{k: preamble.k, n: 0, min_val: :nan, max_val: :nan, levels: [[]]}}
  end

  defp extract_data(binary, preamble, :single, variant) do
    item_size = item_bytes(variant)

    case binary do
      <<_preamble::binary-size(8), item_bin::binary-size(^item_size)>> ->
        value = decode_item(item_bin, variant)
        {:ok, %{k: preamble.k, n: 1, min_val: value, max_val: value, levels: [[value]]}}

      <<_preamble::binary-size(8), _item_bin::binary-size(^item_size), _trailing::binary>> ->
        {:error,
         DeserializationError.exception(reason: "trailing bytes after single-item KLL sketch")}

      _ ->
        {:error, DeserializationError.exception(reason: "truncated single-item KLL sketch")}
    end
  end

  defp extract_data(binary, preamble, :full, variant) do
    case binary do
      <<_preamble8::binary-size(8), n::unsigned-little-64, _min_k::unsigned-little-16,
        num_levels::unsigned-8, _unused::unsigned-8, rest::binary>> ->
        extract_full_levels(rest, preamble.k, n, num_levels, variant)

      _ ->
        {:error, DeserializationError.exception(reason: "truncated full KLL sketch preamble")}
    end
  end

  defp extract_full_levels(_rest, _k, _n, 0, _variant) do
    {:error, DeserializationError.exception(reason: "full KLL structure with NumLevels=0")}
  end

  defp extract_full_levels(rest, k, n, num_levels, variant) do
    item_size = item_bytes(variant)
    levels_bytes = num_levels * 4

    case rest do
      <<levels_bin::binary-size(^levels_bytes), min_bin::binary-size(^item_size),
        max_bin::binary-size(^item_size), items_bin::binary>> ->
        build_full_result(k, n, levels_bin, min_bin, max_bin, items_bin, variant)

      _ ->
        {:error,
         DeserializationError.exception(
           reason:
             "truncated full KLL sketch: expected #{levels_bytes} bytes of levels array + min/max + items"
         )}
    end
  end

  defp build_full_result(k, n, levels_bin, min_bin, max_bin, items_bin, variant) do
    item_size = item_bytes(variant)

    if rem(byte_size(items_bin), item_size) != 0 do
      {:error,
       DeserializationError.exception(
         reason:
           "items array size #{byte_size(items_bin)} is not a multiple of the item width " <>
             "for :variant #{inspect(variant)} -- wrong :variant option?"
       )}
    else
      boundaries = decode_i32_list(levels_bin)
      total_retained = div(byte_size(items_bin), item_size)
      top_boundary = List.first(boundaries) + total_retained
      full_boundaries = Enum.reverse([top_boundary | Enum.reverse(boundaries)])
      level_sizes = boundaries_to_sizes(full_boundaries)

      items = decode_item_list(items_bin, variant)
      levels = split_into_levels(items, level_sizes)

      min_val = decode_item(min_bin, variant)
      max_val = decode_item(max_bin, variant)

      {:ok, %{k: k, n: n, min_val: min_val, max_val: max_val, levels: levels}}
    end
  end

  defp boundaries_to_sizes(full_boundaries) do
    full_boundaries
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map(fn [a, b] -> b - a end)
  end

  defp split_into_levels(items, level_sizes) do
    {levels, _rest} =
      Enum.map_reduce(level_sizes, items, fn size, remaining ->
        Enum.split(remaining, size)
      end)

    levels
  end

  defp decode_i32_list(bin), do: decode_i32_list(bin, [])
  defp decode_i32_list(<<>>, acc), do: Enum.reverse(acc)

  defp decode_i32_list(<<v::signed-little-32, rest::binary>>, acc),
    do: decode_i32_list(rest, [v | acc])

  defp decode_item_list(bin, :double), do: decode_state_f64_list(bin)
  defp decode_item_list(bin, :float), do: decode_f32_list(bin)

  defp decode_f32_list(bin), do: decode_f32_list(bin, [])
  defp decode_f32_list(<<>>, acc), do: Enum.reverse(acc)

  defp decode_f32_list(<<v::float-little-32, rest::binary>>, acc),
    do: decode_f32_list(rest, [v | acc])

  defp decode_item(<<v::float-little-64>>, :double), do: v
  defp decode_item(<<v::float-little-32>>, :float), do: v

  defp item_bytes(:double), do: 8
  defp item_bytes(:float), do: 4
end
