defmodule ExDataSketch.Backend.Pure do
  @moduledoc """
  Pure Elixir backend for ExDataSketch.

  This module implements the `ExDataSketch.Backend` behaviour using only
  Elixir/Erlang standard library functions. It is always available and
  serves as the default backend.

  ## Implementation Notes

  - All state is stored as Elixir binaries with documented layouts.
  - Operations are pure: input binary in, new binary out. No side effects.
  - Batch operations (`update_many`) decode the register/counter array to a
    tuple for O(1) access, fold over all items, then re-encode once to
    minimize binary copying.
  """

  @behaviour ExDataSketch.Backend

  import Bitwise

  @mask64 0xFFFFFFFFFFFFFFFF

  # ============================================================
  # HLL Implementation
  # ============================================================

  @impl true
  @spec hll_new(keyword()) :: binary()
  def hll_new(opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p
    registers = :binary.copy(<<0>>, m)
    <<1::unsigned-8, p::unsigned-8, 0::unsigned-little-16, registers::binary>>
  end

  @impl true
  @spec hll_update(binary(), non_neg_integer(), keyword()) :: binary()
  def hll_update(state_bin, hash64, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    bucket = hash64 >>> (64 - p)
    remaining = hash64 &&& (1 <<< (64 - p)) - 1
    rank = count_leading_zeros(remaining, 64 - p) + 1

    <<header::binary-size(4), registers::binary-size(^m)>> = state_bin

    # Read current register value and set max
    <<before::binary-size(^bucket), old_val::unsigned-8, after_bytes::binary>> = registers

    if rank > old_val do
      <<header::binary, before::binary, rank::unsigned-8, after_bytes::binary>>
    else
      state_bin
    end
  end

  @impl true
  @spec hll_update_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def hll_update_many(state_bin, hashes, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p
    bits = 64 - p

    <<header::binary-size(4), registers::binary-size(^m)>> = state_bin

    # Decode registers to a tuple for O(1) access
    reg_tuple = registers |> :binary.bin_to_list() |> List.to_tuple()

    # Fold over all hashes
    remaining_mask = (1 <<< (64 - p)) - 1

    reg_tuple =
      List.foldl(hashes, reg_tuple, fn hash64, acc ->
        bucket = hash64 >>> (64 - p)
        remaining = hash64 &&& remaining_mask
        rank = count_leading_zeros(remaining, bits) + 1
        old_val = elem(acc, bucket)

        if rank > old_val do
          put_elem(acc, bucket, rank)
        else
          acc
        end
      end)

    # Re-encode tuple to binary
    new_registers = reg_tuple |> Tuple.to_list() |> :erlang.list_to_binary()
    <<header::binary, new_registers::binary>>
  end

  @impl true
  @spec hll_merge(binary(), binary(), keyword()) :: binary()
  def hll_merge(a_bin, b_bin, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    <<header_a::binary-size(4), regs_a::binary-size(^m)>> = a_bin
    <<_header_b::binary-size(4), regs_b::binary-size(^m)>> = b_bin

    merged =
      zip_max_binary(regs_a, regs_b)
      |> IO.iodata_to_binary()

    <<header_a::binary, merged::binary>>
  end

  @impl true
  @spec hll_estimate(binary(), keyword()) :: float()
  def hll_estimate(state_bin, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    <<_header::binary-size(4), registers::binary-size(^m)>> = state_bin

    # Compute raw estimate: alpha * m^2 / sum(2^(-register_i))
    alpha = alpha(m)

    {sum, zeros} =
      binary_fold(registers, {0.0, 0}, fn val, {s, z} ->
        new_z = if val == 0, do: z + 1, else: z
        {s + :math.pow(2.0, -val), new_z}
      end)

    raw_estimate = alpha * m * m / sum

    cond do
      # Small range correction with linear counting
      raw_estimate <= 2.5 * m and zeros > 0 ->
        m * :math.log(m / zeros)

      # Large range correction (effectively unreachable with 64-bit hashes)
      raw_estimate > 0x100000000000000 / 30 ->
        -0x10000000000000000 * :math.log(1.0 - raw_estimate / 0x10000000000000000)

      true ->
        raw_estimate
    end
  end

  # -- HLL Helpers --

  @doc false
  def count_leading_zeros(_value, 0), do: 0

  def count_leading_zeros(0, n), do: n

  def count_leading_zeros(value, n) do
    # Number of bits needed to represent value
    num_bits = num_bits(value)
    n - num_bits
  end

  defp num_bits(0), do: 0

  defp num_bits(value) do
    do_num_bits(value, 0)
  end

  defp do_num_bits(0, acc), do: acc
  defp do_num_bits(v, acc), do: do_num_bits(v >>> 1, acc + 1)

  defp alpha(16), do: 0.673
  defp alpha(32), do: 0.697
  defp alpha(64), do: 0.709
  defp alpha(m) when m >= 128, do: 0.7213 / (1.0 + 1.079 / m)

  defp zip_max_binary(<<>>, <<>>), do: []

  defp zip_max_binary(<<a::unsigned-8, rest_a::binary>>, <<b::unsigned-8, rest_b::binary>>) do
    [max(a, b) | zip_max_binary(rest_a, rest_b)]
  end

  defp binary_fold(<<>>, acc, _fun), do: acc

  defp binary_fold(<<byte::unsigned-8, rest::binary>>, acc, fun) do
    binary_fold(rest, fun.(byte, acc), fun)
  end

  # ============================================================
  # CMS Implementation
  # ============================================================

  # Golden ratio constant for hash family
  @golden64 0x9E3779B97F4A7C15

  @impl true
  @spec cms_new(keyword()) :: binary()
  def cms_new(opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    counter_bytes = div(counter_width, 8)

    counters = :binary.copy(<<0>>, width * depth * counter_bytes)

    <<1::unsigned-8, width::unsigned-little-32, depth::unsigned-little-16,
      counter_width::unsigned-8, 0::unsigned-8, counters::binary>>
  end

  @impl true
  @spec cms_update(binary(), non_neg_integer(), pos_integer(), keyword()) :: binary()
  def cms_update(state_bin, hash64, increment, opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    counter_bytes = div(counter_width, 8)
    max_counter = (1 <<< counter_width) - 1
    header_size = 9
    data_size = width * depth * counter_bytes

    <<header::binary-size(^header_size), counters::binary-size(^data_size)>> = state_bin

    # Decode counters to tuple for efficient update
    counter_tuple = decode_counters_to_tuple(counters, counter_bytes)

    counter_tuple =
      Enum.reduce(0..(depth - 1), counter_tuple, fn row, acc ->
        col = cms_row_index(hash64, row, width)
        idx = row * width + col
        old_val = elem(acc, idx)
        new_val = min(old_val + increment, max_counter)
        put_elem(acc, idx, new_val)
      end)

    new_counters = encode_counters_from_tuple(counter_tuple, counter_bytes)
    <<header::binary, new_counters::binary>>
  end

  @impl true
  @spec cms_update_many(binary(), [{non_neg_integer(), pos_integer()}], keyword()) :: binary()
  def cms_update_many(state_bin, pairs, opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    counter_bytes = div(counter_width, 8)
    max_counter = (1 <<< counter_width) - 1
    header_size = 9
    data_size = width * depth * counter_bytes

    <<header::binary-size(^header_size), counters::binary-size(^data_size)>> = state_bin

    counter_tuple = decode_counters_to_tuple(counters, counter_bytes)

    counter_tuple =
      List.foldl(pairs, counter_tuple, fn {hash64, increment}, acc ->
        Enum.reduce(0..(depth - 1), acc, fn row, inner_acc ->
          col = cms_row_index(hash64, row, width)
          idx = row * width + col
          old_val = elem(inner_acc, idx)
          new_val = min(old_val + increment, max_counter)
          put_elem(inner_acc, idx, new_val)
        end)
      end)

    new_counters = encode_counters_from_tuple(counter_tuple, counter_bytes)
    <<header::binary, new_counters::binary>>
  end

  @impl true
  @spec cms_merge(binary(), binary(), keyword()) :: binary()
  def cms_merge(a_bin, b_bin, opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    counter_bytes = div(counter_width, 8)
    max_counter = (1 <<< counter_width) - 1
    header_size = 9
    total_counters = width * depth
    data_size = total_counters * counter_bytes

    <<header_a::binary-size(^header_size), counters_a::binary-size(^data_size)>> = a_bin
    <<_header_b::binary-size(^header_size), counters_b::binary-size(^data_size)>> = b_bin

    list_a = decode_counters_to_list(counters_a, counter_bytes)
    list_b = decode_counters_to_list(counters_b, counter_bytes)

    merged =
      Enum.zip_with(list_a, list_b, fn a, b -> min(a + b, max_counter) end)

    new_counters = encode_counters_from_list(merged, counter_bytes)
    <<header_a::binary, new_counters::binary>>
  end

  @impl true
  @spec cms_estimate(binary(), non_neg_integer(), keyword()) :: non_neg_integer()
  def cms_estimate(state_bin, hash64, opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    counter_bytes = div(counter_width, 8)
    header_size = 9

    <<_header::binary-size(^header_size), counters::binary>> = state_bin

    # Read each row's counter via direct binary offset (no full decode)
    Enum.reduce(0..(depth - 1), :infinity, fn row, min_val ->
      col = cms_row_index(hash64, row, width)
      offset = (row * width + col) * counter_bytes
      val = decode_single_counter(counters, offset, counter_bytes)
      min(min_val, val)
    end)
  end

  # -- CMS Helpers --

  defp cms_row_index(hash64, row, width) do
    rem(hash64 + row * @golden64 &&& @mask64, width)
  end

  defp decode_counters_to_tuple(binary, 4) do
    do_decode_32(binary, [])
    |> :lists.reverse()
    |> List.to_tuple()
  end

  defp decode_counters_to_tuple(binary, 8) do
    do_decode_64(binary, [])
    |> :lists.reverse()
    |> List.to_tuple()
  end

  defp decode_counters_to_list(binary, 4), do: do_decode_32(binary, []) |> :lists.reverse()
  defp decode_counters_to_list(binary, 8), do: do_decode_64(binary, []) |> :lists.reverse()

  defp do_decode_32(<<>>, acc), do: acc

  defp do_decode_32(<<val::unsigned-little-32, rest::binary>>, acc) do
    do_decode_32(rest, [val | acc])
  end

  defp do_decode_64(<<>>, acc), do: acc

  defp do_decode_64(<<val::unsigned-little-64, rest::binary>>, acc) do
    do_decode_64(rest, [val | acc])
  end

  defp encode_counters_from_tuple(tuple, counter_bytes) do
    tuple
    |> Tuple.to_list()
    |> encode_counters_from_list(counter_bytes)
  end

  defp encode_counters_from_list(list, 4) do
    list
    |> Enum.map(fn val -> <<val::unsigned-little-32>> end)
    |> IO.iodata_to_binary()
  end

  defp encode_counters_from_list(list, 8) do
    list
    |> Enum.map(fn val -> <<val::unsigned-little-64>> end)
    |> IO.iodata_to_binary()
  end

  defp decode_single_counter(binary, offset, 4) do
    <<_::binary-size(^offset), val::unsigned-little-32, _::binary>> = binary
    val
  end

  defp decode_single_counter(binary, offset, 8) do
    <<_::binary-size(^offset), val::unsigned-little-64, _::binary>> = binary
    val
  end

  # ============================================================
  # Theta Implementation
  # ============================================================

  @theta_max_u64 0xFFFFFFFFFFFFFFFF

  @impl true
  @spec theta_new(keyword()) :: binary()
  def theta_new(opts) do
    k = Keyword.fetch!(opts, :k)

    <<1::unsigned-8, k::unsigned-little-32, @theta_max_u64::unsigned-little-64,
      0::unsigned-little-32>>
  end

  @impl true
  @spec theta_update(binary(), non_neg_integer(), keyword()) :: binary()
  def theta_update(state_bin, hash64, _opts) do
    <<1::unsigned-8, k::unsigned-little-32, theta::unsigned-little-64, count::unsigned-little-32,
      entries_bin::binary>> = state_bin

    # Skip if hash is above threshold
    if hash64 >= theta do
      state_bin
    else
      entries = theta_decode_entries(entries_bin)

      # Check membership via sorted list search
      if theta_member?(entries, hash64) do
        state_bin
      else
        theta_insert_and_compact(entries, hash64, k, theta, count)
      end
    end
  end

  @impl true
  @spec theta_update_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def theta_update_many(state_bin, hashes, _opts) do
    <<1::unsigned-8, k::unsigned-little-32, theta::unsigned-little-64, _count::unsigned-little-32,
      entries_bin::binary>> = state_bin

    entry_set = entries_bin |> theta_decode_entries() |> MapSet.new()

    # Add all qualifying hashes
    {new_set, new_theta} =
      Enum.reduce(hashes, {entry_set, theta}, fn hash64, {set, th} ->
        if hash64 >= th do
          {set, th}
        else
          {MapSet.put(set, hash64), th}
        end
      end)

    sorted = new_set |> MapSet.to_list() |> Enum.sort()

    if length(sorted) > k do
      kept = Enum.take(sorted, k)
      # theta = the (k+1)th element (0-indexed: element at position k)
      compact_theta = Enum.at(sorted, k)
      theta_encode_state(k, compact_theta, kept)
    else
      theta_encode_state(k, new_theta, sorted)
    end
  end

  @impl true
  @spec theta_compact(binary(), keyword()) :: binary()
  def theta_compact(state_bin, _opts) do
    <<1::unsigned-8, k::unsigned-little-32, theta::unsigned-little-64, _count::unsigned-little-32,
      entries_bin::binary>> = state_bin

    entries =
      entries_bin
      |> theta_decode_entries()
      |> Enum.filter(&(&1 < theta))
      |> Enum.sort()

    theta_encode_state(k, theta, entries)
  end

  @impl true
  @spec theta_merge(binary(), binary(), keyword()) :: binary()
  def theta_merge(a_bin, b_bin, _opts) do
    <<1::unsigned-8, k_a::unsigned-little-32, theta_a::unsigned-little-64,
      _count_a::unsigned-little-32, entries_a_bin::binary>> = a_bin

    <<1::unsigned-8, _k_b::unsigned-little-32, theta_b::unsigned-little-64,
      _count_b::unsigned-little-32, entries_b_bin::binary>> = b_bin

    new_theta = min(theta_a, theta_b)

    entries_a = theta_decode_entries(entries_a_bin)
    entries_b = theta_decode_entries(entries_b_bin)

    # Union, deduplicate, filter by new theta
    union =
      (entries_a ++ entries_b)
      |> MapSet.new()
      |> MapSet.to_list()
      |> Enum.filter(&(&1 < new_theta))
      |> Enum.sort()

    if length(union) > k_a do
      kept = Enum.take(union, k_a)
      compact_theta = Enum.at(union, k_a)
      theta_encode_state(k_a, compact_theta, kept)
    else
      theta_encode_state(k_a, new_theta, union)
    end
  end

  @impl true
  @spec theta_estimate(binary(), keyword()) :: float()
  def theta_estimate(state_bin, _opts) do
    <<1::unsigned-8, _k::unsigned-little-32, theta::unsigned-little-64, count::unsigned-little-32,
      _entries_bin::binary>> = state_bin

    cond do
      count == 0 ->
        0.0

      theta == @theta_max_u64 ->
        # Exact mode: no sampling, count is exact
        count / 1

      true ->
        # Estimation mode: count / (theta / 2^64)
        count * (@theta_max_u64 + 1) / theta
    end
  end

  @impl true
  @spec theta_from_components(non_neg_integer(), non_neg_integer(), [non_neg_integer()]) ::
          binary()
  def theta_from_components(k, theta, entries) do
    normalized =
      entries
      |> Enum.uniq()
      |> Enum.filter(&(&1 < theta))
      |> Enum.sort()

    {final_theta, final_entries} =
      if length(normalized) > k do
        {kept, [new_theta | _]} = Enum.split(normalized, k)
        {new_theta, kept}
      else
        {theta, normalized}
      end

    theta_encode_state(k, final_theta, final_entries)
  end

  # -- Theta Helpers --

  defp theta_insert_and_compact(entries, hash64, k, theta, count) do
    new_entries = theta_insert_sorted(entries, hash64)
    new_count = count + 1

    if new_count > k do
      {kept, [new_theta | _]} = Enum.split(new_entries, k)
      theta_encode_state(k, new_theta, kept)
    else
      theta_encode_state(k, theta, new_entries)
    end
  end

  defp theta_encode_state(k, theta, sorted_entries) do
    count = length(sorted_entries)

    entries_bin =
      sorted_entries
      |> Enum.map(fn v -> <<v::unsigned-little-64>> end)
      |> IO.iodata_to_binary()

    <<1::unsigned-8, k::unsigned-little-32, theta::unsigned-little-64, count::unsigned-little-32,
      entries_bin::binary>>
  end

  defp theta_decode_entries(<<>>), do: []

  defp theta_decode_entries(binary) do
    do_theta_decode(binary, []) |> :lists.reverse()
  end

  defp do_theta_decode(<<>>, acc), do: acc

  defp do_theta_decode(<<val::unsigned-little-64, rest::binary>>, acc) do
    do_theta_decode(rest, [val | acc])
  end

  defp theta_member?([], _value), do: false

  defp theta_member?([h | _t], value) when h > value, do: false

  defp theta_member?([h | _t], value) when h == value, do: true

  defp theta_member?([_h | t], value), do: theta_member?(t, value)

  defp theta_insert_sorted([], value), do: [value]

  defp theta_insert_sorted([h | t], value) when value < h, do: [value, h | t]

  defp theta_insert_sorted([h | t], value), do: [h | theta_insert_sorted(t, value)]

  # ============================================================
  # KLL Implementation
  # ============================================================
  #
  # KLL (Karnin-Lang-Liberty) quantiles sketch.
  #
  # State binary layout (v1):
  #   version:         u8  = 1
  #   k:               u32 little-endian
  #   n:               u64 little-endian
  #   min_val:         f64 little-endian (NaN = empty sentinel)
  #   max_val:         f64 little-endian (NaN = empty sentinel)
  #   num_levels:      u8
  #   compaction_bits: ceil(num_levels/8) bytes (1 bit per level parity)
  #   level_sizes:     num_levels x u32 little-endian
  #   items:           sum(level_sizes) x f64 little-endian (level 0 first)
  #
  # Capacity per level: max(2, floor(k * (2/3)^(num_levels - 1 - level)) + 1)
  # Compaction: sort level, select even/odd indexed items based on parity bit,
  #             promote selected to next level, flip parity bit for that level.

  @kll_nan <<0, 0, 0, 0, 0, 0, 248, 127>>

  @impl true
  @spec kll_new(keyword()) :: binary()
  def kll_new(opts) do
    k = Keyword.fetch!(opts, :k)
    # Start with 2 levels. Levels grow dynamically as the top level overflows.
    num_levels = 2
    parity_bytes = div(num_levels + 7, 8)
    compaction_bits = :binary.copy(<<0>>, parity_bytes)
    level_sizes = List.duplicate(0, num_levels)
    levels = List.duplicate([], num_levels)
    kll_encode_state(k, 0, :nan, :nan, num_levels, compaction_bits, level_sizes, levels)
  end

  @impl true
  @spec kll_update(binary(), float(), keyword()) :: binary()
  def kll_update(state_bin, value, _opts) do
    state = kll_decode_state(state_bin)
    state = kll_insert_value(state, value)
    kll_encode_from_map(state)
  end

  @impl true
  @spec kll_update_many(binary(), [float()], keyword()) :: binary()
  def kll_update_many(state_bin, values, _opts) do
    state = kll_decode_state(state_bin)
    state = Enum.reduce(values, state, &kll_insert_value(&2, &1))
    kll_encode_from_map(state)
  end

  @impl true
  @spec kll_merge(binary(), binary(), keyword()) :: binary()
  def kll_merge(state_bin_a, state_bin_b, _opts) do
    a = kll_decode_state(state_bin_a)
    b = kll_decode_state(state_bin_b)
    merged = kll_do_merge(a, b)
    kll_encode_from_map(merged)
  end

  @impl true
  @spec kll_quantile(binary(), float(), keyword()) :: float() | nil
  def kll_quantile(state_bin, rank, _opts) do
    state = kll_decode_state(state_bin)

    cond do
      state.n == 0 ->
        nil

      rank == 0.0 ->
        state.min_val

      rank == 1.0 ->
        state.max_val

      true ->
        sorted_view = kll_build_sorted_view(state)
        kll_query_quantile(sorted_view, state.n, rank)
    end
  end

  @impl true
  @spec kll_rank(binary(), float(), keyword()) :: float() | nil
  def kll_rank(state_bin, value, _opts) do
    state = kll_decode_state(state_bin)

    if state.n == 0 do
      nil
    else
      sorted_view = kll_build_sorted_view(state)
      kll_query_rank(sorted_view, state.n, value)
    end
  end

  @impl true
  @spec kll_count(binary(), keyword()) :: non_neg_integer()
  def kll_count(state_bin, _opts) do
    <<_version::8, _k::unsigned-little-32, n::unsigned-little-64, _rest::binary>> = state_bin
    n
  end

  @impl true
  @spec kll_min(binary(), keyword()) :: float() | nil
  def kll_min(state_bin, _opts) do
    <<_version::8, _k::unsigned-little-32, n::unsigned-little-64, min_bin::binary-size(8),
      _rest::binary>> = state_bin

    if n == 0, do: nil, else: kll_decode_f64_value(min_bin)
  end

  @impl true
  @spec kll_max(binary(), keyword()) :: float() | nil
  def kll_max(state_bin, _opts) do
    <<_version::8, _k::unsigned-little-32, n::unsigned-little-64, _min_bin::binary-size(8),
      max_bin::binary-size(8), _rest::binary>> = state_bin

    if n == 0, do: nil, else: kll_decode_f64_value(max_bin)
  end

  # -- KLL Private Helpers --

  defp kll_level_capacity(k, level, num_levels) do
    # DataSketches-style depth-from-top capacity formula.
    # Bottom levels have small capacity (compact frequently), top levels have
    # large capacity (accumulate many items for better resolution).
    depth = num_levels - 1 - level
    max(2, floor(k * :math.pow(2 / 3, depth)) + 1)
  end

  defp kll_encode_state(k, n, min_val, max_val, num_levels, compaction_bits, level_sizes, items) do
    min_bin = kll_encode_f64(min_val)
    max_bin = kll_encode_f64(max_val)
    parity_bytes = div(num_levels + 7, 8)

    # Pad compaction_bits to correct size
    compaction_bin =
      if byte_size(compaction_bits) < parity_bytes do
        <<compaction_bits::binary, 0::size((parity_bytes - byte_size(compaction_bits)) * 8)>>
      else
        binary_part(compaction_bits, 0, parity_bytes)
      end

    level_sizes_bin =
      level_sizes
      |> Enum.map(fn s -> <<s::unsigned-little-32>> end)
      |> IO.iodata_to_binary()

    items_bin =
      items
      |> List.flatten()
      |> Enum.map(fn v -> <<v::float-little-64>> end)
      |> IO.iodata_to_binary()

    <<
      1::unsigned-8,
      k::unsigned-little-32,
      n::unsigned-little-64,
      min_bin::binary-size(8),
      max_bin::binary-size(8),
      num_levels::unsigned-8,
      compaction_bin::binary,
      level_sizes_bin::binary,
      items_bin::binary
    >>
  end

  defp kll_encode_f64(:nan), do: @kll_nan
  defp kll_encode_f64(val) when is_float(val), do: <<val::float-little-64>>

  defp kll_decode_state(state_bin) do
    <<
      1::unsigned-8,
      k::unsigned-little-32,
      n::unsigned-little-64,
      min_bin::binary-size(8),
      max_bin::binary-size(8),
      num_levels::unsigned-8,
      rest::binary
    >> = state_bin

    min_val = kll_decode_f64(min_bin, n)
    max_val = kll_decode_f64(max_bin, n)

    parity_bytes = div(num_levels + 7, 8)

    <<
      compaction_bits::binary-size(^parity_bytes),
      rest2::binary
    >> = rest

    level_sizes_bytes = num_levels * 4

    <<
      level_sizes_bin::binary-size(^level_sizes_bytes),
      items_bin::binary
    >> = rest2

    level_sizes = kll_decode_u32_list(level_sizes_bin, [])

    levels = kll_decode_levels(items_bin, level_sizes, [])

    %{
      k: k,
      n: n,
      min_val: min_val,
      max_val: max_val,
      num_levels: num_levels,
      compaction_bits: compaction_bits,
      level_sizes: level_sizes,
      levels: levels
    }
  end

  defp kll_decode_f64(@kll_nan, _n), do: :nan
  defp kll_decode_f64(_bin, 0), do: :nan
  defp kll_decode_f64(<<val::float-little-64>>, _n), do: val

  defp kll_decode_f64_value(@kll_nan), do: nil
  defp kll_decode_f64_value(<<val::float-little-64>>), do: val

  defp kll_decode_u32_list(<<>>, acc), do: Enum.reverse(acc)

  defp kll_decode_u32_list(<<v::unsigned-little-32, rest::binary>>, acc) do
    kll_decode_u32_list(rest, [v | acc])
  end

  defp kll_decode_levels(<<>>, [], acc), do: Enum.reverse(acc)

  defp kll_decode_levels(bin, [size | rest_sizes], acc) do
    bytes = size * 8
    <<level_bin::binary-size(^bytes), rest_bin::binary>> = bin
    level = kll_decode_f64_list(level_bin, [])
    kll_decode_levels(rest_bin, rest_sizes, [level | acc])
  end

  defp kll_decode_f64_list(<<>>, acc), do: Enum.reverse(acc)

  defp kll_decode_f64_list(<<v::float-little-64, rest::binary>>, acc) do
    kll_decode_f64_list(rest, [v | acc])
  end

  defp kll_encode_from_map(state) do
    kll_encode_state(
      state.k,
      state.n,
      state.min_val,
      state.max_val,
      state.num_levels,
      state.compaction_bits,
      state.level_sizes,
      state.levels
    )
  end

  defp kll_insert_value(state, value) do
    # Update min/max
    new_min =
      case state.min_val do
        :nan -> value
        cur -> min(cur, value)
      end

    new_max =
      case state.max_val do
        :nan -> value
        cur -> max(cur, value)
      end

    # Insert into level 0
    [level0 | rest_levels] = state.levels
    new_level0 = [value | level0]
    new_level0_size = hd(state.level_sizes) + 1

    state = %{
      state
      | n: state.n + 1,
        min_val: new_min,
        max_val: new_max,
        levels: [new_level0 | rest_levels],
        level_sizes: [new_level0_size | tl(state.level_sizes)]
    }

    # Compact if level 0 is at capacity, then check if top level needs growth
    state = kll_compact_if_needed(state, 0)
    kll_check_grow(state)
  end

  defp kll_compact_if_needed(state, level) do
    # Top level (num_levels - 1) never compacts
    if level >= state.num_levels - 1 do
      state
    else
      capacity = kll_level_capacity(state.k, level, state.num_levels)
      level_size = Enum.at(state.level_sizes, level)

      if level_size >= capacity do
        kll_compact_level(state, level)
      else
        state
      end
    end
  end

  defp kll_check_grow(state) do
    top = state.num_levels - 1
    top_cap = kll_level_capacity(state.k, top, state.num_levels)
    top_size = Enum.at(state.level_sizes, top)

    if top_size >= top_cap do
      kll_grow_levels(state)
    else
      state
    end
  end

  defp kll_grow_levels(state) do
    new_num_levels = state.num_levels + 1

    # Extend levels and sizes with empty new top level
    new_levels = state.levels ++ [[]]
    new_level_sizes = state.level_sizes ++ [0]

    # Extend compaction bits if needed
    new_parity_bytes = div(new_num_levels + 7, 8)
    old_parity_bytes = byte_size(state.compaction_bits)

    new_compaction_bits =
      if new_parity_bytes > old_parity_bytes do
        state.compaction_bits <> <<0>>
      else
        state.compaction_bits
      end

    state = %{
      state
      | num_levels: new_num_levels,
        levels: new_levels,
        level_sizes: new_level_sizes,
        compaction_bits: new_compaction_bits
    }

    # Capacities changed (depth increased for all levels). Recompact from bottom up.
    # The old top level (now at num_levels-2) can now compact.
    state = kll_recompact(state, 0)

    # Check if we need to grow again (very rare)
    kll_check_grow(state)
  end

  defp kll_compact_level(state, level) do
    current_level = Enum.at(state.levels, level)
    sorted = Enum.sort(current_level)

    # Get parity bit for this level
    parity = kll_get_parity(state.compaction_bits, level)

    # Clear-the-level compaction (original KLL paper):
    # Half the items are promoted to the next level, the rest are discarded.
    # The current level is cleared.
    promoted = kll_select_half(sorted, parity)

    # Flip parity bit
    new_compaction_bits = kll_flip_parity(state.compaction_bits, level)

    # Clear current level
    new_levels = List.replace_at(state.levels, level, [])
    new_level_sizes = List.replace_at(state.level_sizes, level, 0)

    # Add promoted items to next level
    next_level = Enum.at(new_levels, level + 1)
    new_next_level = promoted ++ next_level
    new_levels = List.replace_at(new_levels, level + 1, new_next_level)
    new_level_sizes = List.replace_at(new_level_sizes, level + 1, length(new_next_level))

    state = %{
      state
      | levels: new_levels,
        level_sizes: new_level_sizes,
        compaction_bits: new_compaction_bits
    }

    # Recursively compact if next level is now full (but not the top level)
    kll_compact_if_needed(state, level + 1)
  end

  defp kll_get_parity(compaction_bits, level) do
    byte_idx = div(level, 8)
    bit_idx = rem(level, 8)
    <<_::binary-size(^byte_idx), byte::unsigned-8, _::binary>> = compaction_bits
    byte >>> bit_idx &&& 1
  end

  defp kll_flip_parity(compaction_bits, level) do
    byte_idx = div(level, 8)
    bit_idx = rem(level, 8)
    <<before::binary-size(^byte_idx), byte::unsigned-8, after_bytes::binary>> = compaction_bits
    new_byte = bxor(byte, 1 <<< bit_idx)
    <<before::binary, new_byte::unsigned-8, after_bytes::binary>>
  end

  defp kll_select_half(sorted, parity) do
    # parity=0: select even-indexed items (0, 2, 4, ...)
    # parity=1: select odd-indexed items (1, 3, 5, ...)
    sorted
    |> Enum.with_index()
    |> Enum.filter(fn {_val, idx} -> rem(idx, 2) == parity end)
    |> Enum.map(&elem(&1, 0))
  end

  defp kll_build_sorted_view(state) do
    # Build weighted (value, weight) pairs from all levels
    state.levels
    |> Enum.with_index()
    |> Enum.flat_map(fn {level, idx} ->
      weight = 1 <<< idx
      Enum.map(level, fn val -> {val, weight} end)
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp kll_query_quantile(sorted_view, _n, rank) do
    total_weight = Enum.reduce(sorted_view, 0, fn {_val, weight}, acc -> acc + weight end)
    target = rank * total_weight
    kll_walk_quantile(sorted_view, target, 0)
  end

  defp kll_walk_quantile([{val, _weight}], _target, _cumulative), do: val

  defp kll_walk_quantile([{val, weight} | rest], target, cumulative) do
    new_cumulative = cumulative + weight

    if new_cumulative > target do
      val
    else
      kll_walk_quantile(rest, target, new_cumulative)
    end
  end

  defp kll_query_rank(sorted_view, _n, value) do
    total_weight = Enum.reduce(sorted_view, 0, fn {_val, weight}, acc -> acc + weight end)

    weight_below =
      Enum.reduce(sorted_view, 0, fn {val, weight}, acc ->
        if val <= value, do: acc + weight, else: acc
      end)

    weight_below / total_weight
  end

  defp kll_do_merge(a, b) do
    # Handle merging with empty sketch
    {new_min, new_max} =
      case {a.min_val, b.min_val} do
        {:nan, :nan} -> {:nan, :nan}
        {:nan, _} -> {b.min_val, b.max_val}
        {_, :nan} -> {a.min_val, a.max_val}
        _ -> {min(a.min_val, b.min_val), max(a.max_val, b.max_val)}
      end

    new_n = a.n + b.n

    # Merge levels: concatenate items at each level
    max_levels = max(a.num_levels, b.num_levels)

    a_levels = a.levels ++ List.duplicate([], max_levels - a.num_levels)
    b_levels = b.levels ++ List.duplicate([], max_levels - b.num_levels)

    merged_levels = Enum.zip_with(a_levels, b_levels, fn al, bl -> al ++ bl end)
    merged_sizes = Enum.map(merged_levels, &length/1)

    # Merge compaction bits (OR them together)
    a_bits = kll_pad_bits(a.compaction_bits, max_levels)
    b_bits = kll_pad_bits(b.compaction_bits, max_levels)

    merged_bits =
      :binary.bin_to_list(a_bits)
      |> Enum.zip(:binary.bin_to_list(b_bits))
      |> Enum.map(fn {ab, bb} -> bor(ab, bb) end)
      |> :binary.list_to_bin()

    state = %{
      k: a.k,
      n: new_n,
      min_val: new_min,
      max_val: new_max,
      num_levels: max_levels,
      compaction_bits: merged_bits,
      level_sizes: merged_sizes,
      levels: merged_levels
    }

    # Re-compact from bottom up
    kll_recompact(state, 0)
  end

  defp kll_pad_bits(bits, num_levels) do
    needed = div(num_levels + 7, 8)
    current = byte_size(bits)

    if current < needed do
      <<bits::binary, 0::size((needed - current) * 8)>>
    else
      binary_part(bits, 0, needed)
    end
  end

  defp kll_recompact(state, level) when level >= state.num_levels, do: state

  defp kll_recompact(state, level) do
    state = kll_compact_if_needed(state, level)
    kll_recompact(state, level + 1)
  end

  # ============================================================
  # DDSketch Implementation
  # ============================================================
  #
  # DDSketch quantiles sketch using logarithmic bucket mapping.
  #
  # State binary layout (DDS1):
  #   Offset  Size  Field
  #   0       4     Magic "DDS1"
  #   4       1     Version (u8, 1)
  #   5       1     Flags (u8, bit0=negative_support, 0 in v0.2.1)
  #   6       2     Reserved (u16 LE, 0)
  #   8       8     alpha (f64 LE)
  #   16      8     gamma (f64 LE)
  #   24      8     log_gamma (f64 LE)
  #   32      8     min_indexable (f64 LE)
  #   40      8     n (u64 LE)
  #   48      8     zero_count (u64 LE)
  #   56      8     min_value (f64 LE, NaN=empty)
  #   64      8     max_value (f64 LE, NaN=empty)
  #   72      4     sparse_count (u32 LE)
  #   76      4     dense_min_index (i32 LE, 0)
  #   80      4     dense_len (u32 LE, 0)
  #   84      4     reserved2 (u32 LE, 0)
  #   88..    sparse_count * 8  Sparse bins (i32 index + u32 count each)

  @dds_magic "DDS1"

  @impl true
  @spec ddsketch_new(keyword()) :: binary()
  def ddsketch_new(opts) do
    alpha = Keyword.fetch!(opts, :alpha)
    gamma = (1.0 + alpha) / (1.0 - alpha)
    log_gamma = :math.log(gamma)

    # Smallest positive value whose log-index maps cleanly
    min_pos = 5.0e-324
    min_idx = floor(:math.log(min_pos) / log_gamma)
    min_indexable = max(min_pos, :math.exp(log_gamma * (min_idx + 1)))

    dds_encode_from_map(%{
      alpha: alpha,
      gamma: gamma,
      log_gamma: log_gamma,
      min_indexable: min_indexable,
      n: 0,
      zero_count: 0,
      min_value: :nan,
      max_value: :nan,
      bins: []
    })
  end

  @impl true
  @spec ddsketch_update(binary(), float(), keyword()) :: binary()
  def ddsketch_update(state_bin, value, opts) do
    ddsketch_update_many(state_bin, [value], opts)
  end

  @impl true
  @spec ddsketch_update_many(binary(), [float()], keyword()) :: binary()
  def ddsketch_update_many(state_bin, [], _opts), do: state_bin

  def ddsketch_update_many(state_bin, values, _opts) do
    state = dds_decode_state(state_bin)

    {n_delta, zero_delta, index_counts, new_min, new_max} =
      Enum.reduce(
        values,
        {0, 0, %{}, state.min_value, state.max_value},
        fn value, {nd, zd, idx_map, mn, mx} ->
          dds_validate_value!(value)

          new_mn = if mn == :nan, do: value, else: min(mn, value)
          new_mx = if mx == :nan, do: value, else: max(mx, value)

          if value == 0.0 do
            {nd + 1, zd + 1, idx_map, new_mn, new_mx}
          else
            idx = dds_compute_index(value, state.min_indexable, state.log_gamma)
            {nd + 1, zd, Map.update(idx_map, idx, 1, &(&1 + 1)), new_mn, new_mx}
          end
        end
      )

    merged_bins = dds_merge_index_counts(state.bins, index_counts)

    dds_encode_from_map(%{
      state
      | n: state.n + n_delta,
        zero_count: state.zero_count + zero_delta,
        min_value: new_min,
        max_value: new_max,
        bins: merged_bins
    })
  end

  @impl true
  @spec ddsketch_merge(binary(), binary(), keyword()) :: binary()
  def ddsketch_merge(state_bin_a, state_bin_b, _opts) do
    # Validate alpha bytes match (byte comparison, not float equality)
    <<_magic_a::binary-size(8), alpha_a_bytes::binary-size(8), _::binary>> = state_bin_a
    <<_magic_b::binary-size(8), alpha_b_bytes::binary-size(8), _::binary>> = state_bin_b

    a = dds_decode_state(state_bin_a)
    b = dds_decode_state(state_bin_b)

    if alpha_a_bytes != alpha_b_bytes do
      raise ExDataSketch.Errors.IncompatibleSketchesError,
        reason: "DDSketch alpha mismatch: #{a.alpha} vs #{b.alpha}"
    end

    {new_min, new_max} =
      case {a.min_value, b.min_value} do
        {:nan, :nan} -> {:nan, :nan}
        {:nan, _} -> {b.min_value, b.max_value}
        {_, :nan} -> {a.min_value, a.max_value}
        _ -> {min(a.min_value, b.min_value), max(a.max_value, b.max_value)}
      end

    merged_bins = dds_merge_sorted_bins(a.bins, b.bins)

    dds_encode_from_map(%{
      a
      | n: a.n + b.n,
        zero_count: a.zero_count + b.zero_count,
        min_value: new_min,
        max_value: new_max,
        bins: merged_bins
    })
  end

  @impl true
  @spec ddsketch_quantile(binary(), float(), keyword()) :: float() | nil
  def ddsketch_quantile(state_bin, rank, _opts) do
    state = dds_decode_state(state_bin)

    cond do
      state.n == 0 ->
        nil

      rank == 0.0 ->
        state.min_value

      rank == 1.0 ->
        state.max_value

      true ->
        target = rank * state.n
        dds_walk_quantile(state.zero_count, state.bins, state.gamma, target)
    end
  end

  @impl true
  @spec ddsketch_count(binary(), keyword()) :: non_neg_integer()
  def ddsketch_count(state_bin, _opts) do
    <<_pre::binary-size(40), n::unsigned-little-64, _rest::binary>> = state_bin
    n
  end

  @impl true
  @spec ddsketch_min(binary(), keyword()) :: float() | nil
  def ddsketch_min(state_bin, _opts) do
    <<_pre::binary-size(40), n::unsigned-little-64, _zero_count::binary-size(8),
      min_bin::binary-size(8), _rest::binary>> = state_bin

    if n == 0, do: nil, else: dds_decode_f64_value(min_bin)
  end

  @impl true
  @spec ddsketch_max(binary(), keyword()) :: float() | nil
  def ddsketch_max(state_bin, _opts) do
    <<_pre::binary-size(40), n::unsigned-little-64, _zero_count::binary-size(8),
      _min_bin::binary-size(8), max_bin::binary-size(8), _rest::binary>> = state_bin

    if n == 0, do: nil, else: dds_decode_f64_value(max_bin)
  end

  # -- DDSketch Private Helpers --

  defp dds_validate_value!(value) when is_float(value) and value < 0.0 do
    raise ArgumentError, "DDSketch does not support negative values, got: #{value}"
  end

  defp dds_validate_value!(value) when is_float(value) do
    <<_sign::1, exponent::11, _mantissa::52>> = <<value::float-64>>

    if exponent == 2047 do
      raise ArgumentError, "DDSketch does not support NaN or Inf values"
    end
  end

  defp dds_compute_index(value, min_indexable, log_gamma) do
    if value < min_indexable do
      floor(:math.log(min_indexable) / log_gamma)
    else
      floor(:math.log(value) / log_gamma)
    end
  end

  defp dds_decode_state(state_bin) do
    <<
      @dds_magic::binary,
      1::unsigned-8,
      _flags::unsigned-8,
      _reserved::unsigned-little-16,
      alpha::float-little-64,
      gamma::float-little-64,
      log_gamma::float-little-64,
      min_indexable::float-little-64,
      n::unsigned-little-64,
      zero_count::unsigned-little-64,
      min_bin::binary-size(8),
      max_bin::binary-size(8),
      sparse_count::unsigned-little-32,
      _dense_min_index::signed-little-32,
      _dense_len::unsigned-little-32,
      _reserved2::unsigned-little-32,
      body::binary
    >> = state_bin

    bins = dds_decode_sparse_bins(body, sparse_count)
    min_value = dds_decode_f64(min_bin, n)
    max_value = dds_decode_f64(max_bin, n)

    %{
      alpha: alpha,
      gamma: gamma,
      log_gamma: log_gamma,
      min_indexable: min_indexable,
      n: n,
      zero_count: zero_count,
      min_value: min_value,
      max_value: max_value,
      bins: bins
    }
  end

  defp dds_decode_f64(@kll_nan, _n), do: :nan
  defp dds_decode_f64(_bin, 0), do: :nan
  defp dds_decode_f64(<<val::float-little-64>>, _n), do: val

  defp dds_decode_f64_value(@kll_nan), do: nil
  defp dds_decode_f64_value(<<val::float-little-64>>), do: val

  defp dds_encode_f64(:nan), do: @kll_nan
  defp dds_encode_f64(val) when is_float(val), do: <<val::float-little-64>>

  defp dds_encode_from_map(state) do
    min_bin = dds_encode_f64(state.min_value)
    max_bin = dds_encode_f64(state.max_value)
    sparse_count = length(state.bins)
    bins_bin = dds_encode_sparse_bins(state.bins)

    <<
      @dds_magic::binary,
      1::unsigned-8,
      0::unsigned-8,
      0::unsigned-little-16,
      state.alpha::float-little-64,
      state.gamma::float-little-64,
      state.log_gamma::float-little-64,
      state.min_indexable::float-little-64,
      state.n::unsigned-little-64,
      state.zero_count::unsigned-little-64,
      min_bin::binary,
      max_bin::binary,
      sparse_count::unsigned-little-32,
      0::signed-little-32,
      0::unsigned-little-32,
      0::unsigned-little-32,
      bins_bin::binary
    >>
  end

  defp dds_decode_sparse_bins(_body, 0), do: []

  defp dds_decode_sparse_bins(body, count) do
    dds_decode_bins_acc(body, count, []) |> Enum.reverse()
  end

  defp dds_decode_bins_acc(_bin, 0, acc), do: acc

  defp dds_decode_bins_acc(
         <<index::signed-little-32, count::unsigned-little-32, rest::binary>>,
         remaining,
         acc
       ) do
    dds_decode_bins_acc(rest, remaining - 1, [{index, count} | acc])
  end

  defp dds_encode_sparse_bins(bins) do
    bins
    |> Enum.map(fn {index, count} ->
      <<index::signed-little-32, count::unsigned-little-32>>
    end)
    |> IO.iodata_to_binary()
  end

  defp dds_merge_index_counts(existing_bins, new_counts) when map_size(new_counts) == 0 do
    existing_bins
  end

  defp dds_merge_index_counts(existing_bins, new_counts) do
    existing_map = Map.new(existing_bins)
    merged = Map.merge(existing_map, new_counts, fn _k, v1, v2 -> v1 + v2 end)
    merged |> Enum.sort_by(&elem(&1, 0))
  end

  defp dds_merge_sorted_bins([], bs), do: bs
  defp dds_merge_sorted_bins(as, []), do: as

  defp dds_merge_sorted_bins([{ia, ca} | rest_a], [{ib, cb} | rest_b]) do
    cond do
      ia < ib -> [{ia, ca} | dds_merge_sorted_bins(rest_a, [{ib, cb} | rest_b])]
      ia > ib -> [{ib, cb} | dds_merge_sorted_bins([{ia, ca} | rest_a], rest_b)]
      true -> [{ia, ca + cb} | dds_merge_sorted_bins(rest_a, rest_b)]
    end
  end

  defp dds_walk_quantile(zero_count, bins, gamma, target) do
    cumulative = zero_count

    if cumulative >= target and zero_count > 0 do
      0.0
    else
      dds_walk_bins(bins, gamma, target, cumulative)
    end
  end

  defp dds_walk_bins([], _gamma, _target, _cumulative), do: nil

  defp dds_walk_bins([{index, count} | rest], gamma, target, cumulative) do
    new_cumulative = cumulative + count

    if new_cumulative >= target do
      dds_bucket_midpoint(gamma, index)
    else
      dds_walk_bins(rest, gamma, target, new_cumulative)
    end
  end

  defp dds_bucket_midpoint(gamma, index) do
    2.0 * :math.pow(gamma, index + 1) / (gamma + 1.0)
  end
end
