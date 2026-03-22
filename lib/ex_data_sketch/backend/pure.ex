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
    remaining_mask = (1 <<< bits) - 1

    <<header::binary-size(4), registers::binary-size(^m)>> = state_bin

    # Pre-aggregate hashes into a map of {bucket => max_rank}.
    # Avoids per-hash tuple copies from put_elem.
    updates =
      List.foldl(hashes, %{}, fn hash64, acc ->
        bucket = hash64 >>> (64 - p)
        remaining = hash64 &&& remaining_mask
        rank = count_leading_zeros(remaining, bits) + 1
        Map.update(acc, bucket, rank, &max(&1, rank))
      end)

    sorted_updates = updates |> Map.to_list() |> List.keysort(0)
    new_registers = hll_splice_updates(registers, sorted_updates, 0, [])
    <<header::binary, IO.iodata_to_binary(new_registers)::binary>>
  end

  defp hll_splice_updates(rest, [], _offset, acc), do: Enum.reverse([rest | acc])

  defp hll_splice_updates(registers, [{bucket, new_val} | tail], offset, acc) do
    skip = bucket - offset
    <<before::binary-size(^skip), old_val::unsigned-8, after_bytes::binary>> = registers
    val = max(old_val, new_val)
    hll_splice_updates(after_bytes, tail, bucket + 1, [val, before | acc])
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

  @impl true
  @spec kll_cdf(binary(), [float()], keyword()) :: [float()] | nil
  def kll_cdf(state_bin, split_points, _opts) do
    state = kll_decode_state(state_bin)

    if state.n == 0 do
      nil
    else
      sorted_view = kll_build_sorted_view(state)
      Enum.map(split_points, fn sp -> kll_query_rank(sorted_view, state.n, sp) end)
    end
  end

  @impl true
  @spec kll_pmf(binary(), [float()], keyword()) :: [float()] | nil
  def kll_pmf(state_bin, split_points, _opts) do
    state = kll_decode_state(state_bin)

    if state.n == 0 do
      nil
    else
      sorted_view = kll_build_sorted_view(state)
      cdf_values = Enum.map(split_points, fn sp -> kll_query_rank(sorted_view, state.n, sp) end)

      # PMF returns m+1 bins: (-inf, s1], (s1, s2], ..., (sm, +inf)
      cdf_with_bounds = [0.0] ++ cdf_values ++ [1.0]

      cdf_with_bounds
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.map(fn [a, b] -> b - a end)
    end
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

  @impl true
  @spec ddsketch_rank(binary(), float(), keyword()) :: float() | nil
  def ddsketch_rank(state_bin, value, _opts) do
    state = dds_decode_state(state_bin)
    dds_compute_rank(state, value)
  end

  defp dds_compute_rank(%{n: 0}, _value), do: nil
  defp dds_compute_rank(_state, value) when value < 0.0, do: 0.0

  defp dds_compute_rank(state, value) do
    value_index = dds_compute_index(value, state.min_indexable, state.log_gamma)

    cumulative =
      Enum.reduce(state.bins, state.zero_count, fn {index, count}, acc ->
        if index <= value_index, do: acc + count, else: acc
      end)

    cumulative / state.n
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

  # ============================================================
  # Bloom Filter Implementation
  # ============================================================
  #
  # Binary layout (BLM1):
  #   HEADER (40 bytes, all little-endian):
  #     0:4   magic              "BLM1"
  #     4:1   version            u8 = 1
  #     5:1   hash_scheme        u8 = 0 (double hashing)
  #     6:2   hash_count         u16
  #     8:4   bit_count          u32
  #     12:4  seed               u32
  #     16:8  target_fpr         f64
  #     24:4  capacity_hint      u32
  #     28:4  bitset_byte_length u32
  #     32:8  reserved           must be 0
  #
  #   BODY (bitset_byte_length bytes):
  #     LSB-first packed bit array. Padding bits in last byte are zero.

  @blm_magic "BLM1"
  @blm_version 1
  @blm_header_size 40

  @impl true
  @spec bloom_new(keyword()) :: binary()
  def bloom_new(opts) do
    bit_count = Keyword.fetch!(opts, :bit_count)
    hash_count = Keyword.fetch!(opts, :hash_count)
    seed = Keyword.get(opts, :seed, 0)
    capacity = Keyword.get(opts, :capacity, 0)
    fpr = Keyword.get(opts, :false_positive_rate, 0.0)
    bitset_byte_length = div(bit_count + 7, 8)
    bitset = :binary.copy(<<0>>, bitset_byte_length)

    <<
      @blm_magic::binary,
      @blm_version::unsigned-8,
      0::unsigned-8,
      hash_count::unsigned-little-16,
      bit_count::unsigned-little-32,
      seed::unsigned-little-32,
      fpr::float-little-64,
      capacity::unsigned-little-32,
      bitset_byte_length::unsigned-little-32,
      0::unsigned-little-64,
      bitset::binary
    >>
  end

  @impl true
  @spec bloom_put(binary(), non_neg_integer(), keyword()) :: binary()
  def bloom_put(state_bin, hash64, _opts) do
    <<header::binary-size(@blm_header_size), bitset::binary>> = state_bin

    <<
      @blm_magic::binary,
      @blm_version::unsigned-8,
      _scheme::unsigned-8,
      hash_count::unsigned-little-16,
      bit_count::unsigned-little-32,
      _rest_header::binary
    >> = header

    h1 = hash64 >>> 32
    h2 = hash64 &&& 0xFFFFFFFF

    new_bitset =
      Enum.reduce(0..(hash_count - 1), bitset, fn i, bs ->
        pos = rem(h1 + i * h2, bit_count)
        blm_set_bit(bs, pos)
      end)

    <<header::binary, new_bitset::binary>>
  end

  @impl true
  @spec bloom_put_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def bloom_put_many(state_bin, [], _opts), do: state_bin

  def bloom_put_many(state_bin, hashes, _opts) do
    <<header::binary-size(@blm_header_size), bitset::binary>> = state_bin

    <<
      @blm_magic::binary,
      @blm_version::unsigned-8,
      _scheme::unsigned-8,
      hash_count::unsigned-little-16,
      bit_count::unsigned-little-32,
      _rest_header::binary
    >> = header

    # Convert bitset to mutable tuple for O(1) updates
    bytes_tuple = bitset |> :binary.bin_to_list() |> List.to_tuple()

    new_tuple =
      Enum.reduce(hashes, bytes_tuple, fn hash64, bt ->
        h1 = hash64 >>> 32
        h2 = hash64 &&& 0xFFFFFFFF

        Enum.reduce(0..(hash_count - 1), bt, fn i, bt2 ->
          pos = rem(h1 + i * h2, bit_count)
          byte_idx = div(pos, 8)
          bit_idx = rem(pos, 8)
          old_byte = elem(bt2, byte_idx)
          put_elem(bt2, byte_idx, old_byte ||| 1 <<< bit_idx)
        end)
      end)

    new_bitset = new_tuple |> Tuple.to_list() |> :binary.list_to_bin()
    <<header::binary, new_bitset::binary>>
  end

  @impl true
  @spec bloom_member?(binary(), non_neg_integer(), keyword()) :: boolean()
  def bloom_member?(state_bin, hash64, _opts) do
    <<
      @blm_magic::binary,
      @blm_version::unsigned-8,
      _scheme::unsigned-8,
      hash_count::unsigned-little-16,
      bit_count::unsigned-little-32,
      _seed::unsigned-little-32,
      _fpr::binary-size(8),
      _capacity::unsigned-little-32,
      _bsl::unsigned-little-32,
      _reserved::binary-size(8),
      bitset::binary
    >> = state_bin

    h1 = hash64 >>> 32
    h2 = hash64 &&& 0xFFFFFFFF

    Enum.all?(0..(hash_count - 1), fn i ->
      pos = rem(h1 + i * h2, bit_count)
      byte_idx = div(pos, 8)
      bit_idx = rem(pos, 8)
      <<_before::binary-size(^byte_idx), byte::unsigned-8, _::binary>> = bitset
      (byte &&& 1 <<< bit_idx) != 0
    end)
  end

  @impl true
  @spec bloom_merge(binary(), binary(), keyword()) :: binary()
  def bloom_merge(state_bin_a, state_bin_b, _opts) do
    <<header_a::binary-size(@blm_header_size), bitset_a::binary>> = state_bin_a
    <<_header_b::binary-size(@blm_header_size), bitset_b::binary>> = state_bin_b

    # Bitwise OR the two bitsets
    new_bitset = blm_bitwise_or(bitset_a, bitset_b)

    <<header_a::binary, new_bitset::binary>>
  end

  @impl true
  @spec bloom_count(binary(), keyword()) :: non_neg_integer()
  def bloom_count(state_bin, _opts) do
    <<_header::binary-size(@blm_header_size), bitset::binary>> = state_bin
    blm_popcount(bitset)
  end

  # -- Bloom private helpers --

  defp blm_set_bit(bitset, pos) do
    byte_idx = div(pos, 8)
    bit_idx = rem(pos, 8)
    <<before::binary-size(^byte_idx), byte::unsigned-8, rest::binary>> = bitset
    <<before::binary, byte ||| 1 <<< bit_idx::unsigned-8, rest::binary>>
  end

  defp blm_bitwise_or(a, b) do
    a_bytes = :binary.bin_to_list(a)
    b_bytes = :binary.bin_to_list(b)

    Enum.zip(a_bytes, b_bytes)
    |> Enum.map(fn {ba, bb} -> ba ||| bb end)
    |> :binary.list_to_bin()
  end

  defp blm_popcount(bitset) do
    bitset
    |> :binary.bin_to_list()
    |> Enum.reduce(0, fn byte, acc -> acc + blm_byte_popcount(byte) end)
  end

  defp blm_byte_popcount(byte) do
    # Kernighan's bit counting
    blm_byte_popcount_acc(byte, 0)
  end

  defp blm_byte_popcount_acc(0, acc), do: acc

  defp blm_byte_popcount_acc(byte, acc) do
    blm_byte_popcount_acc(byte &&& byte - 1, acc + 1)
  end

  # ============================================================
  # Cuckoo Filter Implementation
  # ============================================================
  #
  # Binary layout (CKO1):
  #   HEADER (32 bytes, all little-endian):
  #     0:4   magic              "CKO1"
  #     4:1   version            u8 = 1
  #     5:1   fingerprint_bits   u8 (f)
  #     6:1   bucket_size        u8 (b)
  #     7:1   flags              u8 = 0
  #     8:4   bucket_count       u32 (m, power of 2)
  #     12:4  item_count         u32
  #     16:4  seed               u32
  #     20:4  max_kicks           u32
  #     24:8  reserved           must be 0
  #
  #   BODY (bucket_count * bucket_size * fp_bytes bytes):
  #     Flat array of fingerprint entries, each ceil(f/8) bytes LE.
  #     Empty slot = fingerprint value 0.

  defmodule CkoCtx do
    @moduledoc false
    defstruct [
      :fp_bits,
      :bucket_size,
      :bucket_count,
      :item_count,
      :seed,
      :max_kicks,
      :fp_bytes,
      :body
    ]
  end

  @cko_magic "CKO1"
  @cko_version 1

  @impl true
  @spec cuckoo_new(keyword()) :: binary()
  def cuckoo_new(opts) do
    fp_bits = Keyword.fetch!(opts, :fingerprint_size)
    bucket_size = Keyword.fetch!(opts, :bucket_size)
    bucket_count = Keyword.fetch!(opts, :bucket_count)
    seed = Keyword.get(opts, :seed, 0)
    max_kicks = Keyword.get(opts, :max_kicks, 500)

    fp_bytes = div(fp_bits + 7, 8)
    body_size = bucket_count * bucket_size * fp_bytes
    body = :binary.copy(<<0>>, body_size)

    <<
      @cko_magic::binary,
      @cko_version::unsigned-8,
      fp_bits::unsigned-8,
      bucket_size::unsigned-8,
      0::unsigned-8,
      bucket_count::unsigned-little-32,
      0::unsigned-little-32,
      seed::unsigned-little-32,
      max_kicks::unsigned-little-32,
      0::unsigned-little-64,
      body::binary
    >>
  end

  @impl true
  @spec cuckoo_put(binary(), non_neg_integer(), keyword()) ::
          {:ok, binary()} | {:error, :full}
  def cuckoo_put(state_bin, hash64, _opts) do
    ctx = cko_decode_header(state_bin)
    {i1, fp, i2} = cko_derive_indices(ctx, hash64)

    case cko_find_empty_slot(ctx, i1) do
      {:ok, slot} ->
        ctx = cko_write_slot(ctx, i1, slot, fp)
        {:ok, cko_encode(%{ctx | item_count: ctx.item_count + 1})}

      :full ->
        case cko_find_empty_slot(ctx, i2) do
          {:ok, slot} ->
            ctx = cko_write_slot(ctx, i2, slot, fp)
            {:ok, cko_encode(%{ctx | item_count: ctx.item_count + 1})}

          :full ->
            cko_kick_insert(ctx, i1, i2, fp)
        end
    end
  end

  @impl true
  @spec cuckoo_put_many(binary(), [non_neg_integer()], keyword()) ::
          {:ok, binary()} | {:error, :full, binary()}
  def cuckoo_put_many(state_bin, [], _opts), do: {:ok, state_bin}

  def cuckoo_put_many(state_bin, hashes, opts) do
    Enum.reduce_while(hashes, {:ok, state_bin}, fn hash, {:ok, state} ->
      case cuckoo_put(state, hash, opts) do
        {:ok, new_state} -> {:cont, {:ok, new_state}}
        {:error, :full} -> {:halt, {:error, :full, state}}
      end
    end)
  end

  @impl true
  @spec cuckoo_member?(binary(), non_neg_integer(), keyword()) :: boolean()
  def cuckoo_member?(state_bin, hash64, _opts) do
    ctx = cko_decode_header(state_bin)
    {i1, fp, i2} = cko_derive_indices(ctx, hash64)

    cko_bucket_contains?(ctx, i1, fp) or cko_bucket_contains?(ctx, i2, fp)
  end

  @impl true
  @spec cuckoo_delete(binary(), non_neg_integer(), keyword()) ::
          {:ok, binary()} | {:error, :not_found}
  def cuckoo_delete(state_bin, hash64, _opts) do
    ctx = cko_decode_header(state_bin)
    {i1, fp, i2} = cko_derive_indices(ctx, hash64)

    cond do
      (slot = cko_find_fp_slot(ctx, i1, fp)) != nil ->
        ctx = cko_write_slot(ctx, i1, slot, 0)
        {:ok, cko_encode(%{ctx | item_count: ctx.item_count - 1})}

      (slot = cko_find_fp_slot(ctx, i2, fp)) != nil ->
        ctx = cko_write_slot(ctx, i2, slot, 0)
        {:ok, cko_encode(%{ctx | item_count: ctx.item_count - 1})}

      true ->
        {:error, :not_found}
    end
  end

  @impl true
  @spec cuckoo_count(binary(), keyword()) :: non_neg_integer()
  def cuckoo_count(state_bin, _opts) do
    <<_magic::binary-size(4), _version::8, _fp::8, _bs::8, _flags::8, _bc::unsigned-little-32,
      item_count::unsigned-little-32, _rest::binary>> = state_bin

    item_count
  end

  # -- Cuckoo private helpers --

  defp cko_decode_header(state_bin) do
    <<
      @cko_magic::binary,
      @cko_version::unsigned-8,
      fp_bits::unsigned-8,
      bucket_size::unsigned-8,
      _flags::unsigned-8,
      bucket_count::unsigned-little-32,
      item_count::unsigned-little-32,
      seed::unsigned-little-32,
      max_kicks::unsigned-little-32,
      _reserved::binary-size(8),
      body::binary
    >> = state_bin

    %CkoCtx{
      fp_bits: fp_bits,
      bucket_size: bucket_size,
      bucket_count: bucket_count,
      item_count: item_count,
      seed: seed,
      max_kicks: max_kicks,
      fp_bytes: div(fp_bits + 7, 8),
      body: body
    }
  end

  defp cko_encode(%CkoCtx{} = ctx) do
    <<
      @cko_magic::binary,
      @cko_version::unsigned-8,
      ctx.fp_bits::unsigned-8,
      ctx.bucket_size::unsigned-8,
      0::unsigned-8,
      ctx.bucket_count::unsigned-little-32,
      ctx.item_count::unsigned-little-32,
      ctx.seed::unsigned-little-32,
      ctx.max_kicks::unsigned-little-32,
      0::unsigned-little-64,
      ctx.body::binary
    >>
  end

  defp cko_derive_indices(%CkoCtx{} = ctx, hash64) do
    fp_mask = (1 <<< ctx.fp_bits) - 1
    i1 = hash64 &&& ctx.bucket_count - 1
    fp = cko_ensure_nonzero(hash64 >>> 32 &&& fp_mask)
    i2 = cko_alt_index(i1, fp, ctx.bucket_count)
    {i1, fp, i2}
  end

  defp cko_ensure_nonzero(0), do: 1
  defp cko_ensure_nonzero(fp), do: fp

  defp cko_alt_index(index, fingerprint, bucket_count) do
    bxor(index, cko_fp_hash(fingerprint)) &&& bucket_count - 1
  end

  defp cko_fp_hash(fingerprint) do
    h = fingerprint * 0x5BD1E995 &&& 0xFFFFFFFF
    h = bxor(h, h >>> 13) &&& 0xFFFFFFFF
    h * 0x5BD1E995 &&& 0xFFFFFFFF
  end

  defp cko_slot_offset(%CkoCtx{} = ctx, bucket_idx, slot_idx) do
    bucket_idx * ctx.bucket_size * ctx.fp_bytes + slot_idx * ctx.fp_bytes
  end

  defp cko_read_slot(%CkoCtx{} = ctx, bucket_idx, slot_idx) do
    offset = cko_slot_offset(ctx, bucket_idx, slot_idx)

    case ctx.fp_bytes do
      1 ->
        <<_::binary-size(^offset), val::unsigned-8, _::binary>> = ctx.body
        val

      2 ->
        <<_::binary-size(^offset), val::unsigned-little-16, _::binary>> = ctx.body
        val
    end
  end

  defp cko_write_slot(%CkoCtx{} = ctx, bucket_idx, slot_idx, value) do
    offset = cko_slot_offset(ctx, bucket_idx, slot_idx)

    new_body =
      case ctx.fp_bytes do
        1 ->
          <<before::binary-size(^offset), _::unsigned-8, rest::binary>> = ctx.body
          <<before::binary, value::unsigned-8, rest::binary>>

        2 ->
          <<before::binary-size(^offset), _::unsigned-little-16, rest::binary>> = ctx.body
          <<before::binary, value::unsigned-little-16, rest::binary>>
      end

    %{ctx | body: new_body}
  end

  defp cko_find_empty_slot(%CkoCtx{} = ctx, bucket_idx) do
    Enum.reduce_while(0..(ctx.bucket_size - 1), :full, fn slot, _acc ->
      if cko_read_slot(ctx, bucket_idx, slot) == 0 do
        {:halt, {:ok, slot}}
      else
        {:cont, :full}
      end
    end)
  end

  defp cko_bucket_contains?(%CkoCtx{} = ctx, bucket_idx, fingerprint) do
    Enum.any?(0..(ctx.bucket_size - 1), fn slot ->
      cko_read_slot(ctx, bucket_idx, slot) == fingerprint
    end)
  end

  defp cko_find_fp_slot(%CkoCtx{} = ctx, bucket_idx, fingerprint) do
    Enum.reduce_while(0..(ctx.bucket_size - 1), nil, fn slot, _acc ->
      if cko_read_slot(ctx, bucket_idx, slot) == fingerprint do
        {:halt, slot}
      else
        {:cont, nil}
      end
    end)
  end

  defp cko_kick_insert(%CkoCtx{} = ctx, i1, i2, fp) do
    evict_bucket = if rem(fp, 2) == 0, do: i1, else: i2
    cko_kick_loop(ctx, evict_bucket, fp, 0)
  end

  defp cko_kick_loop(%CkoCtx{max_kicks: max_kicks}, _bucket, _fp, kick_count)
       when kick_count >= max_kicks do
    {:error, :full}
  end

  defp cko_kick_loop(%CkoCtx{} = ctx, bucket, fp, kick_count) do
    evict_slot = rem(fp + kick_count, ctx.bucket_size)

    old_fp = cko_read_slot(ctx, bucket, evict_slot)
    ctx = cko_write_slot(ctx, bucket, evict_slot, fp)

    alt_bucket = cko_alt_index(bucket, old_fp, ctx.bucket_count)

    case cko_find_empty_slot(ctx, alt_bucket) do
      {:ok, slot} ->
        ctx = cko_write_slot(ctx, alt_bucket, slot, old_fp)
        {:ok, cko_encode(%{ctx | item_count: ctx.item_count + 1})}

      :full ->
        cko_kick_loop(ctx, alt_bucket, old_fp, kick_count + 1)
    end
  end

  # ============================================================
  # FrequentItems Implementation (SpaceSaving)
  # ============================================================
  #
  # Binary layout (FI1):
  #   HEADER (32 bytes, all little-endian):
  #     0:4   magic         "FI1\0"
  #     4:1   version       u8 = 1
  #     5:1   flags         u8 (key encoding)
  #     6:2   reserved      u16 = 0
  #     8:4   k             u32
  #     12:8  n             u64 (total observed items)
  #     20:4  entry_count   u32
  #     24:4  reserved2     u32 = 0
  #     28:4  reserved3     u32 = 0
  #
  #   BODY (entry_count entries, sorted by item_bytes ascending):
  #     item_len:4    u32
  #     item_bytes    item_len bytes
  #     count:8       u64
  #     error:8       u64

  @fi_magic "FI1\0"
  @fi_version 1

  @impl true
  @spec fi_new(keyword()) :: binary()
  def fi_new(opts) do
    k = Keyword.fetch!(opts, :k)
    flags = Keyword.get(opts, :flags, 0)
    fi_encode_state(k, flags, 0, [])
  end

  @impl true
  def fi_update(state_bin, item_bytes, opts) do
    fi_update_many(state_bin, [item_bytes], opts)
  end

  @impl true
  def fi_update_many(state_bin, items, _opts) do
    {k, flags, n, entries} = fi_decode_state(state_bin)

    # Pre-aggregate: count occurrences of each unique item_bytes
    freq = Enum.frequencies(items)
    batch_n = length(items)

    # Apply weighted updates in sorted key order for determinism
    updated_entries =
      freq
      |> Enum.sort_by(fn {ib, _w} -> ib end)
      |> Enum.reduce(entries, fn {item_bytes, weight}, acc ->
        fi_apply_weighted_update(acc, k, item_bytes, weight)
      end)

    fi_encode_state(k, flags, n + batch_n, updated_entries)
  end

  @impl true
  def fi_merge(state_a, state_b, _opts) do
    {k_a, flags, n_a, entries_a} = fi_decode_state(state_a)
    {_k_b, _flags_b, n_b, entries_b} = fi_decode_state(state_b)

    # Combine counts additively across union of keys
    map_a = Map.new(entries_a)
    map_b = Map.new(entries_b)

    combined =
      Map.merge(map_a, map_b, fn _key, {count_a, error_a}, {count_b, error_b} ->
        {count_a + count_b, error_a + error_b}
      end)

    # Keep top-k by count (ties: smallest key), sort by key for canonical encoding
    merged_entries =
      if map_size(combined) <= k_a do
        combined |> Enum.sort_by(fn {ib, _} -> ib end)
      else
        combined
        |> Enum.sort_by(fn {ib, {count, _err}} -> {-count, ib} end)
        |> Enum.take(k_a)
        |> Enum.sort_by(fn {ib, _} -> ib end)
      end

    fi_encode_state(k_a, flags, n_a + n_b, merged_entries)
  end

  @impl true
  def fi_estimate(state_bin, item_bytes, _opts) do
    {_k, _flags, _n, entries} = fi_decode_state(state_bin)

    case List.keyfind(entries, item_bytes, 0) do
      {^item_bytes, {count, error}} ->
        {:ok,
         %{
           estimate: count,
           error: error,
           lower: max(count - error, 0),
           upper: count
         }}

      nil ->
        {:error, :not_tracked}
    end
  end

  @impl true
  def fi_top_k(state_bin, limit, _opts) do
    {_k, _flags, _n, entries} = fi_decode_state(state_bin)

    entries
    |> Enum.sort_by(fn {ib, {count, _error}} -> {-count, ib} end)
    |> Enum.take(limit)
    |> Enum.map(fn {item_bytes, {count, error}} ->
      %{
        item: item_bytes,
        estimate: count,
        error: error,
        lower: max(count - error, 0),
        upper: count
      }
    end)
  end

  @impl true
  def fi_count(
        <<@fi_magic, @fi_version, _flags::8, _reserved::16, _k::unsigned-little-32,
          n::unsigned-little-64, _rest::binary>>,
        _opts
      ) do
    n
  end

  @impl true
  def fi_entry_count(
        <<@fi_magic, @fi_version, _flags::8, _reserved::16, _k::unsigned-little-32,
          _n::unsigned-little-64, entry_count::unsigned-little-32, _rest::binary>>,
        _opts
      ) do
    entry_count
  end

  # -- FrequentItems private helpers --

  defp fi_apply_weighted_update(entries, k, item_bytes, weight) do
    case List.keyfind(entries, item_bytes, 0) do
      {^item_bytes, {count, error}} ->
        List.keyreplace(entries, item_bytes, 0, {item_bytes, {count + weight, error}})

      nil when length(entries) < k ->
        [{item_bytes, {weight, 0}} | entries]
        |> Enum.sort_by(fn {ib, _} -> ib end)

      nil ->
        fi_evict_and_insert(entries, item_bytes, weight)
    end
  end

  defp fi_evict_and_insert(entries, item_bytes, weight) do
    {evicted_ib, {min_count, _min_error}} =
      Enum.min_by(entries, fn {ib, {count, _err}} -> {count, ib} end)

    remaining = List.keydelete(entries, evicted_ib, 0)

    [{item_bytes, {min_count + weight, min_count}} | remaining]
    |> Enum.sort_by(fn {ib, _} -> ib end)
  end

  defp fi_decode_state(
         <<@fi_magic, @fi_version, flags::unsigned-8, _reserved::unsigned-little-16,
           k::unsigned-little-32, n::unsigned-little-64, entry_count::unsigned-little-32,
           _reserved2::unsigned-little-32, _reserved3::unsigned-little-32, body::binary>>
       ) do
    entries = fi_decode_entries(body, entry_count, [])
    {k, flags, n, entries}
  end

  defp fi_decode_entries(_body, 0, acc), do: Enum.reverse(acc)

  defp fi_decode_entries(
         <<item_len::unsigned-little-32, item_bytes::binary-size(item_len),
           count::unsigned-little-64, error::unsigned-little-64, rest::binary>>,
         remaining,
         acc
       ) do
    fi_decode_entries(rest, remaining - 1, [{item_bytes, {count, error}} | acc])
  end

  defp fi_encode_state(k, flags, n, entries) do
    sorted = Enum.sort_by(entries, fn {ib, _} -> ib end)
    entry_count = length(sorted)
    body = fi_encode_entries(sorted)

    <<@fi_magic::binary, @fi_version::unsigned-8, flags::unsigned-8, 0::unsigned-little-16,
      k::unsigned-little-32, n::unsigned-little-64, entry_count::unsigned-little-32,
      0::unsigned-little-32, 0::unsigned-little-32, body::binary>>
  end

  defp fi_encode_entries(entries) do
    entries
    |> Enum.map(fn {item_bytes, {count, error}} ->
      item_len = byte_size(item_bytes)

      <<item_len::unsigned-little-32, item_bytes::binary, count::unsigned-little-64,
        error::unsigned-little-64>>
    end)
    |> IO.iodata_to_binary()
  end

  # ============================================================
  # Quotient Filter Implementation
  # ============================================================
  #
  # Binary layout (QOT1):
  #   HEADER (32 bytes, all little-endian):
  #     0:4   magic         "QOT1"
  #     4:1   version       u8 = 1
  #     5:1   q             u8 (quotient bits)
  #     6:1   r             u8 (remainder bits)
  #     7:1   flags         u8 = 0 (reserved)
  #     8:4   slot_count    u32 (= 2^q)
  #     12:4  item_count    u32
  #     16:4  seed          u32
  #     20:12 reserved      must be 0
  #
  #   BODY (slot_count slots, each (3 + r) bits, byte-aligned):
  #     Per slot: meta(3 bits) | remainder(r bits), packed into ceil((3+r)/8) bytes.
  #     meta bit0 = is_occupied, bit1 = is_continuation, bit2 = is_shifted.

  defmodule QotCtx do
    @moduledoc false
    defstruct [:q, :r, :slot_count, :item_count, :seed, :slot_bytes, :slots]
  end

  defmodule CqfCtx do
    @moduledoc false
    defstruct [:q, :r, :slot_count, :occupied_count, :total_count, :seed, :slot_bytes, :slots]
  end

  @qot_magic "QOT1"
  @qot_version 1

  @qot_occ 1
  @qot_con 2
  @qot_shi 4

  @impl true
  @spec quotient_new(keyword()) :: binary()
  def quotient_new(opts) do
    q = Keyword.fetch!(opts, :q)
    r = Keyword.fetch!(opts, :r)
    slot_count = Keyword.fetch!(opts, :slot_count)
    seed = Keyword.get(opts, :seed, 0)

    slot_bytes = div(3 + r + 7, 8)
    body_size = slot_count * slot_bytes
    body = :binary.copy(<<0>>, body_size)

    <<
      @qot_magic::binary,
      @qot_version::unsigned-8,
      q::unsigned-8,
      r::unsigned-8,
      0::unsigned-8,
      slot_count::unsigned-little-32,
      0::unsigned-little-32,
      seed::unsigned-little-32,
      0::unsigned-little-64,
      0::unsigned-little-32,
      body::binary
    >>
  end

  @impl true
  @spec quotient_put(binary(), non_neg_integer(), keyword()) :: binary()
  def quotient_put(state_bin, hash64, _opts) do
    ctx = qot_decode(state_bin)
    {q, r} = qot_split_hash(ctx, hash64)

    if qot_lookup?(ctx, q, r) do
      qot_encode(ctx)
    else
      ctx = qot_do_insert(ctx, q, r)
      qot_encode(%{ctx | item_count: ctx.item_count + 1})
    end
  end

  @impl true
  @spec quotient_put_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def quotient_put_many(state_bin, [], _opts), do: state_bin

  def quotient_put_many(state_bin, hashes, _opts) do
    ctx = qot_decode(state_bin)

    ctx =
      Enum.reduce(hashes, ctx, fn hash64, acc ->
        {q, r} = qot_split_hash(acc, hash64)

        if qot_lookup?(acc, q, r) do
          acc
        else
          new_acc = qot_do_insert(acc, q, r)
          %{new_acc | item_count: new_acc.item_count + 1}
        end
      end)

    qot_encode(ctx)
  end

  @impl true
  @spec quotient_member?(binary(), non_neg_integer(), keyword()) :: boolean()
  def quotient_member?(state_bin, hash64, _opts) do
    ctx = qot_decode(state_bin)
    {q, r} = qot_split_hash(ctx, hash64)
    qot_lookup?(ctx, q, r)
  end

  @impl true
  @spec quotient_delete(binary(), non_neg_integer(), keyword()) :: binary()
  def quotient_delete(state_bin, hash64, _opts) do
    ctx = qot_decode(state_bin)
    {q, r} = qot_split_hash(ctx, hash64)

    case qot_find_slot(ctx, q, r) do
      nil ->
        qot_encode(ctx)

      slot_idx ->
        ctx = qot_do_delete(ctx, q, slot_idx)
        qot_encode(%{ctx | item_count: max(ctx.item_count - 1, 0)})
    end
  end

  @impl true
  @spec quotient_merge(binary(), binary(), keyword()) :: binary()
  def quotient_merge(state_a, state_b, _opts) do
    ctx_a = qot_decode(state_a)
    ctx_b = qot_decode(state_b)

    fps_a = qot_extract_all(ctx_a)
    fps_b = qot_extract_all(ctx_b)

    all = merge_sorted_unique(fps_a, fps_b)

    fresh =
      qot_decode(
        quotient_new(
          q: ctx_a.q,
          r: ctx_a.r,
          slot_count: ctx_a.slot_count,
          seed: ctx_a.seed
        )
      )

    merged =
      Enum.reduce(all, fresh, fn {fq, fr}, acc ->
        acc = qot_do_insert(acc, fq, fr)
        %{acc | item_count: acc.item_count + 1}
      end)

    qot_encode(merged)
  end

  @impl true
  @spec quotient_count(binary(), keyword()) :: non_neg_integer()
  def quotient_count(state_bin, _opts) do
    <<_magic::binary-size(4), _version::8, _q::8, _r::8, _flags::8,
      _slot_count::unsigned-little-32, item_count::unsigned-little-32, _rest::binary>> = state_bin

    item_count
  end

  # -- Quotient: decode/encode --

  defp qot_decode(state_bin) do
    <<
      @qot_magic::binary,
      @qot_version::unsigned-8,
      q::unsigned-8,
      r::unsigned-8,
      _flags::unsigned-8,
      slot_count::unsigned-little-32,
      item_count::unsigned-little-32,
      seed::unsigned-little-32,
      _reserved::binary-size(12),
      body::binary
    >> = state_bin

    sb = div(3 + r + 7, 8)
    slots = qot_body_to_tuple(body, sb, slot_count)

    %QotCtx{
      q: q,
      r: r,
      slot_count: slot_count,
      item_count: item_count,
      seed: seed,
      slot_bytes: sb,
      slots: slots
    }
  end

  defp qot_body_to_tuple(body, sb, slot_count) do
    qot_body_to_list(body, sb, slot_count, [])
    |> :erlang.list_to_tuple()
  end

  defp qot_body_to_list(_body, _sb, 0, acc), do: Enum.reverse(acc)

  defp qot_body_to_list(body, sb, remaining, acc) do
    <<chunk::binary-size(^sb), rest::binary>> = body

    raw =
      chunk
      |> :binary.bin_to_list()
      |> Enum.with_index()
      |> Enum.reduce(0, fn {byte, i}, a -> a ||| byte <<< (i * 8) end)

    meta = raw &&& 0x7
    remainder = raw >>> 3
    qot_body_to_list(rest, sb, remaining - 1, [{meta, remainder} | acc])
  end

  defp qot_encode(%QotCtx{} = ctx) do
    body = qot_tuple_to_body(ctx.slots, ctx.slot_bytes, ctx.slot_count)

    <<
      @qot_magic::binary,
      @qot_version::unsigned-8,
      ctx.q::unsigned-8,
      ctx.r::unsigned-8,
      0::unsigned-8,
      ctx.slot_count::unsigned-little-32,
      ctx.item_count::unsigned-little-32,
      ctx.seed::unsigned-little-32,
      0::unsigned-little-64,
      0::unsigned-little-32,
      body::binary
    >>
  end

  defp qot_tuple_to_body(slots, sb, count) do
    for i <- 0..(count - 1), into: <<>> do
      {meta, remainder} = :erlang.element(i + 1, slots)
      raw = (meta &&& 0x7) ||| remainder <<< 3

      for j <- 0..(sb - 1), into: <<>> do
        <<raw >>> (j * 8) &&& 0xFF::8>>
      end
    end
  end

  defp qot_split_hash(%QotCtx{q: q, r: r}, hash64) do
    quotient = hash64 >>> (64 - q) &&& (1 <<< q) - 1
    remainder = hash64 >>> (64 - q - r) &&& (1 <<< r) - 1
    {quotient, remainder}
  end

  defp qot_split_hash(%CqfCtx{q: q, r: r}, hash64) do
    quotient = hash64 >>> (64 - q) &&& (1 <<< q) - 1
    remainder = hash64 >>> (64 - q - r) &&& (1 <<< r) - 1
    {quotient, remainder}
  end

  # -- Quotient: slot access via tuple (O(1)) --

  defp qot_get(ctx, i), do: :erlang.element(i + 1, ctx.slots)

  defp qot_set(ctx, i, val) do
    %{ctx | slots: :erlang.setelement(i + 1, ctx.slots, val)}
  end

  defp qot_meta(ctx, i) do
    {m, _} = qot_get(ctx, i)
    m
  end

  defp qot_rem(ctx, i) do
    {_, r} = qot_get(ctx, i)
    r
  end

  defp qot_occ?(ctx, i), do: (qot_meta(ctx, i) &&& @qot_occ) != 0
  defp qot_con?(ctx, i), do: (qot_meta(ctx, i) &&& @qot_con) != 0
  defp qot_shi?(ctx, i), do: (qot_meta(ctx, i) &&& @qot_shi) != 0

  defp qot_nxt(ctx, i), do: rem(i + 1, ctx.slot_count)
  defp qot_prv(ctx, i), do: rem(i - 1 + ctx.slot_count, ctx.slot_count)

  defp qot_set_meta_bit(ctx, i, bit) do
    {m, r} = qot_get(ctx, i)
    qot_set(ctx, i, {m ||| bit, r})
  end

  defp qot_clr_meta_bit(ctx, i, bit) do
    {m, r} = qot_get(ctx, i)
    qot_set(ctx, i, {m &&& bnot(bit), r})
  end

  # -- Quotient: find run start --
  # Given a quotient fq whose is_occupied bit is set, find the physical
  # slot where fq's run begins.
  #
  # Algorithm:
  #   1. Walk backward from fq to find the cluster start (first non-shifted slot).
  #   2. Count occupied canonical slots in [cluster_start, fq) -- these are the
  #      runs that precede fq's run in the cluster.
  #   3. Walk forward from cluster_start, skipping that many runs.

  defp qot_find_run_start(ctx, fq) do
    if qot_shi?(ctx, fq) do
      cs = qot_walk_back(ctx, fq)
      n = qot_count_occupied_range(ctx, cs, fq)
      qot_skip_runs_fwd(ctx, cs, n)
    else
      fq
    end
  end

  defp qot_walk_back(ctx, i) do
    p = qot_prv(ctx, i)
    if qot_shi?(ctx, p), do: qot_walk_back(ctx, p), else: p
  end

  # Count occupied canonical slots in [from, to) with wraparound.
  defp qot_count_occupied_range(_ctx, from, to) when from == to, do: 0

  defp qot_count_occupied_range(ctx, from, to) do
    add = if qot_occ?(ctx, from), do: 1, else: 0
    add + qot_count_occupied_range(ctx, qot_nxt(ctx, from), to)
  end

  # Skip n runs forward from pos. Each run ends when the next entry
  # does not have is_continuation set.
  defp qot_skip_runs_fwd(_ctx, pos, 0), do: pos

  defp qot_skip_runs_fwd(ctx, pos, n) do
    nxt = qot_nxt(ctx, pos)
    nxt = qot_skip_continuations(ctx, nxt)
    qot_skip_runs_fwd(ctx, nxt, n - 1)
  end

  defp qot_skip_continuations(ctx, pos) do
    if qot_con?(ctx, pos), do: qot_skip_continuations(ctx, qot_nxt(ctx, pos)), else: pos
  end

  # -- Quotient: lookup --

  defp qot_lookup?(ctx, fq, fr) do
    if qot_occ?(ctx, fq) do
      rs = qot_find_run_start(ctx, fq)
      qot_scan_run(ctx, rs, fr)
    else
      false
    end
  end

  defp qot_scan_run(ctx, pos, fr) do
    r = qot_rem(ctx, pos)

    cond do
      r == fr ->
        true

      r > fr ->
        false

      true ->
        nxt = qot_nxt(ctx, pos)
        if qot_con?(ctx, nxt), do: qot_scan_run(ctx, nxt, fr), else: false
    end
  end

  # Find the slot index of fr in fq's run, or nil.
  defp qot_find_slot(ctx, fq, fr) do
    if qot_occ?(ctx, fq) do
      rs = qot_find_run_start(ctx, fq)
      qot_scan_run_idx(ctx, rs, fr)
    else
      nil
    end
  end

  defp qot_scan_run_idx(ctx, pos, fr) do
    r = qot_rem(ctx, pos)

    cond do
      r == fr ->
        pos

      r > fr ->
        nil

      true ->
        nxt = qot_nxt(ctx, pos)
        if qot_con?(ctx, nxt), do: qot_scan_run_idx(ctx, nxt, fr), else: nil
    end
  end

  # -- Quotient: insert --
  # Key invariant: is_occupied belongs to the POSITION, not the entry.
  # During shift-right, is_occupied stays at each position while the
  # entry (is_continuation, is_shifted, remainder) moves.

  defp qot_do_insert(ctx, fq, fr) do
    was_occ = qot_occ?(ctx, fq)
    had_entry = qot_meta(ctx, fq) != 0

    # Mark canonical slot as occupied
    ctx = qot_set_meta_bit(ctx, fq, @qot_occ)

    cond do
      not was_occ and not had_entry ->
        # Fast path: canonical slot was completely empty
        qot_set(ctx, fq, {@qot_occ, fr})

      was_occ ->
        run_start = qot_find_run_start(ctx, fq)
        qot_insert_into_run(ctx, fq, run_start, fr)

      true ->
        # New run: insert first entry at run_start
        run_start = qot_find_run_start(ctx, fq)
        meta = if run_start == fq, do: 0, else: @qot_shi
        qot_shift_right(ctx, run_start, meta, fr)
    end
  end

  defp qot_insert_into_run(ctx, fq, run_start, fr) do
    {pos, at_start} = qot_sorted_pos(ctx, run_start, fr)

    if at_start do
      # Inserting before the current first element of the run.
      # The old first element becomes a continuation.
      ctx = qot_set_meta_bit(ctx, run_start, @qot_con)
      meta = if pos == fq, do: 0, else: @qot_shi
      qot_shift_right(ctx, pos, meta, fr)
    else
      # Inserting in the middle or after the end of the run.
      meta = @qot_con ||| if(pos == fq, do: 0, else: @qot_shi)
      qot_shift_right(ctx, pos, meta, fr)
    end
  end

  # Find where to insert fr in a run. Returns {position, at_start?}.
  defp qot_sorted_pos(ctx, run_start, fr) do
    if fr < qot_rem(ctx, run_start) do
      {run_start, true}
    else
      qot_sorted_pos_cont(ctx, run_start, fr)
    end
  end

  defp qot_sorted_pos_cont(ctx, pos, fr) do
    nxt = qot_nxt(ctx, pos)

    if qot_con?(ctx, nxt) do
      if fr < qot_rem(ctx, nxt),
        do: {nxt, false},
        else: qot_sorted_pos_cont(ctx, nxt, fr)
    else
      {nxt, false}
    end
  end

  # Shift right: insert {new_meta, new_rem} at pos, pushing existing
  # entries rightward. is_occupied at each position is preserved.
  defp qot_shift_right(ctx, pos, new_meta, new_rem) do
    {old_meta, old_rem} = qot_get(ctx, pos)
    occ_here = old_meta &&& @qot_occ
    ctx = qot_set(ctx, pos, {new_meta ||| occ_here, new_rem})

    if old_meta == 0 do
      ctx
    else
      # Strip is_occupied (stays at position), set is_shifted
      entry_meta = (old_meta &&& bnot(@qot_occ)) ||| @qot_shi
      qot_shift_chain(ctx, qot_nxt(ctx, pos), entry_meta, old_rem)
    end
  end

  defp qot_shift_chain(ctx, pos, meta, remainder) do
    {old_meta, old_rem} = qot_get(ctx, pos)
    occ_here = old_meta &&& @qot_occ
    ctx = qot_set(ctx, pos, {meta ||| occ_here, remainder})

    if old_meta == 0 do
      ctx
    else
      entry_meta = (old_meta &&& bnot(@qot_occ)) ||| @qot_shi
      qot_shift_chain(ctx, qot_nxt(ctx, pos), entry_meta, old_rem)
    end
  end

  # -- Quotient: delete --

  defp qot_do_delete(ctx, fq, slot_idx) do
    run_start = qot_find_run_start(ctx, fq)
    nxt = qot_nxt(ctx, slot_idx)
    is_first = slot_idx == run_start
    nxt_is_con = qot_con?(ctx, nxt)
    is_only = is_first and not nxt_is_con

    # If removing the only entry in the run, clear is_occupied
    ctx = if is_only, do: qot_clr_meta_bit(ctx, fq, @qot_occ), else: ctx

    # If removing the first entry but not the only, next becomes first (clear continuation)
    ctx = if is_first and nxt_is_con, do: qot_clr_meta_bit(ctx, nxt, @qot_con), else: ctx

    # Shift left to fill the gap
    qot_shift_left(ctx, slot_idx)
  end

  # Shift left from pos: move shifted entries leftward to fill gap.
  # is_occupied at each position is preserved.
  defp qot_shift_left(ctx, pos) do
    nxt = qot_nxt(ctx, pos)
    nxt_meta = qot_meta(ctx, nxt)
    occ_here = qot_meta(ctx, pos) &&& @qot_occ

    cond do
      nxt_meta == 0 ->
        # Next slot is empty. Clear this slot (preserve is_occupied).
        qot_set(ctx, pos, {occ_here, 0})

      (nxt_meta &&& @qot_shi) == 0 ->
        # Next entry is not shifted (at its canonical position). Stop.
        qot_set(ctx, pos, {occ_here, 0})

      true ->
        # Move next entry here, preserving is_occupied at pos
        entry_meta = nxt_meta &&& bnot(@qot_occ)
        nxt_rem = qot_rem(ctx, nxt)
        ctx = qot_set(ctx, pos, {occ_here ||| entry_meta, nxt_rem})
        qot_shift_left(ctx, nxt)
    end
  end

  # -- Quotient: extract all fingerprints (for merge) --

  defp qot_extract_all(ctx) do
    {fps, _} =
      Enum.reduce(0..(ctx.slot_count - 1), {[], nil}, fn i, {acc, cur_q} ->
        qot_extract_slot(ctx, i, acc, cur_q)
      end)

    fps |> Enum.reverse() |> Enum.sort() |> Enum.uniq()
  end

  defp qot_extract_slot(ctx, i, acc, cur_q) do
    m = qot_meta(ctx, i)

    if m == 0 do
      {acc, nil}
    else
      q = qot_resolve_quotient(ctx, i, m, cur_q)
      {[{q, qot_rem(ctx, i)} | acc], q}
    end
  end

  defp qot_resolve_quotient(ctx, i, m, cur_q) do
    is_con = (m &&& @qot_con) != 0
    is_shi = (m &&& @qot_shi) != 0

    cond do
      is_con and cur_q != nil -> cur_q
      not is_shi and not is_con -> i
      true -> qot_trace_quotient_for(ctx, i)
    end
  end

  # Determine which quotient the entry at slot_idx belongs to.
  # The entry is the first of its run (not continuation) but is shifted.
  defp qot_trace_quotient_for(ctx, slot_idx) do
    cs = qot_walk_back_to_start(ctx, slot_idx)
    qot_trace_walk(ctx, cs, cs, slot_idx)
  end

  defp qot_walk_back_to_start(ctx, i) do
    if qot_shi?(ctx, i), do: qot_walk_back_to_start(ctx, qot_prv(ctx, i)), else: i
  end

  defp qot_trace_walk(_ctx, pos, cur_q, target) when pos == target, do: cur_q

  defp qot_trace_walk(ctx, pos, cur_q, target) do
    nxt = qot_nxt(ctx, pos)

    new_q =
      if qot_con?(ctx, nxt),
        do: cur_q,
        else: qot_next_occ_canonical(ctx, cur_q)

    qot_trace_walk(ctx, nxt, new_q, target)
  end

  defp qot_next_occ_canonical(ctx, cur_q) do
    qot_find_occ(ctx, qot_nxt(ctx, cur_q), ctx.slot_count)
  end

  defp qot_find_occ(_ctx, _pos, 0), do: 0

  defp qot_find_occ(ctx, pos, remaining) do
    if qot_occ?(ctx, pos), do: pos, else: qot_find_occ(ctx, qot_nxt(ctx, pos), remaining - 1)
  end

  # ============================================================
  # CQF (Counting Quotient Filter)
  # ============================================================
  #
  # CQF extends Quotient with unary counter encoding.
  # Same slot layout (3 meta bits + r remainder bits per slot).
  # Within a run, each distinct remainder's count is stored as
  # N consecutive copies of that remainder value:
  #
  #   count=1: [r]             (one slot)
  #   count=2: [r, r]          (two consecutive identical slots)
  #   count=N: [r, r, ..., r]  (N consecutive identical slots)
  #
  # This is unambiguous because remainders within a run are stored
  # in strictly increasing order -- consecutive identical values
  # can only mean a counter.
  #
  # The CQF1 binary format uses a 40-byte header (vs 32 for QOT1)
  # to accommodate a 64-bit total_count field.
  #
  #   HEADER (40 bytes, little-endian):
  #     0:4   magic         "CQF1"
  #     4:1   version       u8 = 1
  #     5:1   q_bits        u8
  #     6:1   r_bits        u8
  #     7:1   flags         u8 = 0
  #     8:4   slot_count    u32
  #     12:4  occupied_cnt  u32 (distinct fingerprints)
  #     16:8  total_count   u64 (sum of multiplicities)
  #     24:4  seed          u32
  #     28:12 reserved      zeroed
  #
  #   BODY: slot_count * slot_bytes (same packing as QOT1)

  @cqf_magic "CQF1"
  @cqf_version 1

  @impl true
  @spec cqf_new(keyword()) :: binary()
  def cqf_new(opts) do
    q = Keyword.fetch!(opts, :q)
    r = Keyword.fetch!(opts, :r)
    slot_count = Keyword.fetch!(opts, :slot_count)
    seed = Keyword.get(opts, :seed, 0)

    slot_bytes = div(3 + r + 7, 8)
    body_size = slot_count * slot_bytes
    body = :binary.copy(<<0>>, body_size)

    <<
      @cqf_magic::binary,
      @cqf_version::unsigned-8,
      q::unsigned-8,
      r::unsigned-8,
      0::unsigned-8,
      slot_count::unsigned-little-32,
      0::unsigned-little-32,
      0::unsigned-little-64,
      seed::unsigned-little-32,
      0::96,
      body::binary
    >>
  end

  @impl true
  @spec cqf_put(binary(), non_neg_integer(), keyword()) :: binary()
  def cqf_put(state_bin, hash64, _opts) do
    ctx = cqf_decode(state_bin)
    {fq, fr} = qot_split_hash(ctx, hash64)
    ctx = cqf_do_insert(ctx, fq, fr)
    cqf_encode(ctx)
  end

  @impl true
  @spec cqf_put_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def cqf_put_many(state_bin, [], _opts), do: state_bin

  def cqf_put_many(state_bin, hashes, _opts) do
    ctx = cqf_decode(state_bin)

    ctx =
      Enum.reduce(hashes, ctx, fn hash64, acc ->
        {fq, fr} = qot_split_hash(acc, hash64)
        cqf_do_insert(acc, fq, fr)
      end)

    cqf_encode(ctx)
  end

  @impl true
  @spec cqf_member?(binary(), non_neg_integer(), keyword()) :: boolean()
  def cqf_member?(state_bin, hash64, _opts) do
    ctx = cqf_decode(state_bin)
    {fq, fr} = qot_split_hash(ctx, hash64)
    cqf_estimate(ctx, fq, fr) > 0
  end

  @impl true
  @spec cqf_estimate_count(binary(), non_neg_integer(), keyword()) :: non_neg_integer()
  def cqf_estimate_count(state_bin, hash64, _opts) do
    ctx = cqf_decode(state_bin)
    {fq, fr} = qot_split_hash(ctx, hash64)
    cqf_estimate(ctx, fq, fr)
  end

  @impl true
  @spec cqf_delete(binary(), non_neg_integer(), keyword()) :: binary()
  def cqf_delete(state_bin, hash64, _opts) do
    ctx = cqf_decode(state_bin)
    {fq, fr} = qot_split_hash(ctx, hash64)

    case cqf_estimate(ctx, fq, fr) do
      0 ->
        cqf_encode(ctx)

      _count ->
        ctx = cqf_do_delete(ctx, fq, fr)
        cqf_encode(ctx)
    end
  end

  @impl true
  @spec cqf_merge(binary(), binary(), keyword()) :: binary()
  def cqf_merge(state_a, state_b, _opts) do
    ctx_a = cqf_decode(state_a)
    ctx_b = cqf_decode(state_b)

    triples_a = cqf_extract_all_counted(ctx_a)
    triples_b = cqf_extract_all_counted(ctx_b)

    all = cqf_merge_counted(triples_a, triples_b)

    fresh =
      cqf_decode(
        cqf_new(
          q: ctx_a.q,
          r: ctx_a.r,
          slot_count: ctx_a.slot_count,
          seed: ctx_a.seed
        )
      )

    merged =
      Enum.reduce(all, fresh, fn {fq, fr, count}, acc ->
        cqf_insert_with_count(acc, fq, fr, count)
      end)

    cqf_encode(merged)
  end

  @impl true
  @spec cqf_count(binary(), keyword()) :: non_neg_integer()
  def cqf_count(state_bin, _opts) do
    <<_magic::binary-size(4), _version::8, _q::8, _r::8, _flags::8,
      _slot_count::unsigned-little-32, _occupied::unsigned-little-32,
      total_count::unsigned-little-64, _rest::binary>> = state_bin

    total_count
  end

  # -- CQF: decode/encode --

  defp cqf_decode(state_bin) do
    <<
      @cqf_magic::binary,
      @cqf_version::unsigned-8,
      q::unsigned-8,
      r::unsigned-8,
      _flags::unsigned-8,
      slot_count::unsigned-little-32,
      occupied_count::unsigned-little-32,
      total_count::unsigned-little-64,
      seed::unsigned-little-32,
      _reserved::binary-size(12),
      body::binary
    >> = state_bin

    sb = div(3 + r + 7, 8)
    slots = qot_body_to_tuple(body, sb, slot_count)

    %CqfCtx{
      q: q,
      r: r,
      slot_count: slot_count,
      occupied_count: occupied_count,
      total_count: total_count,
      seed: seed,
      slot_bytes: sb,
      slots: slots
    }
  end

  defp cqf_encode(%CqfCtx{} = ctx) do
    body = qot_tuple_to_body(ctx.slots, ctx.slot_bytes, ctx.slot_count)

    <<
      @cqf_magic::binary,
      @cqf_version::unsigned-8,
      ctx.q::unsigned-8,
      ctx.r::unsigned-8,
      0::unsigned-8,
      ctx.slot_count::unsigned-little-32,
      ctx.occupied_count::unsigned-little-32,
      ctx.total_count::unsigned-little-64,
      ctx.seed::unsigned-little-32,
      0::96,
      body::binary
    >>
  end

  # -- CQF: insert (always increments count) --

  defp cqf_do_insert(ctx, fq, fr) do
    if qot_occ?(ctx, fq) do
      # Existing quotient -- find remainder in run and increment, or insert new
      cqf_do_insert_existing(ctx, fq, fr)
    else
      # New fingerprint -- insert remainder with count 1
      cqf_do_insert_new(ctx, fq, fr)
    end
  end

  defp cqf_do_insert_new(ctx, fq, fr) do
    ctx = qot_set_meta_bit(ctx, fq, @qot_occ)
    had_entry = qot_meta(ctx, fq) != @qot_occ

    ctx =
      if had_entry do
        run_start = qot_find_run_start(ctx, fq)
        meta = if run_start == fq, do: 0, else: @qot_shi
        qot_shift_right(ctx, run_start, meta, fr)
      else
        qot_set(ctx, fq, {@qot_occ, fr})
      end

    %{ctx | occupied_count: ctx.occupied_count + 1, total_count: ctx.total_count + 1}
  end

  defp cqf_do_insert_existing(ctx, fq, fr) do
    run_start = qot_find_run_start(ctx, fq)

    case cqf_find_remainder_in_run(ctx, run_start, fr) do
      nil ->
        ctx = qot_insert_into_run(ctx, fq, run_start, fr)
        %{ctx | occupied_count: ctx.occupied_count + 1, total_count: ctx.total_count + 1}

      pos ->
        last = cqf_last_copy(ctx, pos, fr)
        nxt = qot_nxt(ctx, last)
        ctx = qot_shift_right(ctx, nxt, @qot_con ||| @qot_shi, fr)
        %{ctx | total_count: ctx.total_count + 1}
    end
  end

  # Insert a fingerprint with a specific count (used by merge).
  # Inserts the remainder once, then adds (count-1) duplicate copies.
  defp cqf_insert_with_count(ctx, fq, fr, count) do
    was_occ = qot_occ?(ctx, fq)
    had_entry = qot_meta(ctx, fq) != 0

    ctx = qot_set_meta_bit(ctx, fq, @qot_occ)

    ctx =
      cond do
        not was_occ and not had_entry ->
          qot_set(ctx, fq, {@qot_occ, fr})

        was_occ ->
          run_start = qot_find_run_start(ctx, fq)
          qot_insert_into_run(ctx, fq, run_start, fr)

        true ->
          run_start = qot_find_run_start(ctx, fq)
          meta = if run_start == fq, do: 0, else: @qot_shi
          qot_shift_right(ctx, run_start, meta, fr)
      end

    ctx = %{ctx | occupied_count: ctx.occupied_count + 1, total_count: ctx.total_count + count}

    # Add (count-1) duplicate copies after the remainder
    if count <= 1 do
      ctx
    else
      run_start = qot_find_run_start(ctx, fq)
      pos = cqf_find_remainder_in_run(ctx, run_start, fr)

      Enum.reduce(1..(count - 1), ctx, fn _i, acc ->
        last = cqf_last_copy(acc, pos, fr)
        nxt = qot_nxt(acc, last)
        qot_shift_right(acc, nxt, @qot_con ||| @qot_shi, fr)
      end)
    end
  end

  # -- CQF: find remainder in run (returns position or nil) --

  # Scan through a run, skipping counter duplicates, looking for a remainder.
  defp cqf_find_remainder_in_run(ctx, run_start, target_fr) do
    cqf_scan_run(ctx, run_start, target_fr)
  end

  defp cqf_scan_run(ctx, pos, target_fr) do
    r = qot_rem(ctx, pos)

    cond do
      r == target_fr ->
        pos

      r > target_fr ->
        nil

      true ->
        # Skip past all duplicate copies of this remainder
        next_pos = cqf_skip_duplicates(ctx, pos, r)

        if qot_con?(ctx, next_pos) do
          cqf_scan_run(ctx, next_pos, target_fr)
        else
          nil
        end
    end
  end

  # Skip past all consecutive copies of remainder_val.
  # Returns the position of the first slot that is NOT a copy.
  defp cqf_skip_duplicates(ctx, pos, remainder_val) do
    nxt = qot_nxt(ctx, pos)

    if qot_con?(ctx, nxt) and qot_rem(ctx, nxt) == remainder_val do
      cqf_skip_duplicates(ctx, nxt, remainder_val)
    else
      nxt
    end
  end

  # Find the last consecutive copy of remainder_val starting from pos.
  defp cqf_last_copy(ctx, pos, remainder_val) do
    nxt = qot_nxt(ctx, pos)

    if qot_con?(ctx, nxt) and qot_rem(ctx, nxt) == remainder_val do
      cqf_last_copy(ctx, nxt, remainder_val)
    else
      pos
    end
  end

  # -- CQF: read counter (count consecutive copies) --

  defp cqf_read_counter(ctx, pos, remainder_val) do
    nxt = qot_nxt(ctx, pos)

    if qot_con?(ctx, nxt) and qot_rem(ctx, nxt) == remainder_val do
      1 + cqf_read_counter(ctx, nxt, remainder_val)
    else
      1
    end
  end

  # -- CQF: estimate count for a fingerprint --

  defp cqf_estimate(ctx, fq, fr) do
    if qot_occ?(ctx, fq) do
      run_start = qot_find_run_start(ctx, fq)

      case cqf_find_remainder_in_run(ctx, run_start, fr) do
        nil -> 0
        pos -> cqf_read_counter(ctx, pos, fr)
      end
    else
      0
    end
  end

  # -- CQF: delete (remove one duplicate copy) --

  defp cqf_do_delete(ctx, fq, fr) do
    run_start = qot_find_run_start(ctx, fq)
    pos = cqf_find_remainder_in_run(ctx, run_start, fr)

    if pos == nil do
      ctx
    else
      count = cqf_read_counter(ctx, pos, fr)

      if count == 1 do
        # Remove the remainder entirely (like QOT delete)
        ctx = qot_do_delete(ctx, fq, pos)

        %{
          ctx
          | occupied_count: max(ctx.occupied_count - 1, 0),
            total_count: max(ctx.total_count - 1, 0)
        }
      else
        # Remove one duplicate copy (the last one)
        last = cqf_last_copy(ctx, pos, fr)
        ctx = qot_shift_left(ctx, last)
        %{ctx | total_count: max(ctx.total_count - 1, 0)}
      end
    end
  end

  # -- CQF: extract all counted fingerprints (for merge) --

  # Iterate over all occupied quotients and extract {q, r, count} triples.
  defp cqf_extract_all_counted(ctx) do
    Enum.reduce(0..(ctx.slot_count - 1), [], fn i, acc ->
      if qot_occ?(ctx, i) do
        run_start = qot_find_run_start(ctx, i)
        run_triples = cqf_extract_run_counted(ctx, run_start, i)
        run_triples ++ acc
      else
        acc
      end
    end)
    |> Enum.reverse()
    |> Enum.sort()
  end

  defp cqf_extract_run_counted(ctx, pos, quotient) do
    cqf_extract_run_loop(ctx, pos, quotient, [])
  end

  defp cqf_extract_run_loop(ctx, pos, quotient, acc) do
    fr = qot_rem(ctx, pos)
    count = cqf_read_counter(ctx, pos, fr)
    acc = [{quotient, fr, count} | acc]

    next_pos = cqf_skip_duplicates(ctx, pos, fr)

    if qot_con?(ctx, next_pos) do
      cqf_extract_run_loop(ctx, next_pos, quotient, acc)
    else
      Enum.reverse(acc)
    end
  end

  # Merge two sorted lists of {quotient, remainder, count} triples, summing counts
  defp cqf_merge_counted([], b), do: b
  defp cqf_merge_counted(a, []), do: a

  defp cqf_merge_counted([{qa, ra, ca} = ha | ta], [{qb, rb, cb} = hb | tb]) do
    cond do
      {qa, ra} < {qb, rb} -> [ha | cqf_merge_counted(ta, [hb | tb])]
      {qa, ra} > {qb, rb} -> [hb | cqf_merge_counted([ha | ta], tb)]
      true -> [{qa, ra, ca + cb} | cqf_merge_counted(ta, tb)]
    end
  end

  # Merge two sorted lists of {quotient, remainder} tuples, removing duplicates
  defp merge_sorted_unique([], b), do: b
  defp merge_sorted_unique(a, []), do: a

  defp merge_sorted_unique([ha | ta], [hb | tb]) do
    cond do
      ha < hb -> [ha | merge_sorted_unique(ta, [hb | tb])]
      ha > hb -> [hb | merge_sorted_unique([ha | ta], tb)]
      true -> [ha | merge_sorted_unique(ta, tb)]
    end
  end

  # ============================================================
  # XorFilter Implementation
  # ============================================================
  #
  # XOR1 Binary Format (32-byte header + fingerprint array):
  #
  #   Offset  Size  Field
  #   0       4     magic            "XOR1"
  #   4       1     version          u8 = 1
  #   5       1     fingerprint_bits u8 (8 or 16)
  #   6       1     variant          u8 (0=xor8, 1=xor16)
  #   7       1     flags            u8, reserved = 0
  #   8       4     item_count       u32-LE
  #   12      4     segment_size     u32-LE
  #   16      4     seed             u32-LE
  #   20      4     array_length     u32-LE (3 * segment_size)
  #   24      8     reserved         zeroed
  #
  #   Body: fingerprint array (1 byte per entry for xor8, 2 bytes LE for xor16)
  # ============================================================

  @xor1_magic "XOR1"
  @xor1_version 1
  @xor_max_retries 100

  @impl true
  @spec xor_build([non_neg_integer()], keyword()) ::
          {:ok, binary()} | {:error, :build_failed}
  def xor_build(hashes, opts) do
    fingerprint_bits = Keyword.get(opts, :fingerprint_bits, 8)
    seed = Keyword.get(opts, :seed, 0)
    variant = if fingerprint_bits == 16, do: 1, else: 0

    unique_hashes = Enum.uniq(hashes)
    n = length(unique_hashes)

    if n == 0 do
      segment_size = 1
      array_length = 3

      empty_body =
        case fingerprint_bits do
          8 -> :binary.copy(<<0>>, array_length)
          16 -> :binary.copy(<<0, 0>>, array_length)
        end

      {:ok,
       xor_encode_state(
         0,
         segment_size,
         array_length,
         seed,
         fingerprint_bits,
         variant,
         empty_body
       )}
    else
      # Standard xor filter sizing: capacity = ceil(1.23 * n) + 32, then divide by 3
      # The 1.23 factor ensures the random 3-partite hypergraph is peelable w.h.p.
      capacity = ceil(1.23 * n) + 32
      segment_size = max(div(capacity + 2, 3), 1)
      array_length = 3 * segment_size

      case xor_try_build(unique_hashes, n, segment_size, array_length, fingerprint_bits, seed, 0) do
        {:ok, fingerprint_array, final_seed} ->
          body = xor_encode_body(fingerprint_array, array_length, fingerprint_bits)

          {:ok,
           xor_encode_state(
             n,
             segment_size,
             array_length,
             final_seed,
             fingerprint_bits,
             variant,
             body
           )}

        :error ->
          {:error, :build_failed}
      end
    end
  end

  @impl true
  @spec xor_member?(binary(), non_neg_integer(), keyword()) :: boolean()
  def xor_member?(state_bin, hash64, _opts) do
    <<@xor1_magic, @xor1_version::unsigned-8, fp_bits::unsigned-8, _variant::unsigned-8,
      _flags::unsigned-8, _item_count::unsigned-little-32, segment_size::unsigned-little-32,
      seed::unsigned-little-32, _array_length::unsigned-little-32, _reserved::binary-size(8),
      body::binary>> = state_bin

    {h0, h1, h2} = xor_hash_positions(hash64, seed, segment_size)
    fp = xor_fingerprint(hash64, fp_bits)

    f0 = xor_read_fp(body, h0, fp_bits)
    f1 = xor_read_fp(body, h1, fp_bits)
    f2 = xor_read_fp(body, h2, fp_bits)

    bxor(bxor(f0, f1), f2) == fp
  end

  @impl true
  @spec xor_count(binary(), keyword()) :: non_neg_integer()
  def xor_count(state_bin, _opts) do
    <<@xor1_magic, @xor1_version::unsigned-8, _fp_bits::unsigned-8, _variant::unsigned-8,
      _flags::unsigned-8, item_count::unsigned-little-32, _rest::binary>> = state_bin

    item_count
  end

  # -- XorFilter private helpers --

  defp xor_try_build(_hashes, _n, _seg_size, _arr_len, _fp_bits, _seed, retry)
       when retry >= @xor_max_retries,
       do: :error

  defp xor_try_build(hashes, n, seg_size, arr_len, fp_bits, seed, retry) do
    current_seed = seed + retry

    # Build degree counts and sets for each position
    {degrees, sets} = xor_build_graph(hashes, current_seed, seg_size, arr_len)

    # Peel the hypergraph
    case xor_peel(hashes, degrees, sets, n, current_seed, seg_size) do
      {:ok, stack} ->
        # Assign fingerprints
        fingerprint_array = xor_assign(stack, arr_len, fp_bits, current_seed, seg_size)
        {:ok, fingerprint_array, current_seed}

      :retry ->
        xor_try_build(hashes, n, seg_size, arr_len, fp_bits, seed, retry + 1)
    end
  end

  defp xor_build_graph(hashes, seed, seg_size, arr_len) do
    # degrees: tuple of arr_len integers tracking how many hashes map to each position
    # sets: tuple of arr_len MapSets tracking which hashes map to each position
    degrees = :erlang.make_tuple(arr_len, 0)
    sets = :erlang.make_tuple(arr_len, MapSet.new())

    Enum.reduce(hashes, {degrees, sets}, fn hash, {deg, s} ->
      {h0, h1, h2} = xor_hash_positions(hash, seed, seg_size)

      deg =
        deg
        |> put_elem(h0, elem(deg, h0) + 1)
        |> put_elem(h1, elem(deg, h1) + 1)
        |> put_elem(h2, elem(deg, h2) + 1)

      s =
        s
        |> put_elem(h0, MapSet.put(elem(s, h0), hash))
        |> put_elem(h1, MapSet.put(elem(s, h1), hash))
        |> put_elem(h2, MapSet.put(elem(s, h2), hash))

      {deg, s}
    end)
  end

  defp xor_peel(_hashes, degrees, sets, n, seed, seg_size) do
    arr_len = tuple_size(degrees)

    # Find initial queue of degree-1 positions
    queue =
      for i <- 0..(arr_len - 1), elem(degrees, i) == 1, do: i

    xor_peel_loop(queue, degrees, sets, [], 0, n, seed, seg_size)
  end

  defp xor_peel_loop([], _degrees, _sets, stack, peeled, n, _seed, _seg_size) do
    if peeled == n, do: {:ok, Enum.reverse(stack)}, else: :retry
  end

  defp xor_peel_loop([pos | rest], degrees, sets, stack, peeled, n, seed, seg_size) do
    if elem(degrees, pos) != 1 do
      # Position is no longer degree-1, skip it
      xor_peel_loop(rest, degrees, sets, stack, peeled, n, seed, seg_size)
    else
      # Get the single hash at this position
      hash_set = elem(sets, pos)
      hash = MapSet.to_list(hash_set) |> hd()

      # Get all 3 positions for this hash
      {h0, h1, h2} = xor_hash_positions(hash, seed, seg_size)

      # Push to stack and remove hash from all 3 positions
      stack = [{hash, pos} | stack]

      {degrees, sets, new_queue} =
        Enum.reduce([h0, h1, h2], {degrees, sets, rest}, fn p, acc ->
          xor_remove_edge(acc, p, pos, hash)
        end)

      xor_peel_loop(new_queue, degrees, sets, stack, peeled + 1, n, seed, seg_size)
    end
  end

  defp xor_remove_edge({deg, s, q}, p, peel_pos, hash) do
    new_set = MapSet.delete(elem(s, p), hash)
    new_deg = elem(deg, p) - 1
    deg = put_elem(deg, p, new_deg)
    s = put_elem(s, p, new_set)
    q = if new_deg == 1 and p != peel_pos, do: [p | q], else: q
    {deg, s, q}
  end

  defp xor_assign(stack, arr_len, fp_bits, seed, seg_size) do
    # Process stack in reverse (it was already reversed in peel)
    # stack is in peel order, we need reverse order
    b = :erlang.make_tuple(arr_len, 0)

    Enum.reduce(Enum.reverse(stack), b, fn {hash, peel_pos}, b ->
      {h0, h1, h2} = xor_hash_positions(hash, seed, seg_size)
      fp = xor_fingerprint(hash, fp_bits)

      # Get the other two positions
      [other1, other2] = Enum.reject([h0, h1, h2], &(&1 == peel_pos))

      value = bxor(bxor(fp, elem(b, other1)), elem(b, other2))
      put_elem(b, peel_pos, value)
    end)
  end

  defp xor_fingerprint(hash64, fp_bits) do
    fp_mask = (1 <<< fp_bits) - 1
    fp = hash64 &&& fp_mask
    # Ensure non-zero fingerprint
    if fp == 0, do: 1, else: fp
  end

  defp xor_hash_positions(hash64, seed, seg_size) do
    # Remix hash with seed via splitmix64 for full independence per seed
    h = xor_splitmix64(hash64 + seed * 0x9E3779B97F4A7C15 &&& @mask64)

    # Use 64-bit rotation to derive 3 independent positions
    # Fastrange maps each rotated hash to [0, seg_size)
    h0 = xor_fastrange(h, seg_size)
    h1 = xor_fastrange(xor_rotl64(h, 21), seg_size) + seg_size
    h2 = xor_fastrange(xor_rotl64(h, 42), seg_size) + seg_size * 2

    {h0, h1, h2}
  end

  # splitmix64 finalizer: strong bijective 64-bit -> 64-bit mix
  defp xor_splitmix64(x) do
    x = bxor(x, x >>> 30) * 0xBF58476D1CE4E5B9 &&& @mask64
    x = bxor(x, x >>> 27) * 0x94D049BB133111EB &&& @mask64
    bxor(x, x >>> 31) &&& @mask64
  end

  # 64-bit left rotation
  defp xor_rotl64(x, r) do
    (x <<< r ||| x >>> (64 - r)) &&& @mask64
  end

  # Fastrange: maps a 64-bit hash to [0, range) via multiply-shift
  # Equivalent to (uint128_t(hash) * range) >> 64
  defp xor_fastrange(hash, range) do
    (hash * range) >>> 64
  end

  defp xor_read_fp(body, index, 8) do
    <<_before::binary-size(^index), fp::unsigned-8, _rest::binary>> = body
    fp
  end

  defp xor_read_fp(body, index, 16) do
    byte_offset = index * 2
    <<_before::binary-size(^byte_offset), fp::unsigned-little-16, _rest::binary>> = body
    fp
  end

  defp xor_encode_body(fingerprint_tuple, arr_len, fp_bits) do
    entries = for i <- 0..(arr_len - 1), do: elem(fingerprint_tuple, i)

    case fp_bits do
      8 ->
        entries |> Enum.map(fn v -> <<v::unsigned-8>> end) |> IO.iodata_to_binary()

      16 ->
        entries |> Enum.map(fn v -> <<v::unsigned-little-16>> end) |> IO.iodata_to_binary()
    end
  end

  defp xor_encode_state(item_count, segment_size, array_length, seed, fp_bits, variant, body) do
    <<
      @xor1_magic::binary,
      @xor1_version::unsigned-8,
      fp_bits::unsigned-8,
      variant::unsigned-8,
      0::unsigned-8,
      item_count::unsigned-little-32,
      segment_size::unsigned-little-32,
      seed::unsigned-little-32,
      array_length::unsigned-little-32,
      0::unsigned-64,
      body::binary
    >>
  end

  # ============================================================
  # MisraGries Implementation
  # ============================================================
  #
  # Misra-Gries algorithm for deterministic heavy hitter detection.
  # Maintains at most k counters. Any item with true frequency > n/k
  # is guaranteed to be tracked (deterministic guarantee).
  #
  # State binary layout (MG01):
  #   magic:       4 bytes  "MG01"
  #   version:     u8       1
  #   reserved:    u8       0
  #   k:           u32 LE   max counters
  #   n:           u64 LE   total count
  #   entry_count: u32 LE   number of entries
  #   entries:     entry_count x (key_len: u32 LE, key: key_len bytes, count: u64 LE)

  @mg_magic "MG01"

  @impl true
  @spec mg_new(keyword()) :: binary()
  def mg_new(opts) do
    k = Keyword.fetch!(opts, :k)

    <<
      @mg_magic::binary,
      1::unsigned-8,
      0::unsigned-8,
      k::unsigned-little-32,
      0::unsigned-little-64,
      0::unsigned-little-32
    >>
  end

  @impl true
  @spec mg_update(binary(), binary(), keyword()) :: binary()
  def mg_update(state_bin, item_bytes, _opts) do
    state = mg_decode_state(state_bin)
    state = mg_insert(state, item_bytes)
    mg_encode_state(state)
  end

  @impl true
  @spec mg_update_many(binary(), [binary()], keyword()) :: binary()
  def mg_update_many(state_bin, items, _opts) do
    state = mg_decode_state(state_bin)
    state = Enum.reduce(items, state, &mg_insert(&2, &1))
    mg_encode_state(state)
  end

  @impl true
  @spec mg_merge(binary(), binary(), keyword()) :: binary()
  def mg_merge(state_bin_a, state_bin_b, _opts) do
    a = mg_decode_state(state_bin_a)
    b = mg_decode_state(state_bin_b)

    if a.k != b.k do
      raise ExDataSketch.Errors.IncompatibleSketchesError,
        reason: "MisraGries k mismatch: #{a.k} vs #{b.k}"
    end

    # Merge: combine counters additively
    merged_entries =
      Map.merge(a.entries, b.entries, fn _key, c1, c2 -> c1 + c2 end)

    new_n = a.n + b.n

    # If more than k entries, prune: subtract the (k+1)th largest count,
    # then remove entries with count <= 0
    pruned =
      if map_size(merged_entries) > a.k do
        counts = merged_entries |> Map.values() |> Enum.sort(:desc)
        # The (k+1)th largest count (0-indexed: index k)
        threshold = Enum.at(counts, a.k, 0)

        merged_entries
        |> Enum.map(fn {key, count} -> {key, count - threshold} end)
        |> Enum.filter(fn {_key, count} -> count > 0 end)
        |> Map.new()
      else
        merged_entries
      end

    mg_encode_state(%{k: a.k, n: new_n, entries: pruned})
  end

  @impl true
  @spec mg_estimate(binary(), binary(), keyword()) :: non_neg_integer()
  def mg_estimate(state_bin, item_bytes, _opts) do
    state = mg_decode_state(state_bin)
    Map.get(state.entries, item_bytes, 0)
  end

  @impl true
  @spec mg_top_k(binary(), non_neg_integer(), keyword()) :: [{binary(), non_neg_integer()}]
  def mg_top_k(state_bin, limit, _opts) do
    state = mg_decode_state(state_bin)

    state.entries
    |> Enum.sort_by(fn {_key, count} -> count end, :desc)
    |> Enum.take(limit)
  end

  @impl true
  @spec mg_count(binary(), keyword()) :: non_neg_integer()
  def mg_count(state_bin, _opts) do
    <<@mg_magic, 1::8, _::8, _k::32, n::unsigned-little-64, _::binary>> = state_bin
    n
  end

  @impl true
  @spec mg_entry_count(binary(), keyword()) :: non_neg_integer()
  def mg_entry_count(state_bin, _opts) do
    <<@mg_magic, 1::8, _::8, _k::32, _n::64, entry_count::unsigned-little-32, _::binary>> =
      state_bin

    entry_count
  end

  # -- MisraGries Private Helpers --

  defp mg_decode_state(state_bin) do
    <<
      @mg_magic::binary,
      1::unsigned-8,
      _reserved::unsigned-8,
      k::unsigned-little-32,
      n::unsigned-little-64,
      entry_count::unsigned-little-32,
      body::binary
    >> = state_bin

    entries = mg_decode_entries(body, entry_count, %{})
    %{k: k, n: n, entries: entries}
  end

  defp mg_decode_entries(_bin, 0, acc), do: acc

  defp mg_decode_entries(bin, remaining, acc) do
    <<key_len::unsigned-little-32, rest::binary>> = bin
    <<key::binary-size(^key_len), count::unsigned-little-64, rest2::binary>> = rest
    mg_decode_entries(rest2, remaining - 1, Map.put(acc, key, count))
  end

  defp mg_encode_state(%{k: k, n: n, entries: entries}) do
    entry_count = map_size(entries)

    entries_bin =
      entries
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, count} ->
        <<byte_size(key)::unsigned-little-32, key::binary, count::unsigned-little-64>>
      end)
      |> IO.iodata_to_binary()

    <<
      @mg_magic::binary,
      1::unsigned-8,
      0::unsigned-8,
      k::unsigned-little-32,
      n::unsigned-little-64,
      entry_count::unsigned-little-32,
      entries_bin::binary
    >>
  end

  defp mg_insert(state, item_bytes) do
    new_n = state.n + 1

    cond do
      # Item already tracked: increment
      Map.has_key?(state.entries, item_bytes) ->
        new_entries = Map.update!(state.entries, item_bytes, &(&1 + 1))
        %{state | n: new_n, entries: new_entries}

      # Room for new entry
      map_size(state.entries) < state.k ->
        new_entries = Map.put(state.entries, item_bytes, 1)
        %{state | n: new_n, entries: new_entries}

      # No room: decrement all counters by 1, remove zeros
      true ->
        new_entries =
          state.entries
          |> Enum.map(fn {key, count} -> {key, count - 1} end)
          |> Enum.filter(fn {_key, count} -> count > 0 end)
          |> Map.new()

        %{state | n: new_n, entries: new_entries}
    end
  end

  # ============================================================
  # IBLT (Invertible Bloom Lookup Table) Implementation
  # ============================================================

  @iblt_magic "IBL1"
  @iblt_version 1
  @iblt_header_size 24
  @iblt_cell_size 24

  @impl true
  @spec iblt_new(keyword()) :: binary()
  def iblt_new(opts) do
    cell_count = Keyword.get(opts, :cell_count, 1000)
    hash_count = Keyword.get(opts, :hash_count, 3)
    seed = Keyword.get(opts, :seed, 0)

    body = :binary.copy(<<0::size(@iblt_cell_size * 8)>>, cell_count)

    <<
      @iblt_magic::binary,
      @iblt_version::unsigned-8,
      hash_count::unsigned-8,
      0::unsigned-16,
      cell_count::unsigned-little-32,
      0::unsigned-little-32,
      seed::unsigned-little-32,
      0::unsigned-little-32,
      body::binary
    >>
  end

  @impl true
  @spec iblt_put(binary(), non_neg_integer(), non_neg_integer(), keyword()) :: binary()
  def iblt_put(state_bin, key_hash, value_hash, _opts) do
    <<header::binary-size(@iblt_header_size), body::binary>> = state_bin

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      item_count::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32
    >> = header

    check = iblt_check_hash(key_hash)
    positions = iblt_positions(key_hash, seed, hash_count, cell_count)

    new_body =
      Enum.reduce(positions, body, fn pos, acc ->
        iblt_update_cell(acc, pos, 1, key_hash, value_hash, check)
      end)

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      item_count + 1::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32,
      new_body::binary
    >>
  end

  @impl true
  @spec iblt_put_many(binary(), [{non_neg_integer(), non_neg_integer()}], keyword()) :: binary()
  def iblt_put_many(state_bin, pairs, _opts) do
    Enum.reduce(pairs, state_bin, fn {key_hash, value_hash}, acc ->
      iblt_put(acc, key_hash, value_hash, [])
    end)
  end

  @impl true
  @spec iblt_member?(binary(), non_neg_integer(), keyword()) :: boolean()
  def iblt_member?(state_bin, key_hash, _opts) do
    <<_header::binary-size(@iblt_header_size), body::binary>> = state_bin

    <<
      _magic::binary-size(4),
      _version::unsigned-8,
      hash_count::unsigned-8,
      _reserved::unsigned-16,
      cell_count::unsigned-little-32,
      _item_count::unsigned-little-32,
      seed::unsigned-little-32,
      _reserved2::unsigned-little-32
    >> = binary_part(state_bin, 0, @iblt_header_size)

    positions = iblt_positions(key_hash, seed, hash_count, cell_count)

    Enum.all?(positions, fn pos ->
      offset = pos * @iblt_cell_size
      <<_before::binary-size(^offset), count::signed-little-32, _rest::binary>> = body
      count != 0
    end)
  end

  @impl true
  @spec iblt_delete(binary(), non_neg_integer(), non_neg_integer(), keyword()) :: binary()
  def iblt_delete(state_bin, key_hash, value_hash, _opts) do
    <<header::binary-size(@iblt_header_size), body::binary>> = state_bin

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      item_count::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32
    >> = header

    check = iblt_check_hash(key_hash)
    positions = iblt_positions(key_hash, seed, hash_count, cell_count)

    new_body =
      Enum.reduce(positions, body, fn pos, acc ->
        iblt_update_cell(acc, pos, -1, key_hash, value_hash, check)
      end)

    new_item_count = if item_count > 0, do: item_count - 1, else: 0

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      new_item_count::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32,
      new_body::binary
    >>
  end

  @impl true
  @spec iblt_subtract(binary(), binary(), keyword()) :: binary()
  def iblt_subtract(state_a, state_b, _opts) do
    <<header_a::binary-size(@iblt_header_size), body_a::binary>> = state_a
    <<_header_b::binary-size(@iblt_header_size), body_b::binary>> = state_b

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      item_count_a::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32
    >> = header_a

    <<
      @iblt_magic::binary,
      _::unsigned-8,
      _::unsigned-8,
      _::unsigned-16,
      _::unsigned-little-32,
      item_count_b::unsigned-little-32,
      _::unsigned-little-32,
      _::unsigned-little-32
    >> = binary_part(state_b, 0, @iblt_header_size)

    new_body = iblt_cellwise_op(body_a, body_b, cell_count, :subtract)

    diff = abs(item_count_a - item_count_b)

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      diff::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32,
      new_body::binary
    >>
  end

  @impl true
  @spec iblt_list_entries(binary(), keyword()) ::
          {:ok, %{positive: list(), negative: list()}} | {:error, :decode_failed}
  def iblt_list_entries(state_bin, _opts) do
    <<_header::binary-size(@iblt_header_size), body::binary>> = state_bin

    <<
      @iblt_magic::binary,
      _version::unsigned-8,
      hash_count::unsigned-8,
      _reserved::unsigned-16,
      cell_count::unsigned-little-32,
      _item_count::unsigned-little-32,
      seed::unsigned-little-32,
      _reserved2::unsigned-little-32
    >> = binary_part(state_bin, 0, @iblt_header_size)

    cells = iblt_decode_cells(body, cell_count)
    iblt_peel(cells, hash_count, seed, cell_count)
  end

  @impl true
  @spec iblt_count(binary(), keyword()) :: non_neg_integer()
  def iblt_count(state_bin, _opts) do
    <<_magic::binary-size(4), _version::unsigned-8, _hc::unsigned-8, _r::unsigned-16,
      _cc::unsigned-little-32, item_count::unsigned-little-32, _rest::binary>> = state_bin

    item_count
  end

  @impl true
  @spec iblt_merge(binary(), binary(), keyword()) :: binary()
  def iblt_merge(state_a, state_b, _opts) do
    <<header_a::binary-size(@iblt_header_size), body_a::binary>> = state_a
    <<_header_b::binary-size(@iblt_header_size), body_b::binary>> = state_b

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      item_count_a::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32
    >> = header_a

    <<
      @iblt_magic::binary,
      _::unsigned-8,
      _::unsigned-8,
      _::unsigned-16,
      _::unsigned-little-32,
      item_count_b::unsigned-little-32,
      _::unsigned-little-32,
      _::unsigned-little-32
    >> = binary_part(state_b, 0, @iblt_header_size)

    new_body = iblt_cellwise_op(body_a, body_b, cell_count, :merge)

    <<
      @iblt_magic::binary,
      version::unsigned-8,
      hash_count::unsigned-8,
      reserved1::unsigned-16,
      cell_count::unsigned-little-32,
      item_count_a + item_count_b::unsigned-little-32,
      seed::unsigned-little-32,
      reserved2::unsigned-little-32,
      new_body::binary
    >>
  end

  # -- IBLT private helpers --

  defp iblt_check_hash(key_hash) do
    mixed = iblt_splitmix64(key_hash * 0x517CC1B727220A95 &&& @mask64)
    mixed >>> 32 &&& 0xFFFFFFFF
  end

  defp iblt_splitmix64(x) do
    x = bxor(x, x >>> 30) * 0xBF58476D1CE4E5B9 &&& @mask64
    x = bxor(x, x >>> 27) * 0x94D049BB133111EB &&& @mask64
    bxor(x, x >>> 31) &&& @mask64
  end

  defp iblt_positions(key_hash, seed, hash_count, cell_count) do
    positions =
      Enum.reduce(0..(hash_count - 1), [], fn i, acc ->
        input = key_hash + (seed + i) * 0x9E3779B97F4A7C15 &&& @mask64
        h = iblt_splitmix64(input)
        pos = rem(h, cell_count)

        # Ensure distinct positions
        pos = iblt_resolve_collision(pos, acc, h, cell_count, 1)
        [pos | acc]
      end)

    Enum.reverse(positions)
  end

  defp iblt_resolve_collision(pos, existing, _h, cell_count, attempt)
       when attempt > cell_count do
    # Fallback: find first unused position
    all = MapSet.new(existing)

    Enum.find(0..(cell_count - 1), pos, fn p ->
      not MapSet.member?(all, p)
    end)
  end

  defp iblt_resolve_collision(pos, existing, h, cell_count, attempt) do
    if pos in existing do
      new_h = iblt_splitmix64(h + attempt &&& @mask64)
      new_pos = rem(new_h, cell_count)
      iblt_resolve_collision(new_pos, existing, new_h, cell_count, attempt + 1)
    else
      pos
    end
  end

  defp iblt_update_cell(body, pos, count_delta, key_hash, value_hash, check_hash) do
    offset = pos * @iblt_cell_size

    <<before::binary-size(^offset), count::signed-little-32, key_sum::unsigned-little-64,
      value_sum::unsigned-little-64, check_sum::unsigned-little-32, rest::binary>> = body

    <<before::binary, count + count_delta::signed-little-32,
      bxor(key_sum, key_hash)::unsigned-little-64,
      bxor(value_sum, value_hash)::unsigned-little-64,
      bxor(check_sum, check_hash)::unsigned-little-32, rest::binary>>
  end

  defp iblt_cellwise_op(body_a, body_b, cell_count, op) do
    Enum.reduce(0..(cell_count - 1), <<>>, fn i, acc ->
      offset = i * @iblt_cell_size

      <<_::binary-size(^offset), ca::signed-little-32, ksa::unsigned-little-64,
        vsa::unsigned-little-64, csa::unsigned-little-32, _::binary>> = body_a

      <<_::binary-size(^offset), cb::signed-little-32, ksb::unsigned-little-64,
        vsb::unsigned-little-64, csb::unsigned-little-32, _::binary>> = body_b

      new_count =
        case op do
          :subtract -> ca - cb
          :merge -> ca + cb
        end

      <<acc::binary, new_count::signed-little-32, bxor(ksa, ksb)::unsigned-little-64,
        bxor(vsa, vsb)::unsigned-little-64, bxor(csa, csb)::unsigned-little-32>>
    end)
  end

  defp iblt_decode_cells(body, cell_count) do
    for i <- 0..(cell_count - 1) do
      offset = i * @iblt_cell_size

      <<_::binary-size(^offset), count::signed-little-32, key_sum::unsigned-little-64,
        value_sum::unsigned-little-64, check_sum::unsigned-little-32, _::binary>> = body

      {count, key_sum, value_sum, check_sum}
    end
    |> :erlang.list_to_tuple()
  end

  defp iblt_peel(cells, hash_count, seed, cell_count) do
    iblt_peel_loop(cells, hash_count, seed, cell_count, [], [])
  end

  defp iblt_peel_loop(cells, hash_count, seed, cell_count, pos_acc, neg_acc) do
    # Find pure cells
    pure_cells =
      for i <- 0..(cell_count - 1),
          {count, key_sum, _value_sum, check_sum} = elem(cells, i),
          abs(count) == 1,
          iblt_check_hash(key_sum) == check_sum do
        i
      end

    if pure_cells == [] do
      # Check if all done
      all_zero =
        Enum.all?(0..(cell_count - 1), fn i ->
          {count, _, _, _} = elem(cells, i)
          count == 0
        end)

      if all_zero do
        {:ok, %{positive: Enum.reverse(pos_acc), negative: Enum.reverse(neg_acc)}}
      else
        {:error, :decode_failed}
      end
    else
      {new_cells, new_pos, new_neg} =
        Enum.reduce(pure_cells, {cells, pos_acc, neg_acc}, fn i, {c_acc, p_acc, n_acc} ->
          iblt_peel_one(c_acc, i, p_acc, n_acc, hash_count, seed, cell_count)
        end)

      iblt_peel_loop(new_cells, hash_count, seed, cell_count, new_pos, new_neg)
    end
  end

  defp iblt_peel_one(c_acc, i, p_acc, n_acc, hash_count, seed, cell_count) do
    {count, key_sum, value_sum, check_sum} = elem(c_acc, i)

    # Only process if still pure (may have been modified by earlier peel in this batch)
    if abs(count) == 1 and iblt_check_hash(key_sum) == check_sum do
      entry = {key_sum, value_sum}

      {p_acc, n_acc} =
        if count == 1, do: {[entry | p_acc], n_acc}, else: {p_acc, [entry | n_acc]}

      c_acc = iblt_remove_entry(c_acc, key_sum, value_sum, count, hash_count, seed, cell_count)
      {c_acc, p_acc, n_acc}
    else
      {c_acc, p_acc, n_acc}
    end
  end

  # ============================================================
  # REQ (Relative Error Quantiles) Implementation
  # ============================================================
  #
  # REQ sketch provides relative error guarantees on quantile values,
  # with asymmetric accuracy: HRA mode gives better accuracy at high
  # ranks (p99, p99.9), LRA mode at low ranks.
  #
  # The key difference from KLL is biased compaction: in HRA mode,
  # compaction preferentially discards low-value items, preserving
  # more data points at the high end of the distribution.
  #
  # State binary layout (REQ1):
  #   magic:           4 bytes  "REQ1"
  #   version:         u8       1
  #   flags:           u8       bit0 = hra (1=HRA, 0=LRA)
  #   reserved:        u16      0
  #   k:               u32 LE   accuracy parameter
  #   n:               u64 LE   total count
  #   min_val:         f64 LE   (NaN sentinel for empty)
  #   max_val:         f64 LE   (NaN sentinel for empty)
  #   num_levels:      u8
  #   compaction_bits: ceil(num_levels/8) bytes (1 bit per level parity)
  #   level_sizes:     num_levels x u32 LE
  #   items:           sum(level_sizes) x f64 LE (level 0 first)

  @req_magic "REQ1"
  @req_nan <<0, 0, 0, 0, 0, 0, 248, 127>>

  defmodule REQState do
    @moduledoc false
    @enforce_keys [
      :k,
      :hra,
      :n,
      :min_val,
      :max_val,
      :num_levels,
      :compaction_bits,
      :level_sizes,
      :levels
    ]
    defstruct [
      :k,
      :hra,
      :n,
      :min_val,
      :max_val,
      :num_levels,
      :compaction_bits,
      :level_sizes,
      :levels
    ]
  end

  @impl true
  @spec req_new(keyword()) :: binary()
  def req_new(opts) do
    k = Keyword.fetch!(opts, :k)
    hra = Keyword.get(opts, :hra, true)
    num_levels = 2
    parity_bytes = div(num_levels + 7, 8)
    compaction_bits = :binary.copy(<<0>>, parity_bytes)
    level_sizes = List.duplicate(0, num_levels)
    levels = List.duplicate([], num_levels)

    req_encode_state(%REQState{
      k: k,
      hra: hra,
      n: 0,
      min_val: :nan,
      max_val: :nan,
      num_levels: num_levels,
      compaction_bits: compaction_bits,
      level_sizes: level_sizes,
      levels: levels
    })
  end

  @impl true
  @spec req_update(binary(), float(), keyword()) :: binary()
  def req_update(state_bin, value, _opts) do
    state = req_decode_state(state_bin)
    state = req_insert_value(state, value)
    req_encode_from_map(state)
  end

  @impl true
  @spec req_update_many(binary(), [float()], keyword()) :: binary()
  def req_update_many(state_bin, values, _opts) do
    state = req_decode_state(state_bin)
    state = Enum.reduce(values, state, &req_insert_value(&2, &1))
    req_encode_from_map(state)
  end

  @impl true
  @spec req_merge(binary(), binary(), keyword()) :: binary()
  def req_merge(state_bin_a, state_bin_b, _opts) do
    a = req_decode_state(state_bin_a)
    b = req_decode_state(state_bin_b)
    merged = req_do_merge(a, b)
    req_encode_from_map(merged)
  end

  @impl true
  @spec req_quantile(binary(), float(), keyword()) :: float() | nil
  def req_quantile(state_bin, rank, _opts) do
    state = req_decode_state(state_bin)

    cond do
      state.n == 0 ->
        nil

      rank == 0.0 ->
        state.min_val

      rank == 1.0 ->
        state.max_val

      true ->
        sorted_view = req_build_sorted_view(state)
        kll_query_quantile(sorted_view, state.n, rank)
    end
  end

  @impl true
  @spec req_rank(binary(), float(), keyword()) :: float() | nil
  def req_rank(state_bin, value, _opts) do
    state = req_decode_state(state_bin)

    if state.n == 0 do
      nil
    else
      sorted_view = req_build_sorted_view(state)
      kll_query_rank(sorted_view, state.n, value)
    end
  end

  @impl true
  @spec req_cdf(binary(), [float()], keyword()) :: [float()] | nil
  def req_cdf(state_bin, split_points, _opts) do
    state = req_decode_state(state_bin)

    if state.n == 0 do
      nil
    else
      sorted_view = req_build_sorted_view(state)
      Enum.map(split_points, fn sp -> kll_query_rank(sorted_view, state.n, sp) end)
    end
  end

  @impl true
  @spec req_pmf(binary(), [float()], keyword()) :: [float()] | nil
  def req_pmf(state_bin, split_points, _opts) do
    state = req_decode_state(state_bin)

    if state.n == 0 do
      nil
    else
      sorted_view = req_build_sorted_view(state)
      cdf_values = Enum.map(split_points, fn sp -> kll_query_rank(sorted_view, state.n, sp) end)
      cdf_with_bounds = [0.0] ++ cdf_values ++ [1.0]

      cdf_with_bounds
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.map(fn [a, b] -> b - a end)
    end
  end

  @impl true
  @spec req_count(binary(), keyword()) :: non_neg_integer()
  def req_count(state_bin, _opts) do
    <<@req_magic, 1::8, _flags::8, _reserved::16, _k::32, n::unsigned-little-64, _::binary>> =
      state_bin

    n
  end

  @impl true
  @spec req_min(binary(), keyword()) :: float() | nil
  def req_min(state_bin, _opts) do
    <<@req_magic, 1::8, _flags::8, _reserved::16, _k::32, n::unsigned-little-64,
      min_bin::binary-size(8), _::binary>> = state_bin

    if n == 0, do: nil, else: req_decode_f64_value(min_bin)
  end

  @impl true
  @spec req_max(binary(), keyword()) :: float() | nil
  def req_max(state_bin, _opts) do
    <<@req_magic, 1::8, _flags::8, _reserved::16, _k::32, n::unsigned-little-64,
      _min_bin::binary-size(8), max_bin::binary-size(8), _::binary>> = state_bin

    if n == 0, do: nil, else: req_decode_f64_value(max_bin)
  end

  # -- REQ Private Helpers --

  defp req_level_capacity(k, level, num_levels) do
    depth = num_levels - 1 - level
    max(2, floor(k * :math.pow(2 / 3, depth)) + 1)
  end

  defp req_encode_state(%REQState{
         k: k,
         hra: hra,
         n: n,
         min_val: min_val,
         max_val: max_val,
         num_levels: num_levels,
         compaction_bits: compaction_bits,
         levels: items,
         level_sizes: level_sizes
       }) do
    min_bin = req_encode_f64(min_val)
    max_bin = req_encode_f64(max_val)
    flags = if hra, do: 1, else: 0
    parity_bytes = div(num_levels + 7, 8)

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
      @req_magic::binary,
      1::unsigned-8,
      flags::unsigned-8,
      0::unsigned-little-16,
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

  defp req_encode_f64(:nan), do: @req_nan
  defp req_encode_f64(val) when is_float(val), do: <<val::float-little-64>>

  defp req_decode_state(state_bin) do
    <<
      @req_magic::binary,
      1::unsigned-8,
      flags::unsigned-8,
      _reserved::unsigned-little-16,
      k::unsigned-little-32,
      n::unsigned-little-64,
      min_bin::binary-size(8),
      max_bin::binary-size(8),
      num_levels::unsigned-8,
      rest::binary
    >> = state_bin

    hra = (flags &&& 1) == 1
    min_val = req_decode_f64(min_bin, n)
    max_val = req_decode_f64(max_bin, n)

    parity_bytes = div(num_levels + 7, 8)
    <<compaction_bits::binary-size(^parity_bytes), rest2::binary>> = rest

    level_sizes_bytes = num_levels * 4
    <<level_sizes_bin::binary-size(^level_sizes_bytes), items_bin::binary>> = rest2

    level_sizes = kll_decode_u32_list(level_sizes_bin, [])
    levels = kll_decode_levels(items_bin, level_sizes, [])

    %REQState{
      k: k,
      hra: hra,
      n: n,
      min_val: min_val,
      max_val: max_val,
      num_levels: num_levels,
      compaction_bits: compaction_bits,
      level_sizes: level_sizes,
      levels: levels
    }
  end

  defp req_decode_f64(@req_nan, _n), do: :nan
  defp req_decode_f64(_bin, 0), do: :nan
  defp req_decode_f64(<<val::float-little-64>>, _n), do: val

  defp req_decode_f64_value(@req_nan), do: nil
  defp req_decode_f64_value(<<val::float-little-64>>), do: val

  defp req_encode_from_map(state) do
    req_encode_state(state)
  end

  defp req_insert_value(state, value) do
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

    state = req_compact_if_needed(state, 0)
    req_check_grow(state)
  end

  defp req_compact_if_needed(state, level) do
    if level >= state.num_levels - 1 do
      state
    else
      capacity = req_level_capacity(state.k, level, state.num_levels)
      level_size = Enum.at(state.level_sizes, level)

      if level_size >= capacity do
        req_compact_level(state, level)
      else
        state
      end
    end
  end

  defp req_check_grow(state) do
    top = state.num_levels - 1
    top_cap = req_level_capacity(state.k, top, state.num_levels)
    top_size = Enum.at(state.level_sizes, top)

    if top_size >= top_cap do
      req_grow_levels(state)
    else
      state
    end
  end

  defp req_grow_levels(state) do
    new_num_levels = state.num_levels + 1
    new_levels = state.levels ++ [[]]
    new_level_sizes = state.level_sizes ++ [0]

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

    state = req_recompact(state, 0)
    req_check_grow(state)
  end

  defp req_compact_level(state, level) do
    current_level = Enum.at(state.levels, level)
    sorted = Enum.sort(current_level)
    n = length(sorted)
    parity = kll_get_parity(state.compaction_bits, level)

    # REQ biased compaction:
    # HRA: compact (halve) the LOWER portion, keep UPPER intact at this level
    # LRA: compact (halve) the UPPER portion, keep LOWER intact at this level
    half = div(n, 2)

    {stay, promoted} =
      if state.hra do
        {lower, upper} = Enum.split(sorted, half)
        {upper, kll_select_half(lower, parity)}
      else
        {lower, upper} = Enum.split(sorted, n - half)
        {lower, kll_select_half(upper, parity)}
      end

    new_compaction_bits = kll_flip_parity(state.compaction_bits, level)

    new_levels = List.replace_at(state.levels, level, stay)
    new_level_sizes = List.replace_at(state.level_sizes, level, length(stay))

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

    req_compact_if_needed(state, level + 1)
  end

  defp req_build_sorted_view(state) do
    state.levels
    |> Enum.with_index()
    |> Enum.flat_map(fn {level, idx} ->
      weight = 1 <<< idx
      Enum.map(level, fn val -> {val, weight} end)
    end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp req_do_merge(a, b) do
    if a.hra != b.hra do
      raise ExDataSketch.Errors.IncompatibleSketchesError,
        reason: "REQ mode mismatch: cannot merge HRA and LRA sketches"
    end

    {new_min, new_max} =
      case {a.min_val, b.min_val} do
        {:nan, :nan} -> {:nan, :nan}
        {:nan, _} -> {b.min_val, b.max_val}
        {_, :nan} -> {a.min_val, a.max_val}
        _ -> {min(a.min_val, b.min_val), max(a.max_val, b.max_val)}
      end

    new_n = a.n + b.n
    max_levels = max(a.num_levels, b.num_levels)

    a_levels = a.levels ++ List.duplicate([], max_levels - a.num_levels)
    b_levels = b.levels ++ List.duplicate([], max_levels - b.num_levels)

    merged_levels = Enum.zip_with(a_levels, b_levels, fn al, bl -> al ++ bl end)
    merged_sizes = Enum.map(merged_levels, &length/1)

    a_bits = kll_pad_bits(a.compaction_bits, max_levels)
    b_bits = kll_pad_bits(b.compaction_bits, max_levels)

    merged_bits =
      :binary.bin_to_list(a_bits)
      |> Enum.zip(:binary.bin_to_list(b_bits))
      |> Enum.map(fn {ab, bb} -> bor(ab, bb) end)
      |> :binary.list_to_bin()

    state = %REQState{
      k: a.k,
      hra: a.hra,
      n: new_n,
      min_val: new_min,
      max_val: new_max,
      num_levels: max_levels,
      compaction_bits: merged_bits,
      level_sizes: merged_sizes,
      levels: merged_levels
    }

    req_recompact(state, 0)
  end

  defp req_recompact(state, level) when level >= state.num_levels, do: state

  defp req_recompact(state, level) do
    state = req_compact_if_needed(state, level)
    req_recompact(state, level + 1)
  end

  defp iblt_remove_entry(cells, key_sum, value_sum, count, hash_count, seed, cell_count) do
    positions = iblt_positions(key_sum, seed, hash_count, cell_count)
    check = iblt_check_hash(key_sum)

    Enum.reduce(positions, cells, fn pos, ca ->
      {pc, pk, pv, pck} = elem(ca, pos)
      new_cell = {pc - count, bxor(pk, key_sum), bxor(pv, value_sum), bxor(pck, check)}
      put_elem(ca, pos, new_cell)
    end)
  end

  # ============================================================
  # ULL (UltraLogLog) Implementation
  # ============================================================
  #
  # UltraLogLog (Ertl, 2023) uses the same 2^p register array as HLL,
  # but stores a different value per register that encodes both the
  # geometric rank and a sub-bucket bit, then uses the FGRA estimator
  # (sigma/tau from Ertl 2017) for ~20% better accuracy at same memory.
  #
  # State binary layout (ULL1):
  #   magic:     4 bytes  "ULL1"
  #   version:   1 byte   (u8, 1)
  #   precision: 1 byte   (u8, 4..26)
  #   reserved:  2 bytes  (u16 LE, 0)
  #   registers: 2^p bytes (one u8 per register)
  # Total: 8 + 2^p bytes.

  @ull_magic "ULL1"
  @ull_header_size 8

  @impl true
  @spec ull_new(keyword()) :: binary()
  def ull_new(opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p
    registers = :binary.copy(<<0>>, m)
    <<@ull_magic::binary, 1::unsigned-8, p::unsigned-8, 0::unsigned-little-16, registers::binary>>
  end

  @impl true
  @spec ull_update(binary(), non_neg_integer(), keyword()) :: binary()
  def ull_update(state_bin, hash64, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    bucket = hash64 >>> (64 - p)
    reg_value = ull_register_value(hash64, p)

    <<header::binary-size(@ull_header_size), registers::binary-size(^m)>> = state_bin

    <<before::binary-size(^bucket), old_val::unsigned-8, after_bytes::binary>> = registers

    if reg_value > old_val do
      <<header::binary, before::binary, reg_value::unsigned-8, after_bytes::binary>>
    else
      state_bin
    end
  end

  @impl true
  @spec ull_update_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def ull_update_many(state_bin, hashes, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    <<header::binary-size(@ull_header_size), registers::binary-size(^m)>> = state_bin

    # Pre-aggregate hashes into a map of {bucket => max_reg_value}.
    # This avoids materializing the full register array as a tuple,
    # which would OOM at high precision values (e.g. p=26 => 67M registers).
    updates =
      List.foldl(hashes, %{}, fn hash64, acc ->
        bucket = hash64 >>> (64 - p)
        reg_value = ull_register_value(hash64, p)
        Map.update(acc, bucket, reg_value, &max(&1, reg_value))
      end)

    sorted_updates = updates |> Map.to_list() |> List.keysort(0)
    new_registers = ull_splice_updates(registers, sorted_updates, 0, [])
    <<header::binary, IO.iodata_to_binary(new_registers)::binary>>
  end

  defp ull_splice_updates(registers, [], _offset, acc), do: Enum.reverse([registers | acc])

  defp ull_splice_updates(registers, [{bucket, new_val} | tail], offset, acc) do
    skip = bucket - offset
    <<before::binary-size(^skip), old_val::unsigned-8, after_bytes::binary>> = registers
    val = max(old_val, new_val)
    ull_splice_updates(after_bytes, tail, bucket + 1, [val, before | acc])
  end

  @impl true
  @spec ull_merge(binary(), binary(), keyword()) :: binary()
  def ull_merge(a_bin, b_bin, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    <<header_a::binary-size(@ull_header_size), regs_a::binary-size(^m)>> = a_bin
    <<_header_b::binary-size(@ull_header_size), regs_b::binary-size(^m)>> = b_bin

    merged =
      zip_max_binary(regs_a, regs_b)
      |> IO.iodata_to_binary()

    <<header_a::binary, merged::binary>>
  end

  @impl true
  @spec ull_estimate(binary(), keyword()) :: float()
  def ull_estimate(state_bin, opts) do
    p = Keyword.fetch!(opts, :p)
    m = 1 <<< p

    <<_header::binary-size(@ull_header_size), registers::binary-size(^m)>> = state_bin

    # Count registers at each value level
    # q_max = max register value
    {counts, q_max} = ull_register_counts(registers, m)

    if q_max == 0 do
      # All registers are zero => no items inserted
      0.0
    else
      ull_fgra_estimate(counts, m, q_max)
    end
  end

  # -- ULL Helpers --

  # Compute the ULL register value from a 64-bit hash and precision p.
  # register_value = 2 * geometric_rank - sub_bit
  # where sub_bit is the bit just after the leading zeros in the suffix.
  defp ull_register_value(hash64, p) do
    bits = 64 - p
    remaining_mask = (1 <<< bits) - 1
    remaining = hash64 &&& remaining_mask

    geometric_rank = count_leading_zeros(remaining, bits) + 1

    # sub_bit: the bit at position (p + geometric_rank) in the original hash,
    # which is the bit just after the leading zeros in the suffix.
    # This is bit (bits - geometric_rank) in `remaining` (0-indexed from MSB of suffix).
    # If geometric_rank > bits, all suffix bits are zero, sub_bit = 0.
    sub_bit =
      if geometric_rank > bits do
        0
      else
        bit_pos = bits - geometric_rank
        remaining >>> bit_pos &&& 1
      end

    value = 2 * geometric_rank - sub_bit
    # Clamp to 0..255
    min(value, 255)
  end

  # Count how many registers have each value 0..255.
  # Returns {counters_ref, q_max} where counters_ref is a :counters reference
  # with 256 slots (1-indexed; register value v is at slot v+1).
  # Uses :counters for O(1) mutable increment, avoiding per-byte tuple copies.
  defp ull_register_counts(registers, _m) do
    counts = :counters.new(256, [:atomics])
    q_max = ull_register_counts_loop(registers, counts, 0)
    {counts, q_max}
  end

  defp ull_register_counts_loop(<<>>, _counts, q_max), do: q_max

  defp ull_register_counts_loop(<<val::unsigned-8, rest::binary>>, counts, q_max) do
    :counters.add(counts, val + 1, 1)
    ull_register_counts_loop(rest, counts, max(q_max, val))
  end

  # FGRA estimator (Ertl 2017 "new HLL" estimator applied to ULL register values).
  #
  # Uses the Horner-scheme computation from Algorithm 4 of Ertl 2017:
  #   z = m * tau(1 - C[q]/m)
  #   for k from q-1 down to 1: z = (z + C[k]) * 0.5
  #   z += m * sigma(C[0]/m)
  #   estimate = alpha_inf * m^2 / z
  #
  # where alpha_inf = 1 / (2 * ln(2))
  defp ull_fgra_estimate(counts, m, q_max) do
    m_f = m * 1.0
    alpha_inf = 1.0 / (2.0 * :math.log(2.0))

    c0 = :counters.get(counts, 0 + 1) * 1.0
    c_q = :counters.get(counts, q_max + 1) * 1.0

    # Start with tau term (handles the boundary at q_max)
    z = m_f * ull_tau(1.0 - c_q / m_f)

    # Horner scheme: for k from q_max-1 down to 1
    z = ull_horner_loop(z, counts, q_max - 1)

    # Add sigma term (handles the C_0 / empty registers)
    z = z + m_f * ull_sigma(c0 / m_f)

    if z == 0.0 do
      0.0
    else
      alpha_inf * m_f * m_f / z
    end
  end

  # Horner loop: for k from start down to 1, z = (z + C[k]) * 0.5
  defp ull_horner_loop(z, _counts, k) when k < 1, do: z

  defp ull_horner_loop(z, counts, k) do
    c_k = :counters.get(counts, k + 1) * 1.0
    z = (z + c_k) * 0.5
    ull_horner_loop(z, counts, k - 1)
  end

  # sigma(x) from Ertl 2017 reference implementation:
  #   z = x, y = 1
  #   loop: x = x^2, z += x*y, y *= 2
  #   until convergence
  defp ull_sigma(x) when x <= 0.0, do: 0.0

  defp ull_sigma(x) do
    ull_sigma_loop(x, x, 1.0)
  end

  defp ull_sigma_loop(x, z, y) do
    x2 = x * x
    z2 = z + x2 * y
    y2 = y + y

    if z2 == z do
      z
    else
      ull_sigma_loop(x2, z2, y2)
    end
  end

  # tau(x) from Ertl 2017 reference implementation:
  #   z = 1 - x, y = 1
  #   loop: x = sqrt(x), y *= 0.5, z -= (1-x)^2 * y
  #   until convergence
  #   return z / 3
  defp ull_tau(x) when x <= 0.0 or x >= 1.0, do: 0.0

  defp ull_tau(x) do
    ull_tau_loop(x, 1.0 - x, 1.0)
  end

  defp ull_tau_loop(x, z, y) do
    x2 = :math.sqrt(x)
    y2 = y * 0.5
    t = 1.0 - x2
    z2 = z - t * t * y2

    if z2 == z do
      z / 3.0
    else
      ull_tau_loop(x2, z2, y2)
    end
  end
end
