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
end
