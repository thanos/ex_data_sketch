defmodule ExDataSketch.Backend.Rust do
  @moduledoc """
  Rust NIF-accelerated backend for ExDataSketch.

  Delegates batch and full-state traversal operations to Rust NIFs for
  performance, while falling back to `ExDataSketch.Backend.Pure` for
  lightweight operations like `new`, single `update`, and point queries.

  ## Availability

  This backend is only available when the Rust NIF has been compiled.
  Check with `ExDataSketch.Backend.Rust.available?/0`.

  ## Dirty Scheduler Thresholds

  Batch operations automatically use dirty CPU schedulers when the input
  size exceeds configurable thresholds. Defaults:

  - `hll_update_many`: 10,000 hashes
  - `cms_update_many`: 10,000 pairs
  - `theta_update_many`: 10,000 hashes
  - `cms_merge`: 100,000 total counters
  - `theta_compact`: 50,000 entries
  - `theta_merge`: 50,000 combined entries
  - `kll_update_many`: 10,000 values
  - `kll_merge`: 50,000 combined items
  - `ddsketch_update_many`: 10,000 values
  - `ddsketch_merge`: 50,000 combined count
  - `fi_update_many`: 10,000 items
  - `fi_merge`: 50,000 combined entries
  - `fi_nif_query`: 256 (k threshold for `fi_top_k`/`fi_estimate`; below this, Pure is used)

  Override globally via application config:

      config :ex_data_sketch, :dirty_thresholds, %{
        hll_update_many: 5_000,
        cms_update_many: 20_000
      }

  Or per-call via the `:dirty_threshold` option.
  """

  @behaviour ExDataSketch.Backend

  alias ExDataSketch.Backend.Pure

  @default_thresholds %{
    hll_update_many: 10_000,
    cms_update_many: 10_000,
    theta_update_many: 10_000,
    cms_merge: 100_000,
    theta_compact: 50_000,
    theta_merge: 50_000,
    kll_update_many: 10_000,
    kll_merge: 50_000,
    ddsketch_update_many: 10_000,
    ddsketch_merge: 50_000,
    fi_update_many: 10_000,
    fi_merge: 50_000,
    fi_nif_query: 256
  }

  @doc """
  Returns `true` if the Rust NIF is loaded and available.

  ## Examples

      iex> is_boolean(ExDataSketch.Backend.Rust.available?())
      true

  """
  @spec available?() :: boolean()
  def available? do
    Code.ensure_loaded?(ExDataSketch.Nif) and nif_loaded?()
  end

  defp nif_loaded? do
    ExDataSketch.Nif.nif_loaded() == :ok
  rescue
    _ -> false
  end

  # -- HLL callbacks --

  @impl true
  def hll_new(opts), do: Pure.hll_new(opts)

  @impl true
  def hll_update(state_bin, hash64, opts), do: Pure.hll_update(state_bin, hash64, opts)

  @impl true
  def hll_update_many(state_bin, hashes, opts) do
    p = Keyword.fetch!(opts, :p)
    hashes_bin = encode_hashes(hashes)
    threshold = dirty_threshold(:hll_update_many, opts)

    result =
      if length(hashes) > threshold do
        ExDataSketch.Nif.hll_update_many_dirty_nif(state_bin, hashes_bin, p)
      else
        ExDataSketch.Nif.hll_update_many_nif(state_bin, hashes_bin, p)
      end

    unwrap_ok!(result)
  end

  @impl true
  def hll_merge(a_bin, b_bin, opts) do
    p = Keyword.fetch!(opts, :p)
    unwrap_ok!(ExDataSketch.Nif.hll_merge_nif(a_bin, b_bin, p))
  end

  @impl true
  def hll_estimate(state_bin, opts) do
    p = Keyword.fetch!(opts, :p)
    unwrap_ok!(ExDataSketch.Nif.hll_estimate_nif(state_bin, p))
  end

  # -- CMS callbacks --

  @impl true
  def cms_new(opts), do: Pure.cms_new(opts)

  @impl true
  def cms_update(state_bin, hash64, increment, opts) do
    Pure.cms_update(state_bin, hash64, increment, opts)
  end

  @impl true
  def cms_update_many(state_bin, pairs, opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    pairs_bin = encode_pairs(pairs)
    threshold = dirty_threshold(:cms_update_many, opts)

    result =
      if length(pairs) > threshold do
        ExDataSketch.Nif.cms_update_many_dirty_nif(
          state_bin,
          pairs_bin,
          width,
          depth,
          counter_width
        )
      else
        ExDataSketch.Nif.cms_update_many_nif(state_bin, pairs_bin, width, depth, counter_width)
      end

    unwrap_ok!(result)
  end

  @impl true
  def cms_merge(a_bin, b_bin, opts) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)

    threshold = dirty_threshold(:cms_merge, opts)
    total_counters = width * depth

    result =
      if total_counters > threshold do
        ExDataSketch.Nif.cms_merge_dirty_nif(a_bin, b_bin, width, depth, counter_width)
      else
        ExDataSketch.Nif.cms_merge_nif(a_bin, b_bin, width, depth, counter_width)
      end

    unwrap_ok!(result)
  end

  @impl true
  def cms_estimate(state_bin, hash64, opts), do: Pure.cms_estimate(state_bin, hash64, opts)

  # -- Theta callbacks --

  @impl true
  def theta_new(opts), do: Pure.theta_new(opts)

  @impl true
  def theta_update(state_bin, hash64, opts), do: Pure.theta_update(state_bin, hash64, opts)

  @impl true
  def theta_update_many(state_bin, hashes, opts) do
    hashes_bin = encode_hashes(hashes)
    threshold = dirty_threshold(:theta_update_many, opts)

    result =
      if length(hashes) > threshold do
        ExDataSketch.Nif.theta_update_many_dirty_nif(state_bin, hashes_bin)
      else
        ExDataSketch.Nif.theta_update_many_nif(state_bin, hashes_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def theta_compact(state_bin, opts) do
    threshold = dirty_threshold(:theta_compact, opts)
    entry_count = theta_entry_count(state_bin)

    result =
      if entry_count > threshold do
        ExDataSketch.Nif.theta_compact_dirty_nif(state_bin)
      else
        ExDataSketch.Nif.theta_compact_nif(state_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def theta_merge(a_bin, b_bin, opts) do
    threshold = dirty_threshold(:theta_merge, opts)
    count_a = theta_entry_count(a_bin)
    count_b = theta_entry_count(b_bin)

    result =
      if count_a + count_b > threshold do
        ExDataSketch.Nif.theta_merge_dirty_nif(a_bin, b_bin)
      else
        ExDataSketch.Nif.theta_merge_nif(a_bin, b_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def theta_estimate(state_bin, opts), do: Pure.theta_estimate(state_bin, opts)

  @impl true
  def theta_from_components(k, theta, entries) do
    Pure.theta_from_components(k, theta, entries)
  end

  # -- KLL callbacks --

  @impl true
  def kll_new(opts), do: Pure.kll_new(opts)

  @impl true
  def kll_update(state_bin, value, opts), do: Pure.kll_update(state_bin, value, opts)

  @impl true
  def kll_update_many(state_bin, values, opts) do
    threshold = dirty_threshold(:kll_update_many, opts)
    values_bin = encode_f64s(values)

    result =
      if length(values) > threshold do
        ExDataSketch.Nif.kll_update_many_dirty_nif(state_bin, values_bin)
      else
        ExDataSketch.Nif.kll_update_many_nif(state_bin, values_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def kll_merge(a_bin, b_bin, opts) do
    threshold = dirty_threshold(:kll_merge, opts)

    # Use total items as proxy for work size
    a_n = kll_count(a_bin, opts)
    b_n = kll_count(b_bin, opts)

    result =
      if a_n + b_n > threshold do
        ExDataSketch.Nif.kll_merge_dirty_nif(a_bin, b_bin)
      else
        ExDataSketch.Nif.kll_merge_nif(a_bin, b_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def kll_quantile(state_bin, rank, opts), do: Pure.kll_quantile(state_bin, rank, opts)

  @impl true
  def kll_rank(state_bin, value, opts), do: Pure.kll_rank(state_bin, value, opts)

  @impl true
  def kll_count(state_bin, opts), do: Pure.kll_count(state_bin, opts)

  @impl true
  def kll_min(state_bin, opts), do: Pure.kll_min(state_bin, opts)

  @impl true
  def kll_max(state_bin, opts), do: Pure.kll_max(state_bin, opts)

  # -- DDSketch callbacks --

  @impl true
  def ddsketch_new(opts), do: Pure.ddsketch_new(opts)

  @impl true
  def ddsketch_update(state_bin, value, opts), do: Pure.ddsketch_update(state_bin, value, opts)

  @impl true
  def ddsketch_update_many(state_bin, values, opts) do
    threshold = dirty_threshold(:ddsketch_update_many, opts)
    values_bin = encode_f64s(values)

    result =
      if length(values) > threshold do
        ExDataSketch.Nif.ddsketch_update_many_dirty_nif(state_bin, values_bin)
      else
        ExDataSketch.Nif.ddsketch_update_many_nif(state_bin, values_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def ddsketch_merge(a_bin, b_bin, opts) do
    threshold = dirty_threshold(:ddsketch_merge, opts)

    a_n = ddsketch_count(a_bin, opts)
    b_n = ddsketch_count(b_bin, opts)

    result =
      if a_n + b_n > threshold do
        ExDataSketch.Nif.ddsketch_merge_dirty_nif(a_bin, b_bin)
      else
        ExDataSketch.Nif.ddsketch_merge_nif(a_bin, b_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def ddsketch_quantile(state_bin, rank, opts),
    do: Pure.ddsketch_quantile(state_bin, rank, opts)

  @impl true
  def ddsketch_count(state_bin, opts), do: Pure.ddsketch_count(state_bin, opts)

  @impl true
  def ddsketch_min(state_bin, opts), do: Pure.ddsketch_min(state_bin, opts)

  @impl true
  def ddsketch_max(state_bin, opts), do: Pure.ddsketch_max(state_bin, opts)

  # -- Bloom callbacks --

  @impl true
  def bloom_new(opts), do: Pure.bloom_new(opts)

  @impl true
  def bloom_put(state_bin, hash64, opts), do: Pure.bloom_put(state_bin, hash64, opts)

  @impl true
  def bloom_put_many(state_bin, hashes, opts), do: Pure.bloom_put_many(state_bin, hashes, opts)

  @impl true
  def bloom_member?(state_bin, hash64, opts), do: Pure.bloom_member?(state_bin, hash64, opts)

  @impl true
  def bloom_merge(state_bin_a, state_bin_b, opts),
    do: Pure.bloom_merge(state_bin_a, state_bin_b, opts)

  @impl true
  def bloom_count(state_bin, opts), do: Pure.bloom_count(state_bin, opts)

  # -- FrequentItems callbacks --

  @impl true
  def fi_new(opts) do
    k = Keyword.fetch!(opts, :k)
    flags = Keyword.get(opts, :flags, 0)
    unwrap_ok!(ExDataSketch.Nif.fi_new_nif(k, flags))
  end

  @impl true
  def fi_update(state_bin, item_bytes, opts) do
    fi_update_many(state_bin, [item_bytes], opts)
  end

  @impl true
  def fi_update_many(state_bin, items, opts) do
    packed_items_bin = encode_packed_items(items)
    threshold = dirty_threshold(:fi_update_many, opts)

    result =
      if length(items) > threshold do
        ExDataSketch.Nif.fi_update_many_dirty_nif(state_bin, packed_items_bin)
      else
        ExDataSketch.Nif.fi_update_many_nif(state_bin, packed_items_bin)
      end

    unwrap_ok!(result)
  end

  @impl true
  def fi_merge(state_a, state_b, opts) do
    threshold = dirty_threshold(:fi_merge, opts)

    a_ec = fi_entry_count(state_a, opts)
    b_ec = fi_entry_count(state_b, opts)

    result =
      if a_ec + b_ec > threshold do
        ExDataSketch.Nif.fi_merge_dirty_nif(state_a, state_b)
      else
        ExDataSketch.Nif.fi_merge_nif(state_a, state_b)
      end

    unwrap_ok!(result)
  end

  @impl true
  def fi_estimate(state_bin, item_bytes, opts) do
    k = Keyword.fetch!(opts, :k)

    if k >= dirty_threshold(:fi_nif_query, opts) do
      ExDataSketch.Nif.fi_estimate_nif(state_bin, item_bytes)
    else
      Pure.fi_estimate(state_bin, item_bytes, opts)
    end
  end

  @impl true
  def fi_top_k(state_bin, limit, opts) do
    k = Keyword.fetch!(opts, :k)

    if k >= dirty_threshold(:fi_nif_query, opts) do
      unwrap_ok!(ExDataSketch.Nif.fi_top_k_nif(state_bin, limit))
    else
      Pure.fi_top_k(state_bin, limit, opts)
    end
  end

  # O(1) header reads — Pure always wins over NIF boundary crossing
  @impl true
  def fi_count(state_bin, opts), do: Pure.fi_count(state_bin, opts)

  @impl true
  def fi_entry_count(state_bin, opts), do: Pure.fi_entry_count(state_bin, opts)

  # -- Private helpers --

  defp encode_f64s(values) do
    values
    |> Enum.map(fn v -> <<v::float-little-64>> end)
    |> IO.iodata_to_binary()
  end

  defp encode_hashes(hashes) do
    hashes
    |> Enum.map(fn h -> <<h::unsigned-little-64>> end)
    |> IO.iodata_to_binary()
  end

  defp encode_packed_items(items) do
    items
    |> Enum.map(fn item ->
      <<byte_size(item)::unsigned-little-32, item::binary>>
    end)
    |> IO.iodata_to_binary()
  end

  defp encode_pairs(pairs) do
    pairs
    |> Enum.map(fn {hash, inc} ->
      <<hash::unsigned-little-64, inc::unsigned-little-32>>
    end)
    |> IO.iodata_to_binary()
  end

  defp theta_entry_count(state_bin) do
    <<_v::8, _k::32, _theta::64, count::unsigned-little-32, _::binary>> = state_bin
    count
  end

  defp dirty_threshold(op, opts) do
    case Keyword.get(opts, :dirty_threshold) do
      nil ->
        app_thresholds = Application.get_env(:ex_data_sketch, :dirty_thresholds, %{})
        Map.get(app_thresholds, op, @default_thresholds[op])

      threshold when is_integer(threshold) ->
        threshold
    end
  end

  defp unwrap_ok!({:ok, value}), do: value

  defp unwrap_ok!({:error, reason}) do
    raise "Rust NIF error: #{reason}"
  end
end
