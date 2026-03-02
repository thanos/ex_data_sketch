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
  - `theta_merge`: 50,000 combined entries

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
    theta_merge: 50_000
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
  def theta_compact(state_bin, opts), do: Pure.theta_compact(state_bin, opts)

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

  # -- Private helpers --

  defp encode_hashes(hashes) do
    hashes
    |> Enum.map(fn h -> <<h::unsigned-little-64>> end)
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
