defmodule ExDataSketch.Backend.Pure do
  @moduledoc """
  Pure Elixir backend for ExDataSketch.

  This module implements the `ExDataSketch.Backend` behaviour using only
  Elixir/Erlang standard library functions. It is always available and
  serves as the default backend.

  ## Phase 0 Status

  All functions are currently stubs that raise `ExDataSketch.Errors.NotImplementedError`.
  Full implementations will be provided in Phase 1.

  ## Implementation Notes

  - All state is stored as Elixir binaries with documented layouts.
  - Operations are pure: input binary in, new binary out. No side effects.
  - Batch operations (`update_many`) process all items in a single pass
    to minimize binary copying.
  """

  @behaviour ExDataSketch.Backend

  alias ExDataSketch.Errors

  # -- HLL --

  @impl true
  @spec hll_new(keyword()) :: binary()
  def hll_new(_opts) do
    Errors.not_implemented!(__MODULE__, "hll_new")
  end

  @impl true
  @spec hll_update(binary(), non_neg_integer(), keyword()) :: binary()
  def hll_update(_state_bin, _hash64, _opts) do
    Errors.not_implemented!(__MODULE__, "hll_update")
  end

  @impl true
  @spec hll_update_many(binary(), [non_neg_integer()], keyword()) :: binary()
  def hll_update_many(_state_bin, _hashes, _opts) do
    Errors.not_implemented!(__MODULE__, "hll_update_many")
  end

  @impl true
  @spec hll_merge(binary(), binary(), keyword()) :: binary()
  def hll_merge(_a_bin, _b_bin, _opts) do
    Errors.not_implemented!(__MODULE__, "hll_merge")
  end

  @impl true
  @spec hll_estimate(binary(), keyword()) :: float()
  def hll_estimate(_state_bin, _opts) do
    Errors.not_implemented!(__MODULE__, "hll_estimate")
  end

  # -- CMS --

  @impl true
  @spec cms_new(keyword()) :: binary()
  def cms_new(_opts) do
    Errors.not_implemented!(__MODULE__, "cms_new")
  end

  @impl true
  @spec cms_update(binary(), non_neg_integer(), pos_integer(), keyword()) :: binary()
  def cms_update(_state_bin, _hash64, _increment, _opts) do
    Errors.not_implemented!(__MODULE__, "cms_update")
  end

  @impl true
  @spec cms_update_many(binary(), [{non_neg_integer(), pos_integer()}], keyword()) :: binary()
  def cms_update_many(_state_bin, _pairs, _opts) do
    Errors.not_implemented!(__MODULE__, "cms_update_many")
  end

  @impl true
  @spec cms_merge(binary(), binary(), keyword()) :: binary()
  def cms_merge(_a_bin, _b_bin, _opts) do
    Errors.not_implemented!(__MODULE__, "cms_merge")
  end

  @impl true
  @spec cms_estimate(binary(), non_neg_integer(), keyword()) :: non_neg_integer()
  def cms_estimate(_state_bin, _hash64, _opts) do
    Errors.not_implemented!(__MODULE__, "cms_estimate")
  end
end
