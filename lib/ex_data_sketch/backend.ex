defmodule ExDataSketch.Backend do
  @moduledoc """
  Behaviour defining the computation backend for ExDataSketch.

  All sketch computations are dispatched through a backend module. This
  abstraction allows swapping between the pure Elixir implementation and
  an optional Rust NIF backend without changing the public API.

  ## Implementing a Backend

  A backend module must implement all callbacks defined in this behaviour.
  The canonical sketch state is always an Elixir binary. Backend functions
  receive and return binaries; they never own persistent sketch state.

  ## Available Backends

  - `ExDataSketch.Backend.Pure` -- Pure Elixir (always available, default).
  - `ExDataSketch.Backend.Rust` -- Rust NIF acceleration (optional).
    Accelerates batch operations (`update_many`, `merge`, `estimate`).
    Falls back to Pure if the NIF is not compiled.
    Check with `ExDataSketch.Backend.Rust.available?/0`.

  ## Backend Selection

  The backend is resolved in this order:

  1. Per-sketch `:backend` option (e.g., `HLL.new(backend: Backend.Rust)`).
  2. Application config: `config :ex_data_sketch, backend: Backend.Pure`.
  3. Default: `ExDataSketch.Backend.Pure`.
  """

  @type state_bin :: binary()
  @type hash64 :: non_neg_integer()
  @type opts :: keyword()

  # -- HLL callbacks --

  @doc "Create a new HLL state binary with the given options."
  @callback hll_new(opts()) :: state_bin()

  @doc "Update HLL state with a single hash64 value."
  @callback hll_update(state_bin(), hash64(), opts()) :: state_bin()

  @doc "Update HLL state with a list of hash64 values in a single pass."
  @callback hll_update_many(state_bin(), [hash64()], opts()) :: state_bin()

  @doc "Merge two HLL state binaries (register-wise max)."
  @callback hll_merge(state_bin(), state_bin(), opts()) :: state_bin()

  @doc "Estimate cardinality from HLL state."
  @callback hll_estimate(state_bin(), opts()) :: float()

  # -- CMS callbacks --

  @doc "Create a new CMS state binary with the given options."
  @callback cms_new(opts()) :: state_bin()

  @doc "Update CMS state with a single hash64 and increment."
  @callback cms_update(state_bin(), hash64(), pos_integer(), opts()) :: state_bin()

  @doc "Update CMS state with a list of {hash64, increment} pairs."
  @callback cms_update_many(state_bin(), [{hash64(), pos_integer()}], opts()) :: state_bin()

  @doc "Merge two CMS state binaries (element-wise add)."
  @callback cms_merge(state_bin(), state_bin(), opts()) :: state_bin()

  @doc "Estimate the count for a given hash64 from CMS state."
  @callback cms_estimate(state_bin(), hash64(), opts()) :: non_neg_integer()

  # -- Theta callbacks --

  @doc "Create a new Theta state binary with the given options."
  @callback theta_new(opts()) :: state_bin()

  @doc "Update Theta state with a single hash64 value."
  @callback theta_update(state_bin(), hash64(), opts()) :: state_bin()

  @doc "Update Theta state with a list of hash64 values in a single pass."
  @callback theta_update_many(state_bin(), [hash64()], opts()) :: state_bin()

  @doc "Compact Theta state: sort entries and discard any above theta."
  @callback theta_compact(state_bin(), opts()) :: state_bin()

  @doc "Merge two Theta state binaries (set union)."
  @callback theta_merge(state_bin(), state_bin(), opts()) :: state_bin()

  @doc "Estimate cardinality from Theta state."
  @callback theta_estimate(state_bin(), opts()) :: float()

  @doc "Build Theta state binary from raw components (k, theta, sorted entries list)."
  @callback theta_from_components(non_neg_integer(), non_neg_integer(), [non_neg_integer()]) ::
              state_bin()

  # -- KLL callbacks --

  @doc "Create a new KLL state binary with the given options."
  @callback kll_new(opts()) :: state_bin()

  @doc "Update KLL state with a single float64 value."
  @callback kll_update(state_bin(), float(), opts()) :: state_bin()

  @doc "Update KLL state with a list of float64 values in a single pass."
  @callback kll_update_many(state_bin(), [float()], opts()) :: state_bin()

  @doc "Merge two KLL state binaries."
  @callback kll_merge(state_bin(), state_bin(), opts()) :: state_bin()

  @doc "Return the approximate value at a given normalized rank from KLL state."
  @callback kll_quantile(state_bin(), float(), opts()) :: float() | nil

  @doc "Return the approximate normalized rank of a given value from KLL state."
  @callback kll_rank(state_bin(), float(), opts()) :: float() | nil

  @doc "Return the count of items inserted into KLL state."
  @callback kll_count(state_bin(), opts()) :: non_neg_integer()

  @doc "Return the minimum value in KLL state, or nil if empty."
  @callback kll_min(state_bin(), opts()) :: float() | nil

  @doc "Return the maximum value in KLL state, or nil if empty."
  @callback kll_max(state_bin(), opts()) :: float() | nil

  # -- DDSketch callbacks --

  @doc "Create a new DDSketch state binary with the given options."
  @callback ddsketch_new(opts()) :: state_bin()

  @doc "Update DDSketch state with a single float64 value."
  @callback ddsketch_update(state_bin(), float(), opts()) :: state_bin()

  @doc "Update DDSketch state with a list of float64 values in a single pass."
  @callback ddsketch_update_many(state_bin(), [float()], opts()) :: state_bin()

  @doc "Merge two DDSketch state binaries."
  @callback ddsketch_merge(state_bin(), state_bin(), opts()) :: state_bin()

  @doc "Return the approximate value at a given normalized rank from DDSketch state."
  @callback ddsketch_quantile(state_bin(), float(), opts()) :: float() | nil

  @doc "Return the count of items inserted into DDSketch state."
  @callback ddsketch_count(state_bin(), opts()) :: non_neg_integer()

  @doc "Return the minimum value in DDSketch state, or nil if empty."
  @callback ddsketch_min(state_bin(), opts()) :: float() | nil

  @doc "Return the maximum value in DDSketch state, or nil if empty."
  @callback ddsketch_max(state_bin(), opts()) :: float() | nil

  @doc """
  Returns the default backend module.

  Checks application config first, falls back to `ExDataSketch.Backend.Pure`.

  ## Examples

      iex> backend = ExDataSketch.Backend.default()
      iex> backend == ExDataSketch.Backend.Pure
      true

  """
  @spec default() :: module()
  def default do
    alias __MODULE__.{Pure, Rust}

    case Application.get_env(:ex_data_sketch, :backend) do
      nil ->
        Pure

      Rust ->
        if Rust.available?(), do: Rust, else: Pure

      other ->
        other
    end
  end

  @doc """
  Resolves the backend from options or application config.

  ## Examples

      iex> ExDataSketch.Backend.resolve(backend: ExDataSketch.Backend.Pure)
      ExDataSketch.Backend.Pure

      iex> ExDataSketch.Backend.resolve([])
      ExDataSketch.Backend.Pure

  """
  @spec resolve(keyword()) :: module()
  def resolve(opts) do
    Keyword.get(opts, :backend, default())
  end
end
