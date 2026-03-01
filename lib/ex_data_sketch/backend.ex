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
  - `ExDataSketch.Backend.Rust` -- Rust NIF acceleration (Phase 2, optional).

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
    Application.get_env(:ex_data_sketch, :backend, ExDataSketch.Backend.Pure)
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
