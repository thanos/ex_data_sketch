defmodule ExDataSketch.Sketch do
  @moduledoc """
  Behaviour implemented by every concrete sketch module.

  This is the unified contract referenced by `ExDataSketch.sketches/0` and by
  the top-level facade in `ExDataSketch` (`new/2`, `update/2`, `merge/2`, and
  so on). Implementing it lets generic code -- `ExDataSketch.GenStage.SketchConsumer`,
  `ExDataSketch.FilterChain`, the facade itself -- dispatch on the behaviour
  instead of probing with `function_exported?/3`.

  ## Which functions are required

  Every concrete sketch module (`ExDataSketch.HLL`, `ExDataSketch.Bloom`, and
  so on) implements `serialize/1`, `deserialize/1`, `size_bytes/1`, and
  `capabilities/0` unconditionally. `new/1`, `update/2`, `update_many/2`, and
  `merge/2` are declared `@optional_callbacks` because not every family
  supports every operation with that exact shape:

    - `ExDataSketch.XorFilter` is immutable once built (`build/2`) and has no
      `update/2`, `update_many/2`, or `merge/2`.
    - `ExDataSketch.FilterChain` constructs with `new/0` (no options), not
      `new/1`.
    - `ExDataSketch.Cuckoo` has no `merge/2` (per-bucket state is not
      associatively mergeable).

  A module's `capabilities/0` is the source of truth for which of these
  optional operations it actually supports at runtime; `@optional_callbacks`
  only relaxes the compile-time contract so dialyzer does not require every
  module to implement every function.

  ## `capabilities/0` returns a `MapSet`, not a boolean map

  Seven filter modules (`ExDataSketch.Bloom`, `Cuckoo`, `Quotient`, `CQF`,
  `XorFilter`, `IBLT`, `FilterChain`) already ship `capabilities/0` returning
  a `MapSet.t(atom())` of supported operation names (`:put`, `:merge`,
  `:member?`, `:delete`, and so on), consumed today by
  `ExDataSketch.FilterChain`'s stage-composition checks. This behaviour
  standardizes on that existing, tested shape for all 16 families rather than
  introducing a second, incompatible `capabilities/0` return type.

  ## Adding `@behaviour ExDataSketch.Sketch` to a module

      defmodule MyApp.Sketch do
        @behaviour ExDataSketch.Sketch

        defstruct [:state]

        @impl true
        def new(_opts \\\\ []), do: %__MODULE__{state: <<>>}

        @impl true
        def update(sketch, _item), do: sketch

        @impl true
        def serialize(%__MODULE__{state: state}), do: state

        @impl true
        def deserialize(binary), do: {:ok, %__MODULE__{state: binary}}

        @impl true
        def size_bytes(%__MODULE__{state: state}), do: byte_size(state)

        @impl true
        def capabilities, do: MapSet.new([:new, :update, :serialize, :deserialize])
      end

  ## Examples

      iex> ExDataSketch.Sketch.implemented?(ExDataSketch.Bloom)
      true

      iex> ExDataSketch.Sketch.implemented?(String)
      false

  """

  @typedoc "A struct produced by a module implementing this behaviour."
  @type sketch :: struct()

  @typedoc """
  The set of operation names a module reports as supported, as returned by
  `c:capabilities/0`. Existing members in production code include `:new`,
  `:put`, `:put_many`, `:member?`, `:merge`, `:merge_many`, `:count`,
  `:serialize`, `:deserialize`, `:compatible_with?`, `:delete`,
  `:estimate_count`, `:subtract`, `:list_entries`, and `:add_stage`.
  """
  @type capabilities :: MapSet.t(atom())

  @doc """
  Creates a new sketch with the given options.

  Optional: `ExDataSketch.XorFilter` has no `new/1` (its constructor is
  `build/2`, which requires the full item set upfront) and
  `ExDataSketch.FilterChain` has `new/0` instead (no per-sketch options to
  configure).
  """
  @callback new(opts :: keyword()) :: sketch()

  @doc """
  Updates a sketch with a single item, returning the updated sketch.

  Optional: not implemented by `ExDataSketch.XorFilter` (immutable once
  built).
  """
  @callback update(sketch(), item :: term()) :: sketch()

  @doc """
  Updates a sketch with every item in an enumerable, returning the updated
  sketch.

  Optional: not implemented by `ExDataSketch.XorFilter` (immutable once
  built).
  """
  @callback update_many(sketch(), Enumerable.t()) :: sketch()

  @doc """
  Merges two sketches of the same family and compatible parameters.

  Returns the merged sketch, or `{:error, exception}` when the two sketches
  have incompatible parameters (for example, different HLL precision).

  Optional: not implemented by `ExDataSketch.Cuckoo` (bucket state is not
  associatively mergeable) or `ExDataSketch.XorFilter` (immutable).
  """
  @callback merge(sketch(), sketch()) :: sketch() | {:error, Exception.t()}

  @doc """
  Serializes a sketch to its canonical binary representation.
  """
  @callback serialize(sketch()) :: binary()

  @doc """
  Deserializes a binary produced by `c:serialize/1` back into a sketch.

  Returns `{:error, exception}` for malformed or corrupted input rather than
  raising, so callers can handle untrusted or persisted binaries safely.
  """
  @callback deserialize(binary()) :: {:ok, sketch()} | {:error, Exception.t()}

  @doc """
  Returns the size, in bytes, of the sketch's serialized state.
  """
  @callback size_bytes(sketch()) :: non_neg_integer()

  @doc """
  Returns the set of operation names this module supports.

  See `t:capabilities/0` for the vocabulary of operation names in use.
  """
  @callback capabilities() :: capabilities()

  @optional_callbacks new: 1, update: 2, update_many: 2, merge: 2

  @doc """
  Returns `true` if `module` implements every non-optional callback of this
  behaviour (`serialize/1`, `deserialize/1`, `size_bytes/1`, `capabilities/0`).

  This checks function export, not the `@behaviour` declaration itself, so it
  also recognizes modules that satisfy the contract without formally
  declaring `@behaviour ExDataSketch.Sketch`.

  ## Examples

      iex> ExDataSketch.Sketch.implemented?(ExDataSketch.Bloom)
      true

      iex> ExDataSketch.Sketch.implemented?(Enum)
      false

      iex> ExDataSketch.Sketch.implemented?(ExDataSketch.HLL)
      true

  """
  @spec implemented?(module()) :: boolean()
  def implemented?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and
      function_exported?(module, :serialize, 1) and
      function_exported?(module, :deserialize, 1) and
      function_exported?(module, :size_bytes, 1) and
      function_exported?(module, :capabilities, 0)
  end
end
