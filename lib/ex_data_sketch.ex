defmodule ExDataSketch do
  @moduledoc """
  Production-grade streaming data sketching algorithms for Elixir.

  ExDataSketch provides probabilistic data structures for approximate counting
  and frequency estimation on streaming data. All sketch state is stored as
  Elixir-owned binaries, enabling straightforward serialization, distribution,
  and persistence.

  ## Sketch Families

  - `ExDataSketch.HLL` -- HyperLogLog for cardinality (distinct count) estimation.
  - `ExDataSketch.CMS` -- Count-Min Sketch for frequency estimation.
  - `ExDataSketch.Theta` -- Theta Sketch for set operations on cardinalities.
  - `ExDataSketch.KLL` -- KLL Sketch for rank and quantile estimation.
  - `ExDataSketch.DDSketch` -- DDSketch for value-relative-accuracy quantile estimation.
  - `ExDataSketch.FrequentItems` -- SpaceSaving for approximate heavy-hitter detection.
  - `ExDataSketch.Bloom` -- Bloom filter for probabilistic membership testing.
  - `ExDataSketch.Cuckoo` -- Cuckoo filter for membership testing with deletion support.
  - `ExDataSketch.Quotient` -- Quotient filter for membership testing with deletion and merge.
  - `ExDataSketch.CQF` -- Counting Quotient Filter for multiset membership with approximate counting.
  - `ExDataSketch.XorFilter` -- Xor filter for static, immutable membership testing.
  - `ExDataSketch.IBLT` -- Invertible Bloom Lookup Table for set reconciliation.
  - `ExDataSketch.FilterChain` -- Capability-aware composition framework for membership filters.
  - `ExDataSketch.REQ` -- REQ Sketch for relative error quantiles with tail accuracy.
  - `ExDataSketch.MisraGries` -- Misra-Gries for deterministic heavy hitter detection.
  - `ExDataSketch.Quantiles` -- Facade for quantile sketch algorithms.

  ## Unified Facade

  `sketches/0` is the registry mapping each family atom (`:hll`, `:bloom`,
  and so on) to its module -- the single source of truth other generic code
  (`ExDataSketch.Sketch.implemented?/1`, the capability-matrix test) is
  checked against. `new/2`, `update/2`, `merge/2`, `merge_many/1`,
  `estimate/1`, `serialize/1`, `deserialize/2`, `size_bytes/1`, and
  `capabilities/1` are generic dispatch functions that work across all 16
  concrete families without the caller needing to know which module a
  struct belongs to. They are additive: every per-family module
  (`HLL.new/1`, `Bloom.put/2`, and so on) remains the documented, primary
  API and is unaffected. See `baoulo/plans/plan-0.10.0.md` (Phase 1) and
  `baoulo/plans/0.10.0_phase1_stub_review.md` for the design rationale,
  including the small set of documented exceptions (`ExDataSketch.XorFilter`
  has no incremental `update/2`; `ExDataSketch.Cuckoo`, `XorFilter`, and
  `ExDataSketch.FilterChain` have no `merge/2`; `CMS` and the filter
  families have no single-value `estimate/1`).

  ## Architecture

  - **Binary state**: All sketch state is canonical Elixir binaries. No opaque
    NIF resources.
  - **Backend system**: Computation is dispatched through backend modules.
    `ExDataSketch.Backend.Pure` (pure Elixir) is always available.
    `ExDataSketch.Backend.Rust` (optional, precompiled binaries provided) provides NIF acceleration.
  - **Serialization**: ExDataSketch-native format (EXSK) for all sketches,
    plus Apache DataSketches interop for Theta CompactSketch.
  - **Deterministic hashing**: `ExDataSketch.Hash` provides a stable 64-bit
    hash interface for reproducible results.

  ## Quick Example

      # Cardinality estimation with HLL
      sketch = ExDataSketch.HLL.new(p: 14)
      sketch = ExDataSketch.update_many(sketch, ["alice", "bob", "alice"])
      ExDataSketch.HLL.estimate(sketch)

      # Frequency estimation with CMS
      sketch = ExDataSketch.CMS.new(width: 2048, depth: 5)
      sketch = ExDataSketch.update_many(sketch, ["page_a", "page_a", "page_b"])
      ExDataSketch.CMS.estimate(sketch, "page_a")

  ## Integration Patterns

  Each sketch module provides convenience functions for ecosystem integration:

  - `from_enumerable/2` — build a sketch from any `Enumerable` in one call.
  - `merge_many/1` — merge a collection of sketches (e.g. from parallel workers).
  - `reducer/1` — returns a 2-arity function for use with `Enum.reduce/3`, Flow, etc.
  - `merger/1` — returns a 2-arity function for merging sketches in reduce operations.

  ## Stream Integration

  `ExDataSketch.Stream` provides terminal stream consumers that build sketches
  from lazy enumerables without buffering the entire input:

      1..100_000
      |> Stream.map(&to_string/1)
      |> ExDataSketch.Stream.hll(p: 14)
      |> ExDataSketch.HLL.estimate()

  For partition-local reduction:

      1..1_000_000
      |> ExDataSketch.Stream.reduce_partitioned(ExDataSketch.HLL, partitions: 8, p: 14)

  ## Collectable

  All mergeable sketches implement the `Collectable` protocol, enabling
  `Enum.into/2` usage:

      sketch = Enum.into(1..1000, ExDataSketch.HLL.new(p: 14))

  See the [Integration Guide](integrations.md) for examples with Flow, Broadway,
  Explorer, Nx, and other ecosystem libraries.

  See the [Quick Start guide](quick_start.md) for more examples.
  """

  alias ExDataSketch.{
    Bloom,
    CMS,
    CQF,
    Cuckoo,
    DDSketch,
    Errors,
    FilterChain,
    FrequentItems,
    HLL,
    IBLT,
    KLL,
    MisraGries,
    Quotient,
    REQ,
    Sketch,
    Theta,
    ULL,
    XorFilter
  }

  @typedoc """
  A registry key identifying one of the 16 concrete sketch families. See
  `sketches/0` for the full atom-to-module mapping.
  """
  @type sketch_type ::
          :hll
          | :ull
          | :cms
          | :theta
          | :kll
          | :ddsketch
          | :req
          | :frequent_items
          | :misra_gries
          | :bloom
          | :cuckoo
          | :quotient
          | :cqf
          | :xor_filter
          | :iblt
          | :filter_chain

  @registry %{
    hll: HLL,
    ull: ULL,
    cms: CMS,
    theta: Theta,
    kll: KLL,
    ddsketch: DDSketch,
    req: REQ,
    frequent_items: FrequentItems,
    misra_gries: MisraGries,
    bloom: Bloom,
    cuckoo: Cuckoo,
    quotient: Quotient,
    cqf: CQF,
    xor_filter: XorFilter,
    iblt: IBLT,
    filter_chain: FilterChain
  }

  @doc """
  Returns the registry mapping every sketch family's atom key to its module.

  This is the single source of truth for which atom names which concrete
  sketch module. `update_many/2` below and `ExDataSketch.Sketch.implemented?/1`
  are checked against this map (see the capability-matrix test in
  `test/ex_data_sketch_sketch_test.exs`), so they cannot drift from it.
  `ExDataSketch.Stream`'s per-family functions and `ExDataSketch.Collectable`'s
  `defimpl` blocks still enumerate families independently and are not yet
  reconciled against this registry.

  ## Examples

      iex> ExDataSketch.sketches()[:hll]
      ExDataSketch.HLL

      iex> map_size(ExDataSketch.sketches())
      16

      iex> Enum.all?(ExDataSketch.sketches(), fn {_type, module} -> Code.ensure_loaded?(module) end)
      true

  """
  @spec sketches() :: %{sketch_type() => module()}
  def sketches, do: @registry

  @doc """
  Creates a new sketch of the given family.

  Dispatches to `module.new(opts)` for the module registered under `type` in
  `sketches/0`.

  Two families do not follow the common `new/1` shape and are special-cased:

    - `:filter_chain` -- `ExDataSketch.FilterChain.new/0` takes no options.
      Passing a non-empty `opts` raises `ExDataSketch.Errors.InvalidOptionError`.
    - `:xor_filter` -- `ExDataSketch.XorFilter` has no incremental constructor;
      it is built once from a complete item set via
      `ExDataSketch.XorFilter.build/2`. Calling `new/2` with `:xor_filter`
      raises `ExDataSketch.Errors.UnsupportedOperationError` directing
      callers to `build/2`.

  Raises `ExDataSketch.Errors.InvalidOptionError` if `type` is not a key in
  `sketches/0`.

  ## Examples

      iex> sketch = ExDataSketch.new(:hll, p: 10)
      iex> match?(%ExDataSketch.HLL{}, sketch)
      true

      iex> chain = ExDataSketch.new(:filter_chain)
      iex> ExDataSketch.FilterChain.stages(chain)
      []

      iex> try do
      ...>   ExDataSketch.new(:xor_filter)
      ...> rescue
      ...>   e in ExDataSketch.Errors.UnsupportedOperationError -> e.message
      ...> end
      "ExDataSketch.XorFilter (build-once; use ExDataSketch.XorFilter.build/2 with the full item set) does not support new/2"

      iex> try do
      ...>   ExDataSketch.new(:no_such_family)
      ...> rescue
      ...>   e in ExDataSketch.Errors.InvalidOptionError -> e.option
      ...> end
      :type

  """
  @spec new(sketch_type(), keyword()) :: Sketch.sketch()
  def new(type, opts \\ [])

  def new(:filter_chain, []), do: FilterChain.new()

  def new(:filter_chain, opts) do
    raise Errors.InvalidOptionError,
      option: :opts,
      value: opts,
      message: "ExDataSketch.FilterChain.new/0 takes no options, got: #{inspect(opts)}"
  end

  def new(:xor_filter, _opts) do
    raise Errors.UnsupportedOperationError,
      operation: "new/2",
      structure:
        "ExDataSketch.XorFilter (build-once; use ExDataSketch.XorFilter.build/2 with the full item set)"
  end

  def new(type, opts) when is_atom(type) and is_list(opts) do
    apply(fetch_module!(type), :new, [opts])
  end

  @doc """
  Updates a sketch with a single item, dispatching on the sketch's struct
  type.

  `ExDataSketch.XorFilter` has no incremental update (it is immutable once
  built) and raises `ExDataSketch.Errors.UnsupportedOperationError` directing
  callers to `ExDataSketch.XorFilter.build/2`.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> sketch = ExDataSketch.update(sketch, "a")
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

      iex> try do
      ...>   {:ok, filter} = ExDataSketch.XorFilter.build(["a"])
      ...>   ExDataSketch.update(filter, "b")
      ...> rescue
      ...>   e in ExDataSketch.Errors.UnsupportedOperationError -> e.message
      ...> end
      "ExDataSketch.XorFilter (immutable once built; see ExDataSketch.XorFilter.build/2) does not support update/2"

  """
  @spec update(Sketch.sketch(), term()) :: Sketch.sketch()
  def update(%XorFilter{}, _item) do
    raise Errors.UnsupportedOperationError,
      operation: "update/2",
      structure:
        "ExDataSketch.XorFilter (immutable once built; see ExDataSketch.XorFilter.build/2)"
  end

  def update(%mod{} = sketch, item), do: mod.update(sketch, item)

  @doc """
  Merges two sketches of the same family and compatible parameters,
  dispatching on the sketches' struct type.

  Three families have no associative merge and raise
  `ExDataSketch.Errors.UnsupportedOperationError`: `ExDataSketch.Cuckoo`
  (bucket state is not associatively mergeable), `ExDataSketch.XorFilter`
  (immutable once built), and `ExDataSketch.FilterChain` (chains have no
  merge semantics). Merging two sketches of different families returns
  `{:error, %ExDataSketch.Errors.IncompatibleSketchesError{}}`. Merging two
  sketches of the same family with incompatible parameters (for example, two
  HLLs with different `p`) raises `ExDataSketch.Errors.IncompatibleSketchesError`,
  same as calling the family module's own `merge/2` directly.

  ## Examples

      iex> a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> merged = ExDataSketch.merge(a, b)
      iex> ExDataSketch.HLL.estimate(merged) > 0.0
      true

      iex> {:error, error} = ExDataSketch.merge(ExDataSketch.HLL.new(p: 10), ExDataSketch.CMS.new())
      iex> error.__struct__
      ExDataSketch.Errors.IncompatibleSketchesError

  """
  @spec merge(Sketch.sketch(), Sketch.sketch()) :: Sketch.sketch() | {:error, Exception.t()}
  def merge(%Cuckoo{}, %Cuckoo{}) do
    raise Errors.UnsupportedOperationError,
      operation: "merge/2",
      structure: "ExDataSketch.Cuckoo (bucket state is not associatively mergeable)"
  end

  def merge(%XorFilter{}, %XorFilter{}) do
    raise Errors.UnsupportedOperationError,
      operation: "merge/2",
      structure: "ExDataSketch.XorFilter (immutable once built)"
  end

  def merge(%FilterChain{}, %FilterChain{}) do
    raise Errors.UnsupportedOperationError,
      operation: "merge/2",
      structure: "ExDataSketch.FilterChain (chains have no merge semantics)"
  end

  def merge(%mod{} = a, %mod{} = b), do: mod.merge(a, b)

  def merge(%mod_a{}, %mod_b{}) do
    Errors.error(
      Errors.IncompatibleSketchesError.exception(
        reason: "cannot merge #{inspect(mod_a)} with #{inspect(mod_b)}"
      )
    )
  end

  @doc """
  Merges a non-empty list of sketches of the same family, dispatching on the
  sketches' struct type via `merge/2`.

  ## Examples

      iex> sketches = [
      ...>   ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a"),
      ...>   ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b"),
      ...>   ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("c")
      ...> ]
      iex> merged = ExDataSketch.merge_many(sketches)
      iex> ExDataSketch.HLL.estimate(merged) > 0.0
      true

  """
  @spec merge_many([Sketch.sketch(), ...]) :: Sketch.sketch()
  def merge_many([%_{} | _] = sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns the sketch's headline scalar estimate, dispatching on the sketch's
  struct type.

  Cardinality families (`HLL`, `ULL`, `Theta`) answer with `estimate/1`.
  Quantile families (`KLL`, `DDSketch`, `REQ`) and the two heavy-hitter
  families (`FrequentItems`, `MisraGries`) answer with `count/1` (the total
  number of ingested values). `CMS` and all membership filters have no
  single-value cardinality reading -- `CMS.estimate/2` and
  `FrequentItems.estimate/2` / `MisraGries.estimate/2` are per-item
  questions, and a filter's own `count/1` (where present) measures something
  else entirely (for example, `Bloom.count/1` is a bitset popcount, not a
  cardinality estimate). Calling `estimate/1` on any of these raises
  `ExDataSketch.Errors.UnsupportedOperationError` directing callers to the
  family module.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.estimate(sketch) > 0.0
      true

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update(1.0)
      iex> ExDataSketch.estimate(sketch)
      1

      iex> try do
      ...>   ExDataSketch.estimate(ExDataSketch.CMS.new())
      ...> rescue
      ...>   e in ExDataSketch.Errors.UnsupportedOperationError -> e.message
      ...> end
      "ExDataSketch.CMS (no single-value cardinality estimate; call the family module directly) does not support estimate/1"

  """
  @spec estimate(Sketch.sketch()) :: number()
  def estimate(%HLL{} = sketch), do: HLL.estimate(sketch)
  def estimate(%Theta{} = sketch), do: Theta.estimate(sketch)
  def estimate(%ULL{} = sketch), do: ULL.estimate(sketch)
  def estimate(%KLL{} = sketch), do: KLL.count(sketch)
  def estimate(%DDSketch{} = sketch), do: DDSketch.count(sketch)
  def estimate(%REQ{} = sketch), do: REQ.count(sketch)
  def estimate(%FrequentItems{} = sketch), do: FrequentItems.count(sketch)
  def estimate(%MisraGries{} = sketch), do: MisraGries.count(sketch)

  def estimate(%mod{}) do
    raise Errors.UnsupportedOperationError,
      operation: "estimate/1",
      structure:
        "#{inspect(mod)} (no single-value cardinality estimate; call the family module directly)"
  end

  @doc """
  Serializes a sketch to its canonical binary representation, dispatching on
  the sketch's struct type.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> is_binary(ExDataSketch.serialize(sketch))
      true

  """
  @spec serialize(Sketch.sketch()) :: binary()
  def serialize(%mod{} = sketch), do: mod.serialize(sketch)

  @doc """
  Deserializes a binary produced by `serialize/1` back into a sketch of the
  given family.

  Takes an explicit `type` (unlike each family module's own `deserialize/1`)
  because the facade cannot assume which module should own the returned
  struct purely from the binary; each family module's own `deserialize/1`
  independently validates the binary's embedded sketch-family marker, so
  passing the wrong `type` for a binary returns `{:error, %ExDataSketch.Errors.DeserializationError{}}`
  rather than silently misinterpreting the bytes.

  Raises `ExDataSketch.Errors.InvalidOptionError` if `type` is not a key in
  `sketches/0`.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> binary = ExDataSketch.serialize(sketch)
      iex> {:ok, restored} = ExDataSketch.deserialize(binary, :hll)
      iex> restored.opts[:p]
      10

      iex> {:error, error} = ExDataSketch.deserialize(<<0, 1, 2>>, :hll)
      iex> error.__struct__
      ExDataSketch.Errors.DeserializationError

  """
  @spec deserialize(binary(), sketch_type()) :: {:ok, Sketch.sketch()} | {:error, Exception.t()}
  def deserialize(binary, type) when is_binary(binary) and is_atom(type) do
    fetch_module!(type).deserialize(binary)
  end

  @doc """
  Returns the size, in bytes, of a sketch's serialized state, dispatching on
  the sketch's struct type.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> ExDataSketch.size_bytes(sketch) > 0
      true

  """
  @spec size_bytes(Sketch.sketch()) :: non_neg_integer()
  def size_bytes(%mod{} = sketch), do: mod.size_bytes(sketch)

  @doc """
  Returns the set of operation names supported by a sketch family, accepting
  either a family atom (looked up via `sketches/0`) or a sketch struct
  (dispatching on `sketch.__struct__`).

  Raises `ExDataSketch.Errors.InvalidOptionError` if given an atom that is
  not a key in `sketches/0`.

  ## Examples

      iex> ExDataSketch.capabilities(:hll) |> MapSet.member?(:estimate)
      true

      iex> ExDataSketch.capabilities(ExDataSketch.HLL.new()) |> MapSet.member?(:estimate)
      true

  """
  @spec capabilities(sketch_type() | Sketch.sketch()) :: Sketch.capabilities()
  def capabilities(type) when is_atom(type), do: fetch_module!(type).capabilities()
  def capabilities(%mod{}), do: mod.capabilities()

  @spec fetch_module!(sketch_type()) :: module()
  defp fetch_module!(type) do
    case Map.fetch(@registry, type) do
      {:ok, module} ->
        module

      :error ->
        raise Errors.InvalidOptionError,
          option: :type,
          value: type,
          message:
            "unknown sketch type #{inspect(type)}; must be one of #{inspect(Map.keys(@registry))}"
    end
  end

  @doc """
  Updates a sketch with multiple items in a single pass, dispatching on the
  sketch's struct type.

  Works for all 16 concrete families except `ExDataSketch.XorFilter`, which
  has no incremental update (it is immutable once built) and raises
  `ExDataSketch.Errors.UnsupportedOperationError` directing callers to
  `ExDataSketch.XorFilter.build/2`.

  Before this release, this function was a hand-written list of 13 struct
  clauses covering the families that predated the `ExDataSketch.Sketch`
  behaviour's `update_many/2` callback. Every mergeable family now
  implements `update_many/2` directly (delegating to `put_many/2` where that
  remains the family-idiomatic name), so this function dispatches generically
  instead of re-enumerating the family list -- one of the three
  independently-drifting lists named in `sketches/0`'s documentation.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> sketch = ExDataSketch.update_many(sketch, ["a", "b"])
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

      iex> chain = ExDataSketch.FilterChain.new() |> ExDataSketch.FilterChain.add_stage(ExDataSketch.Bloom.new(capacity: 100))
      iex> chain = ExDataSketch.update_many(chain, ["a", "b", "c"])
      iex> ExDataSketch.FilterChain.member?(chain, "a")
      true

  """
  @spec update_many(Sketch.sketch(), Enumerable.t()) :: Sketch.sketch()
  def update_many(%XorFilter{}, _items) do
    raise Errors.UnsupportedOperationError,
      operation: "update_many/2",
      structure:
        "ExDataSketch.XorFilter (immutable once built; see ExDataSketch.XorFilter.build/2)"
  end

  def update_many(%mod{} = sketch, items), do: mod.update_many(sketch, items)
end
