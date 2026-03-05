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
  - `ExDataSketch.Quantiles` -- Facade for quantile sketch algorithms.

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

  See the [Integration Guide](integrations.md) for examples with Flow, Broadway,
  Explorer, Nx, and other ecosystem libraries.

  See the [Quick Start guide](quick_start.md) for more examples.
  """

  alias ExDataSketch.{CMS, DDSketch, HLL, KLL, Theta}

  @doc """
  Updates a sketch with multiple items in a single pass.

  Delegates to the appropriate sketch module's `update_many/2` based on
  the struct type.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> sketch = ExDataSketch.update_many(sketch, ["a", "b"])
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec update_many(
          HLL.t() | CMS.t() | Theta.t() | KLL.t() | DDSketch.t(),
          Enumerable.t()
        ) ::
          HLL.t() | CMS.t() | Theta.t() | KLL.t() | DDSketch.t()
  def update_many(%HLL{} = sketch, items), do: HLL.update_many(sketch, items)
  def update_many(%CMS{} = sketch, items), do: CMS.update_many(sketch, items)
  def update_many(%Theta{} = sketch, items), do: Theta.update_many(sketch, items)
  def update_many(%KLL{} = sketch, items), do: KLL.update_many(sketch, items)
  def update_many(%DDSketch{} = sketch, items), do: DDSketch.update_many(sketch, items)
end
