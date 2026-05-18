defmodule ExDataSketch.Stream do
  @moduledoc """
  Stream-native sketch construction and reduction.

  This module provides terminal stream consumers that build sketches from
  lazy enumerables without buffering the entire input into memory. Each
  function consumes an `Enumerable` and returns a completed sketch struct.

  Patterns are delegated to the per-sketch `from_enumerable/2` and
  `merge_many/1` APIs so that no ingestion logic is duplicated.

  ## Stream Pipeline Example

      1..100_000
      |> Stream.map(&to_string/1)
      |> ExDataSketch.Stream.hll(p: 14)
      |> ExDataSketch.HLL.estimate()

  ## Partitioned Processing

  For large streams that benefit from intermediate aggregation, use
  `reduce_partitioned/3` to chunk the input, build partial sketches, and
  merge them:

      1..1_000_000
      |> Stream.map(&to_string/1)
      |> ExDataSketch.Stream.reduce_partitioned(ExDataSketch.HLL, p: 14)

  ## Collectable

  All mergeable sketches implement the `Collectable` protocol, enabling
  `Enum.into/2` usage:

      sketch = Enum.into(1..1000, ExDataSketch.HLL.new(p: 14))

  See `Collectable` documentation for each sketch module.
  """

  alias ExDataSketch.{
    Bloom,
    CMS,
    CQF,
    DDSketch,
    FrequentItems,
    HLL,
    IBLT,
    KLL,
    MisraGries,
    Quotient,
    REQ,
    Telemetry,
    Theta,
    ULL
  }

  @doc """
  Builds an HLL sketch from a stream.

  Delegates to `ExDataSketch.HLL.from_enumerable/2`.

  ## Examples

      iex> sketch = 1..100 |> Stream.map(&to_string/1) |> ExDataSketch.Stream.hll(p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec hll(Enumerable.t(), keyword()) :: HLL.t()
  def hll(enumerable, opts \\ []), do: HLL.from_enumerable(enumerable, opts)

  @doc """
  Builds a CMS sketch from a stream.

  Delegates to `ExDataSketch.CMS.from_enumerable/2`.

  ## Examples

      iex> sketch = ["a", "b", "c", "a"] |> ExDataSketch.Stream.cms(width: 64, depth: 3)
      iex> ExDataSketch.CMS.estimate(sketch, "a") >= 2
      true

  """
  @spec cms(Enumerable.t(), keyword()) :: CMS.t()
  def cms(enumerable, opts \\ []), do: CMS.from_enumerable(enumerable, opts)

  @doc """
  Builds a Theta sketch from a stream.

  Delegates to `ExDataSketch.Theta.from_enumerable/2`.

  ## Examples

      iex> sketch = 1..50 |> Stream.map(&to_string/1) |> ExDataSketch.Stream.theta(k: 100)
      iex> ExDataSketch.Theta.estimate(sketch) > 0.0
      true

  """
  @spec theta(enumerable :: Enumerable.t(), opts :: keyword()) :: Theta.t()
  def theta(enumerable, opts \\ []), do: Theta.from_enumerable(enumerable, opts)

  @doc """
  Builds a KLL sketch from a stream.

  Delegates to `ExDataSketch.KLL.from_enumerable/2`.

  ## Examples

      iex> sketch = 1..100 |> ExDataSketch.Stream.kll(k: 200)
      iex> is_float(ExDataSketch.KLL.quantile(sketch, 0.5))
      true

  """
  @spec kll(Enumerable.t(), keyword()) :: KLL.t()
  def kll(enumerable, opts \\ []), do: KLL.from_enumerable(enumerable, opts)

  @doc """
  Builds a DDSketch from a stream.

  Delegates to `ExDataSketch.DDSketch.from_enumerable/2`.

  ## Examples

      iex> sketch = 1..100 |> ExDataSketch.Stream.ddsketch(alpha: 0.01)
      iex> is_float(ExDataSketch.DDSketch.quantile(sketch, 0.5))
      true

  """
  @spec ddsketch(Enumerable.t(), keyword()) :: DDSketch.t()
  def ddsketch(enumerable, opts \\ []), do: DDSketch.from_enumerable(enumerable, opts)

  @doc """
  Builds an REQ sketch from a stream.

  Delegates to `ExDataSketch.REQ.from_enumerable/2`.

  ## Examples

      iex> sketch = 1..100 |> ExDataSketch.Stream.req(k: 200)
      iex> is_float(ExDataSketch.REQ.quantile(sketch, 0.5)) or is_nil(ExDataSketch.REQ.quantile(sketch, 0.5))
      true

  """
  @spec req(Enumerable.t(), keyword()) :: REQ.t()
  def req(enumerable, opts \\ []), do: REQ.from_enumerable(enumerable, opts)

  @doc """
  Builds a ULL sketch from a stream.

  Delegates to `ExDataSketch.ULL.from_enumerable/2`.

  ## Examples

      iex> sketch = 1..100 |> Stream.map(&to_string/1) |> ExDataSketch.Stream.ull(p: 10)
      iex> ExDataSketch.ULL.estimate(sketch) > 0.0
      true

  """
  @spec ull(Enumerable.t(), keyword()) :: ULL.t()
  def ull(enumerable, opts \\ []), do: ULL.from_enumerable(enumerable, opts)

  @doc """
  Builds a FrequentItems sketch from a stream.

  Delegates to `ExDataSketch.FrequentItems.from_enumerable/2`.

  ## Examples

      iex> items = Stream.map(1..200, fn i -> "item_" <> Integer.to_string(rem(i, 20)) end)
      iex> sketch = ExDataSketch.Stream.frequent_items(items, k: 10)
      iex> length(ExDataSketch.FrequentItems.top_k(sketch, 5)) <= 10
      true

  """
  @spec frequent_items(Enumerable.t(), keyword()) :: FrequentItems.t()
  def frequent_items(enumerable, opts \\ []),
    do: FrequentItems.from_enumerable(enumerable, opts)

  @doc """
  Builds a MisraGries sketch from a stream.

  Delegates to `ExDataSketch.MisraGries.from_enumerable/2`.

  ## Examples

      iex> items = Stream.map(1..200, fn i -> "item_" <> Integer.to_string(rem(i, 20)) end)
      iex> sketch = ExDataSketch.Stream.misra_gries(items, k: 10)
      iex> ExDataSketch.MisraGries.count(sketch) == 200
      true

  """
  @spec misra_gries(Enumerable.t(), keyword()) :: MisraGries.t()
  def misra_gries(enumerable, opts \\ []),
    do: MisraGries.from_enumerable(enumerable, opts)

  @doc """
  Builds a Bloom filter from a stream.

  Delegates to `ExDataSketch.Bloom.from_enumerable/2`.

  ## Examples

      iex> items = 1..100 |> Stream.map(&to_string/1)
      iex> bloom = ExDataSketch.Stream.bloom(items, capacity: 200)
      iex> ExDataSketch.Bloom.member?(bloom, "1")
      true

  """
  @spec bloom(Enumerable.t(), keyword()) :: Bloom.t()
  def bloom(enumerable, opts \\ []), do: Bloom.from_enumerable(enumerable, opts)

  @doc """
  Builds a Quotient filter from a stream.

  Delegates to `ExDataSketch.Quotient.from_enumerable/2`.

  ## Examples

      iex> items = 1..50 |> Stream.map(&to_string/1)
      iex> qf = ExDataSketch.Stream.quotient(items, capacity: 100)
      iex> ExDataSketch.Quotient.member?(qf, "1")
      true

  """
  @spec quotient(Enumerable.t(), keyword()) :: Quotient.t()
  def quotient(enumerable, opts \\ []), do: Quotient.from_enumerable(enumerable, opts)

  @doc """
  Builds a CQF (Counting Quotient Filter) from a stream.

  Delegates to `ExDataSketch.CQF.from_enumerable/2`.

  ## Examples

      iex> items = 1..50 |> Stream.map(&to_string/1)
      iex> cqf = ExDataSketch.Stream.cqf(items, capacity: 100)
      iex> ExDataSketch.CQF.member?(cqf, "1")
      true

  """
  @spec cqf(Enumerable.t(), keyword()) :: CQF.t()
  def cqf(enumerable, opts \\ []), do: CQF.from_enumerable(enumerable, opts)

  @doc """
  Builds an IBLT from a stream.

  Delegates to `ExDataSketch.IBLT.from_enumerable/2`.

  ## Examples

      iex> items = 1..20 |> Stream.map(&to_string/1)
      iex> iblt = ExDataSketch.Stream.iblt(items, m: 40, num_hashes: 3)
      iex> ExDataSketch.IBLT.member?(iblt, "1")
      true

  """
  @spec iblt(Enumerable.t(), keyword()) :: IBLT.t()
  def iblt(enumerable, opts \\ []), do: IBLT.from_enumerable(enumerable, opts)

  @doc """
  Reduces a stream into an existing or new sketch.

  When `sketch_or_module` is a sketch struct, items are reduced into it
  using the appropriate update function. When it is a module atom
  (e.g., `ExDataSketch.HLL`), a new sketch is created with the given
  options and items are reduced into it.

  This is a convenience function that wraps `Enum.reduce/3` with the
  sketch's `reducer/0` function.

  ## Examples

      # Using a module atom
      iex> sketch = 1..100 |> ExDataSketch.Stream.reduce_into(ExDataSketch.HLL, p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

      # Using an existing sketch
      iex> existing = ExDataSketch.HLL.new(p: 10)
      iex> sketch = ["a", "b"] |> ExDataSketch.Stream.reduce_into(existing)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec reduce_into(Enumerable.t(), module() | struct(), keyword()) :: struct()
  def reduce_into(enumerable, sketch_or_module, opts \\ [])

  def reduce_into(enumerable, module, opts) when is_atom(module) do
    sketch = module.new(opts)
    do_reduce_into(enumerable, sketch, module)
  end

  def reduce_into(enumerable, %module{} = sketch, _opts) do
    do_reduce_into(enumerable, sketch, module)
  end

  defp do_reduce_into(enumerable, sketch, module) do
    reducer = module.reducer()
    Enum.reduce(enumerable, sketch, reducer)
  end

  @doc """
  Reduces a stream into a sketch using partitioned processing.

  Splits the enumerable into chunks, builds a partial sketch per chunk,
  and merges all partial sketches into a final result. This leverages
  merge associativity to produce results identical to a single-pass
  `from_enumerable/2`.

  ## Options

  - `:partitions` - number of chunks to split the input into
    (default: `System.schedulers_online()`). Must be a positive integer.
  - All other options are forwarded to the sketch module's `new/1` and
    `from_enumerable/2`.

  ## Examples

      iex> sketch = 1..1000 |> ExDataSketch.Stream.reduce_partitioned(ExDataSketch.HLL, partitions: 4, p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec reduce_partitioned(Enumerable.t(), module(), keyword()) :: struct()
  def reduce_partitioned(enumerable, module, opts \\ []) do
    {partitions, sketch_opts} = Keyword.pop(opts, :partitions, System.schedulers_online())
    chunk_size = max(1, div(10_000, partitions))

    Telemetry.span_with_result(
      Telemetry.event_name(:stream, :partition_merge),
      %{partition_count: partitions},
      %{sketch_type: sketch_type_from_module(module)},
      :stream,
      fn ->
        enumerable
        |> Stream.chunk_every(chunk_size)
        |> Enum.map(fn chunk -> module.from_enumerable(chunk, sketch_opts) end)
        |> module.merge_many()
      end,
      fn _result -> %{} end
    )
  end

  defp sketch_type_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> String.to_atom()
  end
end
