defmodule ExDataSketch.Quantiles do
  @moduledoc """
  Facade for quantile sketch algorithms.

  Provides a unified API for creating and querying quantile sketches.

  ## Supported Types

  - `:kll` (default) -- KLL quantiles sketch. See `ExDataSketch.KLL`.
  - `:ddsketch` -- DDSketch quantiles sketch. See `ExDataSketch.DDSketch`.
  - `:req` -- REQ relative error quantiles sketch. See `ExDataSketch.REQ`.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new(type: :kll)
      iex> sketch.__struct__
      ExDataSketch.KLL

      iex> sketch = ExDataSketch.Quantiles.new()
      iex> sketch = ExDataSketch.Quantiles.update(sketch, 42.0)
      iex> ExDataSketch.Quantiles.count(sketch)
      1

  """

  alias ExDataSketch.{DDSketch, KLL, REQ}

  @type sketch :: KLL.t() | DDSketch.t() | REQ.t()

  @doc """
  Creates a new quantile sketch.

  ## Options

  - `:type` - sketch type, `:kll` (default), `:ddsketch`, or `:req`.
  - All other options are passed to the underlying sketch constructor.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new(type: :kll, k: 200)
      iex> sketch.opts
      [k: 200]

  """
  @spec new(keyword()) :: sketch()
  def new(opts \\ []) do
    {type, sketch_opts} = Keyword.pop(opts, :type, :kll)
    dispatch_new(type, sketch_opts)
  end

  @doc """
  Updates the sketch with a single numeric value.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update(1.0)
      iex> ExDataSketch.Quantiles.count(sketch)
      1

  """
  @spec update(sketch(), number()) :: sketch()
  def update(%KLL{} = sketch, value), do: KLL.update(sketch, value)
  def update(%DDSketch{} = sketch, value), do: DDSketch.update(sketch, value)
  def update(%REQ{} = sketch, value), do: REQ.update(sketch, value)

  @doc """
  Updates the sketch with multiple numeric values.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update_many([1.0, 2.0])
      iex> ExDataSketch.Quantiles.count(sketch)
      2

  """
  @spec update_many(sketch(), Enumerable.t()) :: sketch()
  def update_many(%KLL{} = sketch, items), do: KLL.update_many(sketch, items)
  def update_many(%DDSketch{} = sketch, items), do: DDSketch.update_many(sketch, items)
  def update_many(%REQ{} = sketch, items), do: REQ.update_many(sketch, items)

  @doc """
  Merges two quantile sketches of the same type.

  ## Examples

      iex> a = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update(1.0)
      iex> b = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update(2.0)
      iex> merged = ExDataSketch.Quantiles.merge(a, b)
      iex> ExDataSketch.Quantiles.count(merged)
      2

  """
  @spec merge(sketch(), sketch()) :: sketch()
  def merge(%KLL{} = a, %KLL{} = b), do: KLL.merge(a, b)
  def merge(%DDSketch{} = a, %DDSketch{} = b), do: DDSketch.merge(a, b)
  def merge(%REQ{} = a, %REQ{} = b), do: REQ.merge(a, b)

  @doc """
  Returns the approximate value at the given normalized rank.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update_many(1..100)
      iex> q = ExDataSketch.Quantiles.quantile(sketch, 0.5)
      iex> is_float(q)
      true

  """
  @spec quantile(sketch(), float()) :: float() | nil
  def quantile(%KLL{} = sketch, rank), do: KLL.quantile(sketch, rank)
  def quantile(%DDSketch{} = sketch, rank), do: DDSketch.quantile(sketch, rank)
  def quantile(%REQ{} = sketch, rank), do: REQ.quantile(sketch, rank)

  @doc """
  Returns the approximate values at the given normalized ranks.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update_many(1..100)
      iex> qs = ExDataSketch.Quantiles.quantiles(sketch, [0.25, 0.5, 0.75])
      iex> length(qs)
      3

  """
  @spec quantiles(sketch(), [float()]) :: [float() | nil]
  def quantiles(%KLL{} = sketch, ranks), do: KLL.quantiles(sketch, ranks)
  def quantiles(%DDSketch{} = sketch, ranks), do: DDSketch.quantiles(sketch, ranks)
  def quantiles(%REQ{} = sketch, ranks), do: REQ.quantiles(sketch, ranks)

  @doc """
  Returns the approximate normalized rank of a given value.

  The rank is the fraction of items in the sketch that are less than or
  equal to the given value. Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update_many(1..100)
      iex> r = ExDataSketch.Quantiles.rank(sketch, 50.0)
      iex> is_float(r)
      true

  """
  @spec rank(sketch(), number()) :: float() | nil
  def rank(%KLL{} = sketch, value), do: KLL.rank(sketch, value)
  def rank(%DDSketch{} = sketch, value), do: DDSketch.rank(sketch, value)
  def rank(%REQ{} = sketch, value), do: REQ.rank(sketch, value)

  @doc """
  Returns the CDF at the given split points.

  Given split points `[s1, s2, ..., sm]`, returns `[rank(s1), rank(s2), ..., rank(sm)]`.
  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update_many(1..100)
      iex> cdf = ExDataSketch.Quantiles.cdf(sketch, [25.0, 75.0])
      iex> length(cdf)
      2

  """
  @spec cdf(sketch(), [number()]) :: [float()] | nil
  def cdf(%KLL{} = sketch, split_points), do: KLL.cdf(sketch, split_points)
  def cdf(%REQ{} = sketch, split_points), do: REQ.cdf(sketch, split_points)

  def cdf(%DDSketch{}, _split_points) do
    raise ArgumentError, "cdf/2 is not supported for DDSketch"
  end

  @doc """
  Returns the PMF at the given split points.

  Given split points `[s1, s2, ..., sm]`, returns `m+1` values representing
  the approximate fraction of items in each interval. Returns `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update_many(1..100)
      iex> pmf = ExDataSketch.Quantiles.pmf(sketch, [50.0])
      iex> length(pmf)
      2

  """
  @spec pmf(sketch(), [number()]) :: [float()] | nil
  def pmf(%KLL{} = sketch, split_points), do: KLL.pmf(sketch, split_points)
  def pmf(%REQ{} = sketch, split_points), do: REQ.pmf(sketch, split_points)

  def pmf(%DDSketch{}, _split_points) do
    raise ArgumentError, "pmf/2 is not supported for DDSketch"
  end

  @doc """
  Returns the total number of items inserted into the sketch.

  ## Examples

      iex> ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.count()
      0

  """
  @spec count(sketch()) :: non_neg_integer()
  def count(%KLL{} = sketch), do: KLL.count(sketch)
  def count(%DDSketch{} = sketch), do: DDSketch.count(sketch)
  def count(%REQ{} = sketch), do: REQ.count(sketch)

  @doc """
  Returns the minimum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update(5.0)
      iex> ExDataSketch.Quantiles.min_value(sketch)
      5.0

  """
  @spec min_value(sketch()) :: float() | nil
  def min_value(%KLL{} = sketch), do: KLL.min_value(sketch)
  def min_value(%DDSketch{} = sketch), do: DDSketch.min_value(sketch)
  def min_value(%REQ{} = sketch), do: REQ.min_value(sketch)

  @doc """
  Returns the maximum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.Quantiles.new() |> ExDataSketch.Quantiles.update(5.0)
      iex> ExDataSketch.Quantiles.max_value(sketch)
      5.0

  """
  @spec max_value(sketch()) :: float() | nil
  def max_value(%KLL{} = sketch), do: KLL.max_value(sketch)
  def max_value(%DDSketch{} = sketch), do: DDSketch.max_value(sketch)
  def max_value(%REQ{} = sketch), do: REQ.max_value(sketch)

  # -- Private --

  defp dispatch_new(:kll, opts), do: KLL.new(opts)

  defp dispatch_new(:ddsketch, opts), do: DDSketch.new(opts)

  defp dispatch_new(:req, opts), do: REQ.new(opts)

  defp dispatch_new(type, _opts) do
    raise ArgumentError, "unknown quantile sketch type: #{inspect(type)}"
  end
end
