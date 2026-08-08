defmodule ExDataSketch.KLL do
  @moduledoc """
  KLL (Karnin-Lang-Liberty) quantiles sketch for rank and quantile estimation.

  KLL maintains multiple levels of sorted float64 items to provide approximate
  quantile queries with guaranteed rank accuracy. Unlike HLL, CMS, and Theta
  sketches, KLL operates on raw numeric values rather than hashed items.

  ## Accuracy

  The rank error bound is approximately `1.65 / k`, where `k` is the accuracy
  parameter. For the default `k = 200`, this gives roughly 0.8% rank error.

  | k   | ~Rank Error |
  |-----|-------------|
  | 50  | 3.30%       |
  | 100 | 1.65%       |
  | 200 | 0.83%       |
  | 500 | 0.33%       |

  ## Binary State Layout (v1)

  All multi-byte fields are little-endian.

      Offset  Size                    Field
      ------  ------                  -----
      0       1                       Version (u8, currently 1)
      1       4                       k parameter (u32 little-endian)
      5       8                       n total items seen (u64 little-endian)
      13      8                       min_val (f64 little-endian, NaN = empty)
      21      8                       max_val (f64 little-endian, NaN = empty)
      29      1                       num_levels (u8)
      30      ceil(num_levels/8)      compaction parity bits (1 bit per level)
      30+P    num_levels * 4          level_sizes (u32 little-endian each)
      30+P+L  sum(level_sizes) * 8    items (f64 little-endian, level 0 first)

  ## Apache DataSketches Interop

  `serialize_datasketches/2` and `deserialize_datasketches/2` implement
  the Apache DataSketches KLL compact binary format (`KllFloatsSketch`/
  `KllDoublesSketch`), enabling cross-language compatibility with Java,
  C++, and Python DataSketches libraries. See
  `ExDataSketch.DataSketches.KLLSketch` for the binary layout and
  `guides/apache_interop.md` for the full interop story. Unlike Theta,
  KLL does not hash its inputs, so this is a full item-level round trip
  with no hash-equality caveat.

  ## Options

  - `:k` - accuracy parameter, integer 8..65535 (default: 200).
    Higher values use more memory but give better accuracy.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Merge Properties

  KLL merge is **associative** and **commutative** at the estimate level.
  Quantile query results from merged sketches are equivalent regardless of
  merge order, though internal state may differ due to compaction parity.
  """

  alias ExDataSketch.{Backend, Binary, Codec, Errors, Telemetry}
  alias ExDataSketch.DataSketches.KLLSketch

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @behaviour ExDataSketch.Sketch

  @default_k 200
  @min_k 8
  @max_k 65_535

  @doc """
  Creates a new KLL sketch.

  ## Options

  - `:k` - accuracy parameter, integer #{@min_k}..#{@max_k} (default: #{@default_k}).
    Higher values use more memory but give better accuracy.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> sketch = ExDataSketch.KLL.new(k: 200)
      iex> sketch.opts
      [k: 200]
      iex> ExDataSketch.KLL.count(sketch)
      0

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    validate_k!(k)
    backend = Backend.resolve(opts)
    clean_opts = [k: k]
    state = backend.kll_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single numeric value.

  The value is converted to float64. Unlike HLL/CMS/Theta, KLL does not
  hash the input -- it stores the actual numeric value for quantile estimation.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update(42.0)
      iex> ExDataSketch.KLL.count(sketch)
      1

  """
  @spec update(t(), number()) :: t()
  def update(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, value)
      when is_number(value) do
    new_state = backend.kll_update(state, value * 1.0, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Updates the sketch with multiple numeric values in a single pass.

  More efficient than calling `update/2` repeatedly because it minimizes
  intermediate binary allocations.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many([1.0, 2.0, 3.0])
      iex> ExDataSketch.KLL.count(sketch)
      3

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    new_state = backend.kll_update_many(state, values, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Merges two KLL sketches.

  Both sketches must have the same `k` parameter. The result is a sketch
  whose quantile estimates approximate the union of both input multisets.

  Returns the merged sketch. Raises `ExDataSketch.Errors.IncompatibleSketchesError`
  if the sketches have different parameters.

  ## Examples

      iex> a = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..50)
      iex> b = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(51..100)
      iex> merged = ExDataSketch.KLL.merge(a, b)
      iex> ExDataSketch.KLL.count(merged)
      100

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:k] != opts_b[:k] do
      raise Errors.IncompatibleSketchesError,
        reason: "KLL k mismatch: #{opts_a[:k]} vs #{opts_b[:k]}"
    end

    new_state = backend.kll_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of KLL sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> sketches = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.KLL.new() |> ExDataSketch.KLL.update(i * 1.0)
      ...> end)
      iex> merged = ExDataSketch.KLL.merge_many(sketches)
      iex> ExDataSketch.KLL.count(merged)
      3

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    sketches_list = Enum.to_list(sketches)

    Telemetry.span(
      Telemetry.event_name(:sketch, :merge),
      %{merge_count: length(sketches_list)},
      %{sketch_type: :kll},
      :sketch,
      fn -> Enum.reduce(sketches_list, fn sketch, acc -> merge(acc, sketch) end) end
    )
  end

  @doc """
  Returns the approximate value at the given normalized rank.

  The rank must be in the range `[0.0, 1.0]`, where 0.0 is the minimum
  and 1.0 is the maximum. For example, `quantile(sketch, 0.5)` returns
  the approximate median.

  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100)
      iex> median = ExDataSketch.KLL.quantile(sketch, 0.5)
      iex> abs(median - 50.0) < 5.0
      true

  """
  @spec quantile(t(), float()) :: float() | nil
  def quantile(%__MODULE__{state: state, opts: opts, backend: backend}, rank)
      when is_float(rank) and rank >= 0.0 and rank <= 1.0 do
    backend.kll_quantile(state, rank, opts)
  end

  @doc """
  Returns approximate values at multiple normalized ranks.

  Convenience wrapper around `quantile/2` for batch queries.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100)
      iex> [q25, q50, q75] = ExDataSketch.KLL.quantiles(sketch, [0.25, 0.5, 0.75])
      iex> q25 < q50 and q50 < q75
      true

  """
  @spec quantiles(t(), [float()]) :: [float() | nil]
  def quantiles(%__MODULE__{} = sketch, ranks) when is_list(ranks) do
    Enum.map(ranks, fn rank -> quantile(sketch, rank) end)
  end

  @doc """
  Returns the approximate normalized rank of the given value.

  The result is in the range `[0.0, 1.0]`. For example, if `rank(sketch, x)`
  returns 0.75, approximately 75% of the values in the sketch are <= x.

  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100)
      iex> r = ExDataSketch.KLL.rank(sketch, 50.0)
      iex> abs(r - 0.5) < 0.05
      true

  """
  @spec rank(t(), number()) :: float() | nil
  def rank(%__MODULE__{state: state, opts: opts, backend: backend}, value)
      when is_number(value) do
    backend.kll_rank(state, value * 1.0, opts)
  end

  @doc """
  Returns the Cumulative Distribution Function (CDF) at the given split points.

  Given split points `[s1, s2, ..., sm]`, returns `[rank(s1), rank(s2), ..., rank(sm)]`.
  Each value is the approximate fraction of items less than or equal to the
  corresponding split point.

  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100)
      iex> cdf = ExDataSketch.KLL.cdf(sketch, [25.0, 50.0, 75.0])
      iex> length(cdf)
      3

  """
  @spec cdf(t(), [number()]) :: [float()] | nil
  def cdf(%__MODULE__{state: state, opts: opts, backend: backend}, split_points)
      when is_list(split_points) do
    floats = Enum.map(split_points, fn v when is_number(v) -> v * 1.0 end)
    backend.kll_cdf(state, floats, opts)
  end

  @doc """
  Returns the Probability Mass Function (PMF) at the given split points.

  Given split points `[s1, s2, ..., sm]`, returns `m+1` values representing
  the approximate fraction of items in the intervals
  `(-inf, s1], (s1, s2], ..., (sm, +inf)`.

  The returned values sum to 1.0.

  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100)
      iex> pmf = ExDataSketch.KLL.pmf(sketch, [50.0])
      iex> length(pmf)
      2

  """
  @spec pmf(t(), [number()]) :: [float()] | nil
  def pmf(%__MODULE__{state: state, opts: opts, backend: backend}, split_points)
      when is_list(split_points) do
    floats = Enum.map(split_points, fn v when is_number(v) -> v * 1.0 end)
    backend.kll_pmf(state, floats, opts)
  end

  @doc """
  Returns the total number of items inserted into the sketch.

  ## Examples

      iex> ExDataSketch.KLL.new() |> ExDataSketch.KLL.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.kll_count(state, opts)
  end

  @doc """
  Returns the minimum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many([3.0, 1.0, 2.0])
      iex> ExDataSketch.KLL.min_value(sketch)
      1.0

  """
  @spec min_value(t()) :: float() | nil
  def min_value(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.kll_min(state, opts)
  end

  @doc """
  Returns the maximum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many([3.0, 1.0, 2.0])
      iex> ExDataSketch.KLL.max_value(sketch)
      3.0

  """
  @spec max_value(t()) :: float() | nil
  def max_value(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.kll_max(state, opts)
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new()
      iex> ExDataSketch.KLL.size_bytes(sketch) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}) do
    byte_size(state)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  The serialized binary includes magic bytes, version, sketch type,
  parameters, and state. See `ExDataSketch.Codec` for format details.

  ## Options

  - `:format` - serialization format: `:v2` (default, EXSK v2 with CRC32C)
    or `:v1` (legacy EXSK v1, compatible with v0.7.x readers). KLL does
    not hash its inputs, so v1 has no hash-strategy restriction.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new()
      iex> binary = ExDataSketch.KLL.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

      iex> sketch = ExDataSketch.KLL.new()
      iex> binary = ExDataSketch.KLL.serialize(sketch, format: :v1)
      iex> <<"EXSK", 1, 4, _rest::binary>> = binary

  """
  @spec serialize(t(), keyword()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}, serialize_opts \\ []) do
    format = Keyword.get(serialize_opts, :format, :v2)
    start_time = System.monotonic_time()
    k = Keyword.fetch!(opts, :k)
    params_bin = <<k::unsigned-little-32>>

    binary =
      case format do
        :v2 ->
          Binary.encode(
            Binary.metadata_from_opts(Codec.sketch_id_kll(), 1, opts),
            Binary.build_payload(params_bin, state)
          )

        :v1 ->
          Codec.encode(Codec.sketch_id_kll(), 1, params_bin, state)
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:sketch, :serialize),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: :kll},
        :sketch
      )

    binary
  end

  @doc """
  Deserializes an EXSK binary into a KLL sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.KLL.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    start_time = System.monotonic_time()

    result =
      with {:ok, decoded} <- Binary.decode(binary),
           :ok <- validate_sketch_id(decoded.sketch_id),
           {:ok, opts} <- decode_params(decoded.params) do
        backend = Backend.default()

        {:ok,
         %__MODULE__{
           state: decoded.state,
           opts: opts,
           backend: backend
         }}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:sketch, :deserialize),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: :kll},
        :sketch
      )

    result
  end

  @doc """
  Serializes the sketch to the Apache DataSketches KLL compact binary format.

  Enables cross-language interoperability with Java, C++, and Python
  DataSketches libraries. See `ExDataSketch.DataSketches.KLLSketch` for
  the binary layout.

  ## Options

  - `:variant` - `:float` or `:double` (default: `:double`). Apache's
    wire format doesn't self-describe item width, so the variant must be
    chosen explicitly -- see `ExDataSketch.DataSketches.KLLSketch`.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new(k: 200) |> ExDataSketch.KLL.update_many(1..100)
      iex> binary = ExDataSketch.KLL.serialize_datasketches(sketch)
      iex> {:ok, restored} = ExDataSketch.KLL.deserialize_datasketches(binary)
      iex> ExDataSketch.KLL.quantile(restored, 0.5) == ExDataSketch.KLL.quantile(sketch, 0.5)
      true

  """
  @spec serialize_datasketches(t(), keyword()) :: binary()
  def serialize_datasketches(%__MODULE__{} = sketch, opts \\ []) do
    KLLSketch.encode(sketch, opts)
  end

  @doc """
  Deserializes an Apache DataSketches KLL compact binary into a KLL sketch.

  ## Options

  - `:variant` - `:float` or `:double` (default: `:double`). Must match
    the variant the binary was produced as -- see
    `ExDataSketch.DataSketches.KLLSketch`.

  ## Examples

      iex> sketch = ExDataSketch.KLL.new(k: 200) |> ExDataSketch.KLL.update_many(1..100)
      iex> binary = ExDataSketch.KLL.serialize_datasketches(sketch)
      iex> {:ok, restored} = ExDataSketch.KLL.deserialize_datasketches(binary)
      iex> ExDataSketch.KLL.count(restored)
      100

  """
  @spec deserialize_datasketches(binary(), keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize_datasketches(binary, opts \\ []) when is_binary(binary) do
    case KLLSketch.decode(binary, opts) do
      {:ok, decoded} ->
        backend = Backend.default()

        state =
          backend.kll_from_components(
            decoded.k,
            decoded.n,
            decoded.min_val,
            decoded.max_val,
            decoded.levels
          )

        {:ok, %__MODULE__{state: state, opts: [k: decoded.k], backend: backend}}

      error ->
        error
    end
  end

  @doc """
  Creates a new KLL sketch from an enumerable of numeric items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> sketch = ExDataSketch.KLL.from_enumerable([1.0, 2.0, 3.0], k: 200)
      iex> ExDataSketch.KLL.count(sketch)
      3

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    Telemetry.span_with_result(
      Telemetry.event_name(:sketch, :ingest),
      %{},
      %{sketch_type: :kll},
      :sketch,
      fn -> new(opts) |> update_many(enumerable) end,
      fn sketch -> %{size_bytes: size_bytes(sketch)} end
    )
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3` and similar.

  The returned function calls `update/2` on each item.

  ## Examples

      iex> is_function(ExDataSketch.KLL.reducer(), 2)
      true

  """
  @spec reducer() :: (number(), t() -> t())
  def reducer do
    fn item, sketch -> update(sketch, item) end
  end

  @doc """
  Returns a 2-arity merge function suitable for combining sketches.

  The returned function calls `merge/2` on two sketches.

  ## Examples

      iex> is_function(ExDataSketch.KLL.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  @doc """
  Returns the set of operation names supported by `ExDataSketch.KLL`.

  See `ExDataSketch.Sketch` for the shared capability vocabulary.

  ## Examples

      iex> ExDataSketch.KLL.capabilities() |> MapSet.member?(:count)
      true

      iex> ExDataSketch.KLL.capabilities() |> MapSet.member?(:no_such_operation)
      false

  """
  @spec capabilities() :: ExDataSketch.Sketch.capabilities()
  @dialyzer {:no_opaque, capabilities: 0}
  def capabilities do
    MapSet.new([
      :new,
      :update,
      :update_many,
      :merge,
      :merge_many,
      :count,
      :serialize,
      :deserialize
    ])
  end

  # -- Private --

  defp validate_k!(k) when is_integer(k) and k >= @min_k and k <= @max_k, do: :ok

  defp validate_k!(k) do
    raise Errors.InvalidOptionError,
      option: :k,
      value: k,
      message: "k must be an integer between #{@min_k} and #{@max_k}, got: #{inspect(k)}"
  end

  defp validate_sketch_id(4), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected KLL sketch ID (4), got #{id}")}
  end

  defp decode_params(<<k::unsigned-little-32>>) when k >= @min_k and k <= @max_k do
    {:ok, [k: k]}
  end

  defp decode_params(<<k::unsigned-little-32>>) do
    {:error, Errors.DeserializationError.exception(reason: "invalid KLL k value #{k} in params")}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid KLL params binary")}
  end
end
