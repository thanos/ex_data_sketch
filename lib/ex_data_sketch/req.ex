defmodule ExDataSketch.REQ do
  @moduledoc """
  REQ (Relative Error Quantiles) sketch for tail-accuracy quantile estimation.

  The REQ sketch provides quantile estimates with a relative error guarantee
  on the *value* returned, with asymmetric accuracy that can be biased toward
  either high ranks (HRA) or low ranks (LRA). This makes it ideal for
  tail-latency analysis (p99, p99.9) and SLO monitoring.

  ## Accuracy Modes

  - **HRA** (High Rank Accuracy, default): Better accuracy at high quantiles
    (p95, p99, p99.9). Use for tail-latency monitoring.
  - **LRA** (Low Rank Accuracy): Better accuracy at low quantiles (p1, p5).
    Use when the lower tail matters more.

  ## How It Works

  REQ uses biased compaction: in HRA mode, compaction preferentially discards
  low-value items, preserving more data points at the high end. This gives
  relative error guarantees where the error on a returned value v is
  proportional to v itself.

  ## Binary State Layout (REQ1)

  All multi-byte fields are little-endian.

      HEADER:
        magic:           4 bytes  "REQ1"
        version:         u8       1
        flags:           u8       bit0 = hra (1=HRA, 0=LRA)
        reserved:        u16      0
        k:               u32 LE   accuracy parameter
        n:               u64 LE   total count
        min_val:         f64 LE   (NaN sentinel for empty)
        max_val:         f64 LE   (NaN sentinel for empty)
        num_levels:      u8
        compaction_bits: ceil(num_levels/8) bytes
        level_sizes:     num_levels x u32 LE
        items:           sum(level_sizes) x f64 LE

  ## Options

  - `:k` - accuracy parameter, positive integer (default: 12).
    Larger values give better accuracy but use more memory.
  - `:hra` - high rank accuracy mode, boolean (default: `true`).
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Merge Properties

  REQ merge is **associative** and **commutative**. Both sketches must
  have the same HRA/LRA mode to merge.
  """

  alias ExDataSketch.{Backend, Codec, Errors}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_k 12

  @doc """
  Creates a new REQ sketch.

  ## Options

  - `:k` - accuracy parameter, positive integer (default: #{@default_k}).
  - `:hra` - high rank accuracy mode (default: `true`).
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> sketch = ExDataSketch.REQ.new(k: 12, hra: true)
      iex> sketch.opts
      [k: 12, hra: true]
      iex> ExDataSketch.REQ.count(sketch)
      0

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    hra = Keyword.get(opts, :hra, true)
    validate_k!(k)
    backend = Backend.resolve(opts)
    clean_opts = [k: k, hra: hra]
    state = backend.req_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single numeric value.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update(42.0)
      iex> ExDataSketch.REQ.count(sketch)
      1

  """
  @spec update(t(), number()) :: t()
  def update(%__MODULE__{} = sketch, value) when is_number(value) do
    update_many(sketch, [value])
  end

  @doc """
  Updates the sketch with multiple numeric values in a single pass.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many([1.0, 2.0, 3.0])
      iex> ExDataSketch.REQ.count(sketch)
      3

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    new_state = backend.req_update_many(state, values, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Merges two REQ sketch instances.

  Both sketches must have the same HRA/LRA mode. The result is a sketch
  whose quantile estimates approximate the union of both input multisets.

  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if the sketches have
  different modes.

  ## Examples

      iex> a = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many([1.0, 2.0])
      iex> b = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many([3.0, 4.0])
      iex> merged = ExDataSketch.REQ.merge(a, b)
      iex> ExDataSketch.REQ.count(merged)
      4

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:hra] != opts_b[:hra] do
      raise Errors.IncompatibleSketchesError,
        reason: "REQ mode mismatch: cannot merge HRA and LRA sketches"
    end

    new_state = backend.req_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of REQ sketch instances into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> sketches = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.REQ.new() |> ExDataSketch.REQ.update(i * 1.0)
      ...> end)
      iex> merged = ExDataSketch.REQ.merge_many(sketches)
      iex> ExDataSketch.REQ.count(merged)
      3

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns the approximate value at the given normalized rank.

  The rank must be in the range `[0.0, 1.0]`, where 0.0 is the minimum
  and 1.0 is the maximum. Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many(1..100)
      iex> median = ExDataSketch.REQ.quantile(sketch, 0.5)
      iex> is_float(median)
      true

  """
  @spec quantile(t(), float()) :: float() | nil
  def quantile(%__MODULE__{state: state, opts: opts, backend: backend}, rank)
      when is_float(rank) and rank >= 0.0 and rank <= 1.0 do
    backend.req_quantile(state, rank, opts)
  end

  @doc """
  Returns approximate values at multiple normalized ranks.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many(1..100)
      iex> [q25, q50, q75] = ExDataSketch.REQ.quantiles(sketch, [0.25, 0.5, 0.75])
      iex> q25 < q50 and q50 < q75
      true

  """
  @spec quantiles(t(), [float()]) :: [float() | nil]
  def quantiles(%__MODULE__{} = sketch, ranks) when is_list(ranks) do
    Enum.map(ranks, fn rank -> quantile(sketch, rank) end)
  end

  @doc """
  Returns the approximate normalized rank of a given value.

  The rank is the fraction of items in the sketch that are less than or
  equal to the given value. Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many(1..100)
      iex> r = ExDataSketch.REQ.rank(sketch, 50.0)
      iex> is_float(r)
      true

  """
  @spec rank(t(), number()) :: float() | nil
  def rank(%__MODULE__{state: state, opts: opts, backend: backend}, value)
      when is_number(value) do
    backend.req_rank(state, value * 1.0, opts)
  end

  @doc """
  Returns the CDF at the given split points.

  Given split points `[s1, s2, ..., sm]`, returns `[rank(s1), rank(s2), ..., rank(sm)]`.
  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many(1..100)
      iex> cdf = ExDataSketch.REQ.cdf(sketch, [25.0, 75.0])
      iex> length(cdf)
      2

  """
  @spec cdf(t(), [number()]) :: [float()] | nil
  def cdf(%__MODULE__{state: state, opts: opts, backend: backend}, split_points) do
    backend.req_cdf(state, split_points, opts)
  end

  @doc """
  Returns the PMF at the given split points.

  Given split points `[s1, s2, ..., sm]`, returns `m+1` values representing
  the approximate fraction of items in each interval. Returns `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many(1..100)
      iex> pmf = ExDataSketch.REQ.pmf(sketch, [50.0])
      iex> length(pmf)
      2

  """
  @spec pmf(t(), [number()]) :: [float()] | nil
  def pmf(%__MODULE__{state: state, opts: opts, backend: backend}, split_points) do
    backend.req_pmf(state, split_points, opts)
  end

  @doc """
  Returns the total number of items inserted into the sketch.

  ## Examples

      iex> ExDataSketch.REQ.new() |> ExDataSketch.REQ.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.req_count(state, opts)
  end

  @doc """
  Returns the minimum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many([3.0, 1.0, 2.0])
      iex> ExDataSketch.REQ.min_value(sketch)
      1.0

  """
  @spec min_value(t()) :: float() | nil
  def min_value(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.req_min(state, opts)
  end

  @doc """
  Returns the maximum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new() |> ExDataSketch.REQ.update_many([3.0, 1.0, 2.0])
      iex> ExDataSketch.REQ.max_value(sketch)
      3.0

  """
  @spec max_value(t()) :: float() | nil
  def max_value(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.req_max(state, opts)
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new()
      iex> ExDataSketch.REQ.size_bytes(sketch) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}) do
    byte_size(state)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> sketch = ExDataSketch.REQ.new()
      iex> binary = ExDataSketch.REQ.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    k = Keyword.fetch!(opts, :k)
    hra = if Keyword.fetch!(opts, :hra), do: 1, else: 0
    params_bin = <<k::unsigned-little-32, hra::unsigned-8>>
    Codec.encode(Codec.sketch_id_req(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a REQ sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.REQ.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    with {:ok, decoded} <- Codec.decode(binary),
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
  end

  @doc """
  Creates a new REQ sketch from an enumerable of numeric items.

  ## Examples

      iex> sketch = ExDataSketch.REQ.from_enumerable([1.0, 2.0, 3.0], k: 12)
      iex> ExDataSketch.REQ.count(sketch)
      3

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> update_many(enumerable)
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3`.

  ## Examples

      iex> is_function(ExDataSketch.REQ.reducer(), 2)
      true

  """
  @spec reducer() :: (number(), t() -> t())
  def reducer do
    fn item, sketch -> update(sketch, item) end
  end

  @doc """
  Returns a 2-arity merge function suitable for combining sketches.

  ## Examples

      iex> is_function(ExDataSketch.REQ.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  # -- Private --

  defp validate_k!(k) when is_integer(k) and k >= 2, do: :ok

  defp validate_k!(k) do
    raise Errors.InvalidOptionError,
      option: :k,
      value: k,
      message: "k must be an integer >= 2, got: #{inspect(k)}"
  end

  defp validate_sketch_id(13), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected REQ sketch ID (13), got #{id}")}
  end

  defp decode_params(<<k::unsigned-little-32, hra_byte::unsigned-8>>)
       when k >= 2 and hra_byte in [0, 1] do
    {:ok, [k: k, hra: hra_byte == 1]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid REQ params binary")}
  end
end
