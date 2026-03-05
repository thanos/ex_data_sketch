defmodule ExDataSketch.DDSketch do
  @moduledoc """
  DDSketch quantiles sketch for value-relative-accuracy quantile estimation.

  DDSketch uses logarithmic bucket mapping to provide quantile estimates with
  a guaranteed relative error bound on the value (not the rank). This makes it
  ideal for latency percentiles and telemetry where relative accuracy matters
  more than rank accuracy.

  ## Accuracy

  The `alpha` parameter (relative_accuracy) controls bucket width. A query for
  quantile q returns a value v such that the true value v' satisfies
  `v' * (1 - alpha) <= v <= v' * (1 + alpha)`.

  | alpha | Relative Error | Typical Use |
  |-------|---------------|-------------|
  | 0.05  | 5%            | Coarse monitoring |
  | 0.01  | 1%            | Standard telemetry |
  | 0.005 | 0.5%          | High-precision SLOs |
  | 0.001 | 0.1%          | Scientific measurement |

  ## Constraints

  - Only non-negative float64 values are accepted. Negative values, NaN, and
    Inf are rejected with an error.
  - Zero is tracked in a dedicated `zero_count` and does not flow through
    the logarithmic index mapping.

  ## Binary State Layout (DDS1)

  All multi-byte fields are little-endian.

      HEADER (fixed 96 bytes):
        magic:           4 bytes  "DDS1"
        version:         u8       1
        flags:           u8       bit0=has_negative_support (0 for v0.2.1)
        reserved:        u16      0
        alpha:           f64 LE   relative accuracy parameter
        gamma:           f64 LE   (1 + alpha) / (1 - alpha)
        log_gamma:       f64 LE   :math.log(gamma)
        min_indexable:   f64 LE   smallest positive value mappable through log
        n:               u64 LE   total count (including zeros)
        zero_count:      u64 LE
        min_value:       f64 LE   (NaN sentinel for empty)
        max_value:       f64 LE   (NaN sentinel for empty)
        sparse_count:    u32 LE   number of sparse bin entries
        dense_min_index: i32 LE   (0 when dense_len=0)
        dense_len:       u32 LE   (0 in v0.2.1)
        reserved2:       u32 LE   0

      BODY:
        Sparse region:   sparse_count x (index: i32 LE, count: u32 LE)
        Dense region:    dense_len x u32 LE counts (empty in v0.2.1)

  ## Options

  - `:alpha` - relative accuracy, float in (0.0, 1.0) (default: 0.01).
    Smaller values give tighter accuracy bounds but use more buckets.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Merge Properties

  DDSketch merge is **associative** and **commutative**. Both sketches must
  have identical `alpha` parameters to merge.
  """

  alias ExDataSketch.{Backend, Codec, Errors}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_alpha 0.01

  @doc """
  Creates a new DDSketch.

  ## Options

  - `:alpha` - relative accuracy, float in (0.0, 1.0) (default: #{@default_alpha}).
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new(alpha: 0.01)
      iex> sketch.opts
      [alpha: 0.01]
      iex> ExDataSketch.DDSketch.count(sketch)
      0

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    alpha = Keyword.get(opts, :alpha, @default_alpha)
    validate_alpha!(alpha)
    backend = Backend.resolve(opts)
    clean_opts = [alpha: alpha]
    state = backend.ddsketch_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single non-negative numeric value.

  The value is converted to float64. Negative values, NaN, and Inf are
  rejected with an `ArgumentError`.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update(42.0)
      iex> ExDataSketch.DDSketch.count(sketch)
      1

  """
  @spec update(t(), number()) :: t()
  def update(%__MODULE__{} = sketch, value) when is_number(value) do
    update_many(sketch, [value])
  end

  @doc """
  Updates the sketch with multiple non-negative numeric values in a single pass.

  More efficient than calling `update/2` repeatedly because it decodes and
  encodes the state binary only once.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many([1.0, 2.0, 3.0])
      iex> ExDataSketch.DDSketch.count(sketch)
      3

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    new_state = backend.ddsketch_update_many(state, values, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Merges two DDSketch instances.

  Both sketches must have the same `alpha` parameter. The result is a sketch
  whose quantile estimates approximate the union of both input multisets.

  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if the sketches have
  different alpha parameters.

  ## Examples

      iex> a = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many([1.0, 2.0])
      iex> b = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many([3.0, 4.0])
      iex> merged = ExDataSketch.DDSketch.merge(a, b)
      iex> ExDataSketch.DDSketch.count(merged)
      4

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:alpha] != opts_b[:alpha] do
      raise Errors.IncompatibleSketchesError,
        reason: "DDSketch alpha mismatch: #{opts_a[:alpha]} vs #{opts_b[:alpha]}"
    end

    new_state = backend.ddsketch_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of DDSketch instances into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> sketches = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update(i * 1.0)
      ...> end)
      iex> merged = ExDataSketch.DDSketch.merge_many(sketches)
      iex> ExDataSketch.DDSketch.count(merged)
      3

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns the approximate value at the given normalized rank.

  The rank must be in the range `[0.0, 1.0]`, where 0.0 is the minimum
  and 1.0 is the maximum. For example, `quantile(sketch, 0.5)` returns
  the approximate median.

  Returns `nil` if the sketch is empty.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many(1..100)
      iex> median = ExDataSketch.DDSketch.quantile(sketch, 0.5)
      iex> is_float(median)
      true

  """
  @spec quantile(t(), float()) :: float() | nil
  def quantile(%__MODULE__{state: state, opts: opts, backend: backend}, rank)
      when is_float(rank) and rank >= 0.0 and rank <= 1.0 do
    backend.ddsketch_quantile(state, rank, opts)
  end

  @doc """
  Returns approximate values at multiple normalized ranks.

  Convenience wrapper around `quantile/2` for batch queries.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many(1..100)
      iex> [q25, q50, q75] = ExDataSketch.DDSketch.quantiles(sketch, [0.25, 0.5, 0.75])
      iex> q25 < q50 and q50 < q75
      true

  """
  @spec quantiles(t(), [float()]) :: [float() | nil]
  def quantiles(%__MODULE__{} = sketch, ranks) when is_list(ranks) do
    Enum.map(ranks, fn rank -> quantile(sketch, rank) end)
  end

  @doc """
  Returns the total number of items inserted into the sketch.

  ## Examples

      iex> ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.ddsketch_count(state, opts)
  end

  @doc """
  Returns the minimum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many([3.0, 1.0, 2.0])
      iex> ExDataSketch.DDSketch.min_value(sketch)
      1.0

  """
  @spec min_value(t()) :: float() | nil
  def min_value(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.ddsketch_min(state, opts)
  end

  @doc """
  Returns the maximum value seen by the sketch, or `nil` if empty.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new() |> ExDataSketch.DDSketch.update_many([3.0, 1.0, 2.0])
      iex> ExDataSketch.DDSketch.max_value(sketch)
      3.0

  """
  @spec max_value(t()) :: float() | nil
  def max_value(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.ddsketch_max(state, opts)
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new()
      iex> ExDataSketch.DDSketch.size_bytes(sketch) > 0
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

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.new()
      iex> binary = ExDataSketch.DDSketch.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    alpha = Keyword.fetch!(opts, :alpha)
    params_bin = <<alpha::float-little-64>>
    Codec.encode(Codec.sketch_id_ddsketch(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a DDSketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.DDSketch.deserialize(<<"invalid">>)
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
  Creates a new DDSketch from an enumerable of numeric items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> sketch = ExDataSketch.DDSketch.from_enumerable([1.0, 2.0, 3.0], alpha: 0.01)
      iex> ExDataSketch.DDSketch.count(sketch)
      3

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> update_many(enumerable)
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3` and similar.

  The returned function calls `update/2` on each item.

  ## Examples

      iex> is_function(ExDataSketch.DDSketch.reducer(), 2)
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

      iex> is_function(ExDataSketch.DDSketch.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  # -- Private --

  defp validate_alpha!(alpha)
       when is_float(alpha) and alpha > 0.0 and alpha < 1.0,
       do: :ok

  defp validate_alpha!(alpha) do
    raise Errors.InvalidOptionError,
      option: :alpha,
      value: alpha,
      message: "alpha must be a float in (0.0, 1.0), got: #{inspect(alpha)}"
  end

  defp validate_sketch_id(5), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected DDSketch sketch ID (5), got #{id}")}
  end

  defp decode_params(<<alpha::float-little-64>>) when alpha > 0.0 and alpha < 1.0 do
    {:ok, [alpha: alpha]}
  end

  defp decode_params(<<alpha::float-little-64>>) do
    {:error,
     Errors.DeserializationError.exception(
       reason: "invalid DDSketch alpha value #{alpha} in params"
     )}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid DDSketch params binary")}
  end
end
