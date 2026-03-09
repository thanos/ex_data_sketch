defmodule ExDataSketch.MisraGries do
  @moduledoc """
  MisraGries sketch for deterministic heavy hitter detection.

  The Misra-Gries algorithm maintains at most `k` counters to track frequent
  items in a data stream. It provides a deterministic guarantee: any item
  whose true frequency exceeds `n/k` (where `n` is the total count) is
  guaranteed to be tracked.

  ## Algorithm

  - **Update(x)**: If x is tracked, increment its counter. If there are fewer
    than k entries, insert x with count 1. Otherwise, decrement all counters
    by 1 and remove any that reach zero.

  - **Guarantee**: If an item appears more than `n/k` times, it will be in the
    counter set when queried. The estimated count is a lower bound on the
    true count, with error at most `n/k`.

  ## Comparison with FrequentItems (SpaceSaving)

  | Feature | MisraGries | FrequentItems |
  |---------|-----------|---------------|
  | Algorithm | Decrement-all | SpaceSaving (min-replacement) |
  | Guarantee | Deterministic: freq > n/k always tracked | Probabilistic with error bounds |
  | Counter count | At most k | Exactly k |
  | Estimate | Lower bound | Estimate with overcount error |

  ## Binary State Layout (MG01)

  All multi-byte fields are little-endian.

      HEADER (22 bytes):
        magic:       4 bytes  "MG01"
        version:     u8       1
        reserved:    u8       0
        k:           u32 LE   max counters
        n:           u64 LE   total count
        entry_count: u32 LE   number of entries

      ENTRIES (variable):
        entry_count x:
          key_len:   u32 LE
          key:       key_len bytes
          count:     u64 LE

  ## Options

  - `:k` - maximum number of counters (default: 10, must be >= 1).
  - `:key_encoding` - key encoding policy: `:binary` (default), `:int`,
    or `{:term, :external}`.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Merge Properties

  MisraGries merge is **commutative**. Both sketches must have the same
  `k` parameter. Count (`n`) is always exactly additive.
  """

  alias ExDataSketch.{Backend, Codec, Errors}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_k 10

  @doc """
  Creates a new MisraGries sketch.

  ## Options

  - `:k` - maximum number of counters (default: #{@default_k}, must be >= 1).
  - `:key_encoding` - `:binary` (default), `:int`, or `{:term, :external}`.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new(k: 10)
      iex> sketch.opts[:k]
      10
      iex> ExDataSketch.MisraGries.count(sketch)
      0

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    key_encoding = Keyword.get(opts, :key_encoding, :binary)
    validate_k!(k)
    backend = Backend.resolve(opts)
    clean_opts = [k: k, key_encoding: key_encoding]
    state = backend.mg_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single item.

  The item is encoded using the configured key encoding before insertion.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update("hello")
      iex> ExDataSketch.MisraGries.count(sketch)
      1

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{} = sketch, item) do
    update_many(sketch, [item])
  end

  @doc """
  Updates the sketch with multiple items in a single pass.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update_many(["a", "b", "a"])
      iex> ExDataSketch.MisraGries.count(sketch)
      3

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    key_encoding = Keyword.get(opts, :key_encoding, :binary)
    encoded = Enum.map(items, &encode_key(&1, key_encoding))
    new_state = backend.mg_update_many(state, encoded, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Merges two MisraGries instances.

  Both sketches must have the same `k` parameter.

  ## Examples

      iex> a = ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update_many(["a", "a"])
      iex> b = ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update_many(["a", "b"])
      iex> merged = ExDataSketch.MisraGries.merge(a, b)
      iex> ExDataSketch.MisraGries.count(merged)
      4

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:k] != opts_b[:k] do
      raise Errors.IncompatibleSketchesError,
        reason: "MisraGries k mismatch: #{opts_a[:k]} vs #{opts_b[:k]}"
    end

    new_state = backend.mg_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of MisraGries instances into one.

  ## Examples

      iex> sketches = Enum.map(1..3, fn _ ->
      ...>   ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update("x")
      ...> end)
      iex> merged = ExDataSketch.MisraGries.merge_many(sketches)
      iex> ExDataSketch.MisraGries.count(merged)
      3

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns the estimated frequency of an item.

  The estimate is a lower bound on the true count. If the item is not
  tracked, returns 0.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update_many(["a", "a", "b"])
      iex> ExDataSketch.MisraGries.estimate(sketch, "a")
      2

  """
  @spec estimate(t(), term()) :: non_neg_integer()
  def estimate(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    key_encoding = Keyword.get(opts, :key_encoding, :binary)
    item_bytes = encode_key(item, key_encoding)
    backend.mg_estimate(state, item_bytes, opts)
  end

  @doc """
  Returns the top entries sorted by count descending.

  Each entry is a `{item, count}` tuple where the item is decoded using
  the configured key encoding.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new()
      iex> sketch = ExDataSketch.MisraGries.update_many(sketch, ["a", "a", "a", "b", "b", "c"])
      iex> [{top_item, top_count} | _] = ExDataSketch.MisraGries.top_k(sketch, 2)
      iex> top_item
      "a"
      iex> top_count
      3

  """
  @spec top_k(t(), non_neg_integer()) :: [{term(), non_neg_integer()}]
  def top_k(%__MODULE__{state: state, opts: opts, backend: backend}, limit) do
    key_encoding = Keyword.get(opts, :key_encoding, :binary)
    raw_entries = backend.mg_top_k(state, limit, opts)

    Enum.map(raw_entries, fn {key_bytes, count} ->
      {decode_key(key_bytes, key_encoding), count}
    end)
  end

  @doc """
  Returns entries whose estimated frequency exceeds the given threshold.

  The threshold is a fraction in (0.0, 1.0). Returns entries whose count
  is greater than `threshold * count(sketch)`.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new(k: 5)
      iex> sketch = Enum.reduce(1..100, sketch, fn _, s -> ExDataSketch.MisraGries.update(s, "heavy") end)
      iex> sketch = Enum.reduce(1..10, sketch, fn i, s -> ExDataSketch.MisraGries.update(s, "light_\#{i}") end)
      iex> frequent = ExDataSketch.MisraGries.frequent(sketch, 0.5)
      iex> Enum.any?(frequent, fn {item, _count} -> item == "heavy" end)
      true

  """
  @spec frequent(t(), float()) :: [{term(), non_neg_integer()}]
  def frequent(%__MODULE__{} = sketch, threshold) when is_float(threshold) and threshold > 0.0 do
    n = count(sketch)
    min_count = floor(threshold * n)

    sketch
    |> top_k(entry_count(sketch))
    |> Enum.filter(fn {_item, count} -> count > min_count end)
  end

  @doc """
  Returns the total number of items inserted into the sketch.

  ## Examples

      iex> ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.mg_count(state, opts)
  end

  @doc """
  Returns the number of distinct tracked entries.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new() |> ExDataSketch.MisraGries.update_many(["a", "b", "c"])
      iex> ExDataSketch.MisraGries.entry_count(sketch)
      3

  """
  @spec entry_count(t()) :: non_neg_integer()
  def entry_count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.mg_entry_count(state, opts)
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new()
      iex> ExDataSketch.MisraGries.size_bytes(sketch) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}) do
    byte_size(state)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.new()
      iex> binary = ExDataSketch.MisraGries.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    k = Keyword.fetch!(opts, :k)
    key_encoding = Keyword.get(opts, :key_encoding, :binary)
    enc_byte = encode_key_encoding(key_encoding)
    params_bin = <<k::unsigned-little-32, enc_byte::unsigned-8>>
    Codec.encode(Codec.sketch_id_mg(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a MisraGries sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.MisraGries.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    with {:ok, decoded} <- Codec.decode(binary),
         :ok <- validate_sketch_id(decoded.sketch_id),
         {:ok, opts} <- decode_params(decoded.params),
         :ok <- validate_state_header(decoded.state) do
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
  Creates a new MisraGries sketch from an enumerable.

  ## Examples

      iex> sketch = ExDataSketch.MisraGries.from_enumerable(["a", "b", "a"], k: 5)
      iex> ExDataSketch.MisraGries.count(sketch)
      3

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> update_many(enumerable)
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3`.

  ## Examples

      iex> is_function(ExDataSketch.MisraGries.reducer(), 2)
      true

  """
  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, sketch -> update(sketch, item) end
  end

  @doc """
  Returns a 2-arity merge function suitable for combining sketches.

  ## Examples

      iex> is_function(ExDataSketch.MisraGries.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  # -- Private --

  defp validate_k!(k) when is_integer(k) and k >= 1, do: :ok

  defp validate_k!(k) do
    raise Errors.InvalidOptionError,
      option: :k,
      value: k,
      message: "k must be an integer >= 1, got: #{inspect(k)}"
  end

  defp validate_sketch_id(14), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(
       reason: "expected MisraGries sketch ID (14), got #{id}"
     )}
  end

  defp decode_params(<<k::unsigned-little-32, enc_byte::unsigned-8>>)
       when k >= 1 do
    {:ok, [k: k, key_encoding: decode_key_encoding(enc_byte)]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid MisraGries params binary")}
  end

  defp encode_key(item, :binary) when is_binary(item), do: item
  defp encode_key(item, :int) when is_integer(item), do: <<item::signed-little-64>>
  defp encode_key(item, {:term, :external}), do: :erlang.term_to_binary(item)

  defp decode_key(bytes, :binary), do: bytes
  defp decode_key(<<val::signed-little-64>>, :int), do: val
  defp decode_key(bytes, {:term, :external}), do: :erlang.binary_to_term(bytes, [:safe])

  defp encode_key_encoding(:binary), do: 0
  defp encode_key_encoding(:int), do: 1
  defp encode_key_encoding({:term, :external}), do: 2

  defp decode_key_encoding(0), do: :binary
  defp decode_key_encoding(1), do: :int
  defp decode_key_encoding(2), do: {:term, :external}

  defp validate_state_header(<<"MG01", 1::unsigned-8, _rest::binary>>), do: :ok

  defp validate_state_header(<<"MG01", _::binary>>) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported MG01 version")}
  end

  defp validate_state_header(_state) do
    {:error, Errors.DeserializationError.exception(reason: "invalid MG01 state header")}
  end
end
