defmodule ExDataSketch.FrequentItems do
  @moduledoc """
  FrequentItems sketch for approximate heavy-hitter detection using the
  SpaceSaving algorithm.

  Tracks the top-k most frequent items in a data stream using at most `k`
  counters. Each counter stores an item, its estimated count, and a maximum
  overcount error. The sketch provides approximate frequency estimates with
  bounded memory and deterministic tie-breaking.

  ## Algorithm (SpaceSaving)

  The SpaceSaving algorithm maintains a fixed set of at most `k` counters:

  - **Update(x)**: If x is already tracked, increment its count. If there is
    room (fewer than k entries), insert x with count=1 and error=0. Otherwise,
    evict the entry with the minimum count (ties broken by lexicographically
    smallest `item_bytes`), and replace it with x, count=min_count+1,
    error=min_count.

  - **Weighted update(x, w)**: Same logic but increment by w. Replacement
    count = min_count + w, error = min_count.

  - **Batch optimization**: Pre-aggregate incoming items into a frequency map,
    then apply weighted updates for each unique item in sorted key order.
    Single decode + single encode of the binary state.

  ## Key Encoding

  Items are encoded to binary at the public API boundary using a configurable
  key encoding policy:

  | Encoding | Description |
  |----------|-------------|
  | `:binary` (default) | Keys are raw binaries, passed through as-is |
  | `:int` | Keys are integers, encoded as signed 64-bit little-endian |
  | `{:term, :external}` | Keys are arbitrary Erlang terms, encoded via `:erlang.term_to_binary/1` |

  The backend always receives and returns raw `item_bytes`. Decoding happens
  at the public API boundary when returning results.

  ## Merge Properties

  FrequentItems merge is **commutative**. Both sketches must have the same
  `k` and `key_encoding` parameters. Merge combines counts and errors
  additively across the union of keys, then retains the top-k entries by
  count (ties broken by lexicographically smallest key) to enforce the
  capacity invariant. Count (`n`) is always exactly additive regardless of
  whether entries are dropped.

  Associativity holds for count totals but not necessarily for the retained
  entry set, since intermediate merges may drop entries that a different
  grouping would retain. See `docs/frequent_items_format.md` for details.

  ## Binary State Format (FI1)

  See `docs/frequent_items_format.md` for the complete binary layout
  specification, canonicalization rules, and merge algebra proof sketch.

  ## Options

  - `:k` - maximum number of counters (default: 10, must be >= 1).
  - `:key_encoding` - key encoding policy: `:binary` (default), `:int`,
    or `{:term, :external}`.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).
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
  Creates a new FrequentItems sketch.

  ## Options

  - `:k` - maximum number of counters (default: #{@default_k}, must be >= 1).
  - `:key_encoding` - key encoding policy: `:binary` (default), `:int`,
    or `{:term, :external}`.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 10)
      iex> sketch.opts[:k]
      10
      iex> ExDataSketch.FrequentItems.count(sketch)
      0

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    key_encoding = Keyword.get(opts, :key_encoding, :binary)
    validate_k!(k)
    validate_key_encoding!(key_encoding)
    backend = Backend.resolve(opts)
    flags = encode_flags(key_encoding)
    clean_opts = [k: k, key_encoding: key_encoding, flags: flags]
    state = backend.fi_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single item.

  Delegates to `update_many/2` with a single-element list.

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 5)
      iex> sketch = ExDataSketch.FrequentItems.update(sketch, "hello")
      iex> ExDataSketch.FrequentItems.count(sketch)
      1

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{} = sketch, item) do
    update_many(sketch, [item])
  end

  @doc """
  Updates the sketch with multiple items in a single pass.

  More efficient than calling `update/2` repeatedly because it pre-aggregates
  items into a frequency map, decodes and encodes the binary state only once,
  and applies weighted updates for each unique item.

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 5)
      iex> sketch = ExDataSketch.FrequentItems.update_many(sketch, ["a", "b", "a"])
      iex> ExDataSketch.FrequentItems.count(sketch)
      3

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    encoded_items = Enum.map(items, fn item -> encode_key(item, opts[:key_encoding]) end)
    new_state = backend.fi_update_many(state, encoded_items, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Merges two FrequentItems sketches.

  Both sketches must have the same `k` and `key_encoding` parameters. The
  merge combines counts and errors additively across the union of tracked
  items, then retains the top-k entries by count to enforce the capacity
  invariant.

  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if the sketches
  have different `k` or `key_encoding` values.

  ## Examples

      iex> a = ExDataSketch.FrequentItems.new(k: 5) |> ExDataSketch.FrequentItems.update_many(["x", "x"])
      iex> b = ExDataSketch.FrequentItems.new(k: 5) |> ExDataSketch.FrequentItems.update_many(["y", "y"])
      iex> merged = ExDataSketch.FrequentItems.merge(a, b)
      iex> ExDataSketch.FrequentItems.count(merged)
      4

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:k] != opts_b[:k] do
      raise Errors.IncompatibleSketchesError,
        reason: "FrequentItems k mismatch: #{opts_a[:k]} vs #{opts_b[:k]}"
    end

    if opts_a[:key_encoding] != opts_b[:key_encoding] do
      raise Errors.IncompatibleSketchesError,
        reason:
          "FrequentItems key_encoding mismatch: #{inspect(opts_a[:key_encoding])} vs #{inspect(opts_b[:key_encoding])}"
    end

    new_state = backend.fi_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of FrequentItems sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> sketches = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.FrequentItems.new(k: 5) |> ExDataSketch.FrequentItems.update(to_string(i))
      ...> end)
      iex> merged = ExDataSketch.FrequentItems.merge_many(sketches)
      iex> ExDataSketch.FrequentItems.count(merged)
      3

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns the total number of items observed by the sketch.

  This is the sum of all weights (each `update/2` call contributes 1).

  ## Examples

      iex> ExDataSketch.FrequentItems.new(k: 5) |> ExDataSketch.FrequentItems.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.fi_count(state, opts)
  end

  @doc """
  Returns the frequency estimate for a given item.

  Returns `{:ok, estimate_map}` if the item is tracked, where `estimate_map`
  contains `:estimate`, `:error`, `:lower`, and `:upper` fields.
  Returns `{:error, :not_tracked}` if the item is not in the sketch.

  The estimate map fields:
  - `:estimate` - the estimated count (may overcount but never undercount)
  - `:error` - the maximum possible overcount
  - `:lower` - `max(estimate - error, 0)`, guaranteed lower bound
  - `:upper` - same as estimate, guaranteed upper bound

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 10) |> ExDataSketch.FrequentItems.update("x")
      iex> {:ok, est} = ExDataSketch.FrequentItems.estimate(sketch, "x")
      iex> est.estimate
      1

  """
  @spec estimate(t(), term()) :: {:ok, map()} | {:error, :not_tracked}
  def estimate(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    item_bytes = encode_key(item, opts[:key_encoding])
    backend.fi_estimate(state, item_bytes, opts)
  end

  @doc """
  Returns the top-k most frequent items, sorted by estimated count descending.

  Ties in count are broken by key ascending (lexicographic on raw bytes,
  then decoded via the key encoding policy).

  ## Options

  - `:limit` - maximum number of items to return (default: all entries).

  Returns a list of maps with `:item`, `:estimate`, `:error`, `:lower`,
  and `:upper` fields.

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 10)
      iex> sketch = ExDataSketch.FrequentItems.update_many(sketch, ["a", "a", "b"])
      iex> [first | _] = ExDataSketch.FrequentItems.top_k(sketch)
      iex> first.item
      "a"

  """
  @spec top_k(t(), keyword()) :: [map()]
  def top_k(%__MODULE__{state: state, opts: opts, backend: backend}, query_opts \\ []) do
    limit = Keyword.get(query_opts, :limit, opts[:k])

    backend.fi_top_k(state, limit, opts)
    |> Enum.map(fn entry ->
      Map.update!(entry, :item, fn item_bytes ->
        decode_key(item_bytes, opts[:key_encoding])
      end)
    end)
  end

  @doc """
  Returns items whose estimated frequency exceeds the given threshold.

  The threshold is compared against the lower bound (estimate - error).
  Only items with `lower >= threshold` are returned.

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 10)
      iex> sketch = ExDataSketch.FrequentItems.update_many(sketch, List.duplicate("a", 100) ++ ["b"])
      iex> frequent = ExDataSketch.FrequentItems.frequent(sketch, 50)
      iex> Enum.any?(frequent, fn e -> e.item == "a" end)
      true

  """
  @spec frequent(t(), non_neg_integer()) :: [map()]
  def frequent(%__MODULE__{} = sketch, threshold) when is_integer(threshold) and threshold >= 0 do
    top_k(sketch)
    |> Enum.filter(fn entry -> entry.lower >= threshold end)
  end

  @doc """
  Returns the number of distinct items currently tracked by the sketch.

  This is always <= k.

  ## Examples

      iex> ExDataSketch.FrequentItems.new(k: 5) |> ExDataSketch.FrequentItems.entry_count()
      0

  """
  @spec entry_count(t()) :: non_neg_integer()
  def entry_count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.fi_entry_count(state, opts)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.new(k: 10)
      iex> binary = ExDataSketch.FrequentItems.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    k = Keyword.fetch!(opts, :k)
    flags = Keyword.fetch!(opts, :flags)
    params_bin = <<k::unsigned-little-32, flags::unsigned-8>>
    Codec.encode(Codec.sketch_id_fi(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a FrequentItems sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.FrequentItems.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    with {:ok, decoded} <- Codec.decode(binary),
         :ok <- validate_sketch_id(decoded.sketch_id),
         {:ok, opts} <- decode_params(decoded.params),
         :ok <- validate_state_header(decoded.state, opts) do
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
  Creates a new FrequentItems sketch from an enumerable of items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Examples

      iex> sketch = ExDataSketch.FrequentItems.from_enumerable(["a", "b", "a"], k: 10)
      iex> ExDataSketch.FrequentItems.count(sketch)
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

      iex> is_function(ExDataSketch.FrequentItems.reducer(), 2)
      true

  """
  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, sketch -> update(sketch, item) end
  end

  @doc """
  Returns a 2-arity merge function suitable for combining sketches.

  ## Examples

      iex> is_function(ExDataSketch.FrequentItems.merger(), 2)
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
      message: "k must be a positive integer >= 1, got: #{inspect(k)}"
  end

  defp validate_key_encoding!(:binary), do: :ok
  defp validate_key_encoding!(:int), do: :ok
  defp validate_key_encoding!({:term, :external}), do: :ok

  defp validate_key_encoding!(encoding) do
    raise Errors.InvalidOptionError,
      option: :key_encoding,
      value: encoding,
      message:
        "key_encoding must be :binary, :int, or {:term, :external}, got: #{inspect(encoding)}"
  end

  defp encode_flags(:binary), do: 0
  defp encode_flags(:int), do: 1
  defp encode_flags({:term, :external}), do: 2

  defp decode_flags(0), do: :binary
  defp decode_flags(1), do: :int
  defp decode_flags(2), do: {:term, :external}

  defp encode_key(item, :binary) when is_binary(item), do: item

  defp encode_key(item, :int) when is_integer(item) do
    <<item::signed-little-64>>
  end

  defp encode_key(item, {:term, :external}) do
    :erlang.term_to_binary(item)
  end

  defp decode_key(item_bytes, :binary), do: item_bytes

  defp decode_key(<<value::signed-little-64>>, :int), do: value

  defp decode_key(item_bytes, {:term, :external}) do
    :erlang.binary_to_term(item_bytes, [:safe])
  end

  defp validate_sketch_id(6), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(
       reason: "expected FrequentItems sketch ID (6), got #{id}"
     )}
  end

  defp validate_state_header(
         <<"FI1\0", 1::unsigned-8, flags::unsigned-8, _reserved::binary-size(2),
           k::unsigned-little-32, _rest::binary>>,
         opts
       ) do
    expected_k = Keyword.fetch!(opts, :k)
    expected_flags = Keyword.fetch!(opts, :flags)

    cond do
      k != expected_k ->
        {:error,
         Errors.DeserializationError.exception(
           reason: "FI1 state header k (#{k}) does not match EXSK params k (#{expected_k})"
         )}

      flags != expected_flags ->
        {:error,
         Errors.DeserializationError.exception(
           reason:
             "FI1 state header flags (#{flags}) does not match EXSK params flags (#{expected_flags})"
         )}

      true ->
        :ok
    end
  end

  defp validate_state_header(state, _opts) when byte_size(state) < 32 do
    {:error,
     Errors.DeserializationError.exception(
       reason: "FI1 state too short: expected at least 32 bytes, got #{byte_size(state)}"
     )}
  end

  defp validate_state_header(_state, _opts) do
    {:error,
     Errors.DeserializationError.exception(
       reason: "FI1 state header invalid: bad magic or version"
     )}
  end

  defp decode_params(<<k::unsigned-little-32, flags::unsigned-8>>)
       when k >= 1 and flags in [0, 1, 2] do
    key_encoding = decode_flags(flags)
    {:ok, [k: k, key_encoding: key_encoding, flags: flags]}
  end

  defp decode_params(<<k::unsigned-little-32, _flags::unsigned-8>>) when k < 1 do
    {:error,
     Errors.DeserializationError.exception(reason: "invalid FrequentItems k value #{k} in params")}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid FrequentItems params binary")}
  end
end
