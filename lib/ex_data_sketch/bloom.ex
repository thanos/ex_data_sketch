defmodule ExDataSketch.Bloom do
  @moduledoc """
  Bloom filter for probabilistic membership testing.

  A Bloom filter is a space-efficient probabilistic data structure that tests
  whether an element is a member of a set. False positives are possible, but
  false negatives are not: if `member?/2` returns `false`, the item was
  definitely not inserted; if it returns `true`, the item was probably inserted.

  ## Parameters

  - `:capacity` -- expected number of elements (default: 10,000). Used to
    derive the optimal bitset size.
  - `:false_positive_rate` -- target false positive rate (default: 0.01).
    Must be between 0 and 1 exclusive.
  - `:seed` -- hash seed (default: 0). Filters with different seeds are
    incompatible for merge.

  The optimal `bit_count` and `hash_count` are derived automatically:

      bit_count  = ceil(-capacity * ln(fpr) / ln(2)^2)
      hash_count = max(1, round(bit_count / capacity * ln(2)))

  ## Hash Strategy

  Items are hashed via `ExDataSketch.Hash.hash64/1` at the API boundary.
  The 64-bit hash is split into two 32-bit halves for double hashing
  (Kirsch-Mitzenmacher optimization):

      h1 = hash >>> 32
      h2 = hash &&& 0xFFFFFFFF
      position_i = rem(h1 + i * h2, bit_count)

  ## Binary State Layout (BLM1)

  See `plans/adr/ADR-001-bloom-binary-format.md` for the full specification.
  40-byte header followed by a packed bitset (LSB-first byte order).

  ## Merge Properties

  Bloom merge is **commutative** and **associative** (bitwise OR). Two filters
  can merge only if they have identical `bit_count`, `hash_count`, and `seed`.
  """

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_capacity 10_000
  @default_fpr 0.01
  @default_seed 0
  @max_hash_count 30

  @doc """
  Creates a new empty Bloom filter.

  ## Options

  - `:capacity` -- expected number of elements (default: #{@default_capacity}).
  - `:false_positive_rate` -- target FPR (default: #{@default_fpr}).
  - `:seed` -- hash seed (default: #{@default_seed}).
  - `:backend` -- backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new()
      iex> bloom.opts[:capacity]
      10000

      iex> bloom = ExDataSketch.Bloom.new(capacity: 1000, false_positive_rate: 0.001)
      iex> bloom.opts[:capacity]
      1000

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    capacity = Keyword.get(opts, :capacity, @default_capacity)
    fpr = Keyword.get(opts, :false_positive_rate, @default_fpr)
    seed = Keyword.get(opts, :seed, @default_seed)
    hash_fn = Keyword.get(opts, :hash_fn)

    validate_capacity!(capacity)
    validate_fpr!(fpr)

    bit_count = derive_bit_count(capacity, fpr)
    validate_bit_count!(bit_count, capacity, fpr)
    hash_count = derive_hash_count(bit_count, capacity)

    backend = Backend.resolve(opts)

    clean_opts =
      [
        capacity: capacity,
        false_positive_rate: fpr,
        seed: seed,
        bit_count: bit_count,
        hash_count: hash_count
      ] ++ if(hash_fn, do: [hash_fn: hash_fn], else: [])

    state = backend.bloom_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Inserts a single item into the filter.

  The item is hashed via `ExDataSketch.Hash.hash64/1` before insertion.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new() |> ExDataSketch.Bloom.put("hello")
      iex> ExDataSketch.Bloom.member?(bloom, "hello")
      true

  """
  @spec put(t(), term()) :: t()
  def put(%__MODULE__{state: state, opts: opts, backend: backend} = bloom, item) do
    hash = hash_item(item, opts)
    new_state = backend.bloom_put(state, hash, opts)
    %{bloom | state: new_state}
  end

  @doc """
  Inserts multiple items in a single pass.

  More efficient than calling `put/2` repeatedly because it minimizes
  intermediate binary allocations.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new() |> ExDataSketch.Bloom.put_many(["a", "b", "c"])
      iex> ExDataSketch.Bloom.member?(bloom, "a")
      true

  """
  @spec put_many(t(), Enumerable.t()) :: t()
  def put_many(%__MODULE__{state: state, opts: opts, backend: backend} = bloom, items) do
    hashes = Enum.map(items, &hash_item(&1, opts))
    new_state = backend.bloom_put_many(state, hashes, opts)
    %{bloom | state: new_state}
  end

  @doc """
  Tests whether an item may be a member of the set.

  Returns `true` if the item is possibly in the set (may be a false positive),
  `false` if the item is definitely not in the set.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new() |> ExDataSketch.Bloom.put("hello")
      iex> ExDataSketch.Bloom.member?(bloom, "hello")
      true

      iex> bloom = ExDataSketch.Bloom.new()
      iex> ExDataSketch.Bloom.member?(bloom, "hello")
      false

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.bloom_member?(state, hash, opts)
  end

  @doc """
  Merges two Bloom filters via bitwise OR.

  Both filters must have identical `bit_count`, `hash_count`, and `seed`.
  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if parameters differ.

  ## Examples

      iex> a = ExDataSketch.Bloom.new(capacity: 100) |> ExDataSketch.Bloom.put("x")
      iex> b = ExDataSketch.Bloom.new(capacity: 100) |> ExDataSketch.Bloom.put("y")
      iex> merged = ExDataSketch.Bloom.merge(a, b)
      iex> ExDataSketch.Bloom.member?(merged, "x") and ExDataSketch.Bloom.member?(merged, "y")
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = bloom,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    validate_merge_compat!(opts_a, opts_b)
    new_state = backend.bloom_merge(state_a, state_b, opts_a)
    %{bloom | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of Bloom filters into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> filters = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.Bloom.new(capacity: 100) |> ExDataSketch.Bloom.put("item_\#{i}")
      ...> end)
      iex> merged = ExDataSketch.Bloom.merge_many(filters)
      iex> ExDataSketch.Bloom.member?(merged, "item_1")
      true

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(blooms) do
    Enum.reduce(blooms, fn bloom, acc -> merge(acc, bloom) end)
  end

  @doc """
  Serializes the filter to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new(capacity: 100)
      iex> binary = ExDataSketch.Bloom.serialize(bloom)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    bit_count = Keyword.fetch!(opts, :bit_count)
    hash_count = Keyword.fetch!(opts, :hash_count)
    seed = Keyword.get(opts, :seed, @default_seed)

    params_bin =
      <<bit_count::unsigned-little-32, hash_count::unsigned-little-16, seed::unsigned-little-32>>

    Codec.encode(Codec.sketch_id_bloom(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a Bloom filter.

  Returns `{:ok, bloom}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new(capacity: 100) |> ExDataSketch.Bloom.put("test")
      iex> {:ok, recovered} = ExDataSketch.Bloom.deserialize(ExDataSketch.Bloom.serialize(bloom))
      iex> ExDataSketch.Bloom.member?(recovered, "test")
      true

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
  Returns the configured capacity (expected number of elements).

  ## Examples

      iex> ExDataSketch.Bloom.new(capacity: 5000) |> ExDataSketch.Bloom.capacity()
      5000

  """
  @spec capacity(t()) :: pos_integer()
  def capacity(%__MODULE__{opts: opts}), do: Keyword.fetch!(opts, :capacity)

  @doc """
  Returns the configured target false positive rate.

  ## Examples

      iex> ExDataSketch.Bloom.new(false_positive_rate: 0.05) |> ExDataSketch.Bloom.error_rate()
      0.05

  """
  @spec error_rate(t()) :: float()
  def error_rate(%__MODULE__{opts: opts}), do: Keyword.fetch!(opts, :false_positive_rate)

  @doc """
  Returns the number of set bits (popcount) in the bitset.

  This is NOT the number of inserted elements. Useful for computing fill ratio.

  ## Examples

      iex> ExDataSketch.Bloom.new(capacity: 100) |> ExDataSketch.Bloom.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.bloom_count(state, opts)
  end

  @doc """
  Returns the byte size of the state binary.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.new(capacity: 100)
      iex> ExDataSketch.Bloom.size_bytes(bloom) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}), do: byte_size(state)

  @doc """
  Creates a Bloom filter from an enumerable of items.

  Equivalent to `new(opts) |> put_many(enumerable)`.

  ## Examples

      iex> bloom = ExDataSketch.Bloom.from_enumerable(["a", "b", "c"], capacity: 100)
      iex> ExDataSketch.Bloom.member?(bloom, "a")
      true

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> put_many(enumerable)
  end

  @doc """
  Returns a 2-arity reducer function for use with `Enum.reduce/3`.

  ## Examples

      iex> is_function(ExDataSketch.Bloom.reducer(), 2)
      true

  """
  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, bloom -> put(bloom, item) end
  end

  @doc """
  Returns a 2-arity merge function for combining filters.

  ## Examples

      iex> is_function(ExDataSketch.Bloom.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  # -- Private --

  defp hash_item(item, opts) do
    case Keyword.get(opts, :hash_fn) do
      nil ->
        seed = Keyword.get(opts, :seed, @default_seed)
        Hash.hash64(item, seed: seed)

      hash_fn ->
        Hash.hash64(item, hash_fn: hash_fn)
    end
  end

  defp derive_bit_count(capacity, fpr) do
    raw = -capacity * :math.log(fpr) / (:math.log(2) * :math.log(2))
    ceil(raw)
  end

  defp derive_hash_count(bit_count, capacity) do
    raw = bit_count / capacity * :math.log(2)
    max(1, min(round(raw), @max_hash_count))
  end

  defp validate_capacity!(capacity)
       when is_integer(capacity) and capacity > 0 and capacity <= 0xFFFFFFFF,
       do: :ok

  defp validate_capacity!(capacity) when is_integer(capacity) and capacity > 0xFFFFFFFF do
    raise Errors.InvalidOptionError,
      option: :capacity,
      value: capacity,
      message:
        "capacity must fit in a u32 (max #{0xFFFFFFFF}), got: #{capacity}"
  end

  defp validate_capacity!(capacity) do
    raise Errors.InvalidOptionError,
      option: :capacity,
      value: capacity,
      message: "capacity must be a positive integer, got: #{inspect(capacity)}"
  end

  defp validate_bit_count!(bit_count, capacity, fpr) when bit_count > 0xFFFFFFFF do
    raise Errors.InvalidOptionError,
      option: :capacity,
      value: capacity,
      message:
        "capacity #{capacity} with false_positive_rate #{fpr} requires #{bit_count} bits, " <>
          "which exceeds the u32 maximum (#{0xFFFFFFFF}). Reduce capacity or increase false_positive_rate."
  end

  defp validate_bit_count!(_bit_count, _capacity, _fpr), do: :ok

  defp validate_fpr!(fpr) when is_float(fpr) and fpr > 0.0 and fpr < 1.0, do: :ok

  defp validate_fpr!(fpr) do
    raise Errors.InvalidOptionError,
      option: :false_positive_rate,
      value: fpr,
      message:
        "false_positive_rate must be a float between 0 and 1 exclusive, got: #{inspect(fpr)}"
  end

  defp validate_merge_compat!(opts_a, opts_b) do
    bc_a = opts_a[:bit_count]
    bc_b = opts_b[:bit_count]

    if bc_a != bc_b do
      raise Errors.IncompatibleSketchesError,
        reason: "Bloom bit_count mismatch: #{bc_a} vs #{bc_b}"
    end

    hc_a = opts_a[:hash_count]
    hc_b = opts_b[:hash_count]

    if hc_a != hc_b do
      raise Errors.IncompatibleSketchesError,
        reason: "Bloom hash_count mismatch: #{hc_a} vs #{hc_b}"
    end

    seed_a = Keyword.get(opts_a, :seed, @default_seed)
    seed_b = Keyword.get(opts_b, :seed, @default_seed)

    if seed_a != seed_b do
      raise Errors.IncompatibleSketchesError,
        reason: "Bloom seed mismatch: #{seed_a} vs #{seed_b}"
    end
  end

  defp validate_sketch_id(7), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected Bloom sketch ID (7), got #{id}")}
  end

  defp decode_params(
         <<bit_count::unsigned-little-32, hash_count::unsigned-little-16,
           seed::unsigned-little-32>>
       )
       when bit_count > 0 and hash_count > 0 do
    # Reverse-derive capacity and FPR from bit_count and hash_count
    capacity = round(bit_count * :math.log(2) / hash_count)
    capacity = max(capacity, 1)
    fpr = :math.pow(1.0 - :math.exp(-hash_count / (bit_count / capacity)), hash_count)

    {:ok,
     [
       capacity: capacity,
       false_positive_rate: fpr,
       seed: seed,
       bit_count: bit_count,
       hash_count: hash_count
     ]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid Bloom params binary")}
  end

  defp validate_state_header(
         <<"BLM1", 1::unsigned-8, _scheme::unsigned-8, _hash_count::unsigned-little-16,
           bit_count::unsigned-little-32, _seed::unsigned-little-32, _fpr::binary-size(8),
           _capacity::unsigned-little-32, bitset_byte_length::unsigned-little-32,
           _reserved::binary-size(8), bitset::binary>>,
         opts
       ) do
    expected_bc = Keyword.fetch!(opts, :bit_count)

    cond do
      bit_count != expected_bc ->
        {:error,
         Errors.DeserializationError.exception(
           reason: "BLM1 bit_count #{bit_count} does not match params #{expected_bc}"
         )}

      byte_size(bitset) != bitset_byte_length ->
        {:error, Errors.DeserializationError.exception(reason: "BLM1 bitset length mismatch")}

      true ->
        :ok
    end
  end

  defp validate_state_header(<<"BLM1", _::binary>>, _opts) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported BLM1 version")}
  end

  defp validate_state_header(_state, _opts) do
    {:error, Errors.DeserializationError.exception(reason: "invalid BLM1 state header")}
  end
end
