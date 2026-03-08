defmodule ExDataSketch.Cuckoo do
  @moduledoc """
  Cuckoo filter for probabilistic membership testing with deletion support.

  A Cuckoo filter is a space-efficient probabilistic data structure that supports
  insertion, deletion, and membership queries. It uses partial-key cuckoo hashing
  to store compact fingerprints in a bucket-based hash table.

  Unlike Bloom filters, Cuckoo filters support deletion. Unlike XOR filters,
  Cuckoo filters support incremental insertion. The trade-off is that insertion
  can fail when the filter approaches capacity.

  ## Parameters

  - `:capacity` -- expected number of items (default: 10,000). Used to derive
    the number of buckets.
  - `:fingerprint_size` -- fingerprint width in bits (default: 8). Supported
    values: 8, 12, 16. Determines the false positive rate:
    FPR ~= 2 * bucket_size / 2^fingerprint_size.
  - `:bucket_size` -- slots per bucket (default: 4). Supported values: 2, 4.
    Higher values improve space efficiency at the cost of lookup latency.
  - `:max_kicks` -- maximum relocation attempts before declaring full
    (default: 500). Range: 100..2000.
  - `:seed` -- hash seed (default: 0).

  ## False Positive Rates

  With default bucket_size=4:

  | fingerprint_size | FPR     |
  |------------------|---------|
  | 8                | ~3.1%   |
  | 12               | ~0.2%   |
  | 16               | ~0.012% |

  ## Hash Strategy

  Items are hashed via `ExDataSketch.Hash.hash64/1` at the API boundary.
  The bucket index is derived from the lower bits and the fingerprint from
  the upper bits of the 64-bit hash. Alternate bucket index uses XOR with
  a hash of the fingerprint (partial-key cuckoo hashing).

  ## Deletion Hazard

  Only delete items that were definitely inserted. Deleting a false-positive
  item removes a legitimate fingerprint, creating a false negative for a
  different item. This hazard is inherent to all Cuckoo filters.

  ## Binary State Layout (CKO1)

  See `plans/adr/ADR-102-cuckoo-binary-format.md` for the full specification.
  32-byte header followed by a flat bucket array of fingerprint entries.

  ## Merge Policy

  Merge is explicitly not supported. Cuckoo filter merge would require
  re-inserting all fingerprints which can fail due to capacity limits.
  For mergeable membership filters, use `ExDataSketch.Bloom`.
  """

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_capacity 10_000
  @default_fingerprint_size 8
  @default_bucket_size 4
  @default_max_kicks 500
  @default_seed 0
  @default_load_factor 0.955

  @doc """
  Creates a new empty Cuckoo filter.

  ## Options

  - `:capacity` -- expected number of items (default: #{@default_capacity}).
  - `:fingerprint_size` -- fingerprint width in bits (default: #{@default_fingerprint_size}).
    Supported values: 8, 12, 16.
  - `:bucket_size` -- slots per bucket (default: #{@default_bucket_size}).
    Supported values: 2, 4.
  - `:max_kicks` -- maximum relocation attempts (default: #{@default_max_kicks}).
    Range: 100..2000.
  - `:seed` -- hash seed (default: #{@default_seed}).
  - `:backend` -- backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> cuckoo = ExDataSketch.Cuckoo.new()
      iex> cuckoo.opts[:capacity]
      10000

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 1000, fingerprint_size: 16)
      iex> cuckoo.opts[:fingerprint_size]
      16

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    capacity = Keyword.get(opts, :capacity, @default_capacity)
    fp_size = Keyword.get(opts, :fingerprint_size, @default_fingerprint_size)
    bucket_size = Keyword.get(opts, :bucket_size, @default_bucket_size)
    max_kicks = Keyword.get(opts, :max_kicks, @default_max_kicks)
    seed = Keyword.get(opts, :seed, @default_seed)
    hash_fn = Keyword.get(opts, :hash_fn)

    validate_capacity!(capacity)
    validate_fingerprint_size!(fp_size)
    validate_bucket_size!(bucket_size)
    validate_max_kicks!(max_kicks)

    bucket_count = derive_bucket_count(capacity, bucket_size)

    backend = Backend.resolve(opts)

    clean_opts =
      [
        capacity: capacity,
        fingerprint_size: fp_size,
        bucket_size: bucket_size,
        bucket_count: bucket_count,
        max_kicks: max_kicks,
        seed: seed
      ] ++ if(hash_fn, do: [hash_fn: hash_fn], else: [])

    state = backend.cuckoo_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Inserts a single item into the filter.

  Returns `{:ok, cuckoo}` on success or `{:error, :full}` if the filter
  cannot accommodate the item after max_kicks relocation attempts.

  ## Examples

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.put(cuckoo, "hello")
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "hello")
      true

  """
  @spec put(t(), term()) :: {:ok, t()} | {:error, :full}
  def put(%__MODULE__{state: state, opts: opts, backend: backend} = cuckoo, item) do
    hash = hash_item(item, opts)

    case backend.cuckoo_put(state, hash, opts) do
      {:ok, new_state} -> {:ok, %{cuckoo | state: new_state}}
      {:error, :full} -> {:error, :full}
    end
  end

  @doc """
  Inserts a single item, raising on failure.

  ## Examples

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> cuckoo = ExDataSketch.Cuckoo.put!(cuckoo, "hello")
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "hello")
      true

  """
  @spec put!(t(), term()) :: t()
  def put!(cuckoo, item) do
    case put(cuckoo, item) do
      {:ok, updated} -> updated
      {:error, :full} -> raise "Cuckoo filter is full"
    end
  end

  @doc """
  Inserts multiple items in a single pass.

  Returns `{:ok, cuckoo}` if all items were inserted, or
  `{:error, :full, cuckoo}` with the partially updated filter
  if insertion failed partway through.

  ## Examples

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.put_many(cuckoo, ["a", "b", "c"])
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "a")
      true

  """
  @spec put_many(t(), Enumerable.t()) :: {:ok, t()} | {:error, :full, t()}
  def put_many(%__MODULE__{state: state, opts: opts, backend: backend} = cuckoo, items) do
    hashes = Enum.map(items, &hash_item(&1, opts))

    case backend.cuckoo_put_many(state, hashes, opts) do
      {:ok, new_state} -> {:ok, %{cuckoo | state: new_state}}
      {:error, :full, partial_state} -> {:error, :full, %{cuckoo | state: partial_state}}
    end
  end

  @doc """
  Tests whether an item may be a member of the set.

  Returns `true` if the item is possibly in the set (may be a false positive),
  `false` if the item is definitely not in the set.

  ## Examples

      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.new(capacity: 100) |> ExDataSketch.Cuckoo.put("hello")
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "hello")
      true

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "hello")
      false

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.cuckoo_member?(state, hash, opts)
  end

  @doc """
  Deletes a single item from the filter.

  Returns `{:ok, cuckoo}` if the item's fingerprint was found and removed,
  or `{:error, :not_found}` if no matching fingerprint exists.

  WARNING: Only delete items that were definitely inserted. Deleting a
  false-positive item removes a legitimate fingerprint belonging to a
  different item, creating a false negative.

  ## Examples

      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.new(capacity: 100) |> ExDataSketch.Cuckoo.put("hello")
      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.delete(cuckoo, "hello")
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "hello")
      false

  """
  @spec delete(t(), term()) :: {:ok, t()} | {:error, :not_found}
  def delete(%__MODULE__{state: state, opts: opts, backend: backend} = cuckoo, item) do
    hash = hash_item(item, opts)

    case backend.cuckoo_delete(state, hash, opts) do
      {:ok, new_state} -> {:ok, %{cuckoo | state: new_state}}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  @doc """
  Returns the number of items currently stored in the filter.

  ## Examples

      iex> ExDataSketch.Cuckoo.new(capacity: 100) |> ExDataSketch.Cuckoo.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.cuckoo_count(state, opts)
  end

  @doc """
  Serializes the filter to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> binary = ExDataSketch.Cuckoo.serialize(cuckoo)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    bucket_count = Keyword.fetch!(opts, :bucket_count)
    fp_size = Keyword.fetch!(opts, :fingerprint_size)
    bucket_size = Keyword.fetch!(opts, :bucket_size)
    seed = Keyword.get(opts, :seed, @default_seed)

    params_bin =
      <<bucket_count::unsigned-little-32, fp_size::unsigned-8, bucket_size::unsigned-8,
        seed::unsigned-little-32>>

    Codec.encode(Codec.sketch_id_cuckoo(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a Cuckoo filter.

  Returns `{:ok, cuckoo}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.new(capacity: 100) |> ExDataSketch.Cuckoo.put("test")
      iex> {:ok, recovered} = ExDataSketch.Cuckoo.deserialize(ExDataSketch.Cuckoo.serialize(cuckoo))
      iex> ExDataSketch.Cuckoo.member?(recovered, "test")
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
  Returns `true` if two Cuckoo filters have compatible parameters.

  Compatible filters have the same bucket_count, fingerprint_size,
  bucket_size, and seed.

  ## Examples

      iex> a = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> b = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> ExDataSketch.Cuckoo.compatible_with?(a, b)
      true

  """
  @spec compatible_with?(t(), t()) :: boolean()
  def compatible_with?(%__MODULE__{opts: opts_a}, %__MODULE__{opts: opts_b}) do
    opts_a[:bucket_count] == opts_b[:bucket_count] and
      opts_a[:fingerprint_size] == opts_b[:fingerprint_size] and
      opts_a[:bucket_size] == opts_b[:bucket_size] and
      opts_a[:seed] == opts_b[:seed]
  end

  @doc """
  Returns the set of capabilities supported by the Cuckoo filter.

  ## Examples

      iex> caps = ExDataSketch.Cuckoo.capabilities()
      iex> :put in caps and :delete in caps and :member? in caps
      true

  """
  def capabilities do
    MapSet.new([
      :new,
      :put,
      :put_many,
      :member?,
      :delete,
      :count,
      :serialize,
      :deserialize,
      :compatible_with?
    ])
  end

  @doc """
  Returns the byte size of the state binary.

  ## Examples

      iex> cuckoo = ExDataSketch.Cuckoo.new(capacity: 100)
      iex> ExDataSketch.Cuckoo.size_bytes(cuckoo) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}), do: byte_size(state)

  @doc """
  Creates a Cuckoo filter from an enumerable of items.

  Equivalent to `new(opts) |> put_many(enumerable)`.

  Returns `{:ok, cuckoo}` or `{:error, :full, cuckoo}`.

  ## Examples

      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.from_enumerable(["a", "b", "c"], capacity: 100)
      iex> ExDataSketch.Cuckoo.member?(cuckoo, "a")
      true

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: {:ok, t()} | {:error, :full, t()}
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> put_many(enumerable)
  end

  @doc """
  Returns a 2-arity reducer function for use with `Enum.reduce/3`.

  The reducer calls `put!/2` and raises if the filter becomes full.

  ## Examples

      iex> is_function(ExDataSketch.Cuckoo.reducer(), 2)
      true

  """
  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, cuckoo -> put!(cuckoo, item) end
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

  defp derive_bucket_count(capacity, bucket_size) do
    raw = capacity / (bucket_size * @default_load_factor)
    next_power_of_2(max(ceil(raw), 1))
  end

  defp next_power_of_2(n) when n <= 1, do: 1

  defp next_power_of_2(n) do
    import Bitwise
    p = n - 1
    p = p ||| p >>> 1
    p = p ||| p >>> 2
    p = p ||| p >>> 4
    p = p ||| p >>> 8
    p = p ||| p >>> 16
    p + 1
  end

  defp validate_capacity!(capacity)
       when is_integer(capacity) and capacity > 0 and capacity <= 0xFFFFFFFF,
       do: :ok

  defp validate_capacity!(capacity) do
    raise Errors.InvalidOptionError,
      option: :capacity,
      value: capacity,
      message: "capacity must be a positive integer <= #{0xFFFFFFFF}, got: #{inspect(capacity)}"
  end

  defp validate_fingerprint_size!(f) when f in [8, 12, 16], do: :ok

  defp validate_fingerprint_size!(f) do
    raise Errors.InvalidOptionError,
      option: :fingerprint_size,
      value: f,
      message: "fingerprint_size must be 8, 12, or 16, got: #{inspect(f)}"
  end

  defp validate_bucket_size!(b) when b in [2, 4], do: :ok

  defp validate_bucket_size!(b) do
    raise Errors.InvalidOptionError,
      option: :bucket_size,
      value: b,
      message: "bucket_size must be 2 or 4, got: #{inspect(b)}"
  end

  defp validate_max_kicks!(mk) when is_integer(mk) and mk >= 100 and mk <= 2000, do: :ok

  defp validate_max_kicks!(mk) do
    raise Errors.InvalidOptionError,
      option: :max_kicks,
      value: mk,
      message: "max_kicks must be an integer between 100 and 2000, got: #{inspect(mk)}"
  end

  defp validate_sketch_id(8), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected Cuckoo sketch ID (8), got #{id}")}
  end

  defp decode_params(
         <<bucket_count::unsigned-little-32, fp_size::unsigned-8, bucket_size::unsigned-8,
           seed::unsigned-little-32>>
       )
       when bucket_count > 0 and fp_size in [8, 12, 16] and bucket_size in [2, 4] do
    capacity = trunc(bucket_count * bucket_size * @default_load_factor)

    {:ok,
     [
       capacity: capacity,
       fingerprint_size: fp_size,
       bucket_size: bucket_size,
       bucket_count: bucket_count,
       max_kicks: @default_max_kicks,
       seed: seed
     ]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid Cuckoo params binary")}
  end

  defp validate_state_header(
         <<"CKO1", 1::unsigned-8, _fp_bits::unsigned-8, _bs::unsigned-8, _flags::unsigned-8,
           _rest::binary>>,
         _opts
       ) do
    :ok
  end

  defp validate_state_header(<<"CKO1", _::binary>>, _opts) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported CKO1 version")}
  end

  defp validate_state_header(_state, _opts) do
    {:error, Errors.DeserializationError.exception(reason: "invalid CKO1 state header")}
  end
end
