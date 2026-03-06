defmodule ExDataSketch.Quotient do
  @moduledoc """
  Quotient filter for probabilistic membership testing with safe deletion and merge.

  A Quotient filter is a compact approximate membership data structure that
  splits a fingerprint into a quotient (slot index) and remainder (stored value).
  It supports insertion, safe deletion, membership queries, and merge without
  re-hashing.

  ## Parameters

  - `:q` -- quotient bits (default: 16). Determines the number of slots: 2^q.
  - `:r` -- remainder bits (default: 8). Determines false positive rate: ~1/2^r.
    Constraint: q + r <= 64.
  - `:seed` -- hash seed (default: 0).

  ## False Positive Rates

  | r bits | FPR       |
  |--------|-----------|
  | 4      | ~6.25%    |
  | 8      | ~0.39%    |
  | 12     | ~0.024%   |
  | 16     | ~0.0015%  |

  ## Hash Strategy

  Items are hashed via `ExDataSketch.Hash.hash64/1` at the API boundary.
  The upper q bits of the 64-bit hash are the quotient (slot index).
  The next r bits are the remainder (stored value).

  ## Merge Properties

  Quotient filter merge is **commutative** and **associative**. The sortable
  property of the slot array enables merge via sorted fingerprint extraction
  and merge-sort without re-hashing the original items. Two filters can merge
  only if they have identical q, r, and seed.

  ## Deletion Safety

  Unlike Cuckoo filters, Quotient filter deletion is safe: deleting a
  non-inserted item is a no-op and does not create false negatives for
  other items.

  ## Binary State Layout (QOT1)

  32-byte header followed by a packed slot array. Each slot contains
  3 metadata bits (is_occupied, is_continuation, is_shifted) and r
  remainder bits, packed LSB-first.
  """

  import Bitwise

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_q 16
  @default_r 8
  @default_seed 0

  @doc """
  Creates a new empty Quotient filter.

  ## Options

  - `:q` -- quotient bits (default: #{@default_q}). Range: 1..28.
  - `:r` -- remainder bits (default: #{@default_r}). Range: 1..32.
    Constraint: q + r <= 64.
  - `:seed` -- hash seed (default: #{@default_seed}).
  - `:backend` -- backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new()
      iex> qf.opts[:q]
      16

      iex> qf = ExDataSketch.Quotient.new(q: 12, r: 10)
      iex> qf.opts[:r]
      10

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    q = Keyword.get(opts, :q, @default_q)
    r = Keyword.get(opts, :r, @default_r)
    seed = Keyword.get(opts, :seed, @default_seed)
    hash_fn = Keyword.get(opts, :hash_fn)

    validate_q!(q)
    validate_r!(r)
    validate_qr_sum!(q, r)

    slot_count = 1 <<< q
    backend = Backend.resolve(opts)

    clean_opts =
      [
        q: q,
        r: r,
        slot_count: slot_count,
        seed: seed
      ] ++ if(hash_fn, do: [hash_fn: hash_fn], else: [])

    state = backend.quotient_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Inserts a single item into the filter.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("hello")
      iex> ExDataSketch.Quotient.member?(qf, "hello")
      true

  """
  @spec put(t(), term()) :: t()
  def put(%__MODULE__{state: state, opts: opts, backend: backend} = qf, item) do
    hash = hash_item(item, opts)
    new_state = backend.quotient_put(state, hash, opts)
    %{qf | state: new_state}
  end

  @doc """
  Inserts multiple items in a single pass.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put_many(["a", "b"])
      iex> ExDataSketch.Quotient.member?(qf, "a")
      true

  """
  @spec put_many(t(), Enumerable.t()) :: t()
  def put_many(%__MODULE__{state: state, opts: opts, backend: backend} = qf, items) do
    hashes = Enum.map(items, &hash_item(&1, opts))
    new_state = backend.quotient_put_many(state, hashes, opts)
    %{qf | state: new_state}
  end

  @doc """
  Tests whether an item may be a member of the set.

  Returns `true` if the item is possibly in the set (may be a false positive),
  `false` if the item is definitely not in the set.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("hello")
      iex> ExDataSketch.Quotient.member?(qf, "hello")
      true

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8)
      iex> ExDataSketch.Quotient.member?(qf, "hello")
      false

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.quotient_member?(state, hash, opts)
  end

  @doc """
  Deletes a single item from the filter.

  Unlike Cuckoo filters, this operation is safe: deleting a non-member
  is a no-op and does not create false negatives.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("hello")
      iex> qf = ExDataSketch.Quotient.delete(qf, "hello")
      iex> ExDataSketch.Quotient.member?(qf, "hello")
      false

  """
  @spec delete(t(), term()) :: t()
  def delete(%__MODULE__{state: state, opts: opts, backend: backend} = qf, item) do
    hash = hash_item(item, opts)
    new_state = backend.quotient_delete(state, hash, opts)
    %{qf | state: new_state}
  end

  @doc """
  Merges two Quotient filters via sorted fingerprint merge.

  Both filters must have identical q, r, and seed.
  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if parameters differ.

  ## Examples

      iex> a = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("x")
      iex> b = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("y")
      iex> merged = ExDataSketch.Quotient.merge(a, b)
      iex> ExDataSketch.Quotient.member?(merged, "x") and ExDataSketch.Quotient.member?(merged, "y")
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = qf,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    validate_merge_compat!(opts_a, opts_b)
    new_state = backend.quotient_merge(state_a, state_b, opts_a)
    %{qf | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of Quotient filters into one.

  ## Examples

      iex> filters = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("item_\#{i}")
      ...> end)
      iex> merged = ExDataSketch.Quotient.merge_many(filters)
      iex> ExDataSketch.Quotient.member?(merged, "item_1")
      true

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(filters) do
    Enum.reduce(filters, fn qf, acc -> merge(acc, qf) end)
  end

  @doc """
  Returns the number of items stored in the filter.

  ## Examples

      iex> ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.quotient_count(state, opts)
  end

  @doc """
  Serializes the filter to the EXSK binary format.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8)
      iex> binary = ExDataSketch.Quotient.serialize(qf)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    q = Keyword.fetch!(opts, :q)
    r = Keyword.fetch!(opts, :r)
    seed = Keyword.get(opts, :seed, @default_seed)

    params_bin = <<q::unsigned-8, r::unsigned-8, seed::unsigned-little-32, 0::unsigned-8>>

    Codec.encode(Codec.sketch_id_quotient(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a Quotient filter.

  Returns `{:ok, quotient}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> qf = ExDataSketch.Quotient.new(q: 10, r: 8) |> ExDataSketch.Quotient.put("test")
      iex> {:ok, recovered} = ExDataSketch.Quotient.deserialize(ExDataSketch.Quotient.serialize(qf))
      iex> ExDataSketch.Quotient.member?(recovered, "test")
      true

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
  Returns `true` if two Quotient filters have compatible parameters.

  ## Examples

      iex> a = ExDataSketch.Quotient.new(q: 10, r: 8)
      iex> b = ExDataSketch.Quotient.new(q: 10, r: 8)
      iex> ExDataSketch.Quotient.compatible_with?(a, b)
      true

  """
  @spec compatible_with?(t(), t()) :: boolean()
  def compatible_with?(%__MODULE__{opts: opts_a}, %__MODULE__{opts: opts_b}) do
    opts_a[:q] == opts_b[:q] and opts_a[:r] == opts_b[:r] and opts_a[:seed] == opts_b[:seed]
  end

  def capabilities do
    MapSet.new([
      :new,
      :put,
      :put_many,
      :member?,
      :delete,
      :merge,
      :merge_many,
      :count,
      :serialize,
      :deserialize,
      :compatible_with?
    ])
  end

  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}), do: byte_size(state)

  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> put_many(enumerable)
  end

  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, qf -> put(qf, item) end
  end

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

  defp validate_q!(q) when is_integer(q) and q >= 1 and q <= 28, do: :ok

  defp validate_q!(q) do
    raise Errors.InvalidOptionError,
      option: :q,
      value: q,
      message: "q must be an integer between 1 and 28, got: #{inspect(q)}"
  end

  defp validate_r!(r) when is_integer(r) and r >= 1 and r <= 32, do: :ok

  defp validate_r!(r) do
    raise Errors.InvalidOptionError,
      option: :r,
      value: r,
      message: "r must be an integer between 1 and 32, got: #{inspect(r)}"
  end

  defp validate_qr_sum!(q, r) when q + r <= 64, do: :ok

  defp validate_qr_sum!(q, r) do
    raise Errors.InvalidOptionError,
      option: :q,
      value: {q, r},
      message: "q + r must be <= 64, got: #{q} + #{r} = #{q + r}"
  end

  defp validate_merge_compat!(opts_a, opts_b) do
    if opts_a[:q] != opts_b[:q] do
      raise Errors.IncompatibleSketchesError,
        reason: "Quotient q mismatch: #{opts_a[:q]} vs #{opts_b[:q]}"
    end

    if opts_a[:r] != opts_b[:r] do
      raise Errors.IncompatibleSketchesError,
        reason: "Quotient r mismatch: #{opts_a[:r]} vs #{opts_b[:r]}"
    end

    seed_a = Keyword.get(opts_a, :seed, @default_seed)
    seed_b = Keyword.get(opts_b, :seed, @default_seed)

    if seed_a != seed_b do
      raise Errors.IncompatibleSketchesError,
        reason: "Quotient seed mismatch: #{seed_a} vs #{seed_b}"
    end
  end

  defp validate_sketch_id(9), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected Quotient sketch ID (9), got #{id}")}
  end

  defp decode_params(
         <<q::unsigned-8, r::unsigned-8, seed::unsigned-little-32, _flags::unsigned-8>>
       )
       when q >= 1 and r >= 1 and q + r <= 64 do
    {:ok,
     [
       q: q,
       r: r,
       slot_count: 1 <<< q,
       seed: seed
     ]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid Quotient params binary")}
  end

  defp validate_state_header(<<"QOT1", 1::unsigned-8, _rest::binary>>), do: :ok

  defp validate_state_header(<<"QOT1", _::binary>>) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported QOT1 version")}
  end

  defp validate_state_header(_state) do
    {:error, Errors.DeserializationError.exception(reason: "invalid QOT1 state header")}
  end
end
