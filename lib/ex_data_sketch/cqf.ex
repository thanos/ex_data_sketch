defmodule ExDataSketch.CQF do
  @moduledoc """
  Counting Quotient Filter (CQF) for multiset membership with approximate counting.

  A CQF extends the Quotient filter with variable-length counter encoding,
  enabling not just "is this item present?" but "how many times has this item
  been inserted?" It uses the same quotient/remainder hash split as a standard
  Quotient filter, but stores counts inline using a monotonicity-violation
  encoding scheme within runs.

  ## Counter Encoding

  Remainders within a run are stored in strictly increasing order. A slot value
  that violates this monotonicity is interpreted as a counter for the preceding
  remainder:

  - Count = 1: no extra slots (absence of counter means count 1).
  - Count = 2: one extra slot containing the same remainder value (a duplicate).
  - Count >= 3: the remainder value appears twice (bracketing), with intermediate
    slots encoding the count value.

  ## Parameters

  - `:q` -- quotient bits (default: 16). Determines the number of slots: 2^q.
  - `:r` -- remainder bits (default: 8). Determines false positive rate: ~1/2^r.
    Constraint: q + r <= 64.
  - `:seed` -- hash seed (default: 0).

  ## Merge Semantics

  CQF merge is a **multiset union**: counts for identical fingerprints are
  **summed**, not OR'd. This enables distributed counting use cases where
  partial counts from multiple workers are combined.

  ## Binary State Layout (CQF1)

  40-byte header followed by a packed slot array. Each slot contains
  3 metadata bits (is_occupied, is_continuation, is_shifted) and r
  remainder bits, packed LSB-first. The header includes a 64-bit
  total_count field tracking the sum of all item multiplicities.
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
  Creates a new empty Counting Quotient Filter.

  ## Options

  - `:q` -- quotient bits (default: #{@default_q}). Range: 1..28.
  - `:r` -- remainder bits (default: #{@default_r}). Range: 1..32.
    Constraint: q + r <= 64.
  - `:seed` -- hash seed (default: #{@default_seed}).
  - `:backend` -- backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new()
      iex> cqf.opts[:q]
      16

      iex> cqf = ExDataSketch.CQF.new(q: 12, r: 10)
      iex> cqf.opts[:r]
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

    state = backend.cqf_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Inserts a single item into the filter, incrementing its count.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("hello")
      iex> ExDataSketch.CQF.member?(cqf, "hello")
      true

  """
  @spec put(t(), term()) :: t()
  def put(%__MODULE__{state: state, opts: opts, backend: backend} = cqf, item) do
    hash = hash_item(item, opts)
    new_state = backend.cqf_put(state, hash, opts)
    %{cqf | state: new_state}
  end

  @doc """
  Inserts multiple items in a single pass.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put_many(["a", "b"])
      iex> ExDataSketch.CQF.member?(cqf, "a")
      true

  """
  @spec put_many(t(), Enumerable.t()) :: t()
  def put_many(%__MODULE__{state: state, opts: opts, backend: backend} = cqf, items) do
    hashes = Enum.map(items, &hash_item(&1, opts))
    new_state = backend.cqf_put_many(state, hashes, opts)
    %{cqf | state: new_state}
  end

  @doc """
  Tests whether an item may be a member of the multiset.

  Returns `true` if the item is possibly in the set (may be a false positive),
  `false` if the item is definitely not in the set.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("hello")
      iex> ExDataSketch.CQF.member?(cqf, "hello")
      true

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8)
      iex> ExDataSketch.CQF.member?(cqf, "hello")
      false

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.cqf_member?(state, hash, opts)
  end

  @doc """
  Returns the estimated count (multiplicity) of an item.

  Returns 0 if the item is not present. Due to hash collisions, the count
  may be an overestimate but never an underestimate.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8)
      iex> cqf = cqf |> ExDataSketch.CQF.put("x") |> ExDataSketch.CQF.put("x") |> ExDataSketch.CQF.put("x")
      iex> ExDataSketch.CQF.estimate_count(cqf, "x")
      3

  """
  @spec estimate_count(t(), term()) :: non_neg_integer()
  def estimate_count(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.cqf_estimate_count(state, hash, opts)
  end

  @doc """
  Deletes a single occurrence of an item (decrements its count).

  If the item's count reaches 0, it is removed entirely. Deleting a
  non-member is a no-op.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("hello")
      iex> cqf = ExDataSketch.CQF.delete(cqf, "hello")
      iex> ExDataSketch.CQF.member?(cqf, "hello")
      false

  """
  @spec delete(t(), term()) :: t()
  def delete(%__MODULE__{state: state, opts: opts, backend: backend} = cqf, item) do
    hash = hash_item(item, opts)
    new_state = backend.cqf_delete(state, hash, opts)
    %{cqf | state: new_state}
  end

  @doc """
  Merges two CQFs via multiset union (counts are summed).

  Both filters must have identical q, r, and seed.
  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if parameters differ.

  ## Examples

      iex> a = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("x")
      iex> b = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("y")
      iex> merged = ExDataSketch.CQF.merge(a, b)
      iex> ExDataSketch.CQF.member?(merged, "x") and ExDataSketch.CQF.member?(merged, "y")
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = cqf,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    validate_merge_compat!(opts_a, opts_b)
    new_state = backend.cqf_merge(state_a, state_b, opts_a)
    %{cqf | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of CQFs into one.

  ## Examples

      iex> filters = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("item_\#{i}")
      ...> end)
      iex> merged = ExDataSketch.CQF.merge_many(filters)
      iex> ExDataSketch.CQF.member?(merged, "item_1")
      true

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(filters) do
    Enum.reduce(filters, fn cqf, acc -> merge(acc, cqf) end)
  end

  @doc """
  Returns the total count of all items (sum of all multiplicities).

  ## Examples

      iex> ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.cqf_count(state, opts)
  end

  @doc """
  Serializes the filter to the EXSK binary format.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8)
      iex> binary = ExDataSketch.CQF.serialize(cqf)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    q = Keyword.fetch!(opts, :q)
    r = Keyword.fetch!(opts, :r)
    seed = Keyword.get(opts, :seed, @default_seed)

    params_bin = <<q::unsigned-8, r::unsigned-8, seed::unsigned-little-32>>

    Codec.encode(Codec.sketch_id_cqf(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a CQF.

  Returns `{:ok, cqf}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new(q: 10, r: 8) |> ExDataSketch.CQF.put("test")
      iex> {:ok, recovered} = ExDataSketch.CQF.deserialize(ExDataSketch.CQF.serialize(cqf))
      iex> ExDataSketch.CQF.member?(recovered, "test")
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
  Returns `true` if two CQFs have compatible parameters.

  ## Examples

      iex> a = ExDataSketch.CQF.new(q: 10, r: 8)
      iex> b = ExDataSketch.CQF.new(q: 10, r: 8)
      iex> ExDataSketch.CQF.compatible_with?(a, b)
      true

  """
  @spec compatible_with?(t(), t()) :: boolean()
  def compatible_with?(%__MODULE__{opts: opts_a}, %__MODULE__{opts: opts_b}) do
    opts_a[:q] == opts_b[:q] and opts_a[:r] == opts_b[:r] and opts_a[:seed] == opts_b[:seed]
  end

  @doc """
  Returns the set of capabilities supported by CQF.
  """
  def capabilities do
    MapSet.new([
      :new,
      :put,
      :put_many,
      :member?,
      :estimate_count,
      :delete,
      :merge,
      :merge_many,
      :count,
      :serialize,
      :deserialize,
      :compatible_with?
    ])
  end

  @doc """
  Returns the byte size of the state binary.

  ## Examples

      iex> cqf = ExDataSketch.CQF.new()
      iex> ExDataSketch.CQF.size_bytes(cqf) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}), do: byte_size(state)

  @doc """
  Creates a CQF from an enumerable of items.

  Equivalent to `new(opts) |> put_many(enumerable)`.

  ## Examples

      iex> cqf = ExDataSketch.CQF.from_enumerable(["a", "b", "c"])
      iex> ExDataSketch.CQF.member?(cqf, "a")
      true

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> put_many(enumerable)
  end

  @doc """
  Returns a 2-arity reducer function for use with `Enum.reduce/3`.

  ## Examples

      iex> is_function(ExDataSketch.CQF.reducer(), 2)
      true

  """
  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, cqf -> put(cqf, item) end
  end

  @doc """
  Returns a 2-arity merge function for combining filters.

  ## Examples

      iex> is_function(ExDataSketch.CQF.merger(), 2)
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
        reason: "CQF q mismatch: #{opts_a[:q]} vs #{opts_b[:q]}"
    end

    if opts_a[:r] != opts_b[:r] do
      raise Errors.IncompatibleSketchesError,
        reason: "CQF r mismatch: #{opts_a[:r]} vs #{opts_b[:r]}"
    end

    seed_a = Keyword.get(opts_a, :seed, @default_seed)
    seed_b = Keyword.get(opts_b, :seed, @default_seed)

    if seed_a != seed_b do
      raise Errors.IncompatibleSketchesError,
        reason: "CQF seed mismatch: #{seed_a} vs #{seed_b}"
    end
  end

  defp validate_sketch_id(10), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected CQF sketch ID (10), got #{id}")}
  end

  defp decode_params(<<q::unsigned-8, r::unsigned-8, seed::unsigned-little-32>>)
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
    {:error, Errors.DeserializationError.exception(reason: "invalid CQF params binary")}
  end

  defp validate_state_header(<<"CQF1", 1::unsigned-8, _rest::binary>>), do: :ok

  defp validate_state_header(<<"CQF1", _::binary>>) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported CQF1 version")}
  end

  defp validate_state_header(_state) do
    {:error, Errors.DeserializationError.exception(reason: "invalid CQF1 state header")}
  end
end
