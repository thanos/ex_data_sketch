defmodule ExDataSketch.IBLT do
  @moduledoc """
  Invertible Bloom Lookup Table (IBLT) for set reconciliation.

  An IBLT is a probabilistic data structure that extends Bloom filters with the
  ability to **list its entries** and **subtract** two IBLTs to find set
  differences. Unlike standard Bloom filters which only answer membership queries,
  IBLT supports set reconciliation where two parties each build an IBLT of their
  set, exchange and subtract -- the result contains only items that differ,
  recoverable via `list_entries/1`.

  ## Modes

  - **Set mode**: `put/2` / `delete/2` for items. Value hash is 0.
  - **Key-value mode**: `put/3` / `delete/3` for key-value pairs. Both key and
    value are hashed to 64-bit integers.

  ## Parameters

  - `:cell_count` -- number of cells (default: 1000). More cells reduce decode
    failure probability but increase memory.
  - `:hash_count` -- number of hash functions (default: 3). Typically 3-5.
  - `:seed` -- hash seed (default: 0).

  ## Set Reconciliation

  Two parties A and B each build an IBLT of their set. Party A sends its IBLT
  to B. B computes `subtract(iblt_b, iblt_a)` and calls `list_entries/1` on the
  result. The positive entries are items in B but not A; negative entries are
  items in A but not B. Communication cost scales with the **difference size**,
  not the full set size.

  ## Binary State Layout (IBL1)

  24-byte header followed by a cell array. Each cell is 24 bytes containing:
  count (i32), key_sum (u64), value_sum (u64), check_sum (u32).
  """

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_cell_count 1000
  @default_hash_count 3
  @default_seed 0

  @doc """
  Creates a new empty IBLT.

  ## Options

  - `:cell_count` -- number of cells (default: #{@default_cell_count}). Range: 1..16_777_216.
  - `:hash_count` -- number of hash functions (default: #{@default_hash_count}). Range: 1..10.
  - `:seed` -- hash seed (default: #{@default_seed}).
  - `:backend` -- backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new()
      iex> iblt.opts[:cell_count]
      1000

      iex> iblt = ExDataSketch.IBLT.new(cell_count: 500, hash_count: 4)
      iex> iblt.opts[:hash_count]
      4

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    cell_count = Keyword.get(opts, :cell_count, @default_cell_count)
    hash_count = Keyword.get(opts, :hash_count, @default_hash_count)
    seed = Keyword.get(opts, :seed, @default_seed)
    hash_fn = Keyword.get(opts, :hash_fn)

    validate_cell_count!(cell_count)
    validate_hash_count!(hash_count)

    backend = Backend.resolve(opts)

    clean_opts =
      [
        cell_count: cell_count,
        hash_count: hash_count,
        seed: seed
      ] ++ if(hash_fn, do: [hash_fn: hash_fn], else: [])

    state = backend.iblt_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Inserts an item in set mode (value_hash = 0).

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("hello")
      iex> ExDataSketch.IBLT.member?(iblt, "hello")
      true

  """
  @spec put(t(), term()) :: t()
  def put(%__MODULE__{state: state, opts: opts, backend: backend} = iblt, item) do
    key_hash = hash_item(item, opts)
    new_state = backend.iblt_put(state, key_hash, 0, opts)
    %{iblt | state: new_state}
  end

  @doc """
  Inserts a key-value pair in KV mode.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("key", "value")
      iex> ExDataSketch.IBLT.member?(iblt, "key")
      true

  """
  @spec put(t(), term(), term()) :: t()
  def put(%__MODULE__{state: state, opts: opts, backend: backend} = iblt, key, value) do
    key_hash = hash_item(key, opts)
    value_hash = hash_item(value, opts)
    new_state = backend.iblt_put(state, key_hash, value_hash, opts)
    %{iblt | state: new_state}
  end

  @doc """
  Inserts multiple items in set mode in a single pass.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put_many(["a", "b", "c"])
      iex> ExDataSketch.IBLT.member?(iblt, "b")
      true

  """
  @spec put_many(t(), Enumerable.t()) :: t()
  def put_many(%__MODULE__{state: state, opts: opts, backend: backend} = iblt, items) do
    pairs = Enum.map(items, fn item -> {hash_item(item, opts), 0} end)
    new_state = backend.iblt_put_many(state, pairs, opts)
    %{iblt | state: new_state}
  end

  @doc """
  Tests whether an item may be a member.

  Returns `true` if all k cells for the item have non-zero count (probabilistic,
  may return false positives). Returns `false` if the item is definitely not present.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("hello")
      iex> ExDataSketch.IBLT.member?(iblt, "hello")
      true

      iex> iblt = ExDataSketch.IBLT.new()
      iex> ExDataSketch.IBLT.member?(iblt, "hello")
      false

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    key_hash = hash_item(item, opts)
    backend.iblt_member?(state, key_hash, opts)
  end

  @doc """
  Deletes an item in set mode (value_hash = 0).

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("hello")
      iex> iblt = ExDataSketch.IBLT.delete(iblt, "hello")
      iex> ExDataSketch.IBLT.member?(iblt, "hello")
      false

  """
  @spec delete(t(), term()) :: t()
  def delete(%__MODULE__{state: state, opts: opts, backend: backend} = iblt, item) do
    key_hash = hash_item(item, opts)
    new_state = backend.iblt_delete(state, key_hash, 0, opts)
    %{iblt | state: new_state}
  end

  @doc """
  Deletes a key-value pair in KV mode.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("key", "value")
      iex> iblt = ExDataSketch.IBLT.delete(iblt, "key", "value")
      iex> ExDataSketch.IBLT.member?(iblt, "key")
      false

  """
  @spec delete(t(), term(), term()) :: t()
  def delete(%__MODULE__{state: state, opts: opts, backend: backend} = iblt, key, value) do
    key_hash = hash_item(key, opts)
    value_hash = hash_item(value, opts)
    new_state = backend.iblt_delete(state, key_hash, value_hash, opts)
    %{iblt | state: new_state}
  end

  @doc """
  Subtracts IBLT `b` from IBLT `a` cell-wise.

  The result contains the symmetric difference of the two sets. Use
  `list_entries/1` on the result to recover the differing items.

  Both IBLTs must have compatible parameters (same cell_count, hash_count, seed).

  ## Examples

      iex> a = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("x")
      iex> b = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("y")
      iex> diff = ExDataSketch.IBLT.subtract(a, b)
      iex> {:ok, entries} = ExDataSketch.IBLT.list_entries(diff)
      iex> length(entries.positive) + length(entries.negative) > 0
      true

  """
  @spec subtract(t(), t()) :: t()
  def subtract(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = iblt,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    validate_compat!(opts_a, opts_b)
    new_state = backend.iblt_subtract(state_a, state_b, opts_a)
    %{iblt | state: new_state}
  end

  @doc """
  Lists entries by peeling the IBLT.

  Returns `{:ok, %{positive: entries, negative: entries}}` on success where
  positive entries have count +1 and negative entries have count -1.
  Returns `{:error, :decode_failed}` if the IBLT cannot be fully decoded.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new(cell_count: 100) |> ExDataSketch.IBLT.put("hello")
      iex> {:ok, entries} = ExDataSketch.IBLT.list_entries(iblt)
      iex> length(entries.positive)
      1

  """
  @spec list_entries(t()) ::
          {:ok,
           %{
             positive: [{non_neg_integer(), non_neg_integer()}],
             negative: [{non_neg_integer(), non_neg_integer()}]
           }}
          | {:error, :decode_failed}
  def list_entries(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.iblt_list_entries(state, opts)
  end

  @doc """
  Returns the number of items inserted.

  ## Examples

      iex> ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.count()
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.iblt_count(state, opts)
  end

  @doc """
  Merges two IBLTs cell-wise (set union).

  Both IBLTs must have compatible parameters.

  ## Examples

      iex> a = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("x")
      iex> b = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("y")
      iex> merged = ExDataSketch.IBLT.merge(a, b)
      iex> ExDataSketch.IBLT.member?(merged, "x") and ExDataSketch.IBLT.member?(merged, "y")
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = iblt,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    validate_compat!(opts_a, opts_b)
    new_state = backend.iblt_merge(state_a, state_b, opts_a)
    %{iblt | state: new_state}
  end

  @doc """
  Merges a non-empty enumerable of IBLTs into one.

  ## Examples

      iex> iblts = Enum.map(1..3, fn i ->
      ...>   ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("item_\#{i}")
      ...> end)
      iex> merged = ExDataSketch.IBLT.merge_many(iblts)
      iex> ExDataSketch.IBLT.member?(merged, "item_1")
      true

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(iblts) do
    Enum.reduce(iblts, fn iblt, acc -> merge(acc, iblt) end)
  end

  @doc """
  Serializes the IBLT to the EXSK binary format.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new()
      iex> binary = ExDataSketch.IBLT.serialize(iblt)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    hash_count = Keyword.fetch!(opts, :hash_count)
    seed = Keyword.get(opts, :seed, @default_seed)
    cell_count = Keyword.fetch!(opts, :cell_count)

    params_bin =
      <<hash_count::unsigned-8, 0::unsigned-8, seed::unsigned-little-32,
        cell_count::unsigned-little-32>>

    Codec.encode(Codec.sketch_id_iblt(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into an IBLT.

  Returns `{:ok, iblt}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> iblt = ExDataSketch.IBLT.new() |> ExDataSketch.IBLT.put("test")
      iex> {:ok, recovered} = ExDataSketch.IBLT.deserialize(ExDataSketch.IBLT.serialize(iblt))
      iex> ExDataSketch.IBLT.member?(recovered, "test")
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
  Returns `true` if two IBLTs have compatible parameters.

  ## Examples

      iex> a = ExDataSketch.IBLT.new()
      iex> b = ExDataSketch.IBLT.new()
      iex> ExDataSketch.IBLT.compatible_with?(a, b)
      true

  """
  @spec compatible_with?(t(), t()) :: boolean()
  def compatible_with?(%__MODULE__{opts: opts_a}, %__MODULE__{opts: opts_b}) do
    opts_a[:cell_count] == opts_b[:cell_count] and
      opts_a[:hash_count] == opts_b[:hash_count] and
      opts_a[:seed] == opts_b[:seed]
  end

  @doc """
  Returns the set of capabilities supported by IBLT.
  """
  def capabilities do
    MapSet.new([
      :new,
      :put,
      :put_many,
      :member?,
      :delete,
      :subtract,
      :list_entries,
      :count,
      :merge,
      :merge_many,
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
    fn item, iblt -> put(iblt, item) end
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

  defp validate_cell_count!(c) when is_integer(c) and c >= 1 and c <= 16_777_216, do: :ok

  defp validate_cell_count!(c) do
    raise Errors.InvalidOptionError,
      option: :cell_count,
      value: c,
      message: "cell_count must be an integer between 1 and 16_777_216, got: #{inspect(c)}"
  end

  defp validate_hash_count!(h) when is_integer(h) and h >= 1 and h <= 10, do: :ok

  defp validate_hash_count!(h) do
    raise Errors.InvalidOptionError,
      option: :hash_count,
      value: h,
      message: "hash_count must be an integer between 1 and 10, got: #{inspect(h)}"
  end

  defp validate_compat!(opts_a, opts_b) do
    if opts_a[:cell_count] != opts_b[:cell_count] do
      raise Errors.IncompatibleSketchesError,
        reason: "IBLT cell_count mismatch: #{opts_a[:cell_count]} vs #{opts_b[:cell_count]}"
    end

    if opts_a[:hash_count] != opts_b[:hash_count] do
      raise Errors.IncompatibleSketchesError,
        reason: "IBLT hash_count mismatch: #{opts_a[:hash_count]} vs #{opts_b[:hash_count]}"
    end

    seed_a = Keyword.get(opts_a, :seed, @default_seed)
    seed_b = Keyword.get(opts_b, :seed, @default_seed)

    if seed_a != seed_b do
      raise Errors.IncompatibleSketchesError,
        reason: "IBLT seed mismatch: #{seed_a} vs #{seed_b}"
    end
  end

  defp validate_sketch_id(12), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected IBLT sketch ID (12), got #{id}")}
  end

  defp decode_params(
         <<hash_count::unsigned-8, _reserved::unsigned-8, seed::unsigned-little-32,
           cell_count::unsigned-little-32>>
       )
       when hash_count >= 1 and cell_count >= 1 do
    {:ok,
     [
       cell_count: cell_count,
       hash_count: hash_count,
       seed: seed
     ]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid IBLT params binary")}
  end

  defp validate_state_header(<<"IBL1", 1::unsigned-8, _rest::binary>>), do: :ok

  defp validate_state_header(<<"IBL1", _::binary>>) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported IBL1 version")}
  end

  defp validate_state_header(_state) do
    {:error, Errors.DeserializationError.exception(reason: "invalid IBL1 state header")}
  end
end
