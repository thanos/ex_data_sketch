defmodule ExDataSketch.XorFilter do
  @moduledoc """
  Xor filter for static, immutable probabilistic membership testing.

  An Xor filter is a space-efficient probabilistic data structure that answers
  "is this item in the set?" with no false negatives and a tunable false positive
  rate. Unlike Bloom, Cuckoo, or Quotient filters, Xor filters are **immutable**:
  all items must be provided at construction time via `build/2`. After construction,
  only `member?/2` queries are supported -- no insertion, deletion, or merge.

  ## Algorithm

  Construction builds a 3-partite hypergraph from the input set, peels it
  (iteratively removing degree-1 positions), and assigns fingerprints via
  XOR equations. Queries check if `B[h0(x)] XOR B[h1(x)] XOR B[h2(x)]`
  equals the fingerprint of x -- exactly 3 memory accesses, O(1) time.

  ## Variants

  | Variant | Fingerprint | Bits/item | FPR          |
  |---------|-------------|-----------|--------------|
  | Xor8    | 8-bit       | ~9.84     | ~1/256       |
  | Xor16   | 16-bit      | ~19.68    | ~1/65536     |

  ## Build-once Semantics

  This module intentionally does not define `new/1`, `put/2`, `delete/2`, or
  `merge/2`. The struct has no meaningful empty state. Use `build/2` to construct
  a filter from a complete set of items.

  ## Parameters

  - `:fingerprint_bits` -- 8 (default, Xor8) or 16 (Xor16).
  - `:seed` -- hash seed (default: 0).
  - `:backend` -- backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(["a", "b", "c"])
      iex> ExDataSketch.XorFilter.member?(filter, "a")
      true

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(1..100)
      iex> ExDataSketch.XorFilter.count(filter)
      100

  """

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_fingerprint_bits 8
  @default_seed 0

  @doc """
  Builds an Xor filter from an enumerable of items.

  All items are hashed, deduplicated, and used to construct the filter.
  Returns `{:ok, filter}` on success or `{:error, :build_failed}` if
  the hypergraph cannot be peeled after 100 seed retries.

  ## Options

  - `:fingerprint_bits` -- 8 (default) or 16.
  - `:seed` -- hash seed (default: #{@default_seed}).
  - `:backend` -- backend module.
  - `:hash_fn` -- custom hash function `(term -> non_neg_integer)`.

  ## Examples

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(["x", "y", "z"])
      iex> ExDataSketch.XorFilter.member?(filter, "x")
      true

  """
  @spec build(Enumerable.t(), keyword()) :: {:ok, t()} | {:error, :build_failed}
  def build(items, opts \\ []) do
    fp_bits = Keyword.get(opts, :fingerprint_bits, @default_fingerprint_bits)
    seed = Keyword.get(opts, :seed, @default_seed)
    hash_fn = Keyword.get(opts, :hash_fn)

    validate_fingerprint_bits!(fp_bits)

    backend = Backend.resolve(opts)

    clean_opts =
      [
        fingerprint_bits: fp_bits,
        seed: seed
      ] ++ if(hash_fn, do: [hash_fn: hash_fn], else: [])

    hashes = Enum.map(items, &hash_item(&1, clean_opts))

    case backend.xor_build(hashes, clean_opts) do
      {:ok, state} ->
        {:ok, %__MODULE__{state: state, opts: clean_opts, backend: backend}}

      {:error, :build_failed} ->
        {:error, :build_failed}
    end
  end

  @doc """
  Tests whether an item may be a member of the set.

  Returns `true` if the item is possibly in the set (may be a false positive),
  `false` if the item is definitely not in the set. Never returns false negatives
  for items that were included in `build/2`.

  ## Examples

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(["hello"])
      iex> ExDataSketch.XorFilter.member?(filter, "hello")
      true

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.xor_member?(state, hash, opts)
  end

  @doc """
  Returns the number of items the filter was built from.

  ## Examples

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(["a", "b"])
      iex> ExDataSketch.XorFilter.count(filter)
      2

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.xor_count(state, opts)
  end

  @doc """
  Serializes the filter to the EXSK binary format.

  ## Examples

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(["a"])
      iex> binary = ExDataSketch.XorFilter.serialize(filter)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    fp_bits = Keyword.fetch!(opts, :fingerprint_bits)
    seed = Keyword.get(opts, :seed, @default_seed)
    variant = if fp_bits == 16, do: 1, else: 0

    params_bin = <<fp_bits::unsigned-8, variant::unsigned-8, seed::unsigned-little-32>>

    Codec.encode(Codec.sketch_id_xor(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into an XorFilter.

  Returns `{:ok, filter}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> {:ok, filter} = ExDataSketch.XorFilter.build(["test"])
      iex> {:ok, recovered} = ExDataSketch.XorFilter.deserialize(ExDataSketch.XorFilter.serialize(filter))
      iex> ExDataSketch.XorFilter.member?(recovered, "test")
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
  Returns `true` if two Xor filters have compatible parameters.

  Compatible filters have the same fingerprint_bits and seed.

  ## Examples

      iex> {:ok, a} = ExDataSketch.XorFilter.build(["a"], fingerprint_bits: 8)
      iex> {:ok, b} = ExDataSketch.XorFilter.build(["b"], fingerprint_bits: 8)
      iex> ExDataSketch.XorFilter.compatible_with?(a, b)
      true

  """
  @spec compatible_with?(t(), t()) :: boolean()
  def compatible_with?(%__MODULE__{opts: opts_a}, %__MODULE__{opts: opts_b}) do
    opts_a[:fingerprint_bits] == opts_b[:fingerprint_bits] and
      opts_a[:seed] == opts_b[:seed]
  end

  @doc """
  Returns the set of capabilities supported by XorFilter.
  """
  def capabilities do
    MapSet.new([
      :build,
      :member?,
      :count,
      :serialize,
      :deserialize,
      :compatible_with?
    ])
  end

  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}), do: byte_size(state)

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

  defp validate_fingerprint_bits!(bits) when bits in [8, 16], do: :ok

  defp validate_fingerprint_bits!(bits) do
    raise Errors.InvalidOptionError,
      option: :fingerprint_bits,
      value: bits,
      message: "fingerprint_bits must be 8 or 16, got: #{inspect(bits)}"
  end

  defp validate_sketch_id(11), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected XorFilter sketch ID (11), got #{id}")}
  end

  defp decode_params(<<fp_bits::unsigned-8, _variant::unsigned-8, seed::unsigned-little-32>>)
       when fp_bits in [8, 16] do
    {:ok,
     [
       fingerprint_bits: fp_bits,
       seed: seed
     ]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid XorFilter params binary")}
  end

  defp validate_state_header(<<"XOR1", 1::unsigned-8, _rest::binary>>), do: :ok

  defp validate_state_header(<<"XOR1", _::binary>>) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported XOR1 version")}
  end

  defp validate_state_header(_state) do
    {:error, Errors.DeserializationError.exception(reason: "invalid XOR1 state header")}
  end
end
