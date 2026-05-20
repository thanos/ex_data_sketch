defmodule ExDataSketch.HLL do
  @moduledoc """
  HyperLogLog (HLL) sketch for cardinality estimation.

  HLL provides approximate distinct-count estimates using sublinear memory.
  The precision parameter `p` controls the trade-off between memory usage and
  accuracy: higher `p` means more memory but better estimates.

  ## Memory and Accuracy

  - Register count: `m = 2^p`
  - Memory: `m` bytes (one byte per register in v1 format)
  - Relative standard error: approximately `1.04 / sqrt(m)`

  | p  | Registers | Memory  | ~Error |
  |----|-----------|---------|--------|
  | 10 | 1,024     | 1 KiB  | 3.25%  |
  | 12 | 4,096     | 4 KiB  | 1.63%  |
  | 14 | 16,384    | 16 KiB | 0.81%  |
  | 16 | 65,536    | 64 KiB | 0.41%  |

  ## Binary State Layout (v1)

  All multi-byte fields are little-endian.

      Offset  Size    Field
      ------  ------  -----
      0       1       Version (u8, currently 1)
      1       1       Precision p (u8, 4..16)
      2       2       Reserved flags (u16 little-endian, must be 0)
      4       m       Registers (m = 2^p bytes, one u8 per register)

  Total: 4 + 2^p bytes.

  ## Options

  - `:p` - precision parameter, integer 4..16 (default: 14)
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`)

  ## Merge Properties

  HLL merge is **associative** and **commutative** (register-wise max).
  This means sketches can be merged in any order or grouping and produce the
  same result, making HLL safe for parallel and distributed aggregation.
  """

  alias ExDataSketch.{Backend, Binary, Codec, Errors, Hash, Telemetry}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_p 14
  @min_p 4
  @max_p 16

  @doc """
  Creates a new HLL sketch.

  ## Options

  - `:p` - precision parameter, integer #{@min_p}..#{@max_p} (default: #{@default_p}).
    Higher values use more memory but give better accuracy.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` - custom hash function `(term -> non_neg_integer)`.
  - `:seed` - hash seed (default: 0).
  - `:update_many_chunk_size` - chunk size for `update_many/2` internal
    batching (default: 10000). Must be set at creation time; cannot be
    overridden on a per-call basis.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> sketch.opts[:p]
      10
      iex> ExDataSketch.HLL.size_bytes(sketch)
      1028

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    p = Keyword.get(opts, :p, @default_p)
    validate_p!(p)
    backend = Backend.resolve(opts)
    hash_fn = Keyword.get(opts, :hash_fn)
    seed = Keyword.get(opts, :seed)

    hash_strategy = Hash.resolve_strategy(opts)

    clean_opts =
      [p: p, hash_strategy: hash_strategy] ++
        if(hash_fn, do: [hash_fn: hash_fn], else: []) ++
        if(seed, do: [seed: seed], else: []) ++
        if(Keyword.has_key?(opts, :update_many_chunk_size),
          do: [update_many_chunk_size: Keyword.fetch!(opts, :update_many_chunk_size)],
          else: []
        )

    state = backend.hll_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single item.

  The item is hashed using `ExDataSketch.Hash.hash64/1` before being
  inserted into the sketch.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("hello")
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, item) do
    hash = hash_item(item, opts)
    new_state = backend.hll_update(state, hash, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Updates the sketch with multiple items in a single pass.

  More efficient than calling `update/2` repeatedly because it minimizes
  intermediate binary allocations.

  The internal batch size is controlled by `:update_many_chunk_size`,
  which must be set at `new/1` time and cannot be changed per call.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(["a", "b", "c"])
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @default_update_many_chunk_size 10_000

  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{opts: opts, backend: backend} = sketch, items)
      when backend == Backend.Pure do
    chunk_size = Keyword.get(opts, :update_many_chunk_size, @default_update_many_chunk_size)

    new_state =
      items
      |> Stream.chunk_every(chunk_size)
      |> Enum.reduce(sketch.state, fn chunk, state_acc ->
        hashes = Enum.map(chunk, &hash_item(&1, opts))
        backend.hll_update_many(state_acc, hashes, opts)
      end)

    %{sketch | state: new_state}
  end

  def update_many(%__MODULE__{opts: opts, backend: backend} = sketch, items) do
    chunk_size = Keyword.get(opts, :update_many_chunk_size, @default_update_many_chunk_size)

    use_raw =
      backend == Backend.Rust and Keyword.get(opts, :hash_fn) == nil and
        Keyword.get(opts, :hash_strategy) != :phash2

    new_state =
      items
      |> Stream.chunk_every(chunk_size)
      |> Enum.reduce(sketch.state, fn chunk, state_acc ->
        if use_raw do
          Backend.Rust.hll_update_many_raw(state_acc, chunk, opts)
        else
          hashes = Enum.map(chunk, &hash_item(&1, opts))
          backend.hll_update_many(state_acc, hashes, opts)
        end
      end)

    %{sketch | state: new_state}
  end

  @doc """
  Merges two HLL sketches.

  Both sketches must have the same precision `p`. The result contains the
  register-wise maximum, which corresponds to the union of the two input
  multisets.

  Returns the merged sketch. Raises `ExDataSketch.Errors.IncompatibleSketchesError`
  if the sketches have different parameters.

  ## Examples

      iex> a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("x")
      iex> b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("y")
      iex> merged = ExDataSketch.HLL.merge(a, b)
      iex> ExDataSketch.HLL.estimate(merged) >= ExDataSketch.HLL.estimate(a)
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:p] != opts_b[:p] do
      raise Errors.IncompatibleSketchesError,
        reason: "HLL precision mismatch: #{opts_a[:p]} vs #{opts_b[:p]}"
    end

    Hash.validate_merge_hash_compat!(opts_a, opts_b, "HLL")

    new_state = backend.hll_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Estimates the number of distinct items in the sketch.

  Returns a floating-point estimate. The accuracy depends on the precision
  parameter `p`.

  ## Examples

      iex> ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.estimate()
      0.0

  """
  @spec estimate(t()) :: float()
  def estimate(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.hll_estimate(state, opts)
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.size_bytes()
      1028

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}) do
    byte_size(state)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  As of `ex_data_sketch` v0.8.0 the produced frame is **EXSK v2**: a
  versioned, CRC32C-checked binary wrapping the sketch's params and
  state together with an `ExDataSketch.Hash.Metadata` block recording
  the exact hashing identity used to produce the sketch. v1 frames
  remain decodable via `deserialize/1`.

  See `ExDataSketch.Binary` for the high-level frame contract and
  `ExDataSketch.Binary.Header` for the byte-level layout.

  Accepts an optional keyword list with the following keys:

  - `:format` - serialization format: `:v2` (default, EXSK v2 with CRC32C)
    or `:v1` (legacy EXSK v1, compatible with v0.7.x readers). The v1
    format is only valid for sketches using `:phash2` hash strategy.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> binary = ExDataSketch.HLL.serialize(sketch)
      iex> <<"EXSK", 2, _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

      iex> sketch = ExDataSketch.HLL.new(p: 10, hash_strategy: :phash2)
      iex> binary = ExDataSketch.HLL.serialize(sketch, format: :v1)
      iex> <<"EXSK", 1, 1, _rest::binary>> = binary

  """
  @spec serialize(t(), keyword()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}, serialize_opts \\ []) do
    format = Keyword.get(serialize_opts, :format, :v2)
    start_time = System.monotonic_time()

    binary =
      case format do
        :v2 ->
          p = Keyword.fetch!(opts, :p)
          hs = hash_strategy_byte(opts)
          params_bin = <<p::unsigned-8, hs::unsigned-8>>
          metadata = Binary.metadata_from_opts(Codec.sketch_id_hll(), 1, opts)
          Binary.encode(metadata, Binary.build_payload(params_bin, state))

        :v1 ->
          unless Keyword.get(opts, :hash_strategy, :phash2) == :phash2 do
            raise ArgumentError,
                  "v1 serialization requires :phash2 hash strategy, " <>
                    "got: #{inspect(Keyword.get(opts, :hash_strategy))}"
          end

          p = Keyword.fetch!(opts, :p)
          params_bin = <<p::unsigned-8>>
          Codec.encode(Codec.sketch_id_hll(), 1, params_bin, state)
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:sketch, :serialize),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: :hll},
        :sketch
      )

    binary
  end

  @doc """
  Deserializes an EXSK binary into an HLL sketch.

  Accepts both EXSK v1 (legacy, pre-v0.8.0) and EXSK v2 frames; the
  version is sniffed from the magic-prefixed header. v2 frames carry a
  CRC32C trailer that is verified before decoding; corruption is
  reported as a structured `DeserializationError`.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.HLL.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    start_time = System.monotonic_time()

    result =
      with {:ok, decoded} <- Binary.decode(binary),
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

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:sketch, :deserialize),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: :hll},
        :sketch
      )

    result
  end

  @doc """
  Serializes the sketch to Apache DataSketches HLL format.

  Not implemented. Apache DataSketches HLL interop is not planned for the
  current release series. Only Theta sketches support DataSketches interop
  via `ExDataSketch.Theta.serialize_datasketches/1`. For HLL serialization,
  use `serialize/1` (ExDataSketch-native EXSK format).

  ## Examples

      iex> try do
      ...>   sketch = %ExDataSketch.HLL{state: <<>>, opts: [p: 14], backend: nil}
      ...>   ExDataSketch.HLL.serialize_datasketches(sketch)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.HLL.serialize_datasketches is not yet implemented"

  """
  @spec serialize_datasketches(t()) :: binary()
  @dialyzer {:nowarn_function, serialize_datasketches: 1}
  def serialize_datasketches(%__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "serialize_datasketches")
  end

  @doc """
  Deserializes an Apache DataSketches HLL binary.

  Not implemented. See `serialize_datasketches/1` for details.

  ## Examples

      iex> try do
      ...>   ExDataSketch.HLL.deserialize_datasketches(<<>>)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.HLL.deserialize_datasketches is not yet implemented"

  """
  @spec deserialize_datasketches(binary()) :: {:ok, t()} | {:error, Exception.t()}
  @dialyzer {:nowarn_function, deserialize_datasketches: 1}
  def deserialize_datasketches(_binary) do
    Errors.not_implemented!(__MODULE__, "deserialize_datasketches")
  end

  @doc """
  Creates a new HLL sketch from an enumerable of items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> sketch = ExDataSketch.HLL.from_enumerable(["a", "b", "c"], p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    Telemetry.span_with_result(
      Telemetry.event_name(:sketch, :ingest),
      %{},
      %{sketch_type: :hll},
      :sketch,
      fn -> new(opts) |> update_many(enumerable) end,
      fn sketch -> %{size_bytes: size_bytes(sketch)} end
    )
  end

  @doc """
  Merges a non-empty enumerable of HLL sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("x")
      iex> b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("y")
      iex> merged = ExDataSketch.HLL.merge_many([a, b])
      iex> ExDataSketch.HLL.estimate(merged) > 0.0
      true

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    sketches_list = Enum.to_list(sketches)

    Telemetry.span(
      Telemetry.event_name(:sketch, :merge),
      %{merge_count: length(sketches_list)},
      %{sketch_type: :hll},
      :sketch,
      fn -> Enum.reduce(sketches_list, fn sketch, acc -> merge(acc, sketch) end) end
    )
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3` and similar.

  The returned function calls `update/2` on each item.

  ## Examples

      iex> is_function(ExDataSketch.HLL.reducer(), 2)
      true

  """
  @spec reducer() :: (term(), t() -> t())
  def reducer do
    fn item, sketch -> update(sketch, item) end
  end

  @doc """
  Returns a 2-arity merge function suitable for combining sketches.

  The returned function calls `merge/2` on two sketches.

  ## Examples

      iex> is_function(ExDataSketch.HLL.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  # -- Private --

  @default_seed 0

  defp hash_item(item, opts) do
    case Keyword.get(opts, :hash_fn) do
      nil ->
        seed = Keyword.get(opts, :seed, @default_seed)
        strategy = Keyword.get(opts, :hash_strategy)
        Hash.hash64(item, seed: seed, hash_strategy: strategy)

      hash_fn ->
        Hash.hash64(item, hash_fn: hash_fn)
    end
  end

  defp validate_p!(p) when is_integer(p) and p >= @min_p and p <= @max_p, do: :ok

  defp validate_p!(p) do
    raise Errors.InvalidOptionError,
      option: :p,
      value: p,
      message: "p must be an integer between #{@min_p} and #{@max_p}, got: #{inspect(p)}"
  end

  defp validate_sketch_id(1), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected HLL sketch ID (1), got #{id}")}
  end

  # Legacy 1-byte format (no hash strategy tag)
  defp decode_params(<<p::unsigned-8>>) when p >= @min_p and p <= @max_p do
    {:ok, [p: p, hash_strategy: :phash2]}
  end

  # New 2-byte format with hash strategy tag
  defp decode_params(<<p::unsigned-8, hs::unsigned-8>>) when p >= @min_p and p <= @max_p do
    case decode_hash_strategy(hs) do
      :custom ->
        {:error,
         Errors.DeserializationError.exception(
           reason:
             "HLL was serialized with a custom :hash_fn which cannot be restored; " <>
               "pass the original hash_fn when re-creating the sketch"
         )}

      strategy ->
        {:ok, [p: p, hash_strategy: strategy]}
    end
  end

  defp decode_params(<<p::unsigned-8>>) do
    {:error,
     Errors.DeserializationError.exception(reason: "invalid HLL precision #{p} in params")}
  end

  defp decode_params(<<p::unsigned-8, _hs::unsigned-8>>) do
    {:error,
     Errors.DeserializationError.exception(reason: "invalid HLL precision #{p} in params")}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid HLL params binary")}
  end

  # Sketch-local hash-strategy wire bytes.
  #
  # NOTE: these bytes are local to the sketch's `params` segment and are
  # INDEPENDENT of `ExDataSketch.Hash.Metadata`'s algorithm wire bytes
  # (which use 2=murmur3, 255=custom). The sketch-local mapping was
  # frozen in v0.7.x with 2=:custom; v0.8.0 adds :murmur3 as a new
  # byte (3) rather than reissuing 2, to preserve backward-compatible
  # deserialization of v0.7.x sketches.
  defp hash_strategy_byte(opts) do
    case Keyword.get(opts, :hash_strategy, :phash2) do
      :phash2 -> 0
      :xxhash3 -> 1
      :custom -> 2
      :murmur3 -> 3
    end
  end

  defp decode_hash_strategy(0), do: :phash2
  defp decode_hash_strategy(1), do: :xxhash3
  defp decode_hash_strategy(2), do: :custom
  defp decode_hash_strategy(3), do: :murmur3
  defp decode_hash_strategy(_), do: :phash2
end
