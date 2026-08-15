defmodule ExDataSketch.ULL do
  @moduledoc """
  UltraLogLog (ULL) sketch for cardinality estimation.

  ULL (Ertl, 2023) provides approximately 20% better accuracy than HLL at the
  same memory footprint. It uses the same `2^p` register array as HLL, but
  each register byte stores a compressed 3-bit window of a per-bucket
  accumulator: the position of the highest bit ever recorded for that bucket
  (the geometric rank, via a `pack`/`unpack` encoding) plus the two bits just
  below it as a sub-bucket refinement. Estimation uses the OptimalFGRAEstimator
  from Ertl 2023: closed-form small-range and large-range correction terms
  plus a per-register contribution lookup table for the bulk of the range.

  ## Memory and Accuracy

  - Register count: `m = 2^p`
  - Memory: `8 + m` bytes (8-byte header + one byte per register)
  - Relative standard error: approximately `0.70 / sqrt(m)` (vs `1.04 / sqrt(m)` for HLL),
    measured empirically over repeated trials against this implementation

  | p  | Registers | Memory  | ~Error (ULL) | ~Error (HLL) |
  |----|-----------|---------|--------------|--------------|
  | 10 | 1,024     | ~1 KiB  | 2.17%        | 3.25%        |
  | 12 | 4,096     | ~4 KiB  | 1.09%        | 1.63%        |
  | 14 | 16,384    | ~16 KiB | 0.55%        | 0.81%        |
  | 16 | 65,536    | ~64 KiB | 0.27%        | 0.41%        |

  ## Estimation Strategy

  Every register byte is classified into one of two regimes:

  1. **Small/large-range registers** (values near the encoding's boundaries):
     pooled into closed-form quadratic-root correction terms
     (`smallRangeEstimate`/`largeRangeEstimate` from Ertl 2023), analogous to
     HyperLogLog's linear-counting correction but generalized to this
     encoding's extra sub-bucket bits.
  2. **Normal-range registers**: each contributes a precomputed value from a
     236-entry lookup table indexed by its distance from a precision-dependent
     offset.

  The contributions are summed and combined via
  `estimation_factor[p] * sum^(-1/tau)` (`tau ≈ 0.819`), a single smooth
  formula that scales continuously from small to large cardinalities --
  unlike HLL/the pre-v0.10.2 ULL implementation, there is no separate
  linear-counting branch or explicit large-range correction.

  ## Recommended Precision

  - `p >= 10` is recommended for production use; the measured RSE bound
    (`~0.70/sqrt(m)`) is tight across the full cardinality range at this
    precision and above.

  ## Precision Range (4..26)

  Unlike `ExDataSketch.HLL` (whose `p <= 26` is a practical ceiling with no
  algorithmic basis -- see its moduledoc), ULL's `p <= 26` is a **hard
  limit**: the `estimation_factor[p]` lookup table
  (`ESTIMATION_FACTORS` in the reference implementation) has exactly 24
  entries, indexed by `p - 3`, giving a valid range of `p` in `3..26`. This
  library additionally requires `p >= 4` (one higher than the table's own
  floor) purely for consistency with HLL's own floor, not because `p = 3`
  is unsafe for ULL. Raising the ceiling past 26 would require Ertl 2023's
  authors (or a from-scratch derivation) to publish additional table
  entries -- it cannot be done by simply changing a constant, unlike HLL.

  ## Binary State Layout (ULL1)

  All multi-byte fields are little-endian.

      Offset  Size    Field
      ------  ------  -----
      0       4       Magic bytes: "ULL1"
      4       1       Version (u8, currently 2)
      5       1       Precision p (u8, 4..26)
      6       2       Reserved flags (u16 little-endian, must be 0)
      8       m       Registers (m = 2^p bytes, one u8 per register)

  Total: 8 + 2^p bytes.

  Version 2 (v0.10.2+) replaced the register encoding and estimator used in
  version 1, which was an HLL-derived approximation rather than the real
  UltraLogLog algorithm and produced significantly overestimated cardinality
  once every register had been touched at least once. Version-1 binaries are
  rejected on decode with a clear error rather than silently
  misinterpreted -- see `deserialize/1`.

  ## Options

  - `:p` - precision parameter, integer 4..26 (default: 14)
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`)
  - `:update_many_chunk_size` - chunk size for `update_many/2` internal
    batching (default: 10000). Must be set at creation time; cannot be
    overridden on a per-call basis.

  ## Merge Properties

  ULL merge is **associative** and **commutative** (register-wise max).
  This means sketches can be merged in any order or grouping and produce the
  same result, making ULL safe for parallel and distributed aggregation.
  """

  alias ExDataSketch.{Backend, Binary, Codec, Config, Errors, Hash, Telemetry}
  alias ExDataSketch.Errors.DeserializationError

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @behaviour ExDataSketch.Sketch

  @default_p 14
  @min_p 4
  @max_p 26

  @doc """
  Creates a new ULL sketch.

  ## Options

  - `:p` - precision parameter, integer #{@min_p}..#{@max_p} (default: #{@default_p}).
    Higher values use more memory but give better accuracy.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` - custom hash function `(term -> non_neg_integer)`.
  - `:seed` - hash seed (default: 0).

  ## Examples

      iex> sketch = ExDataSketch.ULL.new(p: 10)
      iex> sketch.opts[:p]
      10
      iex> ExDataSketch.ULL.size_bytes(sketch)
      1032

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    opts = Config.merge_defaults(:ull, opts)
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

    state = backend.ull_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single item.

  The item is hashed using `ExDataSketch.Hash.hash64/1` before being
  inserted into the sketch.

  ## Examples

      iex> sketch = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update("hello")
      iex> ExDataSketch.ULL.estimate(sketch) > 0.0
      true

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, item) do
    hash = hash_item(item, opts)
    new_state = backend.ull_update(state, hash, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Updates the sketch with multiple items in a single pass.

  More efficient than calling `update/2` repeatedly because it minimizes
  intermediate binary allocations.

  The internal batch size is controlled by `:update_many_chunk_size`,
  which must be set at `new/1` time and cannot be changed per call.

  ## Examples

      iex> sketch = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update_many(["a", "b", "c"])
      iex> ExDataSketch.ULL.estimate(sketch) > 0.0
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
        backend.ull_update_many(state_acc, hashes, opts)
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
          Backend.Rust.ull_update_many_raw(state_acc, chunk, opts)
        else
          hashes = Enum.map(chunk, &hash_item(&1, opts))
          backend.ull_update_many(state_acc, hashes, opts)
        end
      end)

    %{sketch | state: new_state}
  end

  @doc """
  Merges two ULL sketches.

  Both sketches must have the same precision `p`. The result contains the
  register-wise maximum, which corresponds to the union of the two input
  multisets.

  Returns the merged sketch. Raises `ExDataSketch.Errors.IncompatibleSketchesError`
  if the sketches have different parameters.

  ## Examples

      iex> a = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update("x")
      iex> b = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update("y")
      iex> merged = ExDataSketch.ULL.merge(a, b)
      iex> ExDataSketch.ULL.estimate(merged) >= ExDataSketch.ULL.estimate(a)
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:p] != opts_b[:p] do
      raise Errors.IncompatibleSketchesError,
        reason: "ULL precision mismatch: #{opts_a[:p]} vs #{opts_b[:p]}"
    end

    Hash.validate_merge_hash_compat!(opts_a, opts_b, "ULL")

    new_state = backend.ull_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Estimates the number of distinct items in the sketch.

  Returns a floating-point estimate. The accuracy depends on the precision
  parameter `p`. ULL typically achieves ~20% lower relative error than HLL
  at the same precision.

  ## Examples

      iex> ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.estimate()
      0.0

  """
  @spec estimate(t()) :: float()
  def estimate(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.ull_estimate(state, opts)
  end

  @doc """
  Alias for `estimate/1`.

  ## Examples

      iex> ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.count()
      0.0

  """
  @spec count(t()) :: float()
  def count(%__MODULE__{} = sketch), do: estimate(sketch)

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.size_bytes()
      1032

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}) do
    byte_size(state)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  The serialized binary includes magic bytes, version, sketch type,
  parameters, and state. See `ExDataSketch.Codec` for format details.

  ## Options

  - `:format` - serialization format: `:v2` (default, EXSK v2 with CRC32C)
    or `:v1` (legacy EXSK v1, compatible with v0.7.x readers). The v1
    format is only valid for sketches using `:phash2` hash strategy.

  ## Examples

      iex> sketch = ExDataSketch.ULL.new(p: 10)
      iex> binary = ExDataSketch.ULL.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

      iex> sketch = ExDataSketch.ULL.new(p: 10, hash_strategy: :phash2)
      iex> binary = ExDataSketch.ULL.serialize(sketch, format: :v1)
      iex> <<"EXSK", 1, 15, _rest::binary>> = binary

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

          Binary.encode(
            Binary.metadata_from_opts(Codec.sketch_id_ull(), 1, opts),
            Binary.build_payload(params_bin, state)
          )

        :v1 ->
          unless Keyword.get(opts, :hash_strategy, :phash2) == :phash2 do
            raise ArgumentError,
                  "v1 serialization requires :phash2 hash strategy, " <>
                    "got: #{inspect(Keyword.get(opts, :hash_strategy))}"
          end

          p = Keyword.fetch!(opts, :p)
          params_bin = <<p::unsigned-8>>
          Codec.encode(Codec.sketch_id_ull(), 1, params_bin, state)
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:sketch, :serialize),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: :ull},
        :sketch
      )

    binary
  end

  @doc """
  Deserializes an EXSK binary into a ULL sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.ULL.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    start_time = System.monotonic_time()

    result =
      with {:ok, decoded} <- Binary.decode(binary),
           :ok <- validate_sketch_id(decoded.sketch_id),
           {:ok, opts} <- decode_params(decoded.params),
           :ok <- validate_state(decoded.state, opts) do
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
        %{sketch_type: :ull},
        :sketch
      )

    result
  end

  @doc """
  Creates a new ULL sketch from an enumerable of items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> sketch = ExDataSketch.ULL.from_enumerable(["a", "b", "c"], p: 10)
      iex> ExDataSketch.ULL.estimate(sketch) > 0.0
      true

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    Telemetry.span_with_result(
      Telemetry.event_name(:sketch, :ingest),
      %{},
      %{sketch_type: :ull},
      :sketch,
      fn -> new(opts) |> update_many(enumerable) end,
      fn sketch -> %{size_bytes: size_bytes(sketch)} end
    )
  end

  @doc """
  Merges a non-empty enumerable of ULL sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> a = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update("x")
      iex> b = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update("y")
      iex> merged = ExDataSketch.ULL.merge_many([a, b])
      iex> ExDataSketch.ULL.estimate(merged) > 0.0
      true

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    sketches_list = Enum.to_list(sketches)

    Telemetry.span(
      Telemetry.event_name(:sketch, :merge),
      %{merge_count: length(sketches_list)},
      %{sketch_type: :ull},
      :sketch,
      fn -> Enum.reduce(sketches_list, fn sketch, acc -> merge(acc, sketch) end) end
    )
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3` and similar.

  The returned function calls `update/2` on each item.

  ## Examples

      iex> is_function(ExDataSketch.ULL.reducer(), 2)
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

      iex> is_function(ExDataSketch.ULL.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  @doc """
  Returns the set of operation names supported by `ExDataSketch.ULL`.

  See `ExDataSketch.Sketch` for the shared capability vocabulary.

  ## Examples

      iex> ExDataSketch.ULL.capabilities() |> MapSet.member?(:estimate)
      true

      iex> ExDataSketch.ULL.capabilities() |> MapSet.member?(:no_such_operation)
      false

  """
  @spec capabilities() :: ExDataSketch.Sketch.capabilities()
  @dialyzer {:no_opaque, capabilities: 0}
  def capabilities do
    MapSet.new([
      :new,
      :update,
      :update_many,
      :merge,
      :merge_many,
      :estimate,
      :count,
      :serialize,
      :deserialize
    ])
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

  defp validate_sketch_id(15), do: :ok

  defp validate_sketch_id(id) do
    {:error, DeserializationError.exception(reason: "expected ULL sketch ID (15), got #{id}")}
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
         DeserializationError.exception(
           reason:
             "ULL was serialized with a custom :hash_fn which cannot be restored; " <>
               "pass the original hash_fn when re-creating the sketch"
         )}

      strategy ->
        {:ok, [p: p, hash_strategy: strategy]}
    end
  end

  defp decode_params(<<p::unsigned-8>>) do
    {:error, DeserializationError.exception(reason: "invalid ULL precision #{p} in params")}
  end

  defp decode_params(<<p::unsigned-8, _hs::unsigned-8>>) do
    {:error, DeserializationError.exception(reason: "invalid ULL precision #{p} in params")}
  end

  defp decode_params(_other) do
    {:error, DeserializationError.exception(reason: "invalid ULL params binary")}
  end

  # Sketch-local hash-strategy wire bytes. See HLL.hash_strategy_byte/1
  # for the rationale; the byte set is identical across HLL/ULL/Theta/CMS
  # and intentionally distinct from `ExDataSketch.Hash.Metadata`'s bytes.
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

  defp validate_state(
         <<"ULL1", version::unsigned-8, state_p::unsigned-8, flags::little-unsigned-16,
           _registers::binary>> = state,
         opts
       ) do
    p = Keyword.fetch!(opts, :p)
    expected_size = 8 + Bitwise.bsl(1, p)

    cond do
      version != 2 ->
        {:error,
         DeserializationError.exception(
           reason: "unsupported ULL state version #{version}, expected 2"
         )}

      flags != 0 ->
        {:error,
         DeserializationError.exception(
           reason: "unsupported ULL state flags #{flags}, expected 0"
         )}

      state_p != p ->
        {:error,
         DeserializationError.exception(
           reason: "ULL state precision #{state_p} does not match params precision #{p}"
         )}

      byte_size(state) != expected_size ->
        {:error,
         DeserializationError.exception(
           reason:
             "ULL state size #{byte_size(state)} does not match expected #{expected_size} for p=#{p}"
         )}

      true ->
        :ok
    end
  end

  defp validate_state(_other, _opts) do
    {:error,
     DeserializationError.exception(reason: "invalid ULL state header, expected ULL1 magic")}
  end
end
