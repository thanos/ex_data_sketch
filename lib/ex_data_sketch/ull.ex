defmodule ExDataSketch.ULL do
  @moduledoc """
  UltraLogLog (ULL) sketch for cardinality estimation.

  ULL (Ertl, 2023) provides approximately 20% better accuracy than HLL at the
  same memory footprint. It uses the same `2^p` register array but stores a
  different value per register that encodes both the geometric rank and an extra
  sub-bucket bit, then applies the FGRA estimator (sigma/tau convergence from
  Ertl 2017) instead of HLL's harmonic mean.

  ## Memory and Accuracy

  - Register count: `m = 2^p`
  - Memory: `8 + m` bytes (8-byte header + one byte per register)
  - Relative standard error: approximately `0.835 / sqrt(m)` (vs `1.04 / sqrt(m)` for HLL)

  | p  | Registers | Memory  | ~Error (ULL) | ~Error (HLL) |
  |----|-----------|---------|--------------|--------------|
  | 10 | 1,024     | ~1 KiB  | 2.61%        | 3.25%        |
  | 12 | 4,096     | ~4 KiB  | 1.30%        | 1.63%        |
  | 14 | 16,384    | ~16 KiB | 0.65%        | 0.81%        |
  | 16 | 65,536    | ~64 KiB | 0.33%        | 0.41%        |

  ## Binary State Layout (ULL1)

  All multi-byte fields are little-endian.

      Offset  Size    Field
      ------  ------  -----
      0       4       Magic bytes: "ULL1"
      4       1       Version (u8, currently 1)
      5       1       Precision p (u8, 4..26)
      6       2       Reserved flags (u16 little-endian, must be 0)
      8       m       Registers (m = 2^p bytes, one u8 per register)

  Total: 8 + 2^p bytes.

  ## Options

  - `:p` - precision parameter, integer 4..26 (default: 14)
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`)

  ## Merge Properties

  ULL merge is **associative** and **commutative** (register-wise max).
  This means sketches can be merged in any order or grouping and produce the
  same result, making ULL safe for parallel and distributed aggregation.
  """

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

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
    p = Keyword.get(opts, :p, @default_p)
    validate_p!(p)
    backend = Backend.resolve(opts)
    hash_fn = Keyword.get(opts, :hash_fn)
    seed = Keyword.get(opts, :seed)

    hash_strategy =
      if hash_fn, do: :custom, else: Hash.default_hash_strategy()

    clean_opts =
      [p: p, hash_strategy: hash_strategy] ++
        if(hash_fn, do: [hash_fn: hash_fn], else: []) ++
        if(seed, do: [seed: seed], else: [])

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

  ## Examples

      iex> sketch = ExDataSketch.ULL.new(p: 10) |> ExDataSketch.ULL.update_many(["a", "b", "c"])
      iex> ExDataSketch.ULL.estimate(sketch) > 0.0
      true

  """
  @update_many_chunk_size 10_000

  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{opts: opts, backend: backend} = sketch, items)
      when backend == Backend.Pure do
    new_state =
      items
      |> Stream.chunk_every(@update_many_chunk_size)
      |> Enum.reduce(sketch.state, fn chunk, state_acc ->
        hashes = Enum.map(chunk, &hash_item(&1, opts))
        backend.ull_update_many(state_acc, hashes, opts)
      end)

    %{sketch | state: new_state}
  end

  def update_many(%__MODULE__{opts: opts, backend: backend} = sketch, items) do
    use_raw =
      backend == Backend.Rust and Keyword.get(opts, :hash_fn) == nil and
        Keyword.get(opts, :hash_strategy) != :phash2

    new_state =
      items
      |> Stream.chunk_every(@update_many_chunk_size)
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

  ## Examples

      iex> sketch = ExDataSketch.ULL.new(p: 10)
      iex> binary = ExDataSketch.ULL.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    p = Keyword.fetch!(opts, :p)
    hs = hash_strategy_byte(opts)
    params_bin = <<p::unsigned-8, hs::unsigned-8>>
    Codec.encode(Codec.sketch_id_ull(), Codec.version(), params_bin, state)
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
    with {:ok, decoded} <- Codec.decode(binary),
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
    new(opts) |> update_many(enumerable)
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
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
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
    {:error,
     Errors.DeserializationError.exception(reason: "expected ULL sketch ID (15), got #{id}")}
  end

  # Legacy 1-byte format (no hash strategy tag)
  defp decode_params(<<p::unsigned-8>>) when p >= @min_p and p <= @max_p do
    {:ok, [p: p, hash_strategy: :phash2]}
  end

  # New 2-byte format with hash strategy tag
  defp decode_params(<<p::unsigned-8, hs::unsigned-8>>) when p >= @min_p and p <= @max_p do
    {:ok, [p: p, hash_strategy: decode_hash_strategy(hs)]}
  end

  defp decode_params(<<p::unsigned-8>>) do
    {:error,
     Errors.DeserializationError.exception(reason: "invalid ULL precision #{p} in params")}
  end

  defp decode_params(<<p::unsigned-8, _hs::unsigned-8>>) do
    {:error,
     Errors.DeserializationError.exception(reason: "invalid ULL precision #{p} in params")}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid ULL params binary")}
  end

  defp hash_strategy_byte(opts) do
    case Keyword.get(opts, :hash_strategy, :phash2) do
      :phash2 -> 0
      :xxhash3 -> 1
      :custom -> 2
    end
  end

  defp decode_hash_strategy(0), do: :phash2
  defp decode_hash_strategy(1), do: :xxhash3
  defp decode_hash_strategy(2), do: :custom
  defp decode_hash_strategy(_), do: :phash2

  defp validate_state(
         <<"ULL1", version::unsigned-8, state_p::unsigned-8, flags::little-unsigned-16,
           _registers::binary>> = state,
         opts
       ) do
    p = Keyword.fetch!(opts, :p)
    expected_size = 8 + Bitwise.bsl(1, p)

    cond do
      version != 1 ->
        {:error,
         Errors.DeserializationError.exception(
           reason: "unsupported ULL state version #{version}, expected 1"
         )}

      flags != 0 ->
        {:error,
         Errors.DeserializationError.exception(
           reason: "unsupported ULL state flags #{flags}, expected 0"
         )}

      state_p != p ->
        {:error,
         Errors.DeserializationError.exception(
           reason: "ULL state precision #{state_p} does not match params precision #{p}"
         )}

      byte_size(state) != expected_size ->
        {:error,
         Errors.DeserializationError.exception(
           reason:
             "ULL state size #{byte_size(state)} does not match expected #{expected_size} for p=#{p}"
         )}

      true ->
        :ok
    end
  end

  defp validate_state(_other, _opts) do
    {:error,
     Errors.DeserializationError.exception(
       reason: "invalid ULL state header, expected ULL1 magic"
     )}
  end
end
