defmodule ExDataSketch.Theta do
  @moduledoc """
  Theta Sketch for set operations on cardinalities.

  Theta sketches support cardinality estimation with set operations (union,
  intersection, difference) that other sketch families do not natively support.
  This makes them ideal for queries like "how many users visited both page A
  and page B?"

  ## How It Works

  A Theta sketch maintains a set of hash values below a threshold (theta).
  When the set exceeds the nominal size `k`, the threshold is lowered and
  entries above it are discarded. The cardinality estimate is derived from
  the number of retained entries and the current theta value.

  ## Options

  - `:k` - nominal number of entries (default: 4096). Controls accuracy.
    Higher values use more memory but give better estimates.
    Must be a power of 2, between 16 and 67,108,864 (2^26).
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Binary State Layout (v1)

  All multi-byte fields are little-endian.

      Offset  Size    Field
      ------  ------  -----
      0       1       Version (u8, currently 1)
      1       4       k nominal entries (u32 little-endian)
      5       8       Theta value (u64 little-endian, max = 2^64-1 = "no threshold")
      13      4       Entry count (u32 little-endian)
      17      N*8     Entries (sorted array of u64 little-endian hash values)

  Total: 17 + entry_count * 8 bytes.

  ## DataSketches Interop

  Theta is the primary target for Apache DataSketches interop.
  `serialize_datasketches/1` and `deserialize_datasketches/1` implement
  the CompactSketch binary format, enabling cross-language compatibility
  with Java, C++, and Python DataSketches libraries.

  ## Merge Properties

  Theta merge (union) is **associative** and **commutative**.
  This means sketches can be merged in any order or grouping and produce the
  same result, making Theta safe for parallel and distributed aggregation.
  """

  import Bitwise, only: [<<<: 2, &&&: 2]

  alias ExDataSketch.{Backend, Codec, Errors, Hash}
  alias ExDataSketch.DataSketches.CompactSketch

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_k 4096
  @min_k 16
  @max_k 1 <<< 26

  @doc """
  Creates a new Theta sketch.

  ## Options

  - `:k` - nominal number of entries (default: #{@default_k}). Must be a
    power of 2, between #{@min_k} and #{@max_k}.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> sketch = ExDataSketch.Theta.new(k: 1024)
      iex> sketch.opts
      [k: 1024]
      iex> ExDataSketch.Theta.size_bytes(sketch)
      17

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    k = Keyword.get(opts, :k, @default_k)
    validate_k!(k)
    backend = Backend.resolve(opts)
    clean_opts = [k: k]
    state = backend.theta_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single item.

  The item is hashed using `ExDataSketch.Hash.hash64/1` before being
  inserted into the sketch.

  ## Examples

      iex> sketch = ExDataSketch.Theta.new() |> ExDataSketch.Theta.update("hello")
      iex> ExDataSketch.Theta.estimate(sketch) > 0.0
      true

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, item) do
    hash = Hash.hash64(item)
    new_state = backend.theta_update(state, hash, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Updates the sketch with multiple items in a single pass.

  More efficient than calling `update/2` repeatedly because it minimizes
  intermediate binary allocations.

  ## Examples

      iex> sketch = ExDataSketch.Theta.new() |> ExDataSketch.Theta.update_many(["a", "b", "c"])
      iex> ExDataSketch.Theta.estimate(sketch) > 0.0
      true

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    hashes = Enum.map(items, &Hash.hash64/1)
    new_state = backend.theta_update_many(state, hashes, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Compacts the sketch into a read-only form with sorted entries.

  Compacting discards any entries above the current theta threshold and
  sorts the remaining entries. This is required before serialization to
  the DataSketches CompactSketch format.

  ## Examples

      iex> sketch = ExDataSketch.Theta.new() |> ExDataSketch.Theta.update("x") |> ExDataSketch.Theta.compact()
      iex> ExDataSketch.Theta.estimate(sketch) > 0.0
      true

  """
  @spec compact(t()) :: t()
  def compact(%__MODULE__{state: state, opts: opts, backend: backend} = sketch) do
    new_state = backend.theta_compact(state, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Estimates the cardinality (distinct count) from the sketch.

  ## Examples

      iex> ExDataSketch.Theta.new() |> ExDataSketch.Theta.estimate()
      0.0

  """
  @spec estimate(t()) :: float()
  def estimate(%__MODULE__{state: state, opts: opts, backend: backend}) do
    backend.theta_estimate(state, opts)
  end

  @doc """
  Merges two Theta sketches (set union).

  Both sketches must have the same `k` value. Returns the merged sketch.
  Raises `ExDataSketch.Errors.IncompatibleSketchesError` if the sketches
  have different parameters.

  ## Examples

      iex> a = ExDataSketch.Theta.new(k: 1024) |> ExDataSketch.Theta.update("x")
      iex> b = ExDataSketch.Theta.new(k: 1024) |> ExDataSketch.Theta.update("y")
      iex> merged = ExDataSketch.Theta.merge(a, b)
      iex> ExDataSketch.Theta.estimate(merged) >= ExDataSketch.Theta.estimate(a)
      true

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:k] != opts_b[:k] do
      raise Errors.IncompatibleSketchesError,
        reason: "Theta k mismatch: #{opts_a[:k]} vs #{opts_b[:k]}"
    end

    new_state = backend.theta_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> ExDataSketch.Theta.new() |> ExDataSketch.Theta.size_bytes()
      17

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

      iex> sketch = ExDataSketch.Theta.new(k: 1024)
      iex> binary = ExDataSketch.Theta.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    k = Keyword.fetch!(opts, :k)
    params_bin = <<k::unsigned-little-32>>
    Codec.encode(Codec.sketch_id_theta(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a Theta sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.Theta.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    with {:ok, decoded} <- Codec.decode(binary),
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
  end

  @doc """
  Serializes the sketch to Apache DataSketches CompactSketch format.

  This is the primary interop target for cross-language compatibility.
  The CompactSketch format uses 64-bit hashes with a seed hash for
  compatibility verification.

  ## Options

  - `:seed` - the seed value for seed hash computation (default: 9001).

  ## Examples

      iex> sketch = ExDataSketch.Theta.new(k: 1024) |> ExDataSketch.Theta.update("hello")
      iex> binary = ExDataSketch.Theta.serialize_datasketches(sketch)
      iex> is_binary(binary) and byte_size(binary) > 0
      true

  """
  @spec serialize_datasketches(t(), keyword()) :: binary()
  def serialize_datasketches(%__MODULE__{} = sketch, opts \\ []) do
    CompactSketch.encode(sketch, opts)
  end

  @doc """
  Deserializes an Apache DataSketches CompactSketch binary into a Theta sketch.

  ## Options

  - `:seed` - expected seed value for seed hash verification (default: 9001).

  ## Examples

      iex> sketch = ExDataSketch.Theta.new(k: 1024) |> ExDataSketch.Theta.update("test")
      iex> binary = ExDataSketch.Theta.serialize_datasketches(sketch)
      iex> {:ok, restored} = ExDataSketch.Theta.deserialize_datasketches(binary)
      iex> ExDataSketch.Theta.estimate(restored) == ExDataSketch.Theta.estimate(sketch)
      true

  """
  @spec deserialize_datasketches(binary(), keyword()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize_datasketches(binary, opts \\ []) when is_binary(binary) do
    case CompactSketch.decode(binary, opts) do
      {:ok, decoded} ->
        backend = Backend.default()
        state = backend.theta_from_components(decoded.k, decoded.theta, decoded.entries)
        {:ok, %__MODULE__{state: state, opts: [k: decoded.k], backend: backend}}

      error ->
        error
    end
  end

  @doc """
  Creates a new Theta sketch from an enumerable of items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> sketch = ExDataSketch.Theta.from_enumerable(["a", "b", "c"], k: 1024)
      iex> ExDataSketch.Theta.estimate(sketch) > 0.0
      true

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> update_many(enumerable)
  end

  @doc """
  Merges a non-empty enumerable of Theta sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> a = ExDataSketch.Theta.new(k: 1024) |> ExDataSketch.Theta.update("x")
      iex> b = ExDataSketch.Theta.new(k: 1024) |> ExDataSketch.Theta.update("y")
      iex> merged = ExDataSketch.Theta.merge_many([a, b])
      iex> ExDataSketch.Theta.estimate(merged) > 0.0
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

      iex> is_function(ExDataSketch.Theta.reducer(), 2)
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

      iex> is_function(ExDataSketch.Theta.merger(), 2)
      true

  """
  @spec merger(keyword()) :: (t(), t() -> t())
  def merger(_opts \\ []) do
    fn a, b -> merge(a, b) end
  end

  # -- Private --

  defp validate_k!(k) when is_integer(k) and k >= @min_k and k <= @max_k do
    if (k &&& k - 1) != 0 do
      raise Errors.InvalidOptionError,
        option: :k,
        value: k,
        message: "k must be a power of 2, got: #{k}"
    end

    :ok
  end

  defp validate_k!(k) do
    raise Errors.InvalidOptionError,
      option: :k,
      value: k,
      message:
        "k must be an integer power of 2 between #{@min_k} and #{@max_k}, got: #{inspect(k)}"
  end

  defp validate_sketch_id(3), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected Theta sketch ID (3), got #{id}")}
  end

  defp decode_params(<<k::unsigned-little-32>>)
       when k >= @min_k and k <= @max_k and (k &&& k - 1) == 0 do
    {:ok, [k: k]}
  end

  defp decode_params(<<k::unsigned-little-32>>) do
    {:error,
     Errors.DeserializationError.exception(
       reason:
         "invalid Theta k=#{k} in params (must be a power of 2 between #{@min_k} and #{@max_k})"
     )}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid Theta params binary")}
  end
end
