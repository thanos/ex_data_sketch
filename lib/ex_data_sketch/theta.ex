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
  `serialize_datasketches/1` and `deserialize_datasketches/1` will implement
  the CompactSketch binary format, enabling cross-language compatibility
  with Java, C++, and Python DataSketches libraries.

  ## Merge Properties

  Theta merge (union) is **associative** and **commutative**.
  This means sketches can be merged in any order or grouping and produce the
  same result, making Theta safe for parallel and distributed aggregation.

  ## Phase 0 Status

  All functions are stubs. Full implementation in Phase 1.5.
  """

  alias ExDataSketch.{Codec, Errors}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_k 4096

  @doc """
  Creates a new Theta sketch.

  ## Options

  - `:k` - nominal number of entries (default: #{@default_k}). Must be a
    positive integer and a power of 2.
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.new()
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.new is not yet implemented"

  """
  @spec new(keyword()) :: t()
  def new(_opts \\ []) do
    Errors.not_implemented!(__MODULE__, "new")
  end

  @doc """
  Updates the sketch with a single item.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.update(%ExDataSketch.Theta{state: <<>>, opts: [], backend: nil}, "hello")
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.update is not yet implemented"

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{}, _item) do
    Errors.not_implemented!(__MODULE__, "update")
  end

  @doc """
  Compacts the sketch into a read-only form with sorted entries.

  Compacting discards any entries above the current theta threshold and
  sorts the remaining entries. This is required before serialization to
  the DataSketches CompactSketch format.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.compact(%ExDataSketch.Theta{state: <<>>, opts: [], backend: nil})
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.compact is not yet implemented"

  """
  @spec compact(t()) :: t()
  def compact(%__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "compact")
  end

  @doc """
  Estimates the cardinality (distinct count) from the sketch.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.estimate(%ExDataSketch.Theta{state: <<>>, opts: [], backend: nil})
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.estimate is not yet implemented"

  """
  @spec estimate(t()) :: float()
  def estimate(%__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "estimate")
  end

  @doc """
  Merges two Theta sketches (set union).

  ## Examples

      iex> try do
      ...>   s = %ExDataSketch.Theta{state: <<>>, opts: [], backend: nil}
      ...>   ExDataSketch.Theta.merge(s, s)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.merge is not yet implemented"

  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{}, %__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "merge")
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.serialize(%ExDataSketch.Theta{state: <<>>, opts: [], backend: nil})
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.serialize is not yet implemented"

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "serialize")
  end

  @doc """
  Deserializes an EXSK binary into a Theta sketch.

  ## Examples

      iex> ExDataSketch.Theta.deserialize(<<"invalid">>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid magic bytes, expected EXSK"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    case Codec.decode(binary) do
      {:ok, decoded} ->
        validate_sketch_id(decoded.sketch_id)

      error ->
        error
    end
  end

  @doc """
  Serializes the sketch to Apache DataSketches CompactSketch format.

  This is the primary interop target for cross-language compatibility.
  The CompactSketch format uses 64-bit hashes with a seed hash for
  compatibility verification.

  Not yet implemented. Will be the first DataSketches interop codec.

  ## Examples

      iex> try do
      ...>   s = %ExDataSketch.Theta{state: <<>>, opts: [], backend: nil}
      ...>   ExDataSketch.Theta.serialize_datasketches(s)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.serialize_datasketches is not yet implemented"

  """
  @spec serialize_datasketches(t()) :: binary()
  def serialize_datasketches(%__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "serialize_datasketches")
  end

  @doc """
  Deserializes an Apache DataSketches CompactSketch binary into a Theta sketch.

  Not yet implemented. Will be the first DataSketches interop codec.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.deserialize_datasketches(<<>>)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.deserialize_datasketches is not yet implemented"

  """
  @spec deserialize_datasketches(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize_datasketches(_binary) do
    Errors.not_implemented!(__MODULE__, "deserialize_datasketches")
  end

  @doc """
  Creates a new Theta sketch from an enumerable of items.

  Equivalent to creating a new sketch and updating it with each item.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Theta.from_enumerable(["a", "b", "c"])
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.new is not yet implemented"

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    # apply/3 used to avoid type-checker warning: new/1 is a stub that always
    # raises, so the compiler infers none() and warns on Enum.reduce/3.
    # This will be replaced with a direct call once new/1 is implemented.
    sketch = apply(__MODULE__, :new, [opts])
    Enum.reduce(enumerable, sketch, fn item, acc -> update(acc, item) end)
  end

  @doc """
  Merges a non-empty enumerable of Theta sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> try do
      ...>   s = %ExDataSketch.Theta{state: <<>>, opts: [], backend: nil}
      ...>   ExDataSketch.Theta.merge_many([s, s])
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Theta.merge is not yet implemented"

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3` and similar.

  The returned function calls `update/2` on each item.

  ## Options

  Same as `new/1`. Used to create the initial sketch accumulator.

  ## Examples

      iex> is_function(ExDataSketch.Theta.reducer(), 2)
      true

  """
  @spec reducer(keyword()) :: (term(), t() -> t())
  def reducer(opts \\ []) do
    fn item, sketch ->
      _ = opts
      update(sketch, item)
    end
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

  defp validate_sketch_id(3) do
    Errors.not_implemented!(__MODULE__, "deserialize")
  end

  defp validate_sketch_id(id) do
    alias ExDataSketch.Errors.DeserializationError

    {:error, DeserializationError.exception(reason: "expected Theta sketch ID (3), got #{id}")}
  end
end
