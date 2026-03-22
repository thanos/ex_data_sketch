defmodule ExDataSketch.CMS do
  @moduledoc """
  Count-Min Sketch (CMS) for frequency estimation.

  CMS provides approximate frequency estimates for items in a data stream.
  It answers: "approximately how many times has this item appeared?"

  The sketch uses `depth` independent hash functions and a `width x depth`
  counter matrix. Estimates are guaranteed to never undercount (for non-negative
  increments) but may overcount.

  ## Memory and Accuracy

  - Counter matrix: `width * depth * (counter_width / 8)` bytes.
  - Error bound: estimates are within `e * N / width` of the true count
    with probability at least `1 - (1/2)^depth`, where `N` is total count
    and `e` is Euler's number.

  | Width | Depth | Counter | Memory     |
  |-------|-------|---------|------------|
  | 2,048 | 5     | 32-bit  | 40 KiB    |
  | 8,192 | 7     | 32-bit  | 224 KiB   |
  | 2,048 | 5     | 64-bit  | 80 KiB    |

  ## Binary State Layout (v1)

  All multi-byte fields are little-endian.

      Offset  Size      Field
      ------  --------  -----
      0       1         Version (u8, currently 1)
      1       4         Width (u32 little-endian)
      5       2         Depth (u16 little-endian)
      7       1         Counter width in bits (u8, 32 or 64)
      8       1         Reserved (u8, must be 0)
      9       W*D*C     Counters (row-major, little-endian per counter)

  Where C = counter_width / 8 (4 or 8 bytes per counter).
  Total: 9 + width * depth * C bytes.

  ## Options

  - `:width` - number of counters per row, pos_integer (default: 2048).
  - `:depth` - number of rows (hash functions), pos_integer (default: 5).
  - `:counter_width` - bits per counter, 32 or 64 (default: 32).
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).

  ## Overflow Policy

  Default: saturating. When a counter reaches its maximum value (2^32-1 or
  2^64-1), further increments leave it at the maximum. This prevents wrap-around
  errors at the cost of potential undercounting at extreme values.

  ## Merge Properties

  CMS merge is **associative** and **commutative** (element-wise counter addition).
  This means sketches can be merged in any order or grouping and produce the
  same result, making CMS safe for parallel and distributed aggregation.
  """

  alias ExDataSketch.{Backend, Codec, Errors, Hash}

  @type t :: %__MODULE__{
          state: binary(),
          opts: keyword(),
          backend: module()
        }

  defstruct [:state, :opts, :backend]

  @default_width 2048
  @default_depth 5
  @default_counter_width 32

  @doc """
  Creates a new CMS sketch.

  ## Options

  - `:width` - counters per row (default: #{@default_width}). Must be positive.
  - `:depth` - number of rows (default: #{@default_depth}). Must be positive.
  - `:counter_width` - bits per counter, 32 or 64 (default: #{@default_counter_width}).
  - `:backend` - backend module (default: `ExDataSketch.Backend.Pure`).
  - `:hash_fn` - custom hash function `(term -> non_neg_integer)`.
  - `:seed` - hash seed (default: 0).

  ## Examples

      iex> sketch = ExDataSketch.CMS.new()
      iex> ExDataSketch.CMS.estimate(sketch, "anything")
      0

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    width = Keyword.get(opts, :width, @default_width)
    depth = Keyword.get(opts, :depth, @default_depth)
    counter_width = Keyword.get(opts, :counter_width, @default_counter_width)

    validate_width!(width)
    validate_depth!(depth)
    validate_counter_width!(counter_width)

    backend = Backend.resolve(opts)
    hash_fn = Keyword.get(opts, :hash_fn)
    seed = Keyword.get(opts, :seed)

    hash_strategy =
      if hash_fn, do: :custom, else: Hash.default_hash_strategy()

    clean_opts =
      [width: width, depth: depth, counter_width: counter_width, hash_strategy: hash_strategy] ++
        if(hash_fn, do: [hash_fn: hash_fn], else: []) ++
        if(seed, do: [seed: seed], else: [])

    state = backend.cms_new(clean_opts)
    %__MODULE__{state: state, opts: clean_opts, backend: backend}
  end

  @doc """
  Updates the sketch with a single item.

  The item is hashed using `ExDataSketch.Hash.hash64/1` before being
  recorded in the sketch.

  ## Parameters

  - `sketch` - the CMS sketch to update.
  - `item` - any Elixir term to count.
  - `increment` - positive integer to add (default: 1).

  ## Examples

      iex> sketch = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update("hello")
      iex> ExDataSketch.CMS.estimate(sketch, "hello")
      1

  """
  @spec update(t(), term(), pos_integer()) :: t()
  def update(
        %__MODULE__{state: state, opts: opts, backend: backend} = sketch,
        item,
        increment \\ 1
      )
      when is_integer(increment) and increment > 0 do
    hash = hash_item(item, opts)
    new_state = backend.cms_update(state, hash, increment, opts)
    %{sketch | state: new_state}
  end

  @doc """
  Updates the sketch with multiple items in a single pass.

  Accepts an enumerable of items (each with implicit increment of 1) or
  an enumerable of `{item, increment}` tuples.

  ## Examples

      iex> sketch = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update_many(["a", "b", "a"])
      iex> ExDataSketch.CMS.estimate(sketch, "a")
      2

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{state: state, opts: opts, backend: backend} = sketch, items) do
    use_raw = backend == Backend.Rust and Keyword.get(opts, :hash_fn) == nil

    new_state =
      if use_raw do
        Backend.Rust.cms_update_many_raw(state, Enum.to_list(items), opts)
      else
        pairs =
          Enum.map(items, fn
            {item, increment} when is_integer(increment) and increment > 0 ->
              {hash_item(item, opts), increment}

            item ->
              {hash_item(item, opts), 1}
          end)

        backend.cms_update_many(state, pairs, opts)
      end

    %{sketch | state: new_state}
  end

  @doc """
  Merges two CMS sketches.

  Both sketches must have the same width, depth, and counter_width.
  Counters are added element-wise with saturating arithmetic.

  ## Examples

      iex> a = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update("x", 3)
      iex> b = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update("x", 5)
      iex> merged = ExDataSketch.CMS.merge(a, b)
      iex> ExDataSketch.CMS.estimate(merged, "x")
      8

  """
  @spec merge(t(), t()) :: t()
  def merge(
        %__MODULE__{state: state_a, opts: opts_a, backend: backend} = sketch,
        %__MODULE__{state: state_b, opts: opts_b}
      ) do
    if opts_a[:width] != opts_b[:width] or opts_a[:depth] != opts_b[:depth] or
         opts_a[:counter_width] != opts_b[:counter_width] do
      raise Errors.IncompatibleSketchesError,
        reason:
          "CMS parameter mismatch: width=#{opts_a[:width]}/#{opts_b[:width]}, " <>
            "depth=#{opts_a[:depth]}/#{opts_b[:depth]}, " <>
            "counter_width=#{opts_a[:counter_width]}/#{opts_b[:counter_width]}"
    end

    Hash.validate_merge_hash_compat!(opts_a, opts_b, "CMS")

    new_state = backend.cms_merge(state_a, state_b, opts_a)
    %{sketch | state: new_state}
  end

  @doc """
  Estimates the frequency of an item in the sketch.

  Returns a non-negative integer. The estimate is guaranteed to be at least
  the true count (no undercounting) but may overcount.

  ## Examples

      iex> sketch = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update("hello", 5)
      iex> ExDataSketch.CMS.estimate(sketch, "hello")
      5

  """
  @spec estimate(t(), term()) :: non_neg_integer()
  def estimate(%__MODULE__{state: state, opts: opts, backend: backend}, item) do
    hash = hash_item(item, opts)
    backend.cms_estimate(state, hash, opts)
  end

  @doc """
  Returns the size of the sketch state in bytes.

  ## Examples

      iex> sketch = ExDataSketch.CMS.new(width: 100, depth: 3, counter_width: 32)
      iex> ExDataSketch.CMS.size_bytes(sketch)
      1209

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{state: state}) do
    byte_size(state)
  end

  @doc """
  Serializes the sketch to the ExDataSketch-native EXSK binary format.

  ## Examples

      iex> sketch = ExDataSketch.CMS.new(width: 100, depth: 3, counter_width: 32)
      iex> binary = ExDataSketch.CMS.serialize(sketch)
      iex> <<"EXSK", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{state: state, opts: opts}) do
    width = Keyword.fetch!(opts, :width)
    depth = Keyword.fetch!(opts, :depth)
    counter_width = Keyword.fetch!(opts, :counter_width)
    hs = hash_strategy_byte(opts)

    params_bin = <<
      width::unsigned-little-32,
      depth::unsigned-little-16,
      counter_width::unsigned-8,
      hs::unsigned-8
    >>

    Codec.encode(Codec.sketch_id_cms(), Codec.version(), params_bin, state)
  end

  @doc """
  Deserializes an EXSK binary into a CMS sketch.

  Returns `{:ok, sketch}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.CMS.deserialize(<<"invalid">>)
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
  Serializes the sketch to Apache DataSketches CMS format.

  Not implemented. Apache DataSketches does not define a standard CMS binary
  format. Only Theta sketches support DataSketches interop via
  `ExDataSketch.Theta.serialize_datasketches/1`. For CMS serialization,
  use `serialize/1` (ExDataSketch-native EXSK format).

  ## Examples

      iex> try do
      ...>   sketch = %ExDataSketch.CMS{state: <<>>, opts: [], backend: nil}
      ...>   ExDataSketch.CMS.serialize_datasketches(sketch)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.CMS.serialize_datasketches is not yet implemented"

  """
  @spec serialize_datasketches(t()) :: binary()
  @dialyzer {:nowarn_function, serialize_datasketches: 1}
  def serialize_datasketches(%__MODULE__{}) do
    Errors.not_implemented!(__MODULE__, "serialize_datasketches")
  end

  @doc """
  Deserializes an Apache DataSketches CMS binary.

  Not implemented. See `serialize_datasketches/1` for details.

  ## Examples

      iex> try do
      ...>   ExDataSketch.CMS.deserialize_datasketches(<<>>)
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.CMS.deserialize_datasketches is not yet implemented"

  """
  @spec deserialize_datasketches(binary()) :: {:ok, t()} | {:error, Exception.t()}
  @dialyzer {:nowarn_function, deserialize_datasketches: 1}
  def deserialize_datasketches(_binary) do
    Errors.not_implemented!(__MODULE__, "deserialize_datasketches")
  end

  @doc """
  Creates a new CMS sketch from an enumerable of items.

  Equivalent to `new(opts) |> update_many(enumerable)`.

  ## Options

  Same as `new/1`.

  ## Examples

      iex> sketch = ExDataSketch.CMS.from_enumerable(["a", "b", "a"])
      iex> ExDataSketch.CMS.estimate(sketch, "a")
      2

  """
  @spec from_enumerable(Enumerable.t(), keyword()) :: t()
  def from_enumerable(enumerable, opts \\ []) do
    new(opts) |> update_many(enumerable)
  end

  @doc """
  Merges a non-empty enumerable of CMS sketches into one.

  Raises `Enum.EmptyError` if the enumerable is empty.

  ## Examples

      iex> a = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update("x")
      iex> b = ExDataSketch.CMS.new() |> ExDataSketch.CMS.update("x")
      iex> merged = ExDataSketch.CMS.merge_many([a, b])
      iex> ExDataSketch.CMS.estimate(merged, "x")
      2

  """
  @spec merge_many(Enumerable.t()) :: t()
  def merge_many(sketches) do
    Enum.reduce(sketches, fn sketch, acc -> merge(acc, sketch) end)
  end

  @doc """
  Returns a 2-arity reducer function suitable for `Enum.reduce/3` and similar.

  The returned function calls `update/2` on each item.

  ## Examples

      iex> is_function(ExDataSketch.CMS.reducer(), 2)
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

      iex> is_function(ExDataSketch.CMS.merger(), 2)
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
        Hash.hash64(item, seed: seed)

      hash_fn ->
        Hash.hash64(item, hash_fn: hash_fn)
    end
  end

  defp validate_width!(w) when is_integer(w) and w > 0, do: :ok

  defp validate_width!(w) do
    raise Errors.InvalidOptionError,
      option: :width,
      value: w,
      message: "width must be a positive integer, got: #{inspect(w)}"
  end

  defp validate_depth!(d) when is_integer(d) and d > 0, do: :ok

  defp validate_depth!(d) do
    raise Errors.InvalidOptionError,
      option: :depth,
      value: d,
      message: "depth must be a positive integer, got: #{inspect(d)}"
  end

  defp validate_counter_width!(cw) when cw in [32, 64], do: :ok

  defp validate_counter_width!(cw) do
    raise Errors.InvalidOptionError,
      option: :counter_width,
      value: cw,
      message: "counter_width must be 32 or 64, got: #{inspect(cw)}"
  end

  defp validate_sketch_id(2), do: :ok

  defp validate_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "expected CMS sketch ID (2), got #{id}")}
  end

  # Legacy 7-byte format (no hash strategy tag)
  defp decode_params(<<width::unsigned-little-32, depth::unsigned-little-16, cw::unsigned-8>>)
       when width > 0 and depth > 0 and cw in [32, 64] do
    {:ok, [width: width, depth: depth, counter_width: cw, hash_strategy: :phash2]}
  end

  # New 8-byte format with hash strategy tag
  defp decode_params(
         <<width::unsigned-little-32, depth::unsigned-little-16, cw::unsigned-8, hs::unsigned-8>>
       )
       when width > 0 and depth > 0 and cw in [32, 64] do
    {:ok,
     [width: width, depth: depth, counter_width: cw, hash_strategy: decode_hash_strategy(hs)]}
  end

  defp decode_params(_other) do
    {:error, Errors.DeserializationError.exception(reason: "invalid CMS params binary")}
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
end
