defmodule ExDataSketch.FilterChain do
  @moduledoc """
  Capability-aware composition framework for chaining membership filters.

  FilterChain composes membership filter structures into ordered query pipelines.
  It enables lifecycle-tier patterns such as a hot Cuckoo filter (absorbing writes)
  followed by a cold XorFilter (compacted snapshot).

  ## Chain Roles

  Each filter type has valid chain positions:

  - **Front/Middle/Terminal**: Bloom, Cuckoo, Quotient, CQF -- dynamic filters
    that support member? and put.
  - **Terminal only**: XorFilter -- static, no incremental insert. Must be the
    last query stage.
  - **Adjunct only**: IBLT -- reconciliation helper, not in the query path.

  ## Query Semantics

  `member?/2` evaluates query stages in order with short-circuit semantics:
  a definite "no" from any stage returns `false` immediately. Adjuncts are
  never queried.

  ## Insert Semantics

  `put/2` forwards to all query stages that support `:put`, skipping static
  stages (XorFilter). Returns `{:ok, chain}` or `{:error, :full}` if a
  Cuckoo stage is full.

  ## Delete Semantics

  `delete/2` checks that ALL query stages support `:delete`. If any stage
  lacks delete support (e.g., Bloom), raises `UnsupportedOperationError`.

  ## Binary Format (FCN1)

  FilterChain serializes each stage independently using its own `serialize/1`
  (always the default `:v2` format -- FilterChain does not currently thread
  a `:format` option through to its stages), wrapped in a chain manifest
  with magic bytes "FCN1". This is a bespoke container format, not an
  `ExDataSketch.Codec`/EXSK frame -- it has no `Codec.sketch_id` of its
  own. The `format: :v1` escape hatch available on every other sketch
  family (opt-in legacy EXSK v1 output for v0.7.x readers, see e.g.
  `ExDataSketch.Bloom.serialize/2`) therefore does not apply to
  `FilterChain.serialize/1` itself.
  """

  alias ExDataSketch.{Bloom, CQF, Cuckoo, Errors, IBLT, Quotient, Sketch, XorFilter}

  @type t :: %__MODULE__{
          stages: [struct()],
          adjuncts: [struct()]
        }

  defstruct stages: [], adjuncts: []

  @behaviour ExDataSketch.Sketch

  @fcn1_magic "FCN1"
  @fcn1_version 1

  @doc """
  Creates an empty FilterChain.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> ExDataSketch.FilterChain.stages(chain)
      []

  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Adds a filter stage to the chain.

  The stage is automatically classified based on its module type:
  - IBLT goes to the adjuncts list (not in query path)
  - XorFilter is appended as a terminal query stage
  - All other filters are appended as query stages

  Raises `InvalidChainCompositionError` if the composition is invalid
  (e.g., adding a query stage after a XorFilter terminal).

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> length(ExDataSketch.FilterChain.stages(chain))
      1

  """
  @spec add_stage(t(), struct()) :: t()
  def add_stage(%__MODULE__{} = chain, %IBLT{} = filter) do
    %{chain | adjuncts: Enum.reverse([filter | Enum.reverse(chain.adjuncts)])}
  end

  def add_stage(%__MODULE__{} = chain, filter) do
    validate_add_stage!(chain, filter)
    %{chain | stages: Enum.reverse([filter | Enum.reverse(chain.stages)])}
  end

  @doc """
  Tests whether an item may be a member by querying all stages in order.

  Short-circuits on the first `false` result. Adjuncts are not queried.
  Returns `false` if the chain has no query stages.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> bloom = ExDataSketch.Bloom.new(capacity: 100) |> ExDataSketch.Bloom.put("hello")
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, bloom)
      iex> ExDataSketch.FilterChain.member?(chain, "hello")
      true

  """
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{stages: []}, _item), do: false

  def member?(%__MODULE__{stages: stages}, item) do
    Enum.all?(stages, fn stage -> member_stage(stage, item) end)
  end

  @doc """
  Inserts an item into all query stages that support `:put`.

  Skips static stages (XorFilter). Returns `{:ok, chain}` on success
  or `{:error, :full}` if a Cuckoo stage is full.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> {:ok, chain} = ExDataSketch.FilterChain.put(chain, "hello")
      iex> ExDataSketch.FilterChain.member?(chain, "hello")
      true

  """
  @spec put(t(), term()) :: {:ok, t()} | {:error, :full}
  def put(%__MODULE__{stages: stages} = chain, item) do
    put_stages(stages, item, [], chain)
  end

  @doc """
  Inserts an item into all query stages that support `:put`, raising if a
  stage is full.

  Added so `ExDataSketch.FilterChain` satisfies the `ExDataSketch.Sketch`
  behaviour's generic `update/2` callback, which requires a bare-struct
  return. `put/2` (returning `{:ok, chain} | {:error, :full}`) remains the
  family-idiomatic name for callers who need to detect a full stage.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> chain = ExDataSketch.FilterChain.update(chain, "hello")
      iex> ExDataSketch.FilterChain.member?(chain, "hello")
      true

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{} = chain, item) do
    case put(chain, item) do
      {:ok, updated} -> updated
      {:error, :full} -> raise Errors.FilterFullError, structure: "FilterChain stage"
    end
  end

  @doc """
  Inserts every item in `items` into all query stages that support
  `:put`, batching through each stage's own `put_many/2` instead of
  looping `put/2` once per item.

  This matters: every family's single-item `put/2` runs in the Pure
  backend regardless of configured `:backend` (there's no per-item Rust
  NIF for any family), and the Pure backend's immutable-binary state
  means a single-item write reconstructs the *entire* state binary --
  O(state size) per call. `put_many/2` decodes once, applies every
  item, and encodes once per stage (Rust-accelerated where a stage's
  own backend supports it), instead of paying that O(state size) cost
  per item per stage. Confirmed: 10,000 items into a two-stage
  `[Bloom, Cuckoo]` chain (500,000 capacity each) via the old
  item-by-item `update_many/2` took ~2.3s; the same batch through this
  function takes under a millisecond.

  Skips static stages (XorFilter). Returns `{:ok, chain}` if every
  stage's batch succeeded, or `{:error, :full, partial_chain}` if a
  stage filled up partway through its own batch -- stages before the
  failing one fully applied their batch; the failing stage reflects
  however much of the batch fit before it filled up; stages after it
  are unchanged.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> {:ok, chain} = ExDataSketch.FilterChain.put_many(chain, ["a", "b", "c"])
      iex> ExDataSketch.FilterChain.member?(chain, "a")
      true

  """
  @spec put_many(t(), Enumerable.t()) :: {:ok, t()} | {:error, :full, t()}
  def put_many(%__MODULE__{stages: stages} = chain, items) do
    put_many_stages(stages, Enum.to_list(items), [], chain)
  end

  @doc """
  Inserts every item in an enumerable via `put_many/2`, raising if a
  stage fills up partway through.

  Added so `ExDataSketch.FilterChain` satisfies the `ExDataSketch.Sketch`
  behaviour's generic `update_many/2` callback, which requires a
  bare-struct return. `put_many/2` remains the family-idiomatic name
  for callers who need to detect a full stage and keep the partial
  result.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> chain = ExDataSketch.FilterChain.update_many(chain, ["a", "b", "c"])
      iex> ExDataSketch.FilterChain.member?(chain, "a")
      true

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{} = chain, items) do
    case put_many(chain, items) do
      {:ok, updated} -> updated
      {:error, :full, _partial} -> raise Errors.FilterFullError, structure: "FilterChain stage"
    end
  end

  @doc """
  Deletes an item from all query stages.

  Raises `UnsupportedOperationError` if any query stage does not support
  `:delete` (e.g., Bloom or XorFilter). Returns a bare chain rather than
  `{:ok, chain}`: a per-stage "item not found" (e.g. `Cuckoo.delete/2`'s
  `{:error, :not_found}`) is already absorbed as a safe no-op before it
  ever reaches this function, and an unsupported stage raises up front
  via `validate_all_support_delete!/1` -- there is no `{:error, ...}`
  case for a caller to ever pattern-match against, so unlike `put/2`/
  `put_many/2` (which really can fail with `{:error, :full}`), wrapping
  the result in `{:ok, ...}` would only add friction.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> cuckoo = ExDataSketch.Cuckoo.new()
      iex> {:ok, cuckoo} = ExDataSketch.Cuckoo.put(cuckoo, "hello")
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, cuckoo)
      iex> chain = ExDataSketch.FilterChain.delete(chain, "hello")
      iex> ExDataSketch.FilterChain.member?(chain, "hello")
      false

  """
  @spec delete(t(), term()) :: t()
  def delete(%__MODULE__{stages: stages} = chain, item) do
    validate_all_support_delete!(stages)

    new_stages =
      Enum.map(stages, fn stage ->
        if supports_capability?(stage, :delete) do
          delete_stage(stage, item)
        else
          stage
        end
      end)

    %{chain | stages: new_stages}
  end

  @doc """
  Returns the list of query stages.

  ## Examples

      iex> ExDataSketch.FilterChain.stages(ExDataSketch.FilterChain.new())
      []

  """
  @spec stages(t()) :: [struct()]
  def stages(%__MODULE__{stages: stages}), do: stages

  @doc """
  Returns the list of adjunct stages.

  ## Examples

      iex> ExDataSketch.FilterChain.adjuncts(ExDataSketch.FilterChain.new())
      []

  """
  @spec adjuncts(t()) :: [struct()]
  def adjuncts(%__MODULE__{adjuncts: adjuncts}), do: adjuncts

  @doc """
  Returns the sum of counts across all query stages.

  ## Examples

      iex> ExDataSketch.FilterChain.count(ExDataSketch.FilterChain.new())
      0

  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{stages: stages}) do
    Enum.reduce(stages, 0, fn stage, acc -> acc + count_stage(stage) end)
  end

  @doc """
  Serializes the FilterChain to the FCN1 binary format.

  Each stage is serialized independently using its own `serialize/1`,
  then wrapped in a chain manifest.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> binary = ExDataSketch.FilterChain.serialize(chain)
      iex> <<"FCN1", _rest::binary>> = binary
      iex> byte_size(binary) > 0
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{stages: stages, adjuncts: adjuncts}) do
    stage_bins = Enum.map(stages, &serialize_stage/1)
    adjunct_bins = Enum.map(adjuncts, &serialize_stage/1)

    stage_data =
      Enum.map(stage_bins, fn bin ->
        <<byte_size(bin)::unsigned-little-32, bin::binary>>
      end)
      |> IO.iodata_to_binary()

    adjunct_data =
      Enum.map(adjunct_bins, fn bin ->
        <<byte_size(bin)::unsigned-little-32, bin::binary>>
      end)
      |> IO.iodata_to_binary()

    <<
      @fcn1_magic::binary,
      @fcn1_version::unsigned-8,
      length(stages)::unsigned-8,
      length(adjuncts)::unsigned-8,
      0::unsigned-8,
      stage_data::binary,
      adjunct_data::binary
    >>
  end

  @doc """
  Deserializes an FCN1 binary into a FilterChain.

  Returns `{:ok, chain}` on success or `{:error, reason}` on failure.

  ## Examples

      iex> chain = ExDataSketch.FilterChain.new()
      iex> chain = ExDataSketch.FilterChain.add_stage(chain, ExDataSketch.Bloom.new(capacity: 100))
      iex> binary = ExDataSketch.FilterChain.serialize(chain)
      iex> {:ok, recovered} = ExDataSketch.FilterChain.deserialize(binary)
      iex> length(ExDataSketch.FilterChain.stages(recovered))
      1

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(
        <<@fcn1_magic::binary, @fcn1_version::unsigned-8, stage_count::unsigned-8,
          adjunct_count::unsigned-8, _reserved::unsigned-8, rest::binary>>
      ) do
    with {:ok, stages, rest} <- decode_stages(rest, stage_count, []),
         {:ok, adjuncts, rest2} <- decode_stages(rest, adjunct_count, []) do
      case rest2 do
        <<>> ->
          {:ok, %__MODULE__{stages: stages, adjuncts: adjuncts}}

        _ ->
          {:error,
           Errors.DeserializationError.exception(
             reason: "trailing data after FCN1 stages (#{byte_size(rest2)} extra bytes)"
           )}
      end
    end
  end

  def deserialize(<<@fcn1_magic::binary, _::binary>>) do
    {:error, Errors.DeserializationError.exception(reason: "unsupported FCN1 version")}
  end

  def deserialize(_binary) do
    {:error, Errors.DeserializationError.exception(reason: "invalid FCN1 header")}
  end

  @doc """
  Returns the set of capabilities supported by FilterChain.
  """
  def capabilities do
    MapSet.new([
      :new,
      :add_stage,
      :put,
      :put_many,
      :update,
      :update_many,
      :member?,
      :delete,
      :count,
      :serialize,
      :deserialize
    ])
  end

  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{stages: stages, adjuncts: adjuncts}) do
    stage_bytes = Enum.reduce(stages, 0, fn s, acc -> acc + size_bytes_stage(s) end)
    adjunct_bytes = Enum.reduce(adjuncts, 0, fn s, acc -> acc + size_bytes_stage(s) end)
    stage_bytes + adjunct_bytes
  end

  # -- Private: put pipeline --

  defp put_stages([], _item, acc, chain) do
    {:ok, %{chain | stages: Enum.reverse(acc)}}
  end

  defp put_stages([stage | rest], item, acc, chain) do
    if supports_capability?(stage, :put) do
      case put_stage(stage, item) do
        {:ok, updated} -> put_stages(rest, item, [updated | acc], chain)
        {:error, :full} -> {:error, :full}
      end
    else
      put_stages(rest, item, [stage | acc], chain)
    end
  end

  # -- Private: put_many pipeline --
  # Each stage batches its own put_many/2 (Rust-accelerated where that
  # stage's backend supports it) instead of looping put/2 once per item
  # -- see put_many/2's doc for why the latter is drastically slower.

  defp put_many_stages([], _items, acc, chain) do
    {:ok, %{chain | stages: Enum.reverse(acc)}}
  end

  defp put_many_stages([stage | rest], items, acc, chain) do
    if supports_capability?(stage, :put) do
      case put_many_stage(stage, items) do
        {:ok, updated} ->
          put_many_stages(rest, items, [updated | acc], chain)

        {:error, :full, partial} ->
          {:error, :full, %{chain | stages: Enum.reverse([partial | acc]) ++ rest}}
      end
    else
      put_many_stages(rest, items, [stage | acc], chain)
    end
  end

  # -- Private: stage dispatch --

  defp put_stage(%Bloom{} = s, item), do: {:ok, Bloom.put(s, item)}
  defp put_stage(%Cuckoo{} = s, item), do: Cuckoo.put(s, item)
  defp put_stage(%Quotient{} = s, item), do: {:ok, Quotient.put(s, item)}
  defp put_stage(%CQF{} = s, item), do: CQF.put(s, item)

  defp put_many_stage(%Bloom{} = s, items), do: {:ok, Bloom.put_many(s, items)}
  defp put_many_stage(%Cuckoo{} = s, items), do: Cuckoo.put_many(s, items)
  defp put_many_stage(%Quotient{} = s, items), do: {:ok, Quotient.put_many(s, items)}
  defp put_many_stage(%CQF{} = s, items), do: CQF.put_many(s, items)

  defp member_stage(%Bloom{} = s, item), do: Bloom.member?(s, item)
  defp member_stage(%Cuckoo{} = s, item), do: Cuckoo.member?(s, item)
  defp member_stage(%Quotient{} = s, item), do: Quotient.member?(s, item)
  defp member_stage(%CQF{} = s, item), do: CQF.member?(s, item)
  defp member_stage(%XorFilter{} = s, item), do: XorFilter.member?(s, item)

  defp delete_stage(%Cuckoo{} = s, item) do
    case Cuckoo.delete(s, item) do
      {:ok, updated} -> updated
      {:error, :not_found} -> s
    end
  end

  defp delete_stage(%Quotient{} = s, item), do: Quotient.delete(s, item)
  defp delete_stage(%CQF{} = s, item), do: CQF.delete(s, item)

  defp count_stage(%Bloom{} = s), do: Bloom.count(s)
  defp count_stage(%Cuckoo{} = s), do: Cuckoo.count(s)
  defp count_stage(%Quotient{} = s), do: Quotient.count(s)
  defp count_stage(%CQF{} = s), do: CQF.count(s)
  defp count_stage(%XorFilter{} = s), do: XorFilter.count(s)

  defp serialize_stage(%Bloom{} = s), do: Bloom.serialize(s)
  defp serialize_stage(%Cuckoo{} = s), do: Cuckoo.serialize(s)
  defp serialize_stage(%Quotient{} = s), do: Quotient.serialize(s)
  defp serialize_stage(%CQF{} = s), do: CQF.serialize(s)
  defp serialize_stage(%XorFilter{} = s), do: XorFilter.serialize(s)
  defp serialize_stage(%IBLT{} = s), do: IBLT.serialize(s)

  defp size_bytes_stage(%Bloom{} = s), do: Bloom.size_bytes(s)
  defp size_bytes_stage(%Cuckoo{} = s), do: Cuckoo.size_bytes(s)
  defp size_bytes_stage(%Quotient{} = s), do: Quotient.size_bytes(s)
  defp size_bytes_stage(%CQF{} = s), do: CQF.size_bytes(s)
  defp size_bytes_stage(%XorFilter{} = s), do: XorFilter.size_bytes(s)
  defp size_bytes_stage(%IBLT{} = s), do: IBLT.size_bytes(s)

  # -- Private: capability checks --

  defp supports_capability?(stage, capability) do
    module = stage.__struct__

    if Sketch.implemented?(module) do
      MapSet.member?(module.capabilities(), capability)
    else
      false
    end
  end

  defp has_xor_terminal?(%__MODULE__{stages: stages}), do: last_is_xor?(stages)

  defp last_is_xor?([%XorFilter{}]), do: true
  defp last_is_xor?([_ | rest]), do: last_is_xor?(rest)
  defp last_is_xor?([]), do: false

  # -- Private: validation --

  defp validate_add_stage!(chain, %XorFilter{}) do
    if has_xor_terminal?(chain) do
      raise Errors.InvalidChainCompositionError,
        reason: "cannot add a stage after a XorFilter terminal"
    end
  end

  defp validate_add_stage!(chain, filter) do
    if has_xor_terminal?(chain) do
      raise Errors.InvalidChainCompositionError,
        reason: "cannot add a query stage after a XorFilter terminal"
    end

    module = filter.__struct__

    if Sketch.implemented?(module) do
      unless MapSet.member?(module.capabilities(), :member?) do
        raise Errors.InvalidChainCompositionError,
          reason: "#{inspect(module)} does not support :member? and cannot be a query stage"
      end
    end
  end

  defp validate_all_support_delete!(stages) do
    Enum.each(stages, fn stage ->
      unless supports_capability?(stage, :delete) do
        module = stage.__struct__

        raise Errors.UnsupportedOperationError,
          operation: :delete,
          structure: inspect(module)
      end
    end)
  end

  # -- Private: deserialization --

  defp decode_stages(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_stages(
         <<stage_len::unsigned-little-32, stage_bin::binary-size(stage_len), rest::binary>>,
         remaining,
         acc
       ) do
    case deserialize_stage(stage_bin) do
      {:ok, stage} -> decode_stages(rest, remaining - 1, [stage | acc])
      {:error, _} = err -> err
    end
  end

  defp decode_stages(_rest, _remaining, _acc) do
    {:error, Errors.DeserializationError.exception(reason: "truncated FCN1 stage data")}
  end

  defp deserialize_stage(
         <<"EXSK", _version::unsigned-8, sketch_id::unsigned-8, _rest::binary>> = exsk_bin
       ) do
    case module_for_sketch_id(sketch_id) do
      {:ok, module} -> module.deserialize(exsk_bin)
      {:error, _} = err -> err
    end
  end

  defp deserialize_stage(_bin) do
    {:error, Errors.DeserializationError.exception(reason: "stage binary is not valid EXSK")}
  end

  defp module_for_sketch_id(7), do: {:ok, Bloom}
  defp module_for_sketch_id(8), do: {:ok, Cuckoo}
  defp module_for_sketch_id(9), do: {:ok, Quotient}
  defp module_for_sketch_id(10), do: {:ok, CQF}
  defp module_for_sketch_id(11), do: {:ok, XorFilter}
  defp module_for_sketch_id(12), do: {:ok, IBLT}

  defp module_for_sketch_id(id) do
    {:error,
     Errors.DeserializationError.exception(reason: "unknown sketch ID #{id} in FilterChain stage")}
  end
end
