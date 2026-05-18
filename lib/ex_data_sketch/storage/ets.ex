defmodule ExDataSketch.Storage.ETS do
  @moduledoc """
  ETS-backed persistence for sketches.

  Stores serialized EXSK v2 frames in an ETS table. ETS provides fast,
  in-memory storage that persists for the lifetime of the owning process.

  ## Table Requirements

  - The ETS table must be `:set` or `:ordered_set` type.
  - The table must be created by the caller before use.

  ## Concurrency

  - Reads are concurrent.
  - `merge/3` uses a read-modify-write cycle with the table's heir or lock
    semantics. For truly atomic merge across concurrent writers, consider
  using `:ets.update_counter/3`-style patterns or Mnesia transactions.

  ## Examples

      # Create an ETS table (application concern)
      :ets.new(:sketches, [:set, :public, :named_table])

      # Save a sketch
      :ok = ExDataSketch.Storage.ETS.save(sketch, :sketches, "cardinality:2024-01")

      # Load a sketch
      {:ok, sketch} = ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :sketches, "cardinality:2024-01")

      # Atomic merge
      :ok = ExDataSketch.Storage.ETS.merge(partial, :sketches, "cardinality:2024-01")

      # Delete
      :ok = ExDataSketch.Storage.ETS.delete(:sketches, "cardinality:2024-01")
  """

  @table_type_error "ETS table must be :set or :ordered_set type"

  @doc """
  Persists a sketch under the given key in the ETS table.

  The sketch is serialized to an EXSK v2 binary frame before storage.

  ## Arguments

  - `sketch` -- a sketch struct (e.g., `%ExDataSketch.HLL{}`).
  - `table` -- the ETS table name (atom).
  - `key` -- the key under which to store the sketch.

  ## Returns

  `:ok` on success.

  ## Raises

  - `ArgumentError` if the table is not `:set` or `:ordered_set`.

  ## Examples

      iex> :ets.new(:test_ets_save, [:set, :public, :named_table])
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.ETS.save(sketch, :test_ets_save, "hll:test")
      :ok
      iex> :ets.delete(:test_ets_save)
      :true

  """
  @spec save(struct(), atom(), ExDataSketch.Storage.key()) :: :ok
  def save(sketch, table, key) do
    validate_table_type!(table)
    binary = sketch.__struct__.serialize(sketch)
    :ets.insert(table, {key, binary})
    :ok
  end

  @doc """
  Loads a sketch from the ETS table by key.

  The binary value is deserialized using the given sketch module's
  `deserialize/1` function.

  ## Arguments

  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `table` -- the ETS table name (atom).
  - `key` -- the key to look up.

  ## Returns

  - `{:ok, sketch}` on success.
  - `{:error, :not_found}` if the key does not exist.
  - `{:error, reason}` if deserialization fails.

  ## Examples

      iex> :ets.new(:test_ets_load, [:set, :public, :named_table])
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.ETS.save(sketch, :test_ets_load, "hll:test")
      iex> {:ok, loaded} = ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :test_ets_load, "hll:test")
      iex> ExDataSketch.HLL.estimate(loaded) > 0.0
      true
      iex> ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :test_ets_load, "nonexistent")
      {:error, :not_found}
      iex> :ets.delete(:test_ets_load)
      :true

  """
  @spec load(module(), atom(), ExDataSketch.Storage.key()) ::
          {:ok, struct()} | {:error, :not_found | term()}
  def load(sketch_module, table, key) do
    case :ets.lookup(table, key) do
      [{^key, binary}] ->
        sketch_module.deserialize(binary)

      [] ->
        {:error, :not_found}
    end
  end

  @doc """
  Atomically merges a sketch into the persisted value at the given key.

  If no sketch exists at the key, this is equivalent to `save/3`. Otherwise,
  the persisted sketch is loaded, merged with the given sketch, and saved back.

  Note: this is not truly atomic under concurrent writers. For distributed
  atomicity, use `ExDataSketch.Storage.Mnesia`.

  ## Arguments

  - `sketch` -- the sketch to merge into the persisted value.
  - `table` -- the ETS table name (atom).
  - `key` -- the key whose persisted sketch to merge into.

  ## Returns

  `:ok` on success.

  ## Examples

      iex> :ets.new(:test_ets_merge, [:set, :public, :named_table])
      iex> sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.ETS.save(sketch_a, :test_ets_merge, "hll:test")
      iex> sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> ExDataSketch.Storage.ETS.merge(sketch_b, :test_ets_merge, "hll:test")
      :ok
      iex> {:ok, merged} = ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :test_ets_merge, "hll:test")
      iex> ExDataSketch.HLL.estimate(merged) >= 1.9
      true
      iex> :ets.delete(:test_ets_merge)
      :true

  """
  @spec merge(struct(), atom(), ExDataSketch.Storage.key()) :: :ok
  def merge(sketch, table, key) do
    sketch_module = sketch.__struct__

    case :ets.lookup(table, key) do
      [{^key, binary}] ->
        {:ok, existing} = sketch_module.deserialize(binary)
        merged = sketch_module.merge(existing, sketch)
        merged_binary = sketch_module.serialize(merged)
        :ets.insert(table, {key, merged_binary})
        :ok

      [] ->
        save(sketch, table, key)
    end
  end

  @doc """
  Deletes a sketch from the ETS table by key.

  ## Arguments

  - `table` -- the ETS table name (atom).
  - `key` -- the key to delete.

  ## Returns

  `:ok` always (even if the key did not exist).

  ## Examples

      iex> :ets.new(:test_ets_del, [:set, :public, :named_table])
      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> ExDataSketch.Storage.ETS.save(sketch, :test_ets_del, "hll:test")
      iex> ExDataSketch.Storage.ETS.delete(:test_ets_del, "hll:test")
      :ok
      iex> ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :test_ets_del, "hll:test")
      {:error, :not_found}
      iex> :ets.delete(:test_ets_del)
      :true

  """
  @spec delete(atom(), ExDataSketch.Storage.key()) :: :ok
  def delete(table, key) do
    :ets.delete(table, key)
    :ok
  end

  defp validate_table_type!(table) do
    case :ets.info(table, :type) do
      :set ->
        :ok

      :ordered_set ->
        :ok

      undefined when undefined in [:undefined, nil] ->
        raise ArgumentError, "ETS table #{inspect(table)} does not exist"

      _other ->
        raise ArgumentError, @table_type_error
    end
  end
end
