defmodule ExDataSketch.Storage.DETS do
  @moduledoc """
  DETS-backed persistence for sketches.

  Stores serialized EXSK v2 frames in a DETS table. DETS provides disk-backed
  storage that survives process and node restarts, at the cost of slower
  operations compared to ETS.

  ## Table Requirements

  - The DETS table must be `:set` type. `:ordered_set` is not supported by DETS.
  - The table must be opened by the caller before use.
  - DETS has a 2GB file size limit.

  ## Concurrency

  - DETS uses file-level locking. Concurrent writes are serialized.
  - `merge/3` performs a read-modify-write cycle while holding the table lock.

  ## Examples

     # Open a DETS table (application concern)
     {:ok, _} = :dets.open_file(:sketches, [type: :set])

     # Save a sketch
     :ok = ExDataSketch.Storage.DETS.save(sketch, :sketches, "cardinality:2024-01")

     # Load a sketch
     {:ok, sketch} = ExDataSketch.Storage.DETS.load(ExDataSketch.HLL, :sketches, "cardinality:2024-01")

     # Close when done
  :ok = :dets.close(:sketches)
  """

  alias ExDataSketch.Telemetry

  @doc """
  Persists a sketch under the given key in the DETS table.

  The sketch is serialized to an EXSK v2 binary frame before storage.

  ## Arguments

  - `sketch` -- a sketch struct.
  - `table` -- the DETS table name (atom).
  - `key` -- the key under which to store the sketch.

  ## Returns

  `:ok` on success, or `{:error, reason}` on failure.

  ## Examples

      iex> :dets.open_file(:test_dets_save, type: :set)
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.DETS.save(sketch, :test_dets_save, "hll:test")
      :ok
      iex> :dets.close(:test_dets_save)
      :ok

  """
  @spec save(struct(), atom(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def save(sketch, table, key) do
    start_time = System.monotonic_time()
    binary = sketch.__struct__.serialize(sketch)
    result = :dets.insert(table, {key, binary})

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :save),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :dets, key: key},
        :persistence
      )

    result
  end

  @doc """
  Loads a sketch from the DETS table by key.

  The binary value is deserialized using the given sketch module's
  `deserialize/1` function.

  ## Arguments

  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `table` -- the DETS table name (atom).
  - `key` -- the key to look up.

  ## Returns

  - `{:ok, sketch}` on success.
  - `{:error, :not_found}` if the key does not exist.
  - `{:error, %DeserializationError{}}` if the stored binary is corrupted.
  - `{:error, reason}` on DETS or other deserialization failures.

  ## Examples

      iex> :dets.open_file(:test_dets_load, type: :set)
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.DETS.save(sketch, :test_dets_load, "hll:test")
      :ok
      iex> {:ok, loaded} = ExDataSketch.Storage.DETS.load(ExDataSketch.HLL, :test_dets_load, "hll:test")
      iex> ExDataSketch.HLL.estimate(loaded) > 0.0
      true
      iex> ExDataSketch.Storage.DETS.load(ExDataSketch.HLL, :test_dets_load, "nonexistent")
      {:error, :not_found}
      iex> :dets.close(:test_dets_load)
      :ok

  """
  @spec load(module(), atom(), ExDataSketch.Storage.key()) ::
          {:ok, struct()} | {:error, :not_found | term()}
  def load(sketch_module, table, key) do
    start_time = System.monotonic_time()

    result =
      case :dets.lookup(table, key) do
        [{^key, binary}] ->
          sketch_module.deserialize(binary)

        [] ->
          {:error, :not_found}

        {:error, reason} ->
          {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :load),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: sketch_type_from_module(sketch_module), backend: :dets, key: key},
        :persistence
      )

    result
  end

  @doc """
  Merges a sketch into the persisted value at the given key.

  If no sketch exists at the key, this is equivalent to `save/3`. Otherwise,
  the persisted sketch is loaded, merged with the given sketch, and saved back.

  The read-modify-write cycle occurs while the DETS table lock is held. This
  provides atomicity for single-node writers. DETS file-level locking does
  not extend across distributed nodes; for distributed atomicity, use
  `ExDataSketch.Storage.Mnesia`.

  ## Arguments

  - `sketch` -- the sketch to merge into the persisted value.
  - `table` -- the DETS table name (atom).
  - `key` -- the key whose persisted sketch to merge into.

  ## Returns

  `:ok` on success, or `{:error, reason}` on failure.

  ## Examples

      iex> :dets.open_file(:test_dets_merge, type: :set)
      iex> sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.DETS.save(sketch_a, :test_dets_merge, "hll:test")
      :ok
      iex> sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> ExDataSketch.Storage.DETS.merge(sketch_b, :test_dets_merge, "hll:test")
      :ok
      iex> {:ok, merged} = ExDataSketch.Storage.DETS.load(ExDataSketch.HLL, :test_dets_merge, "hll:test")
      iex> ExDataSketch.HLL.estimate(merged) >= 1.9
      true
      iex> :dets.close(:test_dets_merge)
      :ok

  """
  @spec merge(struct(), atom(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def merge(sketch, table, key) do
    start_time = System.monotonic_time()
    sketch_module = sketch.__struct__

    result =
      case :dets.lookup(table, key) do
        [{^key, binary}] ->
          {:ok, existing} = sketch_module.deserialize(binary)
          merged = sketch_module.merge(existing, sketch)
          merged_binary = sketch_module.serialize(merged)
          :dets.insert(table, {key, merged_binary})

        [] ->
          save(sketch, table, key)

        {:error, reason} ->
          {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :merge),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :dets, key: key},
        :persistence
      )

    result
  end

  @doc """
  Deletes a sketch from the DETS table by key.

  ## Arguments

  - `table` -- the DETS table name (atom).
  - `key` -- the key to delete.

  ## Returns

  - `:ok` on success (including when the key did not exist).
  - `{:error, reason}` if the DETS operation fails.

  ## Examples

      iex> :dets.open_file(:test_dets_del, type: :set)
      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> ExDataSketch.Storage.DETS.save(sketch, :test_dets_del, "hll:test")
      :ok
      iex> ExDataSketch.Storage.DETS.delete(:test_dets_del, "hll:test")
      :ok
      iex> ExDataSketch.Storage.DETS.load(ExDataSketch.HLL, :test_dets_del, "hll:test")
      {:error, :not_found}
      iex> :dets.close(:test_dets_del)
      :ok

  """
  @spec delete(atom(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def delete(table, key) do
    start_time = System.monotonic_time()

    result =
      case :dets.delete(table, key) do
        :ok -> :ok
        {:error, _} = err -> err
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :delete),
        %{duration: System.monotonic_time() - start_time},
        %{backend: :dets, key: key},
        :persistence
      )

    result
  end

  defp sketch_type_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> String.to_atom()
  end
end
