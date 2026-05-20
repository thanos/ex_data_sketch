defmodule ExDataSketch.Storage.CubDB do
  @moduledoc """
  CubDB-backed persistence for sketches.

  Stores serialized EXSK v2 frames in a CubDB key-value store. CubDB provides
  disk-backed, MVCC-based storage with atomic transactions.

  ## Dependency

  This module requires the `:cubdb` package. If CubDB is not available, calls
  will raise a clear error directing you to add the dependency.

  ## Concurrency

  - CubDB supports concurrent reads via MVCC.
  - `merge/3` uses a CubDB transaction for atomicity.

  > #### Non-executable examples {: .info}
  >
  > The examples below use `CubDB.start_link/1` which requires the `:cubdb`
  > dependency. They are included for documentation purposes and will not
  > run in the standard test suite.

  ## Examples

      # Start a CubDB database (application concern)
      {:ok, db} = CubDB.start_link(data_dir: "/tmp/sketches")

      # Save a sketch
      :ok = ExDataSketch.Storage.CubDB.save(sketch, db, "cardinality:2024-01")

      # Load a sketch
      {:ok, sketch} = ExDataSketch.Storage.CubDB.load(ExDataSketch.HLL, db, "cardinality:2024-01")

      # Atomic merge
      :ok = ExDataSketch.Storage.CubDB.merge(partial, db, "cardinality:2024-01")
  """

  alias ExDataSketch.{Integration, Telemetry}

  @cubdb_available Code.ensure_loaded?(CubDB)

  @doc """
  Returns whether CubDB is available at runtime.

  ## Examples

      iex> is_boolean(ExDataSketch.Storage.CubDB.cubdb_available?())
      true
  """
  @spec cubdb_available?() :: boolean()
  def cubdb_available? do
    configured?(@cubdb_available)
  end

  @doc """
  Persists a sketch under the given key in the CubDB database.

  The sketch is serialized to an EXSK v2 binary frame before storage.

  ## Arguments

  - `sketch` -- a sketch struct.
  - `db` -- the CubDB process (pid or registered name).
  - `key` -- the key under which to store the sketch.

  ## Returns

  `:ok` on success.

  ## Raises

  - `RuntimeError` if CubDB is not available.

  ## Examples

      iex> {:ok, db} = CubDB.start_link(data_dir: System.tmp_dir!())
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.CubDB.save(sketch, db, "hll:test")
      :ok
      iex> GenServer.stop(db, :normal)
      :ok

  """
  @spec save(struct(), pid() | atom(), ExDataSketch.Storage.key()) :: :ok
  def save(sketch, db, key) do
    start_time = System.monotonic_time()
    Integration.require_cubdb!()
    binary = sketch.__struct__.serialize(sketch)
    :ok = CubDB.put(db, key, binary)

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :save),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :cubdb, key: key},
        :persistence
      )

    :ok
  end

  @doc """
  Loads a sketch from the CubDB database by key.

  The binary value is deserialized using the given sketch module's
  `deserialize/1` function.

  ## Arguments

  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `db` -- the CubDB process.
  - `key` -- the key to look up.

  ## Returns

  - `{:ok, sketch}` on success.
  - `{:error, :not_found}` if the key does not exist.
  - `{:error, %DeserializationError{}}` if the stored binary is corrupted.
  - `{:error, reason}` on other deserialization failures.

  ## Raises

  - `RuntimeError` if CubDB is not available.

  ## Examples

      iex> {:ok, db} = CubDB.start_link(data_dir: System.tmp_dir!())
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.CubDB.save(sketch, db, "hll:test")
      :ok
      iex> {:ok, loaded} = ExDataSketch.Storage.CubDB.load(ExDataSketch.HLL, db, "hll:test")
      iex> ExDataSketch.HLL.estimate(loaded) > 0.0
      true
      iex> ExDataSketch.Storage.CubDB.load(ExDataSketch.HLL, db, "nonexistent")
      {:error, :not_found}
      iex> GenServer.stop(db, :normal)
      :ok

  """
  @spec load(module(), pid() | atom(), ExDataSketch.Storage.key()) ::
          {:ok, struct()} | {:error, :not_found | term()}
  def load(sketch_module, db, key) do
    start_time = System.monotonic_time()
    Integration.require_cubdb!()

    result =
      case CubDB.get(db, key) do
        nil -> {:error, :not_found}
        binary -> sketch_module.deserialize(binary)
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :load),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: sketch_type_from_module(sketch_module), backend: :cubdb, key: key},
        :persistence
      )

    result
  end

  @doc """
  Atomically merges a sketch into the persisted value at the given key.

  Uses a CubDB transaction for atomicity. If no sketch exists at the key,
  this is equivalent to `save/3`.

  ## Arguments

  - `sketch` -- the sketch to merge.
  - `db` -- the CubDB process.
  - `key` -- the key whose persisted sketch to merge into.

  ## Returns

  `:ok` on success.

  ## Raises

  - `RuntimeError` if CubDB is not available.

  ## Examples

      iex> {:ok, db} = CubDB.start_link(data_dir: System.tmp_dir!())
      iex> sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.CubDB.save(sketch_a, db, "hll:test")
      :ok
      iex> sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> ExDataSketch.Storage.CubDB.merge(sketch_b, db, "hll:test")
      :ok
      iex> {:ok, merged} = ExDataSketch.Storage.CubDB.load(ExDataSketch.HLL, db, "hll:test")
      iex> ExDataSketch.HLL.estimate(merged) >= 1.9
      true
      iex> GenServer.stop(db, :normal)
      :ok

  """
  @spec merge(struct(), pid() | atom(), ExDataSketch.Storage.key()) :: :ok
  def merge(sketch, db, key) do
    start_time = System.monotonic_time()
    Integration.require_cubdb!()
    sketch_module = sketch.__struct__
    tx_mod = CubDB.Tx

    result =
      CubDB.transaction(db, fn tx ->
        tx =
          case tx_mod.get(tx, key) do
            nil ->
              binary = sketch_module.serialize(sketch)
              tx_mod.put(tx, key, binary)

            binary ->
              {:ok, existing} = sketch_module.deserialize(binary)
              merged = sketch_module.merge(existing, sketch)
              merged_binary = sketch_module.serialize(merged)
              tx_mod.put(tx, key, merged_binary)
          end

        {:commit, tx, :ok}
      end)

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :merge),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :cubdb, key: key},
        :persistence
      )

    result
  end

  @doc """
  Deletes a sketch from the CubDB database by key.

  ## Arguments

  - `db` -- the CubDB process.
  - `key` -- the key to delete.

  ## Returns

  `:ok` always (even if the key did not exist).

  ## Examples

      iex> {:ok, db} = CubDB.start_link(data_dir: System.tmp_dir!())
      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> ExDataSketch.Storage.CubDB.save(sketch, db, "hll:test")
      :ok
      iex> ExDataSketch.Storage.CubDB.delete(db, "hll:test")
      :ok
      iex> ExDataSketch.Storage.CubDB.load(ExDataSketch.HLL, db, "hll:test")
      {:error, :not_found}
      iex> GenServer.stop(db, :normal)
      :ok

  """
  @spec delete(pid() | atom(), ExDataSketch.Storage.key()) :: :ok
  def delete(db, key) do
    start_time = System.monotonic_time()
    Integration.require_cubdb!()
    CubDB.delete(db, key)

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :delete),
        %{duration: System.monotonic_time() - start_time},
        %{backend: :cubdb, key: key},
        :persistence
      )

    :ok
  end

  defp configured?(default) do
    backends = Application.get_env(:ex_data_sketch, :persistence_backends, [])

    case Keyword.get(backends, :cubdb) do
      nil -> default
      config when is_list(config) -> Keyword.get(config, :enabled, default)
      true -> true
      false -> false
    end
  end

  defp sketch_type_from_module(module), do: Telemetry.sketch_type_from_module(module)
end
