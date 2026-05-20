defmodule ExDataSketch.Storage.Mnesia do
  @compile {:no_warn_undefined, :mnesia}

  alias ExDataSketch.Telemetry

  @moduledoc """
  Mnesia-backed persistence for sketches.

  Stores serialized EXSK v2 frames in a Mnesia table. Mnesia provides
  distributed, transactional storage across BEAM cluster nodes.

  ## Table Setup

  Call `setup/1` to create the Mnesia table before use. This creates a
  `:set`-type table with attributes `[:key, :data]`. `setup/1` will start
  Mnesia if it is not already running.

  ## Prerequisite

  Mnesia must be running before calling `save/3`, `load/3`, `merge/3`, or
  `delete/2`. Call `setup/1` first, or start Mnesia manually with
  `:mnesia.start/0` if you manage Mnesia startup yourself.

  ## Distributed Operations

  Mnesia's primary advantage is distributed transactions. `merge/3` uses
  `mnesia:transaction/1` for atomicity across cluster nodes.

  ## Operational Notes

  - Mnesia tables should be created on all participating nodes before use.
  - For large-scale deployments, consider table fragmentation.
  - Network partitions can cause Mnesia to diverge. Refer to the Mnesia
    documentation for recovery procedures.

  ## Examples

      # Setup the Mnesia table (once, on each node)
      :ok = ExDataSketch.Storage.Mnesia.setup(:sketches)

      # Save a sketch
      :ok = ExDataSketch.Storage.Mnesia.save(sketch, :sketches, "cardinality:2024-01")

      # Load a sketch
      {:ok, sketch} = ExDataSketch.Storage.Mnesia.load(ExDataSketch.HLL, :sketches, "cardinality:2024-01")

      # Atomic merge (uses Mnesia transaction)
      :ok = ExDataSketch.Storage.Mnesia.merge(partial, :sketches, "cardinality:2024-01")
  """

  @default_table_attributes [key: :key, data: :data]

  @doc """
  Creates a Mnesia table for sketch storage.

  Creates a `:set`-type table with `[:key, :data]` attributes. If the table
  already exists, this is a no-op.

  ## Arguments

  - `table` -- the Mnesia table name (atom). Defaults to `:ex_data_sketch`.
  - `opts` -- options forwarded to `:mnesia.create_table/2`. Can include
    `:disc_copies`, `:ram_copies`, or other Mnesia table options.

  ## Returns

  - `{:ok, :created}` if the table was created.
  - `{:ok, :already_exists}` if the table already exists.
  - `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.Storage.Mnesia.setup(:test_mnesia_table)
      {:ok, _} = ExDataSketch.Storage.Mnesia.setup(:test_mnesia_table)
  """
  @spec setup(atom(), keyword()) :: {:ok, :created | :already_exists} | {:error, term()}
  def setup(table \\ :ex_data_sketch, opts \\ []) do
    ensure_mnesia_running()

    extra_attrs = Keyword.get(opts, :mnesia_attrs, [])
    attributes = Keyword.merge(@default_table_attributes, extra_attrs)

    mnesia_opts =
      [
        attributes: Keyword.values(attributes),
        type: :set
      ] ++ Keyword.drop(opts, [:mnesia_attrs])

    case :mnesia.create_table(table, mnesia_opts) do
      {:atomic, :ok} -> {:ok, :created}
      {:aborted, {:already_exists, ^table}} -> {:ok, :already_exists}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Persists a sketch under the given key in the Mnesia table.

  Uses a Mnesia transaction for atomicity.

  ## Arguments

  - `sketch` -- a sketch struct.
  - `table` -- the Mnesia table name (atom).
  - `key` -- the key under which to store the sketch.

  ## Returns

  `:ok` on success, `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.Storage.Mnesia.setup(:test_mnesia_save)
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.Mnesia.save(sketch, :test_mnesia_save, "hll:test")
      :ok
  """
  @spec save(struct(), atom(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def save(sketch, table, key) do
    start_time = System.monotonic_time()
    binary = sketch.__struct__.serialize(sketch)
    record = {table, key, binary}

    result =
      case :mnesia.transaction(fn -> :mnesia.write(record) end) do
        {:atomic, :ok} -> :ok
        {:aborted, reason} -> {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :save),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :mnesia, key: key},
        :persistence
      )

    result
  end

  @doc """
  Loads a sketch from the Mnesia table by key.

  Uses a Mnesia transaction for consistency.

  ## Arguments

  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `table` -- the Mnesia table name (atom).
  - `key` -- the key to look up.

  ## Returns

  - `{:ok, sketch}` on success.
  - `{:error, :not_found}` if the key does not exist.
  - `{:error, %DeserializationError{}}` if the stored binary is corrupted.
  - `{:error, reason}` on Mnesia or other deserialization failures.

  ## Examples

      iex> ExDataSketch.Storage.Mnesia.setup(:test_mnesia_load)
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.Mnesia.save(sketch, :test_mnesia_load, "hll:test")
      :ok
      iex> {:ok, loaded} = ExDataSketch.Storage.Mnesia.load(ExDataSketch.HLL, :test_mnesia_load, "hll:test")
      iex> ExDataSketch.HLL.estimate(loaded) > 0.0
      true
  """
  @spec load(module(), atom(), ExDataSketch.Storage.key()) ::
          {:ok, struct()} | {:error, :not_found | term()}
  def load(sketch_module, table, key) do
    start_time = System.monotonic_time()

    result =
      case :mnesia.transaction(fn -> :mnesia.read(table, key) end) do
        {:atomic, records} when is_list(records) and records != [] ->
          [{^table, ^key, binary} | _] = records
          sketch_module.deserialize(binary)

        {:atomic, []} ->
          {:error, :not_found}

        {:aborted, reason} ->
          {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :load),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: sketch_type_from_module(sketch_module), backend: :mnesia, key: key},
        :persistence
      )

    result
  end

  @doc """
  Atomically merges a sketch into the persisted value at the given key.

  Uses a Mnesia transaction for distributed atomicity. If no sketch exists
  at the key, this is equivalent to `save/3`.

  ## Arguments

  - `sketch` -- the sketch to merge.
  - `table` -- the Mnesia table name (atom).
  - `key` -- the key whose persisted sketch to merge into.

  ## Returns

  `:ok` on success, `{:error, reason}` on failure.

  ## Examples

      iex> ExDataSketch.Storage.Mnesia.setup(:test_mnesia_merge)
      iex> sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.Mnesia.save(sketch_a, :test_mnesia_merge, "hll:test")
      :ok
      iex> sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> ExDataSketch.Storage.Mnesia.merge(sketch_b, :test_mnesia_merge, "hll:test")
      :ok
      iex> {:ok, merged} = ExDataSketch.Storage.Mnesia.load(ExDataSketch.HLL, :test_mnesia_merge, "hll:test")
      iex> ExDataSketch.HLL.estimate(merged) >= 1.9
      true
  """
  @spec merge(struct(), atom(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def merge(sketch, table, key) do
    start_time = System.monotonic_time()
    sketch_module = sketch.__struct__

    result =
      case :mnesia.transaction(fn -> do_merge(sketch, sketch_module, table, key) end) do
        {:atomic, :ok} -> :ok
        {:aborted, reason} -> {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :merge),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :mnesia, key: key},
        :persistence
      )

    result
  end

  defp do_merge(sketch, sketch_module, table, key) do
    case :mnesia.read(table, key) do
      [{^table, ^key, binary}] ->
        {:ok, existing} = sketch_module.deserialize(binary)
        merged = sketch_module.merge(existing, sketch)
        merged_binary = sketch_module.serialize(merged)
        :mnesia.write({table, key, merged_binary})

      [] ->
        binary = sketch_module.serialize(sketch)
        :mnesia.write({table, key, binary})
    end
  end

  @doc """
  Deletes a sketch from the Mnesia table by key.

  Uses a Mnesia transaction.

  ## Arguments

  - `table` -- the Mnesia table name (atom).
  - `key` -- the key to delete.

  ## Returns

  - `:ok` on success (including when the key did not exist).
  - `{:error, reason}` if the Mnesia transaction is aborted.

  ## Examples

      iex> ExDataSketch.Storage.Mnesia.setup(:test_mnesia_del)
      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> ExDataSketch.Storage.Mnesia.save(sketch, :test_mnesia_del, "hll:test")
      :ok
      iex> ExDataSketch.Storage.Mnesia.delete(:test_mnesia_del, "hll:test")
      :ok
  """
  @spec delete(atom(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def delete(table, key) do
    start_time = System.monotonic_time()

    result =
      case :mnesia.transaction(fn -> :mnesia.delete(table, key, :write) end) do
        {:atomic, :ok} -> :ok
        {:aborted, reason} -> {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :delete),
        %{duration: System.monotonic_time() - start_time},
        %{backend: :mnesia, key: key},
        :persistence
      )

    result
  end

  defp ensure_mnesia_running do
    case :mnesia.system_info(:is_running) do
      :yes ->
        :ok

      :no ->
        :mnesia.start()

      :starting ->
        wait_for_mnesia(10)
    end
  end

  defp wait_for_mnesia(0), do: :ok

  defp wait_for_mnesia(n) do
    case :mnesia.system_info(:is_running) do
      :yes ->
        :ok

      :starting ->
        Process.sleep(100)
        wait_for_mnesia(n - 1)

      :no ->
        :mnesia.start()
    end
  end

  defp sketch_type_from_module(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
    |> String.to_atom()
  end
end
