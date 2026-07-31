defmodule ExDataSketch.Storage.Ecto do
  @moduledoc """
  Ecto-backed persistence for sketches.

  Stores serialized EXSK v2 frames in a SQL database via Ecto. This backend
  is useful for applications already using Ecto for persistence, or when
  sketches need to survive process and node restarts without managing ETS/DETS.

  ## Dependency

  This module requires `:ecto_sql`. If Ecto is not available, calls will
  raise a clear error directing you to add the dependency.

  ## Setup

  1. Add the migration to your application:

       mix ex_data_sketch.gen.migration --repo MyApp.Repo

  2. Run the migration:

       mix ecto.migrate

  3. Use the storage API:

       ExDataSketch.Storage.Ecto.save(sketch, MyApp.Repo, "cardinality:2024-01")
       {:ok, sketch} = ExDataSketch.Storage.Ecto.load(ExDataSketch.HLL, MyApp.Repo, "cardinality:2024-01")

  > #### Non-executable examples {: .info}
  >
  > The examples below use `MyApp.Repo` which must be configured in your
  > application. They are included for documentation purposes and will not
  > run in the standard test suite.

  ## Examples

      # Save a sketch
      :ok = ExDataSketch.Storage.Ecto.save(sketch, MyApp.Repo, "cardinality:2024-01")

      # Load a sketch
      {:ok, sketch} = ExDataSketch.Storage.Ecto.load(ExDataSketch.HLL, MyApp.Repo, "cardinality:2024-01")

      # Atomic merge (uses Ecto transaction)
      :ok = ExDataSketch.Storage.Ecto.merge(partial, MyApp.Repo, "cardinality:2024-01")

      # Delete
      :ok = ExDataSketch.Storage.Ecto.delete(MyApp.Repo, "cardinality:2024-01")
  """

  alias ExDataSketch.{Integration, Storage.Ecto.Schema, Telemetry}

  @ecto_available Code.ensure_loaded?(Ecto.Adapters.SQL)

  @doc """
  Returns whether Ecto is available at runtime.

  ## Examples

      iex> is_boolean(ExDataSketch.Storage.Ecto.ecto_available?())
      true
  """
  @spec ecto_available?() :: boolean()
  def ecto_available? do
    configured?(@ecto_available)
  end

  @doc """
  Persists a sketch under the given key via Ecto.

  If a sketch with the same key and type already exists, it is replaced
  (upsert). The sketch is serialized to an EXSK v2 binary frame before storage.

  ## Arguments

  - `sketch` -- a sketch struct.
  - `repo` -- the Ecto repo module (e.g., `MyApp.Repo`).
  - `key` -- the key under which to store the sketch.

  ## Returns

  `:ok` on success, `{:error, changeset}` on validation failure.

  ## Raises

  - `RuntimeError` if Ecto is not available.

  ## Examples

      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.Ecto.save(sketch, MyApp.Repo, "hll:test")
      :ok
  """
  @spec save(struct(), module(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def save(sketch, repo, key) do
    start_time = System.monotonic_time()
    Integration.require_ecto!()
    binary = sketch.__struct__.serialize(sketch)

    sketch_type =
      sketch.__struct__
      |> Module.split()
      |> List.last()
      |> String.downcase()

    changeset =
      Schema.changeset(%Schema{}, %{
        key: to_string(key),
        sketch_type: sketch_type,
        data: binary
      })

    result =
      case repo.insert(changeset,
             on_conflict: {:replace, [:data, :updated_at]},
             conflict_target: [:key]
           ) do
        {:ok, _} -> :ok
        {:error, changeset} -> {:error, changeset}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :save),
        %{duration: System.monotonic_time() - start_time, size_bytes: byte_size(binary)},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :ecto, key: key},
        :persistence
      )

    result
  end

  @doc """
  Loads a sketch from the database by key.

  Finds the most recent entry for the given key and deserializes it using
  the given sketch module's `deserialize/1` function.

  ## Arguments

  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `repo` -- the Ecto repo module.
  - `key` -- the key to look up.

  ## Returns

  - `{:ok, sketch}` on success.
  - `{:error, :not_found}` if no entry exists for the key.
  - `{:error, %DeserializationError{}}` if the stored binary is corrupted.
  - `{:error, reason}` on database failure.

  ## Raises

  - `RuntimeError` if Ecto is not available.

  ## Examples

      iex> {:ok, loaded} = ExDataSketch.Storage.Ecto.load(ExDataSketch.HLL, MyApp.Repo, "hll:test")
      iex> ExDataSketch.HLL.estimate(loaded) > 0.0
      true
  """
  @spec load(module(), module(), ExDataSketch.Storage.key()) ::
          {:ok, struct()} | {:error, :not_found | term()}
  def load(sketch_module, repo, key) do
    start_time = System.monotonic_time()
    Integration.require_ecto!()

    import Ecto.Query

    result =
      case repo.one(
             from(s in Schema,
               where: s.key == ^to_string(key),
               order_by: [desc: s.updated_at],
               limit: 1
             )
           ) do
        %Schema{data: binary} ->
          sketch_module.deserialize(binary)

        nil ->
          {:error, :not_found}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :load),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: sketch_type_from_module(sketch_module), backend: :ecto, key: key},
        :persistence
      )

    result
  end

  @doc """
  Atomically merges a sketch into the persisted value at the given key.

  Uses an Ecto transaction for atomicity. If no sketch exists at the key,
  this is equivalent to `save/3`.

  ## Arguments

  - `sketch` -- the sketch to merge.
  - `repo` -- the Ecto repo module.
  - `key` -- the key whose persisted sketch to merge into.

  ## Returns

  `:ok` on success, `{:error, reason}` on failure.

  ## Raises

  - `RuntimeError` if Ecto is not available.

  ## Examples

      iex> sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.Ecto.save(sketch_a, MyApp.Repo, "hll:test")
      :ok
      iex> sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> ExDataSketch.Storage.Ecto.merge(sketch_b, MyApp.Repo, "hll:test")
      :ok
  """
  @spec merge(struct(), module(), ExDataSketch.Storage.key()) :: :ok | {:error, term()}
  def merge(sketch, repo, key) do
    start_time = System.monotonic_time()
    Integration.require_ecto!()
    sketch_module = sketch.__struct__
    string_key = to_string(key)

    result =
      repo.transaction(fn ->
        import Ecto.Query

        case repo.one(
               from(s in Schema,
                 where: s.key == ^string_key,
                 lock: "FOR UPDATE",
                 limit: 1
               )
             ) do
          %Schema{data: binary} = existing ->
            {:ok, existing_sketch} = sketch_module.deserialize(binary)
            merged = sketch_module.merge(existing_sketch, sketch)
            merged_binary = sketch_module.serialize(merged)
            changeset = Schema.changeset(existing, %{data: merged_binary})
            repo.update!(changeset)

          nil ->
            binary = sketch_module.serialize(sketch)

            sketch_type =
              sketch_module
              |> Module.split()
              |> List.last()
              |> String.downcase()

            changeset =
              Schema.changeset(%Schema{}, %{
                key: string_key,
                sketch_type: sketch_type,
                data: binary
              })

            repo.insert!(changeset)
        end
      end)
      |> case do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :merge),
        %{duration: System.monotonic_time() - start_time},
        %{sketch_type: Telemetry.sketch_type(sketch), backend: :ecto, key: key},
        :persistence
      )

    result
  end

  @doc """
  Deletes a sketch from the database by key.

  ## Arguments

  - `repo` -- the Ecto repo module.
  - `key` -- the key to delete.

  ## Returns

  `:ok` always (even if the key did not exist).

  ## Examples

      iex> ExDataSketch.Storage.Ecto.delete(MyApp.Repo, "hll:test")
      :ok
  """
  @spec delete(module(), ExDataSketch.Storage.key()) :: :ok
  def delete(repo, key) do
    start_time = System.monotonic_time()
    Integration.require_ecto!()

    import Ecto.Query

    repo.delete_all(
      from(s in Schema,
        where: s.key == ^to_string(key)
      )
    )

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:persistence, :delete),
        %{duration: System.monotonic_time() - start_time},
        %{backend: :ecto, key: key},
        :persistence
      )

    :ok
  end

  defp configured?(default) do
    backends = Application.get_env(:ex_data_sketch, :persistence_backends, [])

    case Keyword.get(backends, :ecto) do
      nil -> default
      config when is_list(config) -> Keyword.get(config, :enabled, default)
      true -> true
      false -> false
    end
  end

  defp sketch_type_from_module(module), do: Telemetry.sketch_type_from_module(module)
end
