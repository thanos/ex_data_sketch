defmodule ExDataSketch.Storage.Ecto.Migration do
  @moduledoc """
  Migration helper for creating the `ex_data_sketch_sketches` table.

  Use `mix ex_data_sketch.gen.migration` to generate a migration file
  that calls `up_commands/0` and `down_commands/0` from this module.

  ## Table Structure

  The migration creates a table named `ex_data_sketch_sketches` with:

  - `id` -- auto-incrementing primary key
  - `key` -- unique string key for the sketch (with unique index)
  - `sketch_type` -- the sketch family name (e.g., "hll", "cms")
  - `data` -- binary column storing the EXSK v2 frame
  - `inserted_at` -- timestamp
  - `updated_at` -- timestamp

  ## Usage in Migrations

      defmodule MyApp.Repo.Migrations.AddExDataSketchSketches do
        use Ecto.Migration

        def up do
          Enum.each(ExDataSketch.Storage.Ecto.Migration.up_commands(), &execute/1)
        end

        def down do
          Enum.each(ExDataSketch.Storage.Ecto.Migration.down_commands(), &execute/1)
        end
      end
  """

  @table_name "ex_data_sketch_sketches"

  @doc """
  Returns the table name used by the migration.

  ## Examples

      iex> ExDataSketch.Storage.Ecto.Migration.table_name()
      "ex_data_sketch_sketches"
  """
  @spec table_name() :: String.t()
  def table_name, do: @table_name

  @doc """
  Returns the list of SQL commands to create the sketches table and index.

  Each command is a SQL string suitable for passing to `Ecto.Migration.execute/1`.
  """
  @spec up_commands() :: [String.t()]
  def up_commands do
    [
      """
      CREATE TABLE IF NOT EXISTS #{@table_name} (
        id BIGSERIAL PRIMARY KEY,
        key VARCHAR(255) NOT NULL,
        sketch_type VARCHAR(63) NOT NULL,
        data BYTEA NOT NULL,
        inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
      )
      """,
      """
      CREATE UNIQUE INDEX IF NOT EXISTS ex_data_sketch_sketches_key_index
        ON #{@table_name} (key)
      """
    ]
  end

  @doc """
  Returns the list of SQL commands to drop the sketches table.

  Each command is a SQL string suitable for passing to `Ecto.Migration.execute/1`.
  """
  @spec down_commands() :: [String.t()]
  def down_commands do
    [
      "DROP TABLE IF EXISTS #{@table_name}"
    ]
  end
end
