defmodule ExDataSketch.Storage.Ecto.Schema do
  @moduledoc """
  Ecto schema for the `ex_data_sketch_sketches` table.

  This schema maps the persistence table used by `ExDataSketch.Storage.Ecto`.
  The table stores serialized EXSK v2 binary frames with the following columns:

  - `id` -- auto-incrementing primary key
  - `key` -- unique key for the sketch (string)
  - `sketch_type` -- the sketch family name (e.g., "hll", "cms")
  - `data` -- the serialized EXSK v2 binary frame
  - `inserted_at` -- timestamp of insertion
  - `updated_at` -- timestamp of last update

  ## Usage

  This module is used internally by `ExDataSketch.Storage.Ecto`. You should
  not need to interact with it directly. To create the table, use the
  migration generator:

      mix ex_data_sketch.gen.migration --repo MyApp.Repo
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :id, autogenerate: true}
  schema "ex_data_sketch_sketches" do
    field(:key, :string)
    field(:sketch_type, :string)
    field(:data, :binary)
    timestamps()
  end

  @type t :: %__MODULE__{
          id: integer() | nil,
          key: String.t() | nil,
          sketch_type: String.t() | nil,
          data: binary() | nil,
          inserted_at: NaiveDateTime.t() | nil,
          updated_at: NaiveDateTime.t() | nil
        }

  @doc """
  Creates a changeset for a sketch record.

  ## Arguments

  - `schema` -- an existing schema struct or `%Schema{}`.
  - `attrs` -- a map of attributes.

  ## Returns

  An `Ecto.Changeset`.
  """
  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(schema, attrs) do
    schema
    |> cast(attrs, [:key, :sketch_type, :data])
    |> validate_required([:key, :sketch_type, :data])
    |> unique_constraint(:key)
  end
end
