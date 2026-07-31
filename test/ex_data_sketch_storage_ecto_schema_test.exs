defmodule ExDataSketch.Storage.Ecto.SchemaTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Storage.Ecto.Schema

  describe "changeset/2" do
    test "valid changeset with all required fields" do
      attrs = %{
        key: "hll:test",
        sketch_type: "hll",
        data: <<1, 2, 3, 4>>
      }

      changeset = Schema.changeset(%Schema{}, attrs)
      assert changeset.valid?
    end

    test "invalid changeset missing key" do
      attrs = %{
        sketch_type: "hll",
        data: <<1, 2, 3>>
      }

      changeset = Schema.changeset(%Schema{}, attrs)
      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset, :key)
    end

    test "invalid changeset missing sketch_type" do
      attrs = %{
        key: "hll:test",
        data: <<1, 2, 3>>
      }

      changeset = Schema.changeset(%Schema{}, attrs)
      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset, :sketch_type)
    end

    test "invalid changeset missing data" do
      attrs = %{
        key: "hll:test",
        sketch_type: "hll"
      }

      changeset = Schema.changeset(%Schema{}, attrs)
      refute changeset.valid?
      assert "can't be blank" in errors_on(changeset, :data)
    end

    test "invalid changeset with all fields missing" do
      changeset = Schema.changeset(%Schema{}, %{})
      refute changeset.valid?
      assert length(changeset.errors) >= 3
    end

    test "casts extra fields gracefully" do
      attrs = %{
        key: "hll:test",
        sketch_type: "hll",
        data: <<1, 2, 3>>,
        unknown_field: "ignored"
      }

      changeset = Schema.changeset(%Schema{}, attrs)
      assert changeset.valid?
      refute Map.has_key?(changeset.changes, :unknown_field)
    end
  end

  describe "schema struct" do
    test "default struct has nil fields" do
      schema = %Schema{}
      assert schema.id == nil
      assert schema.key == nil
      assert schema.sketch_type == nil
      assert schema.data == nil
    end
  end

  defp errors_on(changeset, field) do
    Keyword.get_values(changeset.errors, field)
    |> Enum.map(fn {msg, _opts} -> msg end)
  end
end
