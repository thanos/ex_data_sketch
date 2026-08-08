defmodule ExDataSketch.Storage.Ecto.MigrationTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Storage.Ecto.Migration

  describe "table_name/0" do
    test "returns the expected table name" do
      assert Migration.table_name() == "ex_data_sketch_sketches"
    end
  end

  describe "up_commands/0" do
    test "returns a list of two SQL commands" do
      commands = Migration.up_commands()
      assert length(commands) == 2
    end

    test "first command creates the table" do
      [create_table | _] = Migration.up_commands()
      assert create_table =~ "CREATE TABLE IF NOT EXISTS ex_data_sketch_sketches"
      assert create_table =~ "id BIGSERIAL PRIMARY KEY"
      assert create_table =~ "key VARCHAR(255) NOT NULL"
      assert create_table =~ "sketch_type VARCHAR(63) NOT NULL"
      assert create_table =~ "data BYTEA NOT NULL"
      assert create_table =~ "inserted_at TIMESTAMP NOT NULL DEFAULT NOW()"
      assert create_table =~ "updated_at TIMESTAMP NOT NULL DEFAULT NOW()"
    end

    test "second command creates unique index" do
      [_, create_index] = Migration.up_commands()
      assert create_index =~ "CREATE UNIQUE INDEX IF NOT EXISTS"
      assert create_index =~ "ex_data_sketch_sketches_key_index"
      assert create_index =~ "ON ex_data_sketch_sketches (key)"
    end
  end

  describe "down_commands/0" do
    test "returns a list of one SQL command" do
      commands = Migration.down_commands()
      assert length(commands) == 1
    end

    test "drops the table" do
      [drop_table] = Migration.down_commands()
      assert drop_table =~ "DROP TABLE IF EXISTS ex_data_sketch_sketches"
    end
  end
end
