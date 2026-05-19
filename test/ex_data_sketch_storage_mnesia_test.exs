defmodule ExDataSketch.Storage.MnesiaTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.Storage.Mnesia

  setup_all do
    :mnesia.start()
    on_exit(fn -> :mnesia.stop() end)
    :ok
  end

  setup do
    table = :"mnesia_test_#{System.unique_integer([:positive])}"
    {:ok, _} = Mnesia.setup(table, ram_copies: [node()])

    on_exit(fn ->
      :mnesia.delete_table(table)
    end)

    %{table: table}
  end

  describe "save/3 and load/3" do
    test "saves and loads an HLL sketch", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      :ok = Mnesia.save(sketch, table, "hll:test")
      {:ok, loaded} = Mnesia.load(ExDataSketch.HLL, table, "hll:test")
      assert_in_delta ExDataSketch.HLL.estimate(loaded), ExDataSketch.HLL.estimate(sketch), 0.01
    end

    test "saves and loads a CMS sketch", %{table: table} do
      sketch = ExDataSketch.CMS.new(width: 64, depth: 3) |> ExDataSketch.CMS.update("hello")
      :ok = Mnesia.save(sketch, table, "cms:test")
      {:ok, loaded} = Mnesia.load(ExDataSketch.CMS, table, "cms:test")
      assert ExDataSketch.CMS.estimate(loaded, "hello") >= 1
    end

    test "saves and loads a Theta sketch", %{table: table} do
      sketch = ExDataSketch.Theta.new(log_k: 8) |> ExDataSketch.Theta.update("x")
      :ok = Mnesia.save(sketch, table, "theta:test")
      {:ok, loaded} = Mnesia.load(ExDataSketch.Theta, table, "theta:test")
      assert ExDataSketch.Theta.estimate(loaded) > 0.0
    end

    test "returns not_found for missing key", %{table: table} do
      assert Mnesia.load(ExDataSketch.HLL, table, "nonexistent") == {:error, :not_found}
    end

    test "overwrites existing key", %{table: table} do
      sketch1 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      sketch2 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = Mnesia.save(sketch1, table, "key")
      :ok = Mnesia.save(sketch2, table, "key")
      {:ok, loaded} = Mnesia.load(ExDataSketch.HLL, table, "key")
      assert_in_delta ExDataSketch.HLL.estimate(loaded), ExDataSketch.HLL.estimate(sketch2), 0.01
    end

    test "saves with atom key", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10)
      :ok = Mnesia.save(sketch, table, :my_key)
      {:ok, loaded} = Mnesia.load(ExDataSketch.HLL, table, :my_key)
      assert ExDataSketch.HLL.estimate(loaded) == 0.0
    end
  end

  describe "merge/3" do
    test "merges into existing key", %{table: table} do
      sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = Mnesia.save(sketch_a, table, "hll:merge")
      :ok = Mnesia.merge(sketch_b, table, "hll:merge")
      {:ok, merged} = Mnesia.load(ExDataSketch.HLL, table, "hll:merge")
      assert ExDataSketch.HLL.estimate(merged) >= 1.9
    end

    test "saves when key does not exist", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("x")
      :ok = Mnesia.merge(sketch, table, "hll:new")
      {:ok, loaded} = Mnesia.load(ExDataSketch.HLL, table, "hll:new")
      assert ExDataSketch.HLL.estimate(loaded) > 0.0
    end
  end

  describe "delete/2" do
    test "deletes an existing key", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10)
      :ok = Mnesia.save(sketch, table, "hll:del")
      :ok = Mnesia.delete(table, "hll:del")
      assert Mnesia.load(ExDataSketch.HLL, table, "hll:del") == {:error, :not_found}
    end

    test "delete is idempotent for missing key", %{table: table} do
      assert Mnesia.delete(table, "nonexistent") == :ok
    end
  end

  describe "setup/1" do
    test "returns already_exists for existing table", %{table: table} do
      assert {:ok, :already_exists} = Mnesia.setup(table, ram_copies: [node()])
    end
  end
end
