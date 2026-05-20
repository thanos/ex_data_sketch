defmodule ExDataSketch.Storage.MnesiaExtraTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.Storage.Mnesia

  setup_all do
    Application.ensure_started(:mnesia)
    on_exit(fn -> :mnesia.stop() end)
    :ok
  end

  setup do
    table = :"mnesia_extra_#{System.unique_integer([:positive])}"
    {:ok, _} = Mnesia.setup(table, ram_copies: [node()])

    on_exit(fn ->
      :mnesia.delete_table(table)
    end)

    %{table: table}
  end

  describe "save/3 error paths" do
    test "save returns :ok for valid sketch", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10)
      assert :ok = Mnesia.save(sketch, table, "valid")
    end
  end

  describe "load/3 error paths" do
    test "load returns error for wrong sketch type", %{table: table} do
      hll_sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      :ok = Mnesia.save(hll_sketch, table, "wrong_type")

      assert {:error, _} = Mnesia.load(ExDataSketch.CMS, table, "wrong_type")
    end
  end

  describe "setup/1 edge cases" do
    test "setup returns :ok :created for new table" do
      new_table = :"mnesia_new_#{System.unique_integer([:positive])}"
      assert {:ok, :created} = Mnesia.setup(new_table, ram_copies: [node()])
      :mnesia.delete_table(new_table)
    end

    test "setup with default table name" do
      table = :"mnesia_default_#{System.unique_integer([:positive])}"
      assert {:ok, :created} = Mnesia.setup(table, ram_copies: [node()])
      :mnesia.delete_table(table)
    end
  end

  describe "save/load/merge with multiple sketch types" do
    test "CMS round-trip through Mnesia", %{table: table} do
      sketch =
        ExDataSketch.CMS.new(width: 128, depth: 5)
        |> ExDataSketch.CMS.update("hello")
        |> ExDataSketch.CMS.update("world")

      :ok = Mnesia.save(sketch, table, "cms:rt")
      {:ok, loaded} = Mnesia.load(ExDataSketch.CMS, table, "cms:rt")
      assert ExDataSketch.CMS.estimate(loaded, "hello") >= 1
      assert ExDataSketch.CMS.estimate(loaded, "world") >= 1
    end

    test "Bloom round-trip through Mnesia", %{table: table} do
      sketch = ExDataSketch.Bloom.new(capacity: 1000) |> ExDataSketch.Bloom.put("hello")
      :ok = Mnesia.save(sketch, table, "bloom:rt")
      {:ok, loaded} = Mnesia.load(ExDataSketch.Bloom, table, "bloom:rt")
      assert ExDataSketch.Bloom.member?(loaded, "hello")
    end

    test "merge with CMS sketch", %{table: table} do
      sketch_a = ExDataSketch.CMS.new(width: 64, depth: 3) |> ExDataSketch.CMS.update("a")
      sketch_b = ExDataSketch.CMS.new(width: 64, depth: 3) |> ExDataSketch.CMS.update("b")
      :ok = Mnesia.save(sketch_a, table, "cms:merge")
      :ok = Mnesia.merge(sketch_b, table, "cms:merge")
      {:ok, merged} = Mnesia.load(ExDataSketch.CMS, table, "cms:merge")
      assert ExDataSketch.CMS.estimate(merged, "a") >= 1
      assert ExDataSketch.CMS.estimate(merged, "b") >= 1
    end
  end

  describe "delete/2 edge cases" do
    test "can save after delete", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      :ok = Mnesia.save(sketch, table, "hll:readd")
      :ok = Mnesia.delete(table, "hll:readd")
      assert Mnesia.load(ExDataSketch.HLL, table, "hll:readd") == {:error, :not_found}

      sketch2 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = Mnesia.save(sketch2, table, "hll:readd")
      {:ok, loaded} = Mnesia.load(ExDataSketch.HLL, table, "hll:readd")
      assert ExDataSketch.HLL.estimate(loaded) > 0.0
    end
  end
end
