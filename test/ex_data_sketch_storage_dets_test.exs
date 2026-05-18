defmodule ExDataSketch.Storage.DETSTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.Storage.DETS

  setup do
    table = :"dets_test_#{System.unique_integer([:positive])}"
    {:ok, _} = :dets.open_file(table, type: :set)
    on_exit(fn -> :dets.close(table) end)
    %{table: table}
  end

  describe "save/3 and load/3" do
    test "saves and loads an HLL sketch", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      :ok = DETS.save(sketch, table, "hll:test")
      {:ok, loaded} = DETS.load(ExDataSketch.HLL, table, "hll:test")
      assert_in_delta ExDataSketch.HLL.estimate(loaded), ExDataSketch.HLL.estimate(sketch), 0.01
    end

    test "saves and loads a CMS sketch", %{table: table} do
      sketch = ExDataSketch.CMS.new(width: 64, depth: 3) |> ExDataSketch.CMS.update("hello")
      assert DETS.save(sketch, table, "cms:test") == :ok
      {:ok, loaded} = DETS.load(ExDataSketch.CMS, table, "cms:test")
      assert ExDataSketch.CMS.estimate(loaded, "hello") >= 1
    end

    test "returns not_found for missing key", %{table: table} do
      assert DETS.load(ExDataSketch.HLL, table, "nonexistent") == {:error, :not_found}
    end

    test "overwrites existing key", %{table: table} do
      sketch1 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      sketch2 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = DETS.save(sketch1, table, "key")
      :ok = DETS.save(sketch2, table, "key")
      {:ok, loaded} = DETS.load(ExDataSketch.HLL, table, "key")
      assert_in_delta ExDataSketch.HLL.estimate(loaded), ExDataSketch.HLL.estimate(sketch2), 0.01
    end
  end

  describe "merge/3" do
    test "merges into existing key", %{table: table} do
      sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = DETS.save(sketch_a, table, "hll:merge")
      :ok = DETS.merge(sketch_b, table, "hll:merge")
      {:ok, merged} = DETS.load(ExDataSketch.HLL, table, "hll:merge")
      assert ExDataSketch.HLL.estimate(merged) >= 1.9
    end

    test "saves when key does not exist", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("x")
      :ok = DETS.merge(sketch, table, "hll:new")
      {:ok, loaded} = DETS.load(ExDataSketch.HLL, table, "hll:new")
      assert ExDataSketch.HLL.estimate(loaded) > 0.0
    end
  end

  describe "delete/2" do
    test "deletes an existing key", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10)
      :ok = DETS.save(sketch, table, "hll:del")
      :ok = DETS.delete(table, "hll:del")
      assert DETS.load(ExDataSketch.HLL, table, "hll:del") == {:error, :not_found}
    end

    test "delete is idempotent for missing key", %{table: table} do
      assert DETS.delete(table, "nonexistent") == :ok
    end
  end
end
