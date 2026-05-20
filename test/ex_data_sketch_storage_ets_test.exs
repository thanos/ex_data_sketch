defmodule ExDataSketch.Storage.ETSTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Storage.ETS

  setup do
    table = :"ets_test_#{System.unique_integer([:positive])}"
    :ets.new(table, [:set, :public, :named_table])

    on_exit(fn ->
      case :ets.info(table) do
        :undefined -> :ok
        _ -> :ets.delete(table)
      end
    end)

    %{table: table}
  end

  describe "save/3 and load/3" do
    test "saves and loads an HLL sketch", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      :ok = ETS.save(sketch, table, "hll:test")
      {:ok, loaded} = ETS.load(ExDataSketch.HLL, table, "hll:test")
      assert_in_delta ExDataSketch.HLL.estimate(loaded), ExDataSketch.HLL.estimate(sketch), 0.01
    end

    test "saves and loads a CMS sketch", %{table: table} do
      sketch = ExDataSketch.CMS.new(width: 64, depth: 3) |> ExDataSketch.CMS.update("hello")
      :ok = ETS.save(sketch, table, "cms:test")
      {:ok, loaded} = ETS.load(ExDataSketch.CMS, table, "cms:test")
      assert ExDataSketch.CMS.estimate(loaded, "hello") >= 1
    end

    test "saves and loads a Theta sketch", %{table: table} do
      sketch = ExDataSketch.Theta.new(k: 256) |> ExDataSketch.Theta.update("x")
      :ok = ETS.save(sketch, table, "theta:test")
      {:ok, loaded} = ETS.load(ExDataSketch.Theta, table, "theta:test")
      assert ExDataSketch.Theta.estimate(loaded) > 0.0
    end

    test "returns not_found for missing key", %{table: table} do
      assert ETS.load(ExDataSketch.HLL, table, "nonexistent") == {:error, :not_found}
    end

    test "overwrites existing key", %{table: table} do
      sketch1 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      sketch2 = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = ETS.save(sketch1, table, "key")
      :ok = ETS.save(sketch2, table, "key")
      {:ok, loaded} = ETS.load(ExDataSketch.HLL, table, "key")
      assert_in_delta ExDataSketch.HLL.estimate(loaded), ExDataSketch.HLL.estimate(sketch2), 0.01
    end

    test "saves with atom key", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10)
      :ok = ETS.save(sketch, table, :my_key)
      {:ok, loaded} = ETS.load(ExDataSketch.HLL, table, :my_key)
      assert ExDataSketch.HLL.estimate(loaded) == 0.0
    end
  end

  describe "merge/3" do
    test "merges into existing key", %{table: table} do
      sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      :ok = ETS.save(sketch_a, table, "hll:merge")
      :ok = ETS.merge(sketch_b, table, "hll:merge")
      {:ok, merged} = ETS.load(ExDataSketch.HLL, table, "hll:merge")
      assert ExDataSketch.HLL.estimate(merged) >= 1.9
    end

    test "saves when key does not exist", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("x")
      :ok = ETS.merge(sketch, table, "hll:new")
      {:ok, loaded} = ETS.load(ExDataSketch.HLL, table, "hll:new")
      assert ExDataSketch.HLL.estimate(loaded) > 0.0
    end

    test "merge is idempotent for same data", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("item")
      :ok = ETS.save(sketch, table, "hll:idem")
      :ok = ETS.merge(sketch, table, "hll:idem")
      {:ok, merged} = ETS.load(ExDataSketch.HLL, table, "hll:idem")
      assert ExDataSketch.HLL.estimate(merged) >= 1.0
    end
  end

  describe "delete/2" do
    test "deletes an existing key", %{table: table} do
      sketch = ExDataSketch.HLL.new(p: 10)
      :ok = ETS.save(sketch, table, "hll:del")
      :ok = ETS.delete(table, "hll:del")
      assert ETS.load(ExDataSketch.HLL, table, "hll:del") == {:error, :not_found}
    end

    test "delete is idempotent for missing key", %{table: table} do
      assert ETS.delete(table, "nonexistent") == :ok
    end
  end

  describe "table type validation" do
    test "raises on bag table type" do
      table = :"ets_bag_#{System.unique_integer([:positive])}"
      :ets.new(table, [:bag, :public, :named_table])

      assert_raise ArgumentError, fn ->
        sketch = ExDataSketch.HLL.new(p: 10)
        ETS.save(sketch, table, "bad")
      end
    end

    test "raises on nonexistent table" do
      assert_raise ArgumentError, fn ->
        sketch = ExDataSketch.HLL.new(p: 10)
        ETS.save(sketch, :nonexistent_table_12345, "bad")
      end
    end
  end

  describe "round-trip with all sketch types" do
    test "CMS round-trip", %{table: table} do
      sketch =
        ExDataSketch.CMS.new(width: 128, depth: 5)
        |> ExDataSketch.CMS.update("hello")
        |> ExDataSketch.CMS.update("world")

      :ok = ETS.save(sketch, table, "cms:rt")
      {:ok, loaded} = ETS.load(ExDataSketch.CMS, table, "cms:rt")
      assert ExDataSketch.CMS.estimate(loaded, "hello") >= 1
      assert ExDataSketch.CMS.estimate(loaded, "world") >= 1
    end

    test "Bloom round-trip", %{table: table} do
      sketch =
        ExDataSketch.Bloom.new(capacity: 1000)
        |> ExDataSketch.Bloom.put("hello")

      :ok = ETS.save(sketch, table, "bloom:rt")
      {:ok, loaded} = ETS.load(ExDataSketch.Bloom, table, "bloom:rt")
      assert ExDataSketch.Bloom.member?(loaded, "hello")
    end
  end
end
