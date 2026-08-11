defmodule ExDataSketch.Storage.CubDBTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.{CMS, HLL, Theta}
  alias ExDataSketch.Storage.CubDB, as: StorageCubDB

  setup context do
    tmp_dir = System.tmp_dir!() |> Path.join("cubdb_test_#{context.test}")
    File.rm_rf!(tmp_dir)
    File.mkdir_p!(tmp_dir)

    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    on_exit(fn ->
      if Process.alive?(db) do
        GenServer.stop(db, :normal)
      end

      File.rm_rf!(tmp_dir)
    end)

    %{db: db}
  end

  describe "save/3 and load/3" do
    test "saves and loads an HLL sketch", %{db: db} do
      sketch = HLL.new(p: 10) |> HLL.update("a")
      :ok = StorageCubDB.save(sketch, db, "hll:test")
      {:ok, loaded} = StorageCubDB.load(HLL, db, "hll:test")
      assert_in_delta HLL.estimate(loaded), HLL.estimate(sketch), 0.01
    end

    test "saves and loads a CMS sketch", %{db: db} do
      sketch = CMS.new(width: 64, depth: 3) |> CMS.update("hello")
      :ok = StorageCubDB.save(sketch, db, "cms:test")
      {:ok, loaded} = StorageCubDB.load(CMS, db, "cms:test")
      assert CMS.estimate(loaded, "hello") >= 1
    end

    test "saves and loads a Theta sketch", %{db: db} do
      sketch = Theta.new(log_k: 8) |> Theta.update("x")
      :ok = StorageCubDB.save(sketch, db, "theta:test")
      {:ok, loaded} = StorageCubDB.load(Theta, db, "theta:test")
      assert Theta.estimate(loaded) > 0.0
    end

    test "returns not_found for missing key", %{db: db} do
      assert StorageCubDB.load(HLL, db, "nonexistent") == {:error, :not_found}
    end

    test "overwrites existing key", %{db: db} do
      sketch1 = HLL.new(p: 10) |> HLL.update("a")
      sketch2 = HLL.new(p: 10) |> HLL.update("b")
      :ok = StorageCubDB.save(sketch1, db, "key")
      :ok = StorageCubDB.save(sketch2, db, "key")
      {:ok, loaded} = StorageCubDB.load(HLL, db, "key")
      assert_in_delta HLL.estimate(loaded), HLL.estimate(sketch2), 0.01
    end

    test "saves with atom key", %{db: db} do
      sketch = HLL.new(p: 10)
      :ok = StorageCubDB.save(sketch, db, :my_key)
      {:ok, loaded} = StorageCubDB.load(HLL, db, :my_key)
      assert HLL.estimate(loaded) == 0.0
    end
  end

  describe "merge/3" do
    test "merges into existing key", %{db: db} do
      sketch_a = HLL.new(p: 10) |> HLL.update("a")
      sketch_b = HLL.new(p: 10) |> HLL.update("b")
      :ok = StorageCubDB.save(sketch_a, db, "hll:merge")
      :ok = StorageCubDB.merge(sketch_b, db, "hll:merge")
      {:ok, merged} = StorageCubDB.load(HLL, db, "hll:merge")
      assert HLL.estimate(merged) > 1.0
    end

    test "saves when key does not exist", %{db: db} do
      sketch = HLL.new(p: 10) |> HLL.update("x")
      :ok = StorageCubDB.merge(sketch, db, "hll:new")
      {:ok, loaded} = StorageCubDB.load(HLL, db, "hll:new")
      assert HLL.estimate(loaded) > 0.0
    end

    test "merge is idempotent for same data", %{db: db} do
      sketch = HLL.new(p: 10) |> HLL.update("item")
      :ok = StorageCubDB.save(sketch, db, "hll:idem")
      :ok = StorageCubDB.merge(sketch, db, "hll:idem")
      {:ok, merged} = StorageCubDB.load(HLL, db, "hll:idem")
      assert HLL.estimate(merged) >= 1.0
    end
  end

  describe "delete/2" do
    test "deletes an existing key", %{db: db} do
      sketch = HLL.new(p: 10)
      :ok = StorageCubDB.save(sketch, db, "hll:del")
      :ok = StorageCubDB.delete(db, "hll:del")
      assert StorageCubDB.load(HLL, db, "hll:del") == {:error, :not_found}
    end

    test "delete is idempotent for missing key", %{db: db} do
      assert StorageCubDB.delete(db, "nonexistent") == :ok
    end
  end

  describe "cubdb_available?/0" do
    test "returns boolean" do
      assert is_boolean(StorageCubDB.cubdb_available?())
    end

    test "returns true when enabled in config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: [enabled: true])
      assert StorageCubDB.cubdb_available?() == true

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "returns false when disabled in config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: [enabled: false])
      assert StorageCubDB.cubdb_available?() == false

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end
  end
end
