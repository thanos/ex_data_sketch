defmodule ExDataSketch.Storage.SharedFacadeTest do
  # Exercises ExDataSketch.Storage's facade (save/3, load/3, merge/3,
  # delete/2) identically across every backend that can be live-tested
  # without an external service: ETS, DETS, CubDB, Mnesia.
  #
  # ExDataSketch.Storage.Ecto is intentionally excluded. It requires a live
  # database connection and a configured Ecto.Repo; the existing
  # ExDataSketch.Storage.EctoTest does not exercise a live database either
  # (only ecto_available?/0 and pure helper functions), so there is no
  # existing test-database setup in this suite to reuse.
  use ExUnit.Case, async: false

  alias ExDataSketch.HLL
  alias ExDataSketch.Storage
  alias ExDataSketch.Storage.CubDB, as: StorageCubDB
  alias ExDataSketch.Storage.{DETS, ETS, Mnesia}

  setup_all do
    Application.ensure_started(:mnesia)
    on_exit(fn -> :mnesia.stop() end)
    :ok
  end

  @backends [:ets, :dets, :cubdb, :mnesia]

  test "save/3, load/3, merge/3, delete/2 behave identically across backends", context do
    for backend_name <- @backends do
      backend_ref = backend_ref(backend_name, context)

      a = HLL.new(p: 10) |> HLL.update("a")
      b = HLL.new(p: 10) |> HLL.update("b")

      assert Storage.save(a, backend_ref, "key") == :ok,
             "#{backend_name}: save/3 did not return :ok"

      assert {:ok, loaded} = Storage.load(HLL, backend_ref, "key")

      assert_in_delta HLL.estimate(loaded),
                      HLL.estimate(a),
                      0.01,
                      "#{backend_name}: load/3 mismatch"

      assert Storage.merge(b, backend_ref, "key") == :ok,
             "#{backend_name}: merge/3 did not return :ok"

      assert {:ok, merged} = Storage.load(HLL, backend_ref, "key")
      assert HLL.estimate(merged) >= HLL.estimate(a), "#{backend_name}: merge/3 did not combine"

      assert Storage.load(HLL, backend_ref, "nonexistent") == {:error, :not_found},
             "#{backend_name}: load/3 did not report :not_found"

      assert Storage.delete(backend_ref, "key") == :ok,
             "#{backend_name}: delete/2 did not return :ok"

      assert Storage.load(HLL, backend_ref, "key") == {:error, :not_found},
             "#{backend_name}: load/3 found a value after delete/2"
    end
  end

  describe "backend_ref resolution" do
    test "a bare ref resolves against the configured default backend" do
      table = :"storage_shared_default_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      original = Application.get_env(:ex_data_sketch, :storage)
      Application.put_env(:ex_data_sketch, :storage, backend: ETS)

      on_exit(fn ->
        if original do
          Application.put_env(:ex_data_sketch, :storage, original)
        else
          Application.delete_env(:ex_data_sketch, :storage)
        end
      end)

      sketch = HLL.new(p: 10) |> HLL.update("a")
      assert Storage.save(sketch, table, "key") == :ok
      assert {:ok, loaded} = Storage.load(HLL, table, "key")
      assert HLL.estimate(loaded) > 0.0
    end

    test "a bare ref with no configured default backend raises InvalidOptionError" do
      original = Application.get_env(:ex_data_sketch, :storage)
      Application.delete_env(:ex_data_sketch, :storage)

      on_exit(fn ->
        if original, do: Application.put_env(:ex_data_sketch, :storage, original)
      end)

      sketch = HLL.new(p: 10)

      assert_raise ExDataSketch.Errors.InvalidOptionError, fn ->
        Storage.save(sketch, :some_bare_ref, "key")
      end
    end
  end

  defp backend_ref(:ets, _context) do
    # No on_exit cleanup: the table is owned by this test process and is
    # destroyed automatically when the process exits, which happens before
    # on_exit callbacks run (in a separate process).
    table = :"storage_shared_ets_#{System.unique_integer([:positive])}"
    :ets.new(table, [:set, :public, :named_table])
    {ETS, table}
  end

  defp backend_ref(:dets, context) do
    table = :"storage_shared_dets_#{System.unique_integer([:positive])}"
    tmp_dir = System.tmp_dir!() |> Path.join("storage_shared_dets_#{context.test}")
    File.mkdir_p!(tmp_dir)
    file = tmp_dir |> Path.join("#{table}.dets") |> String.to_charlist()
    {:ok, _} = :dets.open_file(table, type: :set, file: file)

    on_exit(fn ->
      :dets.close(table)
      File.rm_rf!(tmp_dir)
    end)

    {DETS, table}
  end

  defp backend_ref(:cubdb, context) do
    tmp_dir = System.tmp_dir!() |> Path.join("storage_shared_cubdb_#{context.test}")
    File.rm_rf!(tmp_dir)
    File.mkdir_p!(tmp_dir)
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    on_exit(fn ->
      if Process.alive?(db), do: GenServer.stop(db, :normal)
      File.rm_rf!(tmp_dir)
    end)

    {StorageCubDB, db}
  end

  defp backend_ref(:mnesia, _context) do
    table = :"storage_shared_mnesia_#{System.unique_integer([:positive])}"
    {:ok, _} = Mnesia.setup(table, ram_copies: [node()])
    on_exit(fn -> :mnesia.delete_table(table) end)
    {Mnesia, table}
  end
end
