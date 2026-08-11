defmodule FakeTupleRefBackend do
  @moduledoc false
  # Minimal @behaviour ExDataSketch.Storage implementation whose ref type
  # is a 2-tuple with an atom head, e.g. {:primary, "conn"} -- exactly the
  # ambiguous shape that could otherwise be misread as an explicit
  # {backend_module, ref} pair. Backed by a bare Agent, not a real store;
  # only exists to prove Storage.resolve_backend/1's disambiguation.
  @behaviour ExDataSketch.Storage

  @impl true
  def save(sketch, _ref, key) do
    Agent.update(
      FakeTupleRefBackend.Store,
      &Map.put(&1, key, sketch.__struct__.serialize(sketch))
    )

    :ok
  end

  @impl true
  def load(sketch_module, _ref, key) do
    case Agent.get(FakeTupleRefBackend.Store, &Map.get(&1, key)) do
      nil -> {:error, :not_found}
      binary -> sketch_module.deserialize(binary)
    end
  end

  @impl true
  def merge(sketch, ref, key) do
    case load(sketch.__struct__, ref, key) do
      {:ok, existing} -> save(sketch.__struct__.merge(existing, sketch), ref, key)
      {:error, :not_found} -> save(sketch, ref, key)
    end
  end

  @impl true
  def delete(_ref, key) do
    Agent.update(FakeTupleRefBackend.Store, &Map.delete(&1, key))
    :ok
  end

  defmodule Store do
  end
end

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

  test "merge/3 returns {:error, _} instead of crashing on corrupted stored data", context do
    for backend_name <- @backends do
      {backend_mod, ref} = backend_ref = backend_ref(backend_name, context)

      # Write directly through the backend's own save/3 with a sketch, then
      # corrupt what's stored by writing raw garbage through the same path
      # a real corruption or version mismatch would leave behind.
      sketch = HLL.new(p: 10) |> HLL.update("a")
      assert Storage.save(sketch, backend_ref, "corrupt_key") == :ok
      corrupt_stored_binary(backend_mod, ref, "corrupt_key")

      assert {:error, _reason} = Storage.merge(sketch, backend_ref, "corrupt_key"),
             "#{backend_name}: merge/3 crashed or returned :ok on corrupted data instead of {:error, _}"
    end
  end

  test "merge/3 is atomic under concurrent writers for Mnesia and CubDB, " <>
         "and documented-non-atomic for ETS and DETS",
       context do
    concurrency = 25

    for backend_name <- @backends do
      backend_ref = backend_ref(backend_name, context)
      base = HLL.new(p: 12) |> HLL.update("seed")
      assert Storage.save(base, backend_ref, "concurrent_key") == :ok

      0..(concurrency - 1)
      |> Task.async_stream(
        fn i ->
          sketch = HLL.new(p: 12) |> HLL.update("item_#{i}")
          Storage.merge(sketch, backend_ref, "concurrent_key")
        end,
        max_concurrency: concurrency,
        timeout: 5_000
      )
      |> Stream.run()

      assert {:ok, final} = Storage.load(HLL, backend_ref, "concurrent_key")
      estimate = HLL.estimate(final)

      case backend_name do
        atomic when atomic in [:mnesia, :cubdb] ->
          # All 26 distinct elements (1 seed + 25 concurrent) should survive.
          assert_in_delta estimate,
                          concurrency + 1,
                          3,
                          "#{backend_name}: expected atomic merge to preserve ~#{concurrency + 1} " <>
                            "distinct elements, got #{estimate}"

        lossy when lossy in [:ets, :dets] ->
          # Documented limitation, locked in as a regression guard: a
          # non-atomic read-modify-write cycle loses updates under
          # concurrent writers. If this ever starts passing with the full
          # count, the docs promising non-atomicity should be revisited
          # (or the implementation quietly got fixed, which would be nice
          # but should be a deliberate, documented change).
          assert estimate < concurrency,
                 "#{backend_name}: expected lossy concurrent merge (documented limitation), " <>
                   "but all updates survived (estimate=#{estimate}) -- if this backend is now " <>
                   "atomic, update its docs and this test deliberately"
      end
    end
  end

  defp corrupt_stored_binary(ETS, table, key), do: :ets.insert(table, {key, "not a valid frame"})

  defp corrupt_stored_binary(DETS, table, key),
    do: :dets.insert(table, {key, "not a valid frame"})

  defp corrupt_stored_binary(StorageCubDB, db, key) do
    CubDB.put(db, key, "not a valid frame")
  end

  defp corrupt_stored_binary(Mnesia, table, key) do
    {:atomic, :ok} =
      :mnesia.transaction(fn -> :mnesia.write({table, key, "not a valid frame"}) end)

    :ok
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

    test "a 2-tuple bare ref whose first element isn't a real backend module " <>
           "resolves whole against the default, not misread as {module, ref}" do
      # A custom backend's own ref type can legitimately be a 2-tuple with
      # an atom head (e.g. {:primary, connection} for a pooled connection)
      # -- none of the 5 shipped backends have such a ref shape, so this
      # uses a minimal fake backend (defined below) to exercise it for real
      # rather than merely asserting on resolve_backend/1's internals.
      Agent.start_link(fn -> %{} end, name: FakeTupleRefBackend.Store)

      original = Application.get_env(:ex_data_sketch, :storage)
      Application.put_env(:ex_data_sketch, :storage, backend: FakeTupleRefBackend)

      on_exit(fn ->
        if original do
          Application.put_env(:ex_data_sketch, :storage, original)
        else
          Application.delete_env(:ex_data_sketch, :storage)
        end

        if pid = Process.whereis(FakeTupleRefBackend.Store), do: Agent.stop(pid)
      end)

      sketch = HLL.new(p: 10) |> HLL.update("a")
      # {:not_a_real_backend_module, "conn"} looks structurally identical to
      # an explicit {backend_module, ref} pair, but :not_a_real_backend_module
      # doesn't implement the Storage behaviour -- it must be resolved as a
      # single bare ref against the configured default (the fake backend
      # here, whose ref type is exactly this kind of tuple) instead of
      # being split apart and raising a confusing UndefinedFunctionError.
      bare_ref = {:not_a_real_backend_module, "conn"}
      assert Storage.save(sketch, bare_ref, "key") == :ok
      assert {:ok, loaded} = Storage.load(HLL, bare_ref, "key")
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
