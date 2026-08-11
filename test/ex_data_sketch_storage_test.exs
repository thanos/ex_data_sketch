defmodule ExDataSketch.StorageTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Storage

  alias ExDataSketch.Storage
  alias ExDataSketch.Storage.{CubDB, DETS, Ecto, ETS, Mnesia}

  describe "Storage module" do
    test "ExDataSketch.Storage module is defined" do
      assert Code.ensure_loaded?(Storage)
    end

    test "ETS module is defined" do
      assert Code.ensure_loaded?(ETS)
    end

    test "DETS module is defined" do
      assert Code.ensure_loaded?(DETS)
    end

    test "Mnesia module is defined" do
      assert Code.ensure_loaded?(Mnesia)
    end
  end

  describe "backends/0" do
    test "maps every backend atom to its module" do
      assert Storage.backends() == %{
               ets: ETS,
               dets: DETS,
               cubdb: CubDB,
               mnesia: Mnesia,
               ecto: Ecto
             }
    end
  end

  describe "@behaviour ExDataSketch.Storage" do
    test "ETS, DETS, CubDB, and Mnesia declare the behaviour" do
      for module <- [ETS, DETS, CubDB, Mnesia] do
        behaviours =
          module.module_info(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten()

        assert Storage in behaviours,
               "#{inspect(module)} does not declare @behaviour ExDataSketch.Storage"
      end
    end

    if Code.ensure_loaded?(ExDataSketch.Storage.Ecto) do
      test "Ecto declares the behaviour when compiled" do
        behaviours =
          Ecto.module_info(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten()

        assert Storage in behaviours
      end
    end
  end
end
