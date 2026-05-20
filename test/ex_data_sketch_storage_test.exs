defmodule ExDataSketch.StorageTest do
  use ExUnit.Case, async: true

  describe "Storage module" do
    test "ExDataSketch.Storage module is defined" do
      assert Code.ensure_loaded?(ExDataSketch.Storage)
    end

    test "ETS module is defined" do
      assert Code.ensure_loaded?(ExDataSketch.Storage.ETS)
    end

    test "DETS module is defined" do
      assert Code.ensure_loaded?(ExDataSketch.Storage.DETS)
    end

    test "Mnesia module is defined" do
      assert Code.ensure_loaded?(ExDataSketch.Storage.Mnesia)
    end
  end
end
