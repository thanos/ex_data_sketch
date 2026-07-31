defmodule ExDataSketch.IntegrationTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Integration

  describe "broadway_available?/0" do
    test "returns a boolean" do
      assert is_boolean(Integration.broadway_available?())
    end
  end

  describe "flow_available?/0" do
    test "returns a boolean" do
      assert is_boolean(Integration.flow_available?())
    end
  end

  describe "require_broadway!/0" do
    test "returns :ok when Broadway is available" do
      if Integration.broadway_available?() do
        assert Integration.require_broadway!() == :ok
      end
    end

    test "raises clear error when Broadway is not available" do
      unless Integration.broadway_available?() do
        assert_raise RuntimeError, ~r/Broadway integration requires/, fn ->
          Integration.require_broadway!()
        end
      end
    end
  end

  describe "require_flow!/0" do
    test "returns :ok when Flow is available" do
      if Integration.flow_available?() do
        assert Integration.require_flow!() == :ok
      end
    end

    test "raises clear error when Flow is not available" do
      unless Integration.flow_available?() do
        assert_raise RuntimeError, ~r/Flow integration requires/, fn ->
          Integration.require_flow!()
        end
      end
    end
  end

  describe "cubdb_available?/0" do
    test "returns a boolean" do
      assert is_boolean(Integration.cubdb_available?())
    end
  end

  describe "ecto_available?/0" do
    test "returns a boolean" do
      assert is_boolean(Integration.ecto_available?())
    end
  end

  describe "require_cubdb!/0" do
    test "returns :ok when CubDB is available" do
      if Integration.cubdb_available?() do
        assert Integration.require_cubdb!() == :ok
      end
    end

    test "raises clear error when CubDB is not available" do
      unless Integration.cubdb_available?() do
        assert_raise RuntimeError, ~r/CubDB persistence requires/, fn ->
          Integration.require_cubdb!()
        end
      end
    end
  end

  describe "require_ecto!/0" do
    test "returns :ok when Ecto is available" do
      if Integration.ecto_available?() do
        assert Integration.require_ecto!() == :ok
      end
    end

    test "raises clear error when Ecto is not available" do
      unless Integration.ecto_available?() do
        assert_raise RuntimeError, ~r/Ecto persistence requires/, fn ->
          Integration.require_ecto!()
        end
      end
    end
  end
end
