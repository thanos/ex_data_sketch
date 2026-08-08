defmodule ExDataSketch.IntegrationConfigTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.Integration

  @broadway_available Code.ensure_loaded?(Broadway)
  @flow_available Code.ensure_loaded?(Flow)
  @cubdb_available Code.ensure_loaded?(CubDB)
  @ecto_available Code.ensure_loaded?(Ecto.Adapters.SQL)

  describe "broadway_available?/0 with config override" do
    test "returns false when explicitly disabled" do
      original = Application.get_env(:ex_data_sketch, :integrations)
      Application.put_env(:ex_data_sketch, :integrations, broadway: false)
      assert Integration.broadway_available?() == false

      if original do
        Application.put_env(:ex_data_sketch, :integrations, original)
      else
        Application.delete_env(:ex_data_sketch, :integrations)
      end
    end

    test "respects compile-time availability when explicitly enabled" do
      original = Application.get_env(:ex_data_sketch, :integrations)
      Application.put_env(:ex_data_sketch, :integrations, broadway: true)
      assert Integration.broadway_available?() == @broadway_available

      if original do
        Application.put_env(:ex_data_sketch, :integrations, original)
      else
        Application.delete_env(:ex_data_sketch, :integrations)
      end
    end
  end

  describe "flow_available?/0 with config override" do
    test "respects compile-time availability when explicitly enabled" do
      original = Application.get_env(:ex_data_sketch, :integrations)
      Application.put_env(:ex_data_sketch, :integrations, flow: true)
      assert Integration.flow_available?() == @flow_available

      if original do
        Application.put_env(:ex_data_sketch, :integrations, original)
      else
        Application.delete_env(:ex_data_sketch, :integrations)
      end
    end
  end

  describe "cubdb_available?/0" do
    test "returns boolean by default" do
      assert is_boolean(Integration.cubdb_available?())
    end

    test "respects compile-time availability when backend enabled in config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: [enabled: true])
      assert Integration.cubdb_available?() == @cubdb_available

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "returns false when backend explicitly disabled" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: [enabled: false])
      assert Integration.cubdb_available?() == false

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end
  end

  describe "ecto_available?/0" do
    test "returns boolean by default" do
      assert is_boolean(Integration.ecto_available?())
    end

    test "respects compile-time availability when backend enabled in config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, ecto: [enabled: true])
      assert Integration.ecto_available?() == @ecto_available

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "returns false when backend explicitly disabled" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, ecto: [enabled: false])
      assert Integration.ecto_available?() == false

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end
  end

  describe "require_cubdb!/0" do
    test "returns :ok when CubDB is available and enabled" do
      original_backends = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: [enabled: true])

      if @cubdb_available do
        assert Integration.require_cubdb!() == :ok
      else
        assert_raise RuntimeError, ~r/CubDB persistence requires/, fn ->
          Integration.require_cubdb!()
        end
      end

      if original_backends do
        Application.put_env(:ex_data_sketch, :persistence_backends, original_backends)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "raises when CubDB is disabled" do
      original_backends = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: [enabled: false])

      assert_raise RuntimeError, ~r/CubDB persistence requires/, fn ->
        Integration.require_cubdb!()
      end

      if original_backends do
        Application.put_env(:ex_data_sketch, :persistence_backends, original_backends)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end
  end

  describe "require_ecto!/0" do
    test "raises when Ecto is disabled" do
      original_backends = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, ecto: [enabled: false])

      assert_raise RuntimeError, ~r/Ecto persistence requires/, fn ->
        Integration.require_ecto!()
      end

      if original_backends do
        Application.put_env(:ex_data_sketch, :persistence_backends, original_backends)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end
  end

  describe "configured_with_backends?/2 edge cases" do
    test "respects compile-time availability when backend config is true (not a keyword list)" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, cubdb: true)

      assert Integration.cubdb_available?() == @cubdb_available

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "returns false when backend config is false (not a keyword list)" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, ecto: false)

      assert Integration.ecto_available?() == false

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "returns compile-time default when no backend config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.delete_env(:ex_data_sketch, :persistence_backends)

      result = Integration.cubdb_available?()
      assert is_boolean(result)

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      end
    end
  end
end
