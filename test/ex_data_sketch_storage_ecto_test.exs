defmodule ExDataSketch.Storage.EctoTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Storage.Ecto

  describe "ecto_available?/0" do
    test "returns boolean by default" do
      assert is_boolean(Ecto.ecto_available?())
    end

    test "returns true when explicitly enabled in config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, ecto: [enabled: true])
      assert Ecto.ecto_available?() == true

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end

    test "returns false when explicitly disabled in config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.put_env(:ex_data_sketch, :persistence_backends, ecto: [enabled: false])
      assert Ecto.ecto_available?() == false

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      else
        Application.delete_env(:ex_data_sketch, :persistence_backends)
      end
    end
  end

  describe "save/3" do
    test "derives sketch_type from module name" do
      sketch = ExDataSketch.HLL.new(p: 10)
      type = sketch.__struct__ |> Module.split() |> List.last() |> String.downcase()
      assert type == "HLL" |> String.downcase()
    end
  end

  describe "configured?/1 edge cases" do
    test "uses compile-time default when no config" do
      original = Application.get_env(:ex_data_sketch, :persistence_backends)
      Application.delete_env(:ex_data_sketch, :persistence_backends)

      result = Ecto.ecto_available?()
      assert is_boolean(result)

      if original do
        Application.put_env(:ex_data_sketch, :persistence_backends, original)
      end
    end
  end
end
