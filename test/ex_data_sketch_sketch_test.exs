defmodule ExDataSketch.SketchTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Sketch

  alias ExDataSketch.Sketch

  describe "implemented?/1" do
    test "returns true for every module in the sketch registry (capability matrix)" do
      for {type, module} <- ExDataSketch.sketches() do
        assert Sketch.implemented?(module), "#{inspect(type)} => #{inspect(module)}"
      end
    end

    test "returns false for a non-sketch module" do
      refute Sketch.implemented?(Enum)
      refute Sketch.implemented?(String)
    end

    test "returns false for a module that does not exist" do
      refute Sketch.implemented?(ExDataSketch.DoesNotExist)
    end
  end
end
