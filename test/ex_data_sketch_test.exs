defmodule ExDataSketchTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch

  describe "update_many/2" do
    test "delegates to HLL.update_many for HLL structs" do
      sketch = ExDataSketch.HLL.new(p: 10)
      updated = ExDataSketch.update_many(sketch, ["a", "b"])
      assert ExDataSketch.HLL.estimate(updated) > 0.0
    end

    test "delegates to CMS.update_many for CMS structs" do
      sketch = ExDataSketch.CMS.new()
      updated = ExDataSketch.update_many(sketch, ["a", "b", "a"])
      assert ExDataSketch.CMS.estimate(updated, "a") == 2
    end

    test "delegates to Theta.update_many for Theta structs" do
      sketch = ExDataSketch.Theta.new(k: 1024)
      updated = ExDataSketch.update_many(sketch, ["a", "b"])
      assert ExDataSketch.Theta.estimate(updated) > 0.0
    end
  end
end
