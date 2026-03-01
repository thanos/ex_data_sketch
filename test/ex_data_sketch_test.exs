defmodule ExDataSketchTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch

  describe "update_many/2" do
    test "delegates to HLL.update_many for HLL structs" do
      assert_raise ExDataSketch.Errors.NotImplementedError,
                   ~r/hll_new is not yet implemented/,
                   fn ->
                     ExDataSketch.HLL.new() |> ExDataSketch.update_many(["a", "b"])
                   end
    end

    test "delegates to CMS.update_many for CMS structs" do
      assert_raise ExDataSketch.Errors.NotImplementedError,
                   ~r/cms_new is not yet implemented/,
                   fn ->
                     ExDataSketch.CMS.new() |> ExDataSketch.update_many(["a", "b"])
                   end
    end
  end
end
