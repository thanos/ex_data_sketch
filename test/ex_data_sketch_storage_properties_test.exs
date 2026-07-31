defmodule ExDataSketch.Storage.PropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Storage.{DETS, ETS}

  defp string_list(min_len, max_len, list_min, list_max) do
    StreamData.list_of(
      StreamData.string(:alphanumeric, min_length: min_len, max_length: max_len),
      length: list_min..list_max
    )
  end

  property "ETS save then load produces equivalent HLL sketch" do
    check all(items <- string_list(5, 20, 1, 50)) do
      table = :"ets_prop_rt_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(items)
      ETS.save(sketch, table, "prop:hll")
      {:ok, loaded} = ETS.load(ExDataSketch.HLL, table, "prop:hll")

      assert_in_delta ExDataSketch.HLL.estimate(loaded),
                      ExDataSketch.HLL.estimate(sketch),
                      0.5

      :ets.delete(table)
    end
  end

  property "ETS save then load produces equivalent CMS sketch" do
    check all(items <- string_list(5, 20, 1, 30)) do
      table = :"ets_prop_cms_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      sketch =
        ExDataSketch.CMS.new(width: 128, depth: 5)
        |> ExDataSketch.CMS.update_many(items)

      ETS.save(sketch, table, "prop:cms")
      {:ok, loaded} = ETS.load(ExDataSketch.CMS, table, "prop:cms")

      for item <- Enum.uniq(items) do
        assert ExDataSketch.CMS.estimate(loaded, item) >=
                 ExDataSketch.CMS.estimate(sketch, item) - 1
      end

      :ets.delete(table)
    end
  end

  property "ETS merge produces correct cardinality" do
    check all(
            items_a <- string_list(3, 8, 0, 20),
            items_b <- string_list(3, 8, 0, 20)
          ) do
      table = :"ets_prop_merge_#{System.unique_integer([:positive])}"
      :ets.new(table, [:set, :public, :named_table])

      sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(items_a)
      sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(items_b)

      ETS.save(sketch_a, table, "prop:merge")
      ETS.merge(sketch_b, table, "prop:merge")
      {:ok, merged} = ETS.load(ExDataSketch.HLL, table, "prop:merge")

      expected = MapSet.new(items_a ++ items_b) |> MapSet.size()
      real_estimate = ExDataSketch.HLL.estimate(merged)

      if expected > 0 do
        tolerance = if expected < 5, do: 0.6, else: 0.3
        assert abs(real_estimate - expected) / max(expected, 1) < tolerance
      end

      :ets.delete(table)
    end
  end

  describe "DETS persistence" do
    @tag :integration
    property "DETS save then load produces equivalent HLL sketch" do
      check all(items <- string_list(5, 20, 1, 50)) do
        temp_dir = System.tmp_dir!()
        temp_file = Path.join(temp_dir, "dets_prop_hll_#{System.system_time()}")
        table = :"#{temp_file}"
        {:ok, _} = :dets.open_file(table, type: :set)

        sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(items)
        DETS.save(sketch, table, "prop:hll")
        {:ok, loaded} = DETS.load(ExDataSketch.HLL, table, "prop:hll")

        assert_in_delta ExDataSketch.HLL.estimate(loaded),
                        ExDataSketch.HLL.estimate(sketch),
                        0.5

        :dets.close(table)
      end
    end

    @tag :integration
    property "DETS merge produces correct cardinality" do
      check all(
              items_a <- string_list(3, 8, 5, 30),
              items_b <- string_list(3, 8, 5, 30)
            ) do
        temp_dir = System.tmp_dir!()
        temp_file = Path.join(temp_dir, "dets_prop_hll_#{System.system_time()}")
        table = :"#{temp_file}"
        # table = :"dets_prop_merge_#{System.unique_integer([:positive])}"
        {:ok, _} = :dets.open_file(table, type: :set)

        sketch_a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(items_a)
        sketch_b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update_many(items_b)

        DETS.save(sketch_a, table, "prop:merge")
        DETS.merge(sketch_b, table, "prop:merge")
        {:ok, merged} = DETS.load(ExDataSketch.HLL, table, "prop:merge")

        expected = MapSet.new(items_a ++ items_b) |> MapSet.size()
        real_estimate = ExDataSketch.HLL.estimate(merged)

        tolerance = if expected < 5, do: 0.6, else: 0.3
        assert abs(real_estimate - expected) / max(expected, 1) < tolerance

        :dets.close(table)
      end
    end
  end
end
