defmodule ExDataSketch.KLLDataSketchesFixturesTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.KLL

  @fixtures_dir Path.join([__DIR__, "fixtures", "interop", "kll"])

  # Every case's items are 1.0, 2.0, ..., n*1.0 -- see generate.py and
  # README.md in the fixtures directory for provenance (datasketches
  # 5.2.0, pinned).
  @cases [
    %{name: "empty", n: 0, k: 200, min: nil, max: nil, median: nil},
    %{name: "single", n: 1, k: 200, min: 1.0, max: 1.0, median: 1.0},
    %{name: "small", n: 50, k: 200, min: 1.0, max: 50.0, median: 26.0},
    %{
      name: "large",
      n: 100_000,
      k: 200,
      min: 1.0,
      max: 100_000.0,
      median_floats: 49_977.0,
      median_doubles: 50_010.0
    },
    %{
      name: "k64",
      n: 5_000,
      k: 64,
      min: 1.0,
      max: 5_000.0,
      median_floats: 2_457.0,
      median_doubles: 2_442.0
    }
  ]

  for %{name: name} = case_attrs <- @cases do
    for {ds_variant, variant} <- [{"floats", :float}, {"doubles", :double}] do
      @case_attrs case_attrs
      @path Path.join(@fixtures_dir, "kll_ds_#{ds_variant}_#{name}.bin")
      @variant variant
      @ds_variant ds_variant

      test "#{@ds_variant} #{name} fixture decodes with correct stats" do
        %{n: expected_n, k: expected_k, min: expected_min, max: expected_max} = @case_attrs
        expected_median = Map.get(@case_attrs, :"median_#{@ds_variant}", @case_attrs[:median])

        binary = File.read!(@path)
        assert {:ok, sketch} = KLL.deserialize_datasketches(binary, variant: @variant)

        assert sketch.opts[:k] == expected_k
        assert KLL.count(sketch) == expected_n

        if expected_n > 0 do
          assert_in_delta KLL.min_value(sketch), expected_min, 1.0e-3
          assert_in_delta KLL.max_value(sketch), expected_max, 1.0e-3
          assert_in_delta KLL.quantile(sketch, 0.5), expected_median, 1.0e-3
        else
          assert KLL.min_value(sketch) == nil
          assert KLL.max_value(sketch) == nil
          assert KLL.quantile(sketch, 0.5) == nil
        end
      end
    end
  end

  describe "fixture corpus" do
    test "every declared case has both floats and doubles fixtures on disk" do
      for %{name: name} <- @cases do
        assert File.exists?(Path.join(@fixtures_dir, "kll_ds_floats_#{name}.bin"))
        assert File.exists?(Path.join(@fixtures_dir, "kll_ds_doubles_#{name}.bin"))
      end
    end
  end
end
