defmodule ExDataSketch.QuantilesTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Quantiles

  alias ExDataSketch.Quantiles

  describe "new/1" do
    test "defaults to KLL" do
      sketch = Quantiles.new()
      assert sketch.__struct__ == ExDataSketch.KLL
    end

    test "explicit type: :kll" do
      sketch = Quantiles.new(type: :kll, k: 100)
      assert sketch.__struct__ == ExDataSketch.KLL
      assert sketch.opts == [k: 100]
    end

    test "type: :ddsketch creates DDSketch" do
      sketch = Quantiles.new(type: :ddsketch, alpha: 0.01)
      assert sketch.__struct__ == ExDataSketch.DDSketch
      assert sketch.opts == [alpha: 0.01]
    end

    test "unknown type raises ArgumentError" do
      assert_raise ArgumentError, ~r/unknown quantile sketch type/, fn ->
        Quantiles.new(type: :unknown)
      end
    end
  end

  describe "facade dispatch" do
    test "update/2 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update(42.0)
      assert Quantiles.count(sketch) == 1
    end

    test "update_many/2 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update_many([1.0, 2.0, 3.0])
      assert Quantiles.count(sketch) == 3
    end

    test "merge/2 delegates to KLL" do
      a = Quantiles.new() |> Quantiles.update(1.0)
      b = Quantiles.new() |> Quantiles.update(2.0)
      merged = Quantiles.merge(a, b)
      assert Quantiles.count(merged) == 2
    end

    test "quantile/2 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update(42.0)
      assert Quantiles.quantile(sketch, 0.5) == 42.0
    end

    test "min_value/1 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update(42.0)
      assert Quantiles.min_value(sketch) == 42.0
    end

    test "max_value/1 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update(42.0)
      assert Quantiles.max_value(sketch) == 42.0
    end

    test "cdf/2 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update_many(1..100)
      cdf = Quantiles.cdf(sketch, [25.0, 75.0])
      assert length(cdf) == 2
    end

    test "pmf/2 delegates to KLL" do
      sketch = Quantiles.new() |> Quantiles.update_many(1..100)
      pmf = Quantiles.pmf(sketch, [50.0])
      assert length(pmf) == 2
    end

    test "cdf/2 delegates to REQ" do
      sketch = Quantiles.new(type: :req) |> Quantiles.update_many(1..100)
      cdf = Quantiles.cdf(sketch, [25.0, 75.0])
      assert length(cdf) == 2
    end

    test "pmf/2 delegates to REQ" do
      sketch = Quantiles.new(type: :req) |> Quantiles.update_many(1..100)
      pmf = Quantiles.pmf(sketch, [50.0])
      assert length(pmf) == 2
    end

    test "rank/2 delegates to DDSketch" do
      sketch = Quantiles.new(type: :ddsketch) |> Quantiles.update_many(1..100)
      r = Quantiles.rank(sketch, 50.0)
      assert is_float(r)
    end
  end

  describe "DDSketch unsupported operations" do
    test "cdf/2 raises ArgumentError for DDSketch" do
      sketch = Quantiles.new(type: :ddsketch) |> Quantiles.update(1.0)

      assert_raise ArgumentError, ~r/cdf\/2 is not supported for DDSketch/, fn ->
        Quantiles.cdf(sketch, [0.5])
      end
    end

    test "pmf/2 raises ArgumentError for DDSketch" do
      sketch = Quantiles.new(type: :ddsketch) |> Quantiles.update(1.0)

      assert_raise ArgumentError, ~r/pmf\/2 is not supported for DDSketch/, fn ->
        Quantiles.pmf(sketch, [0.5])
      end
    end
  end
end
