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

    test "type: :ddsketch raises NotImplementedError" do
      assert_raise ExDataSketch.Errors.NotImplementedError, fn ->
        Quantiles.new(type: :ddsketch)
      end
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
  end
end
