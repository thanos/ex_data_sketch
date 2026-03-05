defmodule ExDataSketch.DDSketchTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.DDSketch

  # Fibonacci-like dataset: 16 items, min=1.0, max=144.0
  @values_v1 [
    1.0,
    1.0,
    2.0,
    3.0,
    5.0,
    8.0,
    13.0,
    21.0,
    34.0,
    55.0,
    89.0,
    144.0,
    1.0,
    1.0,
    2.0,
    3.0
  ]

  describe "struct" do
    test "has correct fields" do
      sketch = %DDSketch{state: <<>>, opts: [alpha: 0.01], backend: ExDataSketch.Backend.Pure}
      assert sketch.state == <<>>
      assert sketch.opts == [alpha: 0.01]
      assert sketch.backend == ExDataSketch.Backend.Pure
    end
  end

  describe "option validation" do
    test "alpha defaults to 0.01" do
      sketch = DDSketch.new()
      assert sketch.opts == [alpha: 0.01]
    end

    test "accepts valid alpha values" do
      for alpha <- [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 0.99] do
        sketch = DDSketch.new(alpha: alpha)
        assert sketch.opts == [alpha: alpha]
      end
    end

    test "alpha must be a float in (0.0, 1.0)" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: 0.0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: 1.0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: -0.5)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: 1.5)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: "not a float")
      end
    end
  end

  describe "empty sketch" do
    test "count is 0" do
      sketch = DDSketch.new()
      assert DDSketch.count(sketch) == 0
    end

    test "min_value is nil" do
      sketch = DDSketch.new()
      assert DDSketch.min_value(sketch) == nil
    end

    test "max_value is nil" do
      sketch = DDSketch.new()
      assert DDSketch.max_value(sketch) == nil
    end

    test "quantile returns nil" do
      sketch = DDSketch.new()
      assert DDSketch.quantile(sketch, 0.5) == nil
    end

    test "size_bytes is positive" do
      sketch = DDSketch.new()
      assert DDSketch.size_bytes(sketch) == 88
    end
  end

  describe "single item" do
    test "count is 1" do
      sketch = DDSketch.new() |> DDSketch.update(42.0)
      assert DDSketch.count(sketch) == 1
    end

    test "min equals max equals value" do
      sketch = DDSketch.new() |> DDSketch.update(42.0)
      assert DDSketch.min_value(sketch) == 42.0
      assert DDSketch.max_value(sketch) == 42.0
    end

    test "quantile at 0.0 returns min_value" do
      sketch = DDSketch.new() |> DDSketch.update(42.0)
      assert DDSketch.quantile(sketch, 0.0) == 42.0
    end

    test "quantile at 1.0 returns max_value" do
      sketch = DDSketch.new() |> DDSketch.update(42.0)
      assert DDSketch.quantile(sketch, 1.0) == 42.0
    end

    test "quantile at 0.5 returns approximate value" do
      sketch = DDSketch.new(alpha: 0.01) |> DDSketch.update(42.0)
      q = DDSketch.quantile(sketch, 0.5)
      # Within 1% relative error
      assert abs(q - 42.0) / 42.0 < 0.01
    end
  end

  describe "deterministic dataset" do
    setup do
      sketch = DDSketch.new(alpha: 0.01) |> DDSketch.update_many(@values_v1)
      %{sketch: sketch}
    end

    test "count is 16", %{sketch: sketch} do
      assert DDSketch.count(sketch) == 16
    end

    test "min is 1.0", %{sketch: sketch} do
      assert DDSketch.min_value(sketch) == 1.0
    end

    test "max is 144.0", %{sketch: sketch} do
      assert DDSketch.max_value(sketch) == 144.0
    end

    test "p50 in expected range", %{sketch: sketch} do
      p50 = DDSketch.quantile(sketch, 0.5)
      assert p50 >= 2.5 and p50 <= 5.5, "p50 = #{p50}, expected in [2.5, 5.5]"
    end

    test "p90 in expected range", %{sketch: sketch} do
      p90 = DDSketch.quantile(sketch, 0.9)
      assert p90 >= 50.0 and p90 <= 150.0, "p90 = #{p90}, expected in [50.0, 150.0]"
    end

    test "p99 in expected range", %{sketch: sketch} do
      p99 = DDSketch.quantile(sketch, 0.99)
      assert p99 >= 89.0 and p99 <= 150.0, "p99 = #{p99}, expected in [89.0, 150.0]"
    end

    test "quantile at 0.0 returns min", %{sketch: sketch} do
      assert DDSketch.quantile(sketch, 0.0) == 1.0
    end

    test "quantile at 1.0 returns max", %{sketch: sketch} do
      assert DDSketch.quantile(sketch, 1.0) == 144.0
    end

    test "quantiles are monotonically increasing", %{sketch: sketch} do
      ranks = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
      qs = DDSketch.quantiles(sketch, ranks)
      pairs = Enum.zip(Enum.drop(qs, -1), Enum.drop(qs, 1))
      Enum.each(pairs, fn {a, b} -> assert a <= b end)
    end
  end

  describe "zero handling" do
    test "zeros tracked in zero_count" do
      sketch = DDSketch.new() |> DDSketch.update_many([0.0, 0.0, 1.0, 2.0])
      assert DDSketch.count(sketch) == 4
      assert DDSketch.min_value(sketch) == 0.0
    end

    test "all-zero sketch returns 0.0 for quantiles" do
      sketch = DDSketch.new() |> DDSketch.update_many([0.0, 0.0, 0.0])
      assert DDSketch.count(sketch) == 3
      assert DDSketch.quantile(sketch, 0.5) == 0.0
      assert DDSketch.min_value(sketch) == 0.0
      assert DDSketch.max_value(sketch) == 0.0
    end

    test "mixed zeros and positives" do
      sketch = DDSketch.new() |> DDSketch.update_many([0.0, 0.0, 10.0, 20.0])
      assert DDSketch.count(sketch) == 4
      # p25 should be 0.0 (within zero_count region)
      assert DDSketch.quantile(sketch, 0.25) == 0.0
      # p75 should be a positive value
      q75 = DDSketch.quantile(sketch, 0.75)
      assert q75 > 0.0
    end
  end

  describe "merge" do
    test "merge preserves count" do
      a = DDSketch.new() |> DDSketch.update_many([1.0, 2.0, 3.0])
      b = DDSketch.new() |> DDSketch.update_many([4.0, 5.0])
      merged = DDSketch.merge(a, b)
      assert DDSketch.count(merged) == 5
    end

    test "merge preserves min/max" do
      a = DDSketch.new() |> DDSketch.update_many([5.0, 10.0])
      b = DDSketch.new() |> DDSketch.update_many([1.0, 20.0])
      merged = DDSketch.merge(a, b)
      assert DDSketch.min_value(merged) == 1.0
      assert DDSketch.max_value(merged) == 20.0
    end

    test "merge with empty sketch is identity" do
      a = DDSketch.new() |> DDSketch.update_many([1.0, 2.0, 3.0])
      empty = DDSketch.new()

      merged_left = DDSketch.merge(a, empty)
      assert DDSketch.count(merged_left) == DDSketch.count(a)
      assert DDSketch.min_value(merged_left) == DDSketch.min_value(a)
      assert DDSketch.max_value(merged_left) == DDSketch.max_value(a)

      merged_right = DDSketch.merge(empty, a)
      assert DDSketch.count(merged_right) == DDSketch.count(a)
    end

    test "merge is commutative for count" do
      a = DDSketch.new() |> DDSketch.update_many([1.0, 2.0])
      b = DDSketch.new() |> DDSketch.update_many([3.0, 4.0])
      assert DDSketch.count(DDSketch.merge(a, b)) == DDSketch.count(DDSketch.merge(b, a))
    end

    test "merge_many works" do
      sketches =
        Enum.map(1..5, fn i ->
          DDSketch.new() |> DDSketch.update(i * 1.0)
        end)

      merged = DDSketch.merge_many(sketches)
      assert DDSketch.count(merged) == 5
      assert DDSketch.min_value(merged) == 1.0
      assert DDSketch.max_value(merged) == 5.0
    end

    test "merge alpha mismatch raises IncompatibleSketchesError" do
      a = DDSketch.new(alpha: 0.01) |> DDSketch.update(1.0)
      b = DDSketch.new(alpha: 0.05) |> DDSketch.update(1.0)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/alpha mismatch/, fn ->
        DDSketch.merge(a, b)
      end
    end

    test "merge with zeros" do
      a = DDSketch.new() |> DDSketch.update_many([0.0, 1.0])
      b = DDSketch.new() |> DDSketch.update_many([0.0, 2.0])
      merged = DDSketch.merge(a, b)
      assert DDSketch.count(merged) == 4
      assert DDSketch.min_value(merged) == 0.0
    end
  end

  describe "update_many vs sequential update" do
    test "identical serialization" do
      values = [1.0, 5.0, 10.0, 50.0, 100.0]

      batch = DDSketch.new() |> DDSketch.update_many(values)

      sequential =
        Enum.reduce(values, DDSketch.new(), fn v, s ->
          DDSketch.update(s, v)
        end)

      assert DDSketch.serialize(batch) == DDSketch.serialize(sequential)
    end

    test "empty update_many is no-op" do
      sketch = DDSketch.new() |> DDSketch.update_many([1.0])
      before = DDSketch.serialize(sketch)
      after_empty = DDSketch.serialize(DDSketch.update_many(sketch, []))
      assert before == after_empty
    end
  end

  describe "serialize/deserialize round-trip" do
    test "empty sketch round-trips" do
      sketch = DDSketch.new(alpha: 0.01)
      binary = DDSketch.serialize(sketch)
      assert {:ok, restored} = DDSketch.deserialize(binary)
      assert DDSketch.count(restored) == 0
      assert restored.opts == [alpha: 0.01]
    end

    test "populated sketch round-trips" do
      sketch = DDSketch.new(alpha: 0.01) |> DDSketch.update_many(@values_v1)
      binary = DDSketch.serialize(sketch)
      assert {:ok, restored} = DDSketch.deserialize(binary)
      assert DDSketch.count(restored) == DDSketch.count(sketch)
      assert DDSketch.min_value(restored) == DDSketch.min_value(sketch)
      assert DDSketch.max_value(restored) == DDSketch.max_value(sketch)

      # Quantile results should match
      for rank <- [0.25, 0.5, 0.75, 0.9, 0.99] do
        assert DDSketch.quantile(restored, rank) == DDSketch.quantile(sketch, rank)
      end
    end

    test "deserialize rejects invalid binary" do
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} =
               DDSketch.deserialize(<<"invalid">>)
    end

    test "deserialize rejects wrong sketch ID" do
      params = <<0.01::float-little-64>>
      state = <<0>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_kll(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               DDSketch.deserialize(binary)

      assert msg =~ "expected DDSketch sketch ID (5)"
    end

    test "deserialize rejects invalid alpha in params" do
      params = <<0.0::float-little-64>>
      state = <<0>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_ddsketch(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               DDSketch.deserialize(binary)

      assert msg =~ "invalid DDSketch alpha"
    end
  end

  describe "invalid inputs" do
    test "negative value raises ArgumentError" do
      sketch = DDSketch.new()

      assert_raise ArgumentError, ~r/negative values/, fn ->
        DDSketch.update(sketch, -1.0)
      end
    end

    test "negative in batch raises ArgumentError" do
      sketch = DDSketch.new()

      assert_raise ArgumentError, ~r/negative values/, fn ->
        DDSketch.update_many(sketch, [1.0, -2.0, 3.0])
      end
    end
  end

  describe "value-relative accuracy" do
    test "quantile estimates within alpha relative error for uniform data" do
      alpha = 0.01
      values = Enum.map(1..1000, fn i -> i * 1.0 end)
      sketch = DDSketch.new(alpha: alpha) |> DDSketch.update_many(values)

      for rank <- [0.25, 0.5, 0.75, 0.9, 0.95, 0.99] do
        q = DDSketch.quantile(sketch, rank)
        # Expected value: rank * 1000 (approximately)
        expected = rank * 1000
        relative_error = abs(q - expected) / expected

        assert relative_error < alpha * 2,
               "rank #{rank}: got #{q}, expected ~#{expected}, relative_error=#{relative_error}"
      end
    end

    test "works with different alpha values" do
      values = Enum.map(1..500, fn i -> i * 1.0 end)

      for alpha <- [0.05, 0.01, 0.005] do
        sketch = DDSketch.new(alpha: alpha) |> DDSketch.update_many(values)
        q50 = DDSketch.quantile(sketch, 0.5)
        relative_error = abs(q50 - 250.0) / 250.0
        assert relative_error < alpha * 2, "alpha=#{alpha}: q50=#{q50}"
      end
    end
  end

  describe "codec" do
    test "sketch_id_ddsketch is 5" do
      assert ExDataSketch.Codec.sketch_id_ddsketch() == 5
    end
  end

  describe "convenience functions" do
    test "reducer returns a 2-arity function" do
      assert is_function(DDSketch.reducer(), 2)
    end

    test "merger returns a 2-arity function" do
      assert is_function(DDSketch.merger(), 2)
    end

    test "from_enumerable builds sketch" do
      sketch = DDSketch.from_enumerable([1.0, 2.0, 3.0], alpha: 0.01)
      assert DDSketch.count(sketch) == 3
    end

    test "reducer works with Enum.reduce" do
      sketch = DDSketch.new()
      result = Enum.reduce([1.0, 2.0, 3.0], sketch, DDSketch.reducer())
      assert DDSketch.count(result) == 3
    end

    test "merger works" do
      sketches =
        Enum.map(1..3, fn i ->
          DDSketch.new() |> DDSketch.update(i * 1.0)
        end)

      merged = Enum.reduce(sketches, DDSketch.merger())
      assert DDSketch.count(merged) == 3
    end
  end

  describe "facade integration" do
    alias ExDataSketch.Quantiles

    test "Quantiles.new with type: :ddsketch creates DDSketch" do
      sketch = Quantiles.new(type: :ddsketch, alpha: 0.01)
      assert sketch.__struct__ == DDSketch
    end

    test "Quantiles functions dispatch to DDSketch" do
      sketch = Quantiles.new(type: :ddsketch)
      sketch = Quantiles.update(sketch, 42.0)
      assert Quantiles.count(sketch) == 1
      assert Quantiles.min_value(sketch) == 42.0
      assert Quantiles.max_value(sketch) == 42.0
      assert is_float(Quantiles.quantile(sketch, 0.5))
    end

    test "Quantiles.update_many works with DDSketch" do
      sketch = Quantiles.new(type: :ddsketch)
      sketch = Quantiles.update_many(sketch, [1.0, 2.0, 3.0])
      assert Quantiles.count(sketch) == 3
    end

    test "Quantiles.merge works with DDSketch" do
      a = Quantiles.new(type: :ddsketch) |> Quantiles.update(1.0)
      b = Quantiles.new(type: :ddsketch) |> Quantiles.update(2.0)
      merged = Quantiles.merge(a, b)
      assert Quantiles.count(merged) == 2
    end

    test "ExDataSketch.update_many works with DDSketch" do
      sketch = DDSketch.new() |> ExDataSketch.update_many([1.0, 2.0, 3.0])
      assert DDSketch.count(sketch) == 3
    end
  end

  describe "DDS1 binary format" do
    test "magic bytes are DDS1" do
      sketch = DDSketch.new()
      <<"DDS1", _rest::binary>> = sketch.state
    end

    test "header is 88 bytes for empty sketch" do
      sketch = DDSketch.new()
      assert byte_size(sketch.state) == 88
    end

    test "state grows with sparse bins" do
      sketch = DDSketch.new() |> DDSketch.update(1.0)
      # 88 header + 8 bytes per sparse bin entry (1 entry)
      assert byte_size(sketch.state) == 88 + 8
    end

    test "distinct values create distinct bins" do
      sketch = DDSketch.new() |> DDSketch.update_many([1.0, 10.0, 100.0])
      # 88 header + 3 bins * 8 bytes
      assert byte_size(sketch.state) == 88 + 24
    end

    test "duplicate values share bins" do
      sketch = DDSketch.new() |> DDSketch.update_many([1.0, 1.0, 1.0])
      # 88 header + 1 bin * 8 bytes (all map to same index)
      assert byte_size(sketch.state) == 88 + 8
    end
  end

  describe "edge cases" do
    test "very small positive values" do
      sketch = DDSketch.new() |> DDSketch.update_many([1.0e-300, 1.0e-200, 1.0e-100])
      assert DDSketch.count(sketch) == 3
      assert DDSketch.min_value(sketch) == 1.0e-300
      assert DDSketch.max_value(sketch) == 1.0e-100
    end

    test "very large values" do
      sketch = DDSketch.new() |> DDSketch.update_many([1.0e100, 1.0e200, 1.0e300])
      assert DDSketch.count(sketch) == 3
      assert DDSketch.min_value(sketch) == 1.0e100
      assert DDSketch.max_value(sketch) == 1.0e300
    end

    test "single zero" do
      sketch = DDSketch.new() |> DDSketch.update(0.0)
      assert DDSketch.count(sketch) == 1
      assert DDSketch.min_value(sketch) == 0.0
      assert DDSketch.max_value(sketch) == 0.0
      assert DDSketch.quantile(sketch, 0.5) == 0.0
    end

    test "integer values are converted to float" do
      sketch = DDSketch.new() |> DDSketch.update_many([1, 2, 3])
      assert DDSketch.count(sketch) == 3
      assert DDSketch.min_value(sketch) == 1.0
    end
  end
end
