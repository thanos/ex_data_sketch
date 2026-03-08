defmodule ExDataSketch.REQTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.REQ

  describe "struct" do
    test "has correct fields" do
      sketch = %REQ{state: <<>>, opts: [k: 12, hra: true], backend: ExDataSketch.Backend.Pure}
      assert sketch.state == <<>>
      assert sketch.opts == [k: 12, hra: true]
      assert sketch.backend == ExDataSketch.Backend.Pure
    end
  end

  describe "option validation" do
    test "k defaults to 12" do
      sketch = REQ.new()
      assert sketch.opts[:k] == 12
    end

    test "hra defaults to true" do
      sketch = REQ.new()
      assert sketch.opts[:hra] == true
    end

    test "accepts valid k values" do
      for k <- [2, 6, 12, 50, 200] do
        sketch = REQ.new(k: k)
        assert sketch.opts[:k] == k
      end
    end

    test "k must be >= 2" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        REQ.new(k: 1)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/k must be/, fn ->
        REQ.new(k: 0)
      end
    end

    test "hra can be false for LRA mode" do
      sketch = REQ.new(hra: false)
      assert sketch.opts[:hra] == false
    end
  end

  describe "empty sketch" do
    test "count is 0" do
      assert REQ.count(REQ.new()) == 0
    end

    test "min_value is nil" do
      assert REQ.min_value(REQ.new()) == nil
    end

    test "max_value is nil" do
      assert REQ.max_value(REQ.new()) == nil
    end

    test "quantile returns nil" do
      assert REQ.quantile(REQ.new(), 0.5) == nil
    end

    test "rank returns nil" do
      assert REQ.rank(REQ.new(), 50.0) == nil
    end

    test "cdf returns nil" do
      assert REQ.cdf(REQ.new(), [25.0, 75.0]) == nil
    end

    test "pmf returns nil" do
      assert REQ.pmf(REQ.new(), [50.0]) == nil
    end

    test "size_bytes is positive" do
      assert REQ.size_bytes(REQ.new()) > 0
    end
  end

  describe "single item" do
    test "count is 1" do
      sketch = REQ.new() |> REQ.update(42.0)
      assert REQ.count(sketch) == 1
    end

    test "min equals max equals value" do
      sketch = REQ.new() |> REQ.update(42.0)
      assert REQ.min_value(sketch) == 42.0
      assert REQ.max_value(sketch) == 42.0
    end

    test "quantile at 0.0 returns min_value" do
      sketch = REQ.new() |> REQ.update(42.0)
      assert REQ.quantile(sketch, 0.0) == 42.0
    end

    test "quantile at 1.0 returns max_value" do
      sketch = REQ.new() |> REQ.update(42.0)
      assert REQ.quantile(sketch, 1.0) == 42.0
    end

    test "rank of the value is 1.0" do
      sketch = REQ.new() |> REQ.update(42.0)
      assert REQ.rank(sketch, 42.0) == 1.0
    end
  end

  describe "HRA mode (default)" do
    test "basic quantile queries" do
      sketch = REQ.new(k: 12, hra: true) |> REQ.update_many(1..1000)
      assert REQ.count(sketch) == 1000
      assert REQ.min_value(sketch) == 1.0
      assert REQ.max_value(sketch) == 1000.0

      p50 = REQ.quantile(sketch, 0.5)
      assert p50 > 400.0 and p50 < 600.0, "p50=#{p50}"

      p99 = REQ.quantile(sketch, 0.99)
      assert p99 > 950.0 and p99 <= 1000.0, "p99=#{p99}"
    end

    test "quantiles are monotonically non-decreasing" do
      sketch = REQ.new() |> REQ.update_many(1..500)
      ranks = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
      qs = REQ.quantiles(sketch, ranks)
      pairs = Enum.zip(Enum.drop(qs, -1), Enum.drop(qs, 1))
      Enum.each(pairs, fn {a, b} -> assert a <= b end)
    end
  end

  describe "LRA mode" do
    test "basic quantile queries" do
      sketch = REQ.new(k: 12, hra: false) |> REQ.update_many(1..1000)
      assert REQ.count(sketch) == 1000

      p50 = REQ.quantile(sketch, 0.5)
      assert p50 > 400.0 and p50 < 600.0, "p50=#{p50}"

      p01 = REQ.quantile(sketch, 0.01)
      assert p01 >= 1.0 and p01 < 50.0, "p01=#{p01}"
    end

    test "quantiles are monotonically non-decreasing" do
      sketch = REQ.new(hra: false) |> REQ.update_many(1..500)
      ranks = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
      qs = REQ.quantiles(sketch, ranks)
      pairs = Enum.zip(Enum.drop(qs, -1), Enum.drop(qs, 1))
      Enum.each(pairs, fn {a, b} -> assert a <= b end)
    end
  end

  describe "rank/2" do
    test "rank is monotonically non-decreasing" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      values = [10.0, 25.0, 50.0, 75.0, 90.0]
      ranks = Enum.map(values, fn v -> REQ.rank(sketch, v) end)
      pairs = Enum.zip(Enum.drop(ranks, -1), Enum.drop(ranks, 1))
      Enum.each(pairs, fn {a, b} -> assert a <= b end)
    end

    test "rank of max value is 1.0" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      assert REQ.rank(sketch, 100.0) == 1.0
    end

    test "rank of value above max is 1.0" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      assert REQ.rank(sketch, 1000.0) == 1.0
    end
  end

  describe "cdf/2" do
    test "returns ranks at split points" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      cdf = REQ.cdf(sketch, [25.0, 50.0, 75.0])
      assert length(cdf) == 3
      assert Enum.all?(cdf, fn v -> is_float(v) and v >= 0.0 and v <= 1.0 end)
    end

    test "cdf is monotonically non-decreasing" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      cdf = REQ.cdf(sketch, [10.0, 30.0, 50.0, 70.0, 90.0])
      pairs = Enum.zip(Enum.drop(cdf, -1), Enum.drop(cdf, 1))
      Enum.each(pairs, fn {a, b} -> assert a <= b end)
    end
  end

  describe "pmf/2" do
    test "returns m+1 values for m split points" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      pmf = REQ.pmf(sketch, [50.0])
      assert length(pmf) == 2
    end

    test "pmf sums to approximately 1.0" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      pmf = REQ.pmf(sketch, [25.0, 50.0, 75.0])
      assert length(pmf) == 4
      total = Enum.sum(pmf)
      assert abs(total - 1.0) < 1.0e-10
    end

    test "all pmf values are non-negative" do
      sketch = REQ.new() |> REQ.update_many(1..100)
      pmf = REQ.pmf(sketch, [25.0, 50.0, 75.0])
      assert Enum.all?(pmf, fn v -> v >= 0.0 end)
    end
  end

  describe "merge" do
    test "merge preserves count" do
      a = REQ.new() |> REQ.update_many([1.0, 2.0, 3.0])
      b = REQ.new() |> REQ.update_many([4.0, 5.0])
      merged = REQ.merge(a, b)
      assert REQ.count(merged) == 5
    end

    test "merge preserves min/max" do
      a = REQ.new() |> REQ.update_many([5.0, 10.0])
      b = REQ.new() |> REQ.update_many([1.0, 20.0])
      merged = REQ.merge(a, b)
      assert REQ.min_value(merged) == 1.0
      assert REQ.max_value(merged) == 20.0
    end

    test "merge with empty sketch is identity" do
      a = REQ.new() |> REQ.update_many([1.0, 2.0, 3.0])
      empty = REQ.new()

      merged = REQ.merge(a, empty)
      assert REQ.count(merged) == REQ.count(a)
      assert REQ.min_value(merged) == REQ.min_value(a)
      assert REQ.max_value(merged) == REQ.max_value(a)
    end

    test "merge is commutative for count" do
      a = REQ.new() |> REQ.update_many([1.0, 2.0])
      b = REQ.new() |> REQ.update_many([3.0, 4.0])
      assert REQ.count(REQ.merge(a, b)) == REQ.count(REQ.merge(b, a))
    end

    test "merge_many works" do
      sketches =
        Enum.map(1..5, fn i ->
          REQ.new() |> REQ.update(i * 1.0)
        end)

      merged = REQ.merge_many(sketches)
      assert REQ.count(merged) == 5
      assert REQ.min_value(merged) == 1.0
      assert REQ.max_value(merged) == 5.0
    end

    test "merge HRA/LRA mismatch raises IncompatibleSketchesError" do
      a = REQ.new(hra: true) |> REQ.update(1.0)
      b = REQ.new(hra: false) |> REQ.update(1.0)

      assert_raise ExDataSketch.Errors.IncompatibleSketchesError, ~r/mode mismatch/, fn ->
        REQ.merge(a, b)
      end
    end
  end

  describe "update_many vs sequential update" do
    test "count matches" do
      values = [1.0, 5.0, 10.0, 50.0, 100.0]

      batch = REQ.new() |> REQ.update_many(values)

      sequential =
        Enum.reduce(values, REQ.new(), fn v, s ->
          REQ.update(s, v)
        end)

      assert REQ.count(batch) == REQ.count(sequential)
      assert REQ.min_value(batch) == REQ.min_value(sequential)
      assert REQ.max_value(batch) == REQ.max_value(sequential)
    end

    test "empty update_many is no-op" do
      sketch = REQ.new() |> REQ.update_many([1.0])
      before = REQ.serialize(sketch)
      after_empty = REQ.serialize(REQ.update_many(sketch, []))
      assert before == after_empty
    end
  end

  describe "serialize/deserialize round-trip" do
    test "empty sketch round-trips" do
      sketch = REQ.new(k: 12, hra: true)
      binary = REQ.serialize(sketch)
      assert {:ok, restored} = REQ.deserialize(binary)
      assert REQ.count(restored) == 0
      assert restored.opts == [k: 12, hra: true]
    end

    test "populated sketch round-trips" do
      sketch = REQ.new(k: 12) |> REQ.update_many(1..100)
      binary = REQ.serialize(sketch)
      assert {:ok, restored} = REQ.deserialize(binary)
      assert REQ.count(restored) == REQ.count(sketch)
      assert REQ.min_value(restored) == REQ.min_value(sketch)
      assert REQ.max_value(restored) == REQ.max_value(sketch)

      for rank <- [0.25, 0.5, 0.75, 0.9, 0.99] do
        assert REQ.quantile(restored, rank) == REQ.quantile(sketch, rank)
      end
    end

    test "LRA sketch round-trips with correct mode" do
      sketch = REQ.new(k: 12, hra: false) |> REQ.update_many(1..50)
      binary = REQ.serialize(sketch)
      assert {:ok, restored} = REQ.deserialize(binary)
      assert restored.opts == [k: 12, hra: false]
      assert REQ.count(restored) == 50
    end

    test "EXSK header is correct" do
      sketch = REQ.new()
      binary = REQ.serialize(sketch)
      <<"EXSK", 1, 13, _rest::binary>> = binary
    end

    test "deserialize rejects invalid binary" do
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} =
               REQ.deserialize(<<"invalid">>)
    end

    test "deserialize rejects wrong sketch ID" do
      params = <<12::unsigned-little-32, 1::unsigned-8>>
      state = <<0>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_kll(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               REQ.deserialize(binary)

      assert msg =~ "expected REQ sketch ID (13)"
    end
  end

  describe "convenience functions" do
    test "from_enumerable builds sketch" do
      sketch = REQ.from_enumerable([1.0, 2.0, 3.0], k: 12)
      assert REQ.count(sketch) == 3
    end

    test "reducer works with Enum.reduce" do
      sketch = REQ.new()
      result = Enum.reduce([1.0, 2.0, 3.0], sketch, REQ.reducer())
      assert REQ.count(result) == 3
    end

    test "merger works" do
      sketches =
        Enum.map(1..3, fn i ->
          REQ.new() |> REQ.update(i * 1.0)
        end)

      merged = Enum.reduce(sketches, REQ.merger())
      assert REQ.count(merged) == 3
    end
  end

  describe "facade integration" do
    alias ExDataSketch.Quantiles

    test "Quantiles.new with type: :req creates REQ" do
      sketch = Quantiles.new(type: :req, k: 12)
      assert sketch.__struct__ == REQ
    end

    test "Quantiles functions dispatch to REQ" do
      sketch = Quantiles.new(type: :req)
      sketch = Quantiles.update(sketch, 42.0)
      assert Quantiles.count(sketch) == 1
      assert Quantiles.min_value(sketch) == 42.0
      assert Quantiles.max_value(sketch) == 42.0
      assert is_float(Quantiles.quantile(sketch, 0.5))
      assert is_float(Quantiles.rank(sketch, 42.0))
    end

    test "Quantiles.update_many works with REQ" do
      sketch = Quantiles.new(type: :req) |> Quantiles.update_many([1.0, 2.0, 3.0])
      assert Quantiles.count(sketch) == 3
    end

    test "Quantiles.merge works with REQ" do
      a = Quantiles.new(type: :req) |> Quantiles.update(1.0)
      b = Quantiles.new(type: :req) |> Quantiles.update(2.0)
      merged = Quantiles.merge(a, b)
      assert Quantiles.count(merged) == 2
    end

    test "Quantiles.cdf works with REQ" do
      sketch = Quantiles.new(type: :req) |> Quantiles.update_many(1..100)
      cdf = Quantiles.cdf(sketch, [50.0])
      assert length(cdf) == 1
    end

    test "Quantiles.pmf works with REQ" do
      sketch = Quantiles.new(type: :req) |> Quantiles.update_many(1..100)
      pmf = Quantiles.pmf(sketch, [50.0])
      assert length(pmf) == 2
    end

    test "ExDataSketch.update_many works with REQ" do
      sketch = REQ.new() |> ExDataSketch.update_many([1.0, 2.0, 3.0])
      assert REQ.count(sketch) == 3
    end
  end

  describe "REQ1 binary format" do
    test "magic bytes are REQ1" do
      sketch = REQ.new()
      <<"REQ1", _rest::binary>> = sketch.state
    end

    test "state grows with items" do
      empty = REQ.new()
      one_item = REQ.new() |> REQ.update(1.0)
      assert byte_size(one_item.state) > byte_size(empty.state)
    end

    test "HRA flag is encoded" do
      hra = REQ.new(hra: true)
      lra = REQ.new(hra: false)
      <<"REQ1", 1, hra_flags::8, _::binary>> = hra.state
      <<"REQ1", 1, lra_flags::8, _::binary>> = lra.state
      assert hra_flags == 1
      assert lra_flags == 0
    end
  end

  describe "edge cases" do
    test "large dataset" do
      sketch = REQ.new(k: 12) |> REQ.update_many(1..10_000)
      assert REQ.count(sketch) == 10_000
      assert REQ.min_value(sketch) == 1.0
      assert REQ.max_value(sketch) == 10_000.0

      p50 = REQ.quantile(sketch, 0.5)
      assert p50 > 3000.0 and p50 < 7000.0, "p50=#{p50}"
    end

    test "integer values are converted to float" do
      sketch = REQ.new() |> REQ.update_many([1, 2, 3])
      assert REQ.count(sketch) == 3
      assert REQ.min_value(sketch) == 1.0
    end

    test "negative values" do
      sketch = REQ.new() |> REQ.update_many([-10.0, -5.0, 0.0, 5.0, 10.0])
      assert REQ.count(sketch) == 5
      assert REQ.min_value(sketch) == -10.0
      assert REQ.max_value(sketch) == 10.0
    end
  end

  describe "codec" do
    test "sketch_id_req is 13" do
      assert ExDataSketch.Codec.sketch_id_req() == 13
    end
  end

  describe "property tests" do
    property "CDF is monotonically non-decreasing" do
      check all(
              values <- list_of(float(min: -1000.0, max: 1000.0), min_length: 10, max_length: 200)
            ) do
        sketch = REQ.new(k: 12) |> REQ.update_many(values)
        split_points = Enum.sort(Enum.take_random(values, min(5, length(values))))
        cdf = REQ.cdf(sketch, split_points)

        pairs = Enum.zip(Enum.drop(cdf, -1), Enum.drop(cdf, 1))
        assert Enum.all?(pairs, fn {a, b} -> a <= b end)
      end
    end

    property "PMF sums to 1.0" do
      check all(
              values <- list_of(float(min: -1000.0, max: 1000.0), min_length: 10, max_length: 200)
            ) do
        sketch = REQ.new(k: 12) |> REQ.update_many(values)
        split_points = [-500.0, 0.0, 500.0]
        pmf = REQ.pmf(sketch, split_points)
        total = Enum.sum(pmf)
        assert abs(total - 1.0) < 1.0e-10
      end
    end

    property "count equals total number of insertions" do
      check all(
              values <- list_of(float(min: -1000.0, max: 1000.0), min_length: 1, max_length: 500)
            ) do
        sketch = REQ.new() |> REQ.update_many(values)
        assert REQ.count(sketch) == length(values)
      end
    end
  end
end
