defmodule ExDataSketch.KLLTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  doctest ExDataSketch.KLL

  alias ExDataSketch.Backend
  alias ExDataSketch.Errors.{DeserializationError, IncompatibleSketchesError, InvalidOptionError}
  alias ExDataSketch.KLL

  # Backends to test against
  @backends [Backend.Pure] ++
              if(Backend.Rust.available?(),
                do: [Backend.Rust],
                else: []
              )

  # -- Construction (not parameterized) --

  describe "new/1" do
    test "creates sketch with default k=200" do
      sketch = KLL.new()
      assert sketch.opts == [k: 200]
      assert sketch.backend == Backend.Pure
    end

    test "creates sketch with custom k" do
      for k <- [8, 50, 200, 1000] do
        sketch = KLL.new(k: k)
        assert sketch.opts == [k: k]
      end
    end

    test "empty sketch has count 0" do
      sketch = KLL.new()
      assert KLL.count(sketch) == 0
    end

    test "empty sketch has nil min/max" do
      sketch = KLL.new()
      assert KLL.min_value(sketch) == nil
      assert KLL.max_value(sketch) == nil
    end

    test "empty sketch returns nil for quantile" do
      sketch = KLL.new()
      assert KLL.quantile(sketch, 0.5) == nil
    end

    test "validates k minimum" do
      assert_raise InvalidOptionError, ~r/k must be/, fn ->
        KLL.new(k: 7)
      end
    end

    test "validates k maximum" do
      assert_raise InvalidOptionError, ~r/k must be/, fn ->
        KLL.new(k: 65_536)
      end
    end

    test "validates k type" do
      assert_raise InvalidOptionError, ~r/k must be/, fn ->
        KLL.new(k: "200")
      end
    end
  end

  # -- Parameterized backend tests --

  for backend <- @backends do
    backend_name = backend |> Module.split() |> List.last()

    describe "update/2 [#{backend_name}]" do
      test "single item: count=1, min=max=value" do
        sketch = KLL.new(backend: unquote(backend)) |> KLL.update(42.0)
        assert KLL.count(sketch) == 1
        assert KLL.min_value(sketch) == 42.0
        assert KLL.max_value(sketch) == 42.0
      end

      test "single item: quantile(0.5) returns the value" do
        sketch = KLL.new(backend: unquote(backend)) |> KLL.update(42.0)
        assert KLL.quantile(sketch, 0.5) == 42.0
      end

      test "accepts integers (converts to float)" do
        sketch = KLL.new(backend: unquote(backend)) |> KLL.update(42)
        assert KLL.count(sketch) == 1
        assert KLL.min_value(sketch) == 42.0
      end

      test "tracks min and max across updates" do
        sketch =
          KLL.new(backend: unquote(backend))
          |> KLL.update(10.0)
          |> KLL.update(5.0)
          |> KLL.update(20.0)

        assert KLL.min_value(sketch) == 5.0
        assert KLL.max_value(sketch) == 20.0
      end
    end

    describe "update_many/2 [#{backend_name}]" do
      test "updates count correctly" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(backend: unquote(backend)) |> KLL.update_many(items)
        assert KLL.count(sketch) == 100
      end

      test "matches sequential update" do
        items = Enum.map(1..50, &(&1 * 1.0))

        batch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)

        sequential =
          Enum.reduce(items, KLL.new(k: 200, backend: unquote(backend)), fn v, s ->
            KLL.update(s, v)
          end)

        assert KLL.count(batch) == KLL.count(sequential)
        assert KLL.min_value(batch) == KLL.min_value(sequential)
        assert KLL.max_value(batch) == KLL.max_value(sequential)

        # Quantile estimates should be close
        assert_in_delta KLL.quantile(batch, 0.5), KLL.quantile(sequential, 0.5), 5.0
      end
    end

    describe "quantile/2 [#{backend_name}]" do
      test "sorted 1..100: median near 50" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        median = KLL.quantile(sketch, 0.5)
        assert_in_delta median, 50.0, 10.0
      end

      test "sorted 1..100: p99 near 99" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        p99 = KLL.quantile(sketch, 0.99)
        assert_in_delta p99, 99.0, 5.0
      end

      test "quantile(0.0) returns min" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        q0 = KLL.quantile(sketch, 0.0)
        assert q0 == KLL.min_value(sketch) || abs(q0 - 1.0) < 2.0
      end

      test "quantile(1.0) returns max" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        q1 = KLL.quantile(sketch, 1.0)
        assert q1 == KLL.max_value(sketch) || abs(q1 - 100.0) < 2.0
      end
    end

    describe "quantiles/2 [#{backend_name}]" do
      test "returns list of values at requested ranks" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        [q25, q50, q75] = KLL.quantiles(sketch, [0.25, 0.5, 0.75])
        assert q25 <= q50
        assert q50 <= q75
      end
    end

    describe "rank/2 [#{backend_name}]" do
      test "rank of median value near 0.5" do
        items = Enum.map(1..100, &(&1 * 1.0))
        sketch = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        r = KLL.rank(sketch, 50.0)
        assert_in_delta r, 0.5, 0.1
      end

      test "empty sketch returns nil" do
        sketch = KLL.new(backend: unquote(backend))
        assert KLL.rank(sketch, 50.0) == nil
      end
    end

    describe "merge/2 [#{backend_name}]" do
      test "preserves total count" do
        a = KLL.new(backend: unquote(backend)) |> KLL.update_many(Enum.map(1..50, &(&1 * 1.0)))
        b = KLL.new(backend: unquote(backend)) |> KLL.update_many(Enum.map(51..100, &(&1 * 1.0)))
        merged = KLL.merge(a, b)
        assert KLL.count(merged) == 100
      end

      test "preserves min/max" do
        a = KLL.new(backend: unquote(backend)) |> KLL.update_many([1.0, 2.0, 3.0])
        b = KLL.new(backend: unquote(backend)) |> KLL.update_many([10.0, 20.0, 30.0])
        merged = KLL.merge(a, b)
        assert KLL.min_value(merged) == 1.0
        assert KLL.max_value(merged) == 30.0
      end

      test "merged quantile estimates are reasonable" do
        a =
          KLL.new(k: 200, backend: unquote(backend))
          |> KLL.update_many(Enum.map(1..50, &(&1 * 1.0)))

        b =
          KLL.new(k: 200, backend: unquote(backend))
          |> KLL.update_many(Enum.map(51..100, &(&1 * 1.0)))

        merged = KLL.merge(a, b)
        median = KLL.quantile(merged, 0.5)
        assert_in_delta median, 50.0, 10.0
      end

      test "merge with empty preserves sketch" do
        sketch = KLL.new(backend: unquote(backend)) |> KLL.update_many([1.0, 2.0, 3.0])
        empty = KLL.new(backend: unquote(backend))
        merged = KLL.merge(sketch, empty)
        assert KLL.count(merged) == 3
        assert KLL.min_value(merged) == 1.0
        assert KLL.max_value(merged) == 3.0
      end

      test "raises on k mismatch" do
        a = KLL.new(k: 100, backend: unquote(backend))
        b = KLL.new(k: 200, backend: unquote(backend))

        assert_raise IncompatibleSketchesError, ~r/KLL k mismatch/, fn ->
          KLL.merge(a, b)
        end
      end
    end

    describe "determinism [#{backend_name}]" do
      test "same input twice produces identical state" do
        items = Enum.map(1..100, &(&1 * 1.0))
        a = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        b = KLL.new(k: 200, backend: unquote(backend)) |> KLL.update_many(items)
        assert a.state == b.state
      end
    end

    describe "serialize/deserialize [#{backend_name}]" do
      test "round-trip preserves sketch" do
        sketch =
          KLL.new(k: 200, backend: unquote(backend))
          |> KLL.update_many(Enum.map(1..50, &(&1 * 1.0)))

        binary = KLL.serialize(sketch)
        assert {:ok, restored} = KLL.deserialize(binary)
        assert KLL.count(restored) == KLL.count(sketch)
        assert KLL.min_value(restored) == KLL.min_value(sketch)
        assert KLL.max_value(restored) == KLL.max_value(sketch)
        assert_in_delta KLL.quantile(restored, 0.5), KLL.quantile(sketch, 0.5), 1.0e-9
      end

      test "serialized binary has EXSK header" do
        sketch = KLL.new(backend: unquote(backend))
        binary = KLL.serialize(sketch)
        assert <<"EXSK", _::binary>> = binary
      end
    end
  end

  # -- Non-parameterized tests --

  describe "serialize/1" do
    test "encodes k as u32 LE params" do
      sketch = KLL.new(k: 200)

      <<"EXSK", 1, 4, 4::unsigned-little-32, k_bin::binary-size(4), _rest::binary>> =
        KLL.serialize(sketch)

      <<k::unsigned-little-32>> = k_bin
      assert k == 200
    end
  end

  describe "deserialize/1" do
    test "invalid binary returns error" do
      assert {:error, %DeserializationError{}} = KLL.deserialize(<<"invalid">>)
    end

    test "wrong sketch ID returns error" do
      # Build an HLL binary and try to deserialize as KLL
      hll = ExDataSketch.HLL.new()
      binary = ExDataSketch.HLL.serialize(hll)
      assert {:error, %DeserializationError{}} = KLL.deserialize(binary)
    end
  end

  describe "serialize_datasketches/1" do
    test "raises NotImplementedError" do
      sketch = KLL.new()

      assert_raise ExDataSketch.Errors.NotImplementedError, fn ->
        KLL.serialize_datasketches(sketch)
      end
    end
  end

  describe "deserialize_datasketches/1" do
    test "raises NotImplementedError" do
      assert_raise ExDataSketch.Errors.NotImplementedError, fn ->
        KLL.deserialize_datasketches(<<>>)
      end
    end
  end

  describe "from_enumerable/2" do
    test "builds sketch from enumerable" do
      sketch = KLL.from_enumerable([1.0, 2.0, 3.0], k: 200)
      assert KLL.count(sketch) == 3
    end
  end

  describe "merge_many/1" do
    test "merges multiple sketches" do
      sketches =
        Enum.map(1..5, fn i ->
          KLL.new() |> KLL.update(i * 1.0)
        end)

      merged = KLL.merge_many(sketches)
      assert KLL.count(merged) == 5
    end
  end

  describe "reducer/0" do
    test "returns a 2-arity function" do
      assert is_function(KLL.reducer(), 2)
    end

    test "can be used with Enum.reduce" do
      sketch =
        Enum.reduce([1.0, 2.0, 3.0], KLL.new(), KLL.reducer())

      assert KLL.count(sketch) == 3
    end
  end

  describe "merger/0" do
    test "returns a 2-arity function" do
      assert is_function(KLL.merger(), 2)
    end
  end

  describe "size_bytes/1" do
    test "returns positive size" do
      sketch = KLL.new()
      assert KLL.size_bytes(sketch) > 0
    end
  end

  describe "struct" do
    test "has expected fields" do
      sketch = KLL.new()
      assert Map.has_key?(sketch, :state)
      assert Map.has_key?(sketch, :opts)
      assert Map.has_key?(sketch, :backend)
    end
  end

  # -- Accuracy bounds --

  describe "accuracy" do
    test "k=200 gives reasonable accuracy for 10k items" do
      items = Enum.map(1..10_000, &(&1 * 1.0))
      sketch = KLL.new(k: 200) |> KLL.update_many(items)

      # Expected rank error ~1.65/200 ~= 0.83%
      # For p50, expected value is 5000.0 +/- ~83 items
      median = KLL.quantile(sketch, 0.5)
      assert_in_delta median, 5000.0, 500.0

      p99 = KLL.quantile(sketch, 0.99)
      assert_in_delta p99, 9900.0, 500.0
    end
  end

  # -- Property tests --

  describe "properties" do
    property "count equals number of inserted items" do
      check all(n <- integer(1..200), max_runs: 50) do
        items = Enum.map(1..n, &(&1 * 1.0))
        sketch = KLL.new(k: 200) |> KLL.update_many(items)
        assert KLL.count(sketch) == n
      end
    end

    property "min <= quantile(0.5) <= max" do
      check all(
              items <- list_of(float(min: -1000.0, max: 1000.0), min_length: 1, max_length: 100),
              max_runs: 50
            ) do
        sketch = KLL.new(k: 200) |> KLL.update_many(items)
        median = KLL.quantile(sketch, 0.5)
        assert median >= KLL.min_value(sketch)
        assert median <= KLL.max_value(sketch)
      end
    end

    property "serialize/deserialize round-trip" do
      check all(
              items <- list_of(float(min: -1000.0, max: 1000.0), min_length: 1, max_length: 100),
              max_runs: 30
            ) do
        sketch = KLL.new(k: 200) |> KLL.update_many(items)
        binary = KLL.serialize(sketch)
        assert {:ok, restored} = KLL.deserialize(binary)
        assert KLL.count(restored) == KLL.count(sketch)
      end
    end
  end
end
