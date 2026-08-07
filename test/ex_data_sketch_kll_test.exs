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

      test "successive batch updates match Pure backend" do
        first = Enum.map(1..100, &(&1 * 1.0))
        second = Enum.map(101..300, &(&1 * 1.0))

        pure =
          KLL.new(k: 200, backend: Backend.Pure)
          |> KLL.update_many(first)
          |> KLL.update_many(second)

        tested =
          KLL.new(k: 200, backend: unquote(backend))
          |> KLL.update_many(first)
          |> KLL.update_many(second)

        assert KLL.serialize(pure) == KLL.serialize(tested)
      end

      test "update then update_many matches Pure backend" do
        pure =
          KLL.new(k: 200, backend: Backend.Pure)
          |> KLL.update(0.0)
          |> KLL.update(1.0)
          |> KLL.update_many(Enum.map(2..100, &(&1 * 1.0)))

        tested =
          KLL.new(k: 200, backend: unquote(backend))
          |> KLL.update(0.0)
          |> KLL.update(1.0)
          |> KLL.update_many(Enum.map(2..100, &(&1 * 1.0)))

        assert KLL.serialize(pure) == KLL.serialize(tested)
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
      binary = KLL.serialize(sketch)

      # v2 magic + version, with KLL family byte = 4.
      assert <<"EXSK", 2, 4, _rest::binary>> = binary

      # Decode through the public Binary facade to access the params segment.
      assert {:ok, decoded} = ExDataSketch.Binary.decode(binary)
      assert decoded.sketch_id == 4
      assert decoded.version == 2
      <<k::unsigned-little-32>> = decoded.params
      assert k == 200
    end
  end

  describe "deserialize/1" do
    test "invalid binary returns error" do
      assert {:error, %DeserializationError{}} = KLL.deserialize(<<"invalid">>)
    end

    test "wrong sketch ID returns error" do
      hll = ExDataSketch.HLL.new()
      binary = ExDataSketch.HLL.serialize(hll)
      assert {:error, %DeserializationError{}} = KLL.deserialize(binary)
    end
  end

  describe "serialize_datasketches/deserialize_datasketches" do
    test "round-trip preserves quantiles for empty sketch" do
      sketch = KLL.new(k: 200)
      binary = KLL.serialize_datasketches(sketch)
      assert {:ok, restored} = KLL.deserialize_datasketches(binary)
      assert KLL.count(restored) == 0
      assert KLL.quantile(restored, 0.5) == nil
    end

    test "round-trip preserves quantiles for single item" do
      sketch = KLL.new(k: 200) |> KLL.update(42.0)
      binary = KLL.serialize_datasketches(sketch)
      assert {:ok, restored} = KLL.deserialize_datasketches(binary)
      assert KLL.count(restored) == 1
      assert KLL.min_value(restored) == 42.0
      assert KLL.max_value(restored) == 42.0
      assert KLL.quantile(restored, 0.5) == 42.0
    end

    test "round-trip preserves quantiles for many items (multi-level)" do
      items = for i <- 1..10_000, do: i * 1.0
      sketch = KLL.new(k: 200) |> KLL.update_many(items)
      binary = KLL.serialize_datasketches(sketch)
      assert {:ok, restored} = KLL.deserialize_datasketches(binary)
      assert KLL.count(restored) == 10_000
      assert KLL.min_value(restored) == KLL.min_value(sketch)
      assert KLL.max_value(restored) == KLL.max_value(sketch)

      for r <- [0.1, 0.25, 0.5, 0.75, 0.9] do
        assert KLL.quantile(restored, r) == KLL.quantile(sketch, r)
      end
    end

    test "round-trip preserves quantiles for non-default k" do
      items = for i <- 1..5_000, do: i * 1.0
      sketch = KLL.new(k: 64) |> KLL.update_many(items)
      binary = KLL.serialize_datasketches(sketch)
      assert {:ok, restored} = KLL.deserialize_datasketches(binary)
      assert restored.opts[:k] == 64
      assert KLL.quantile(restored, 0.5) == KLL.quantile(sketch, 0.5)
    end

    test ":float variant round-trips within float32 precision" do
      items = for i <- 1..1_000, do: i * 1.5
      sketch = KLL.new(k: 200) |> KLL.update_many(items)
      binary = KLL.serialize_datasketches(sketch, variant: :float)
      assert {:ok, restored} = KLL.deserialize_datasketches(binary, variant: :float)
      assert KLL.count(restored) == 1_000
      assert_in_delta KLL.min_value(restored), KLL.min_value(sketch), 1.0e-3
      assert_in_delta KLL.max_value(restored), KLL.max_value(sketch), 1.0e-3
      assert_in_delta KLL.quantile(restored, 0.5), KLL.quantile(sketch, 0.5), 1.0e-3
    end

    test "empty sketch produces 8-byte binary" do
      sketch = KLL.new(k: 200)
      binary = KLL.serialize_datasketches(sketch)
      assert byte_size(binary) == 8
    end

    test "single item (:double) produces 16-byte binary" do
      sketch = KLL.new(k: 200) |> KLL.update(1.0)
      binary = KLL.serialize_datasketches(sketch)
      assert byte_size(binary) == 16
    end

    test "single item (:float) produces 12-byte binary" do
      sketch = KLL.new(k: 200) |> KLL.update(1.0)
      binary = KLL.serialize_datasketches(sketch, variant: :float)
      assert byte_size(binary) == 12
    end

    test "preamble has correct family ID and default M" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..500 |> Enum.map(&(&1 * 1.0)))
      binary = KLL.serialize_datasketches(sketch)

      <<_pre_ints::unsigned-8, _ser_ver::unsigned-8, fam_id::unsigned-8, _flags::unsigned-8,
        k::unsigned-little-16, m::unsigned-8, _unused::unsigned-8, _rest::binary>> = binary

      assert fam_id == 15
      assert k == 200
      assert m == 8
    end

    test "rejects too-short binary" do
      assert {:error, %DeserializationError{}} = KLL.deserialize_datasketches(<<1, 2>>)
    end

    test "rejects wrong family ID" do
      binary =
        <<2::unsigned-8, 1::unsigned-8, 99::unsigned-8, 1::unsigned-8, 200::unsigned-little-16,
          8::unsigned-8, 0::unsigned-8>>

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary)

      assert msg =~ "family ID"
    end

    test "rejects non-default M" do
      binary =
        <<2::unsigned-8, 1::unsigned-8, 15::unsigned-8, 1::unsigned-8, 200::unsigned-little-16,
          4::unsigned-8, 0::unsigned-8>>

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary)

      assert msg =~ "M="
    end

    test "rejects updatable (non-compact) structure" do
      binary =
        <<5::unsigned-8, 3::unsigned-8, 15::unsigned-8, 0::unsigned-8, 200::unsigned-little-16,
          8::unsigned-8, 0::unsigned-8, 0::unsigned-little-64, 200::unsigned-little-16,
          2::unsigned-8, 0::unsigned-8>>

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary)

      assert msg =~ "updatable"
    end

    test "rejects invalid preInts/serVer combination" do
      binary =
        <<3::unsigned-8, 1::unsigned-8, 15::unsigned-8, 0::unsigned-8, 200::unsigned-little-16,
          8::unsigned-8, 0::unsigned-8>>

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary)

      assert msg =~ "invalid or unsupported"
    end

    test "rejects k out of valid range" do
      binary =
        <<2::unsigned-8, 1::unsigned-8, 15::unsigned-8, 1::unsigned-8, 3::unsigned-little-16,
          8::unsigned-8, 0::unsigned-8>>

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary)

      assert msg =~ "out of valid range"
    end

    test "rejects mismatched EMPTY flag" do
      binary =
        <<2::unsigned-8, 1::unsigned-8, 15::unsigned-8, 0::unsigned-8, 200::unsigned-little-16,
          8::unsigned-8, 0::unsigned-8>>

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary)

      assert msg =~ "EMPTY"
    end

    test "rejects trailing bytes after single item" do
      sketch = KLL.new(k: 200) |> KLL.update(1.0)
      binary = KLL.serialize_datasketches(sketch)

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary <> <<0>>)

      assert msg =~ "trailing"
    end

    test "rejects truncated full structure" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..500 |> Enum.map(&(&1 * 1.0)))
      binary = KLL.serialize_datasketches(sketch)
      truncated = binary_part(binary, 0, 22)

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(truncated)

      assert msg =~ "truncated"
    end

    test "wrong :variant on decode is caught by size validation (odd item count)" do
      # An odd item count guarantees the item-width arithmetic can't line up
      # under the wrong :variant -- see the moduledoc's "usually (not
      # always)" caveat for why this isn't true of every item count.
      items = for i <- 1..501, do: i * 1.0
      sketch = KLL.new(k: 200) |> KLL.update_many(items)
      binary = KLL.serialize_datasketches(sketch, variant: :float)

      assert {:error, %DeserializationError{message: msg}} =
               KLL.deserialize_datasketches(binary, variant: :double)

      assert msg =~ "variant"
    end
  end

  describe "DataSketches properties" do
    property ":double variant round-trip is exact" do
      check all(
              items <- list_of(float(min: -1000.0, max: 1000.0), min_length: 1, max_length: 200),
              max_runs: 30
            ) do
        sketch = KLL.new(k: 200) |> KLL.update_many(items)
        binary = KLL.serialize_datasketches(sketch)
        assert {:ok, restored} = KLL.deserialize_datasketches(binary)
        assert KLL.count(restored) == KLL.count(sketch)
        assert KLL.min_value(restored) == KLL.min_value(sketch)
        assert KLL.max_value(restored) == KLL.max_value(sketch)
        assert KLL.quantile(restored, 0.5) == KLL.quantile(sketch, 0.5)
      end
    end

    property ":float variant round-trip is within float32 precision" do
      check all(
              items <- list_of(float(min: -1000.0, max: 1000.0), min_length: 1, max_length: 200),
              max_runs: 30
            ) do
        sketch = KLL.new(k: 200) |> KLL.update_many(items)
        binary = KLL.serialize_datasketches(sketch, variant: :float)
        assert {:ok, restored} = KLL.deserialize_datasketches(binary, variant: :float)
        assert KLL.count(restored) == KLL.count(sketch)
        assert_in_delta KLL.min_value(restored), KLL.min_value(sketch), 1.0e-2
        assert_in_delta KLL.max_value(restored), KLL.max_value(sketch), 1.0e-2
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

  # -- CDF and PMF --

  describe "cdf/2" do
    test "returns nil for empty sketch" do
      sketch = KLL.new()
      assert KLL.cdf(sketch, [50.0]) == nil
    end

    test "returns ranks at split points" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..100)
      cdf = KLL.cdf(sketch, [25.0, 50.0, 75.0])
      assert length(cdf) == 3
      assert Enum.all?(cdf, &is_float/1)
      # CDF should be monotonically non-decreasing
      assert cdf == Enum.sort(cdf)
    end

    test "single split point matches rank" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..1000)
      [cdf_val] = KLL.cdf(sketch, [500.0])
      rank_val = KLL.rank(sketch, 500.0)
      assert_in_delta cdf_val, rank_val, 1.0e-10
    end
  end

  describe "pmf/2" do
    test "returns nil for empty sketch" do
      sketch = KLL.new()
      assert KLL.pmf(sketch, [50.0]) == nil
    end

    test "returns m+1 bins for m split points" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..100)
      pmf = KLL.pmf(sketch, [25.0, 50.0, 75.0])
      assert length(pmf) == 4
    end

    test "pmf sums to 1.0" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..1000)
      pmf = KLL.pmf(sketch, [200.0, 500.0, 800.0])
      assert_in_delta Enum.sum(pmf), 1.0, 1.0e-10
    end

    test "all pmf values are non-negative" do
      sketch = KLL.new(k: 200) |> KLL.update_many(1..100)
      pmf = KLL.pmf(sketch, [25.0, 50.0, 75.0])
      assert Enum.all?(pmf, fn v -> v >= 0.0 end)
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
