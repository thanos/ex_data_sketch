defmodule ExDataSketchTest do
  use ExUnit.Case, async: true
  doctest ExDataSketch

  alias ExDataSketch.{
    Bloom,
    CMS,
    Cuckoo,
    Errors,
    FilterChain,
    HLL,
    KLL,
    Theta,
    XorFilter
  }

  describe "update_many/2" do
    test "delegates to HLL.update_many for HLL structs" do
      sketch = HLL.new(p: 10)
      updated = ExDataSketch.update_many(sketch, ["a", "b"])
      assert HLL.estimate(updated) > 0.0
    end

    test "delegates to CMS.update_many for CMS structs" do
      sketch = CMS.new()
      updated = ExDataSketch.update_many(sketch, ["a", "b", "a"])
      assert CMS.estimate(updated, "a") == 2
    end

    test "delegates to Theta.update_many for Theta structs" do
      sketch = Theta.new(k: 1024)
      updated = ExDataSketch.update_many(sketch, ["a", "b"])
      assert Theta.estimate(updated) > 0.0
    end

    test "delegates to Bloom.put_many via Bloom.update_many for Bloom structs" do
      sketch = Bloom.new(capacity: 100)
      updated = ExDataSketch.update_many(sketch, ["a", "b"])
      assert Bloom.member?(updated, "a")
    end

    test "delegates to Cuckoo.update_many, unwrapping the ok/full tuple" do
      sketch = Cuckoo.new(capacity: 100)
      updated = ExDataSketch.update_many(sketch, ["a", "b"])
      assert Cuckoo.member?(updated, "a")
    end

    test "delegates to FilterChain.update_many for FilterChain structs" do
      chain = FilterChain.new() |> FilterChain.add_stage(Bloom.new(capacity: 100))
      updated = ExDataSketch.update_many(chain, ["a", "b"])
      assert FilterChain.member?(updated, "a")
    end

    test "raises UnsupportedOperationError for XorFilter" do
      {:ok, filter} = XorFilter.build(["a"])

      assert_raise Errors.UnsupportedOperationError, ~r/XorFilter/, fn ->
        ExDataSketch.update_many(filter, ["b"])
      end
    end
  end

  describe "new/2" do
    test "dispatches to the registered module's new/1" do
      assert %HLL{} = ExDataSketch.new(:hll, p: 12)
    end

    test "defaults opts to []" do
      assert %HLL{} = ExDataSketch.new(:hll)
    end

    test "calls FilterChain.new/0 when opts is empty" do
      assert %FilterChain{stages: []} = ExDataSketch.new(:filter_chain)
    end

    test "raises InvalidOptionError when :filter_chain is given non-empty opts" do
      assert_raise Errors.InvalidOptionError, fn ->
        ExDataSketch.new(:filter_chain, k: 1)
      end
    end

    test "raises UnsupportedOperationError for :xor_filter" do
      assert_raise Errors.UnsupportedOperationError, ~r/build\/2/, fn ->
        ExDataSketch.new(:xor_filter)
      end
    end

    test "raises InvalidOptionError for an unknown type" do
      error =
        assert_raise Errors.InvalidOptionError, fn ->
          ExDataSketch.new(:no_such_family)
        end

      assert error.option == :type
      assert error.value == :no_such_family
    end
  end

  describe "update/2" do
    test "dispatches on struct type" do
      sketch = HLL.new(p: 10)
      updated = ExDataSketch.update(sketch, "a")
      assert HLL.estimate(updated) > 0.0
    end

    test "raises UnsupportedOperationError for XorFilter" do
      {:ok, filter} = XorFilter.build(["a"])

      assert_raise Errors.UnsupportedOperationError, ~r/XorFilter/, fn ->
        ExDataSketch.update(filter, "b")
      end
    end
  end

  describe "merge/2" do
    test "dispatches to the shared module's merge/2 for same-family sketches" do
      a = HLL.new(p: 10) |> HLL.update("a")
      b = HLL.new(p: 10) |> HLL.update("b")
      merged = ExDataSketch.merge(a, b)
      assert HLL.estimate(merged) > 0.0
    end

    test "raises UnsupportedOperationError for Cuckoo" do
      a = Cuckoo.new(capacity: 100)
      b = Cuckoo.new(capacity: 100)

      assert_raise Errors.UnsupportedOperationError, ~r/Cuckoo/, fn ->
        ExDataSketch.merge(a, b)
      end
    end

    test "raises UnsupportedOperationError for XorFilter" do
      {:ok, a} = XorFilter.build(["a"])
      {:ok, b} = XorFilter.build(["b"])

      assert_raise Errors.UnsupportedOperationError, ~r/XorFilter/, fn ->
        ExDataSketch.merge(a, b)
      end
    end

    test "raises UnsupportedOperationError for FilterChain" do
      a = FilterChain.new()
      b = FilterChain.new()

      assert_raise Errors.UnsupportedOperationError, ~r/FilterChain/, fn ->
        ExDataSketch.merge(a, b)
      end
    end

    test "returns an IncompatibleSketchesError tuple for mismatched families" do
      assert {:error, %Errors.IncompatibleSketchesError{}} =
               ExDataSketch.merge(HLL.new(p: 10), CMS.new())
    end
  end

  describe "merge_many/1" do
    test "folds merge/2 across a list of same-family sketches" do
      sketches = [
        HLL.new(p: 10) |> HLL.update("a"),
        HLL.new(p: 10) |> HLL.update("b"),
        HLL.new(p: 10) |> HLL.update("c")
      ]

      merged = ExDataSketch.merge_many(sketches)
      assert HLL.estimate(merged) > 0.0
    end
  end

  describe "estimate/1" do
    test "dispatches to estimate/1 for cardinality families" do
      sketch = HLL.new(p: 10) |> HLL.update("a")
      assert ExDataSketch.estimate(sketch) > 0.0
    end

    test "dispatches to count/1 for quantile families" do
      sketch = KLL.new() |> KLL.update(1.0)
      assert ExDataSketch.estimate(sketch) == 1
    end

    test "raises UnsupportedOperationError for CMS" do
      assert_raise Errors.UnsupportedOperationError, ~r/CMS/, fn ->
        ExDataSketch.estimate(CMS.new())
      end
    end

    test "raises UnsupportedOperationError for membership filters" do
      assert_raise Errors.UnsupportedOperationError, ~r/Bloom/, fn ->
        ExDataSketch.estimate(Bloom.new(capacity: 100))
      end
    end
  end

  describe "serialize/1 and deserialize/2" do
    test "round-trips a sketch through serialize/1 and deserialize/2" do
      sketch = HLL.new(p: 10) |> HLL.update("a")
      binary = ExDataSketch.serialize(sketch)
      assert {:ok, restored} = ExDataSketch.deserialize(binary, :hll)
      assert restored.opts[:p] == 10
    end

    test "deserialize/2 raises InvalidOptionError for an unknown type" do
      assert_raise Errors.InvalidOptionError, fn ->
        ExDataSketch.deserialize(<<>>, :no_such_family)
      end
    end
  end

  describe "size_bytes/1" do
    test "dispatches on struct type" do
      sketch = HLL.new(p: 10)
      assert ExDataSketch.size_bytes(sketch) > 0
    end
  end

  describe "capabilities/1" do
    test "accepts a family atom" do
      assert MapSet.member?(ExDataSketch.capabilities(:hll), :estimate)
    end

    test "accepts a sketch struct" do
      assert MapSet.member?(ExDataSketch.capabilities(HLL.new()), :estimate)
    end

    test "raises InvalidOptionError for an unknown type" do
      assert_raise Errors.InvalidOptionError, fn ->
        ExDataSketch.capabilities(:no_such_family)
      end
    end
  end

  describe "sketches/0" do
    test "maps every registry atom to a module implementing the Sketch behaviour" do
      for {_type, module} <- ExDataSketch.sketches() do
        assert ExDataSketch.Sketch.implemented?(module)
      end
    end
  end
end
