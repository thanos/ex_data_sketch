defmodule ExDataSketch.CollectableTest do
  use ExUnit.Case, async: true

  describe "Collectable for HLL" do
    test "Enum.into collects items into HLL" do
      sketch = Enum.into(1..100, ExDataSketch.HLL.new(p: 10))
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "Enum.into with lazy stream" do
      sketch =
        1..100
        |> Stream.map(&to_string/1)
        |> Enum.into(ExDataSketch.HLL.new(p: 10))

      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "collecting into pre-populated HLL continues accumulation" do
      existing = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("existing")
      sketch = Enum.into(["a", "b", "c"], existing)
      assert ExDataSketch.HLL.estimate(sketch) >= 3
    end

    test "empty collection returns initial sketch unchanged" do
      initial = ExDataSketch.HLL.new(p: 10)
      result = Enum.into([], initial)
      assert ExDataSketch.HLL.estimate(result) == 0.0
    end

    test "into result matches from_enumerable" do
      items = Enum.map(1..500, &to_string/1)
      a = Enum.into(items, ExDataSketch.HLL.new(p: 12))
      b = ExDataSketch.HLL.from_enumerable(items, p: 12)
      assert_in_delta ExDataSketch.HLL.estimate(a), ExDataSketch.HLL.estimate(b), 0.01
    end
  end

  describe "Collectable for CMS" do
    test "Enum.into collects items into CMS" do
      sketch = Enum.into(["a", "b", "a", "c"], ExDataSketch.CMS.new(width: 64, depth: 3))
      assert ExDataSketch.CMS.estimate(sketch, "a") >= 2
    end

    test "into result matches from_enumerable" do
      items = Enum.map(1..200, fn i -> "item_#{rem(i, 20)}" end)
      a = Enum.into(items, ExDataSketch.CMS.new(width: 128, depth: 3))
      b = ExDataSketch.CMS.from_enumerable(items, width: 128, depth: 3)
      assert ExDataSketch.CMS.estimate(a, "item_1") == ExDataSketch.CMS.estimate(b, "item_1")
    end
  end

  describe "Collectable for Theta" do
    test "Enum.into collects items into Theta" do
      sketch = Enum.into(1..50, ExDataSketch.Theta.new(k: 1024))
      assert ExDataSketch.Theta.estimate(sketch) > 0.0
    end
  end

  describe "Collectable for KLL" do
    test "Enum.into collects items into KLL" do
      sketch = Enum.into(1..100, ExDataSketch.KLL.new(k: 200))
      assert is_float(ExDataSketch.KLL.quantile(sketch, 0.5))
    end
  end

  describe "Collectable for DDSketch" do
    test "Enum.into collects items into DDSketch" do
      sketch = Enum.into(1..100, ExDataSketch.DDSketch.new(alpha: 0.01))
      assert is_float(ExDataSketch.DDSketch.quantile(sketch, 0.5))
    end
  end

  describe "Collectable for REQ" do
    test "Enum.into collects items into REQ" do
      sketch = Enum.into(1..100, ExDataSketch.REQ.new(k: 200))
      result = ExDataSketch.REQ.quantile(sketch, 0.5)
      assert is_float(result) or is_nil(result)
    end
  end

  describe "Collectable for ULL" do
    test "Enum.into collects items into ULL" do
      sketch = Enum.into(1..100, ExDataSketch.ULL.new(p: 10))
      assert ExDataSketch.ULL.estimate(sketch) > 0.0
    end
  end

  describe "Collectable for FrequentItems" do
    test "Enum.into collects items into FrequentItems" do
      items = Enum.map(1..200, fn i -> "item_#{rem(i, 20)}" end)
      sketch = Enum.into(items, ExDataSketch.FrequentItems.new(k: 10))
      top = ExDataSketch.FrequentItems.top_k(sketch, limit: 5)
      assert length(top) <= 10
      assert is_list(top)
    end
  end

  describe "Collectable for MisraGries" do
    test "Enum.into collects items into MisraGries" do
      items = Enum.map(1..200, fn i -> "item_#{rem(i, 20)}" end)
      sketch = Enum.into(items, ExDataSketch.MisraGries.new(k: 10))
      assert ExDataSketch.MisraGries.count(sketch) == 200
    end
  end

  describe "Collectable for Bloom" do
    test "Enum.into collects items into Bloom" do
      items = Enum.map(1..100, &to_string/1)
      bloom = Enum.into(items, ExDataSketch.Bloom.new(capacity: 200))
      assert ExDataSketch.Bloom.member?(bloom, "1")
    end

    test "into result matches from_enumerable" do
      items = Enum.map(1..200, &to_string/1)
      a = Enum.into(items, ExDataSketch.Bloom.new(capacity: 400))
      b = ExDataSketch.Bloom.from_enumerable(items, capacity: 400)
      assert ExDataSketch.Bloom.member?(a, "1") == ExDataSketch.Bloom.member?(b, "1")
      assert ExDataSketch.Bloom.member?(a, "999") == ExDataSketch.Bloom.member?(b, "999")
    end
  end

  describe "Collectable for Quotient" do
    test "Enum.into collects items into Quotient" do
      items = Enum.map(1..50, &to_string/1)
      qf = Enum.into(items, ExDataSketch.Quotient.new(q: 10, r: 8))
      assert ExDataSketch.Quotient.member?(qf, "1")
    end
  end

  describe "Collectable for CQF" do
    test "Enum.into collects items into CQF" do
      items = Enum.map(1..50, &to_string/1)
      cqf = Enum.into(items, ExDataSketch.CQF.new(q: 10, r: 8))
      assert ExDataSketch.CQF.member?(cqf, "1")
    end
  end

  describe "Collectable for IBLT" do
    test "Enum.into collects items into IBLT" do
      items = Enum.map(1..20, &to_string/1)
      iblt = Enum.into(items, ExDataSketch.IBLT.new(m: 40, num_hashes: 3))
      assert ExDataSketch.IBLT.member?(iblt, "1")
    end
  end
end
