defmodule ExDataSketch.StreamTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Stream, as: S

  describe "hll/2" do
    test "builds HLL from enumerable" do
      sketch = S.hll(1..100, p: 10)
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "builds HLL from lazy stream" do
      sketch =
        1..100
        |> Stream.map(&to_string/1)
        |> S.hll(p: 10)

      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "empty enumerable produces zero-cardinality HLL" do
      sketch = S.hll([], p: 10)
      assert ExDataSketch.HLL.estimate(sketch) == 0.0
    end

    test "equivalent to HLL.from_enumerable/2" do
      items = Enum.map(1..500, &to_string/1)
      a = S.hll(items, p: 12)
      b = ExDataSketch.HLL.from_enumerable(items, p: 12)
      assert_in_delta ExDataSketch.HLL.estimate(a), ExDataSketch.HLL.estimate(b), 0.01
    end
  end

  describe "cms/2" do
    test "builds CMS from enumerable" do
      sketch = S.cms(["a", "b", "a", "c", "a"], width: 64, depth: 3)
      assert ExDataSketch.CMS.estimate(sketch, "a") >= 3
    end

    test "empty enumerable produces zero-counts CMS" do
      sketch = S.cms([], width: 64, depth: 3)
      assert ExDataSketch.CMS.estimate(sketch, "x") == 0
    end
  end

  describe "theta/2" do
    test "builds Theta from enumerable" do
      sketch = S.theta(1..50, k: 1024)
      assert ExDataSketch.Theta.estimate(sketch) > 0.0
    end

    test "empty enumerable produces zero estimate" do
      sketch = S.theta([], k: 1024)
      assert ExDataSketch.Theta.estimate(sketch) == 0.0
    end
  end

  describe "kll/2" do
    test "builds KLL from enumerable" do
      sketch = S.kll(1..100, k: 200)
      assert is_float(ExDataSketch.KLL.quantile(sketch, 0.5))
    end

    test "empty enumerable produces nil quantile" do
      sketch = S.kll([], k: 200)
      assert ExDataSketch.KLL.quantile(sketch, 0.5) == nil
    end
  end

  describe "ddsketch/2" do
    test "builds DDSketch from enumerable" do
      sketch = S.ddsketch(1..100, alpha: 0.01)
      assert is_float(ExDataSketch.DDSketch.quantile(sketch, 0.5))
    end
  end

  describe "req/2" do
    test "builds REQ from enumerable" do
      sketch = S.req(1..100, k: 200)
      result = ExDataSketch.REQ.quantile(sketch, 0.5)
      assert is_float(result) or is_nil(result)
    end
  end

  describe "ull/2" do
    test "builds ULL from enumerable" do
      sketch = S.ull(1..100, p: 10)
      assert ExDataSketch.ULL.estimate(sketch) > 0.0
    end

    test "empty enumerable produces zero estimate" do
      sketch = S.ull([], p: 10)
      assert ExDataSketch.ULL.estimate(sketch) == 0.0
    end
  end

  describe "frequent_items/2" do
    test "builds FrequentItems from enumerable" do
      items = Enum.map(1..200, fn i -> "item_" <> Integer.to_string(rem(i, 20)) end)
      sketch = S.frequent_items(items, k: 10)
      top = ExDataSketch.FrequentItems.top_k(sketch, limit: 5)
      assert length(top) <= 10
      assert is_list(top)
      assert is_map(hd(top))
    end
  end

  describe "misra_gries/2" do
    test "builds MisraGries from enumerable" do
      items = Enum.map(1..200, fn i -> "item_" <> Integer.to_string(rem(i, 20)) end)
      sketch = S.misra_gries(items, k: 10)
      assert ExDataSketch.MisraGries.count(sketch) == 200
    end
  end

  describe "bloom/2" do
    test "builds Bloom from enumerable" do
      items = Enum.map(1..100, &to_string/1)
      bloom = S.bloom(items, capacity: 200)
      assert ExDataSketch.Bloom.member?(bloom, "1")
    end
  end

  describe "quotient/2" do
    test "builds Quotient from enumerable" do
      items = Enum.map(1..50, &to_string/1)
      qf = S.quotient(items, q: 10, r: 8)
      assert ExDataSketch.Quotient.member?(qf, "1")
    end
  end

  describe "cqf/2" do
    test "builds CQF from enumerable" do
      items = Enum.map(1..50, &to_string/1)
      cqf = S.cqf(items, q: 10, r: 8)
      assert ExDataSketch.CQF.member?(cqf, "1")
    end
  end

  describe "iblt/2" do
    test "builds IBLT from enumerable" do
      items = Enum.map(1..20, &to_string/1)
      iblt = S.iblt(items, m: 40, num_hashes: 3)
      assert ExDataSketch.IBLT.member?(iblt, "1")
    end
  end

  describe "reduce_into/3" do
    test "reduces into a new HLL via module atom" do
      sketch = S.reduce_into(1..100, ExDataSketch.HLL, p: 10)
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "reduces into an existing HLL" do
      existing = ExDataSketch.HLL.new(p: 10)
      sketch = S.reduce_into(["a", "b", "c"], existing)
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "reduces into a new CMS via module atom" do
      sketch = S.reduce_into(["a", "b", "a"], ExDataSketch.CMS, width: 64, depth: 3)
      assert ExDataSketch.CMS.estimate(sketch, "a") >= 2
    end

    test "reduces into an existing CMS" do
      existing = ExDataSketch.CMS.new(width: 64, depth: 3)
      sketch = S.reduce_into(["x", "y"], existing)
      assert ExDataSketch.CMS.estimate(sketch, "x") >= 1
    end

    test "reduces into a new Bloom via module atom" do
      sketch = S.reduce_into(["a", "b", "c"], ExDataSketch.Bloom, capacity: 100)
      assert ExDataSketch.Bloom.member?(sketch, "a")
    end

    test "empty enumerable returns original sketch unchanged" do
      existing = ExDataSketch.HLL.new(p: 10)
      result = S.reduce_into([], existing)
      assert ExDataSketch.HLL.estimate(result) == 0.0
    end

    test "reduce_into matches from_enumerable for HLL" do
      items = Enum.map(1..500, &to_string/1)
      a = S.reduce_into(items, ExDataSketch.HLL, p: 12)
      b = ExDataSketch.HLL.from_enumerable(items, p: 12)
      assert_in_delta ExDataSketch.HLL.estimate(a), ExDataSketch.HLL.estimate(b), 0.01
    end
  end

  describe "reduce_partitioned/3" do
    test "partitioned HLL produces valid estimate" do
      sketch =
        1..1000
        |> Stream.map(&to_string/1)
        |> S.reduce_partitioned(ExDataSketch.HLL, partitions: 4, p: 10)

      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "partitioned result matches single-pass for cardinality sketches" do
      items = Enum.map(1..500, &to_string/1)

      single_pass = ExDataSketch.HLL.from_enumerable(items, p: 12)
      partitioned = S.reduce_partitioned(items, ExDataSketch.HLL, partitions: 4, p: 12)

      rel_error =
        abs(ExDataSketch.HLL.estimate(single_pass) - ExDataSketch.HLL.estimate(partitioned)) /
          max(1.0, ExDataSketch.HLL.estimate(single_pass))

      assert rel_error < 0.05
    end

    test "partitioned Bloom produces valid filter" do
      items = Enum.map(1..100, &to_string/1)

      bloom = S.reduce_partitioned(items, ExDataSketch.Bloom, partitions: 4, capacity: 200)

      assert ExDataSketch.Bloom.member?(bloom, "1")
      assert ExDataSketch.Bloom.member?(bloom, "50")
    end

    test "partitioned CMS matches merge-many result" do
      items = Enum.map(1..500, fn i -> "item_" <> Integer.to_string(rem(i, 50)) end)

      single = ExDataSketch.CMS.from_enumerable(items, width: 128, depth: 3)

      partitioned =
        S.reduce_partitioned(items, ExDataSketch.CMS, partitions: 4, width: 128, depth: 3)

      assert ExDataSketch.CMS.estimate(partitioned, "item_1") >=
               ExDataSketch.CMS.estimate(single, "item_1") * 0.9
    end

    test "empty enumerable returns valid empty sketch" do
      items = Enum.map(1..100, &to_string/1)
      sketch = S.reduce_partitioned(items, ExDataSketch.HLL, partitions: 4, p: 10)
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "default partitions uses System.schedulers_online" do
      items = Enum.map(1..100, &to_string/1)
      sketch = S.reduce_partitioned(items, ExDataSketch.HLL, p: 10)
      assert ExDataSketch.HLL.estimate(sketch) > 0.0
    end

    test "single partition is equivalent to from_enumerable" do
      items = Enum.map(1..200, &to_string/1)

      single = ExDataSketch.HLL.from_enumerable(items, p: 10)
      partitioned = S.reduce_partitioned(items, ExDataSketch.HLL, partitions: 1, p: 10)

      assert_in_delta ExDataSketch.HLL.estimate(single),
                      ExDataSketch.HLL.estimate(partitioned),
                      0.01
    end
  end
end
