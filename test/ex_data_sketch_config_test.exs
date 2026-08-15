defmodule ExDataSketch.ConfigTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.{Bloom, CMS, Config, CQF, HLL}

  setup do
    original = Application.get_env(:ex_data_sketch, :defaults)

    on_exit(fn ->
      if original == nil do
        Application.delete_env(:ex_data_sketch, :defaults)
      else
        Application.put_env(:ex_data_sketch, :defaults, original)
      end
    end)

    Application.delete_env(:ex_data_sketch, :defaults)
    :ok
  end

  describe "merge_defaults/2" do
    test "returns opts unchanged when no :defaults config is set" do
      assert Config.merge_defaults(:hll, []) == []
      assert Config.merge_defaults(:hll, p: 10) == [p: 10]
    end

    test "returns opts unchanged when :defaults is set but has no entry for this family" do
      Application.put_env(:ex_data_sketch, :defaults, cqf: [q: 20, r: 10])
      assert Config.merge_defaults(:hll, []) == []
      assert Config.merge_defaults(:hll, p: 10) == [p: 10]
    end

    test "merges the family's configured defaults under opts" do
      Application.put_env(:ex_data_sketch, :defaults, hll: [p: 16])
      assert Config.merge_defaults(:hll, []) == [p: 16]
    end

    test "explicit opts win over configured defaults for the same key" do
      Application.put_env(:ex_data_sketch, :defaults, hll: [p: 16])
      assert Config.merge_defaults(:hll, p: 10) == [p: 10]
    end

    test "configured defaults for keys not present in opts are preserved" do
      Application.put_env(:ex_data_sketch, :defaults, cqf: [q: 20, r: 10])
      merged = Config.merge_defaults(:cqf, seed: 42)
      assert Keyword.get(merged, :q) == 20
      assert Keyword.get(merged, :r) == 10
      assert Keyword.get(merged, :seed) == 42
    end
  end

  describe "integration with family new/1" do
    test "HLL.new/1 picks up configured :p" do
      Application.put_env(:ex_data_sketch, :defaults, hll: [p: 16])
      assert HLL.new().opts[:p] == 16
    end

    test "explicit :p still overrides the configured default" do
      Application.put_env(:ex_data_sketch, :defaults, hll: [p: 16])
      assert HLL.new(p: 12).opts[:p] == 12
    end

    test "CQF.new/1 picks up multiple configured keys" do
      Application.put_env(:ex_data_sketch, :defaults, cqf: [q: 20, r: 10])
      cqf = CQF.new()
      assert cqf.opts[:q] == 20
      assert cqf.opts[:r] == 10
    end

    test "Bloom.new/1 picks up configured :capacity" do
      Application.put_env(:ex_data_sketch, :defaults, bloom: [capacity: 50_000])
      assert Bloom.new().opts[:capacity] == 50_000
    end

    test "families without a configured entry are unaffected" do
      Application.put_env(:ex_data_sketch, :defaults, hll: [p: 16])
      assert CMS.new().opts[:width] == 2048
    end
  end
end
