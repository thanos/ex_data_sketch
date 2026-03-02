defmodule ExDataSketch.VectorsTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.{CMS, HLL, Theta}

  @vectors_dir Path.join([__DIR__, "vectors"])

  # -- HLL Vectors --

  describe "HLL test vectors" do
    test "empty p=14 vector matches generated state" do
      expected = HLL.new(p: 14)
      stored = File.read!(Path.join(@vectors_dir, "hll_v1_empty_p14.bin"))
      assert expected.state == stored
    end

    test "100 items p=14 vector matches generated state" do
      items = for i <- 0..99, do: "item_#{i}"
      expected = HLL.from_enumerable(items, p: 14)
      stored = File.read!(Path.join(@vectors_dir, "hll_v1_100items_p14.bin"))
      assert expected.state == stored
    end

    test "10000 items p=14 vector matches generated state" do
      items = for i <- 0..9999, do: "item_#{i}"
      expected = HLL.from_enumerable(items, p: 14)
      stored = File.read!(Path.join(@vectors_dir, "hll_v1_10000items_p14.bin"))
      assert expected.state == stored
    end

    test "empty vector serialize/deserialize round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "hll_v1_empty_p14.bin"))
      sketch = %HLL{state: stored, opts: [p: 14], backend: ExDataSketch.Backend.Pure}
      binary = HLL.serialize(sketch)
      assert {:ok, restored} = HLL.deserialize(binary)
      assert restored.state == stored
      assert restored.opts == [p: 14]
    end

    test "100 items vector serialize/deserialize round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "hll_v1_100items_p14.bin"))
      sketch = %HLL{state: stored, opts: [p: 14], backend: ExDataSketch.Backend.Pure}
      binary = HLL.serialize(sketch)
      assert {:ok, restored} = HLL.deserialize(binary)
      assert restored.state == stored
    end

    test "10000 items vector gives expected estimate" do
      stored = File.read!(Path.join(@vectors_dir, "hll_v1_10000items_p14.bin"))
      sketch = %HLL{state: stored, opts: [p: 14], backend: ExDataSketch.Backend.Pure}
      estimate = HLL.estimate(sketch)
      assert_in_delta estimate, 10_000.0, 10_000 * 0.05
    end
  end

  # -- CMS Vectors --

  describe "CMS test vectors" do
    @cms_opts [width: 2048, depth: 5, counter_width: 32]

    test "empty vector matches generated state" do
      expected = CMS.new(@cms_opts)
      stored = File.read!(Path.join(@vectors_dir, "cms_v1_empty_w2048_d5_c32.bin"))
      assert expected.state == stored
    end

    test "100 items vector matches generated state" do
      items = for i <- 0..99, do: "item_#{i}"
      expected = CMS.from_enumerable(items, @cms_opts)
      stored = File.read!(Path.join(@vectors_dir, "cms_v1_100items_w2048_d5_c32.bin"))
      assert expected.state == stored
    end

    test "empty vector serialize/deserialize round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "cms_v1_empty_w2048_d5_c32.bin"))
      sketch = %CMS{state: stored, opts: @cms_opts, backend: ExDataSketch.Backend.Pure}
      binary = CMS.serialize(sketch)
      assert {:ok, restored} = CMS.deserialize(binary)
      assert restored.state == stored
      assert restored.opts == @cms_opts
    end

    test "100 items vector serialize/deserialize round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "cms_v1_100items_w2048_d5_c32.bin"))
      sketch = %CMS{state: stored, opts: @cms_opts, backend: ExDataSketch.Backend.Pure}
      binary = CMS.serialize(sketch)
      assert {:ok, restored} = CMS.deserialize(binary)
      assert restored.state == stored
    end

    test "100 items vector gives expected estimates" do
      stored = File.read!(Path.join(@vectors_dir, "cms_v1_100items_w2048_d5_c32.bin"))
      sketch = %CMS{state: stored, opts: @cms_opts, backend: ExDataSketch.Backend.Pure}

      # Each item was inserted once, so estimate should be >= 1
      for i <- 0..99 do
        assert CMS.estimate(sketch, "item_#{i}") >= 1
      end
    end
  end

  # -- Theta Vectors --

  describe "Theta test vectors" do
    test "empty k=4096 vector matches generated state" do
      expected = Theta.new(k: 4096)
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_empty_k4096.bin"))
      assert expected.state == stored
    end

    test "100 items k=4096 vector matches generated state" do
      items = for i <- 0..99, do: "item_#{i}"
      expected = Theta.from_enumerable(items, k: 4096)
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_100items_k4096.bin"))
      assert expected.state == stored
    end

    test "10000 items k=4096 vector matches generated state" do
      items = for i <- 0..9999, do: "item_#{i}"
      expected = Theta.from_enumerable(items, k: 4096)
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_10000items_k4096.bin"))
      assert expected.state == stored
    end

    test "empty vector serialize/deserialize round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_empty_k4096.bin"))
      sketch = %Theta{state: stored, opts: [k: 4096], backend: ExDataSketch.Backend.Pure}
      binary = Theta.serialize(sketch)
      assert {:ok, restored} = Theta.deserialize(binary)
      assert restored.state == stored
      assert restored.opts == [k: 4096]
    end

    test "100 items vector serialize/deserialize round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_100items_k4096.bin"))
      sketch = %Theta{state: stored, opts: [k: 4096], backend: ExDataSketch.Backend.Pure}
      binary = Theta.serialize(sketch)
      assert {:ok, restored} = Theta.deserialize(binary)
      assert restored.state == stored
    end

    test "10000 items vector gives expected estimate" do
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_10000items_k4096.bin"))
      sketch = %Theta{state: stored, opts: [k: 4096], backend: ExDataSketch.Backend.Pure}
      estimate = Theta.estimate(sketch)
      assert_in_delta estimate, 10_000.0, 10_000 * 0.1
    end

    test "10000 items vector DataSketches round-trip" do
      stored = File.read!(Path.join(@vectors_dir, "theta_v1_10000items_k4096.bin"))
      sketch = %Theta{state: stored, opts: [k: 4096], backend: ExDataSketch.Backend.Pure}
      binary = Theta.serialize_datasketches(sketch)
      assert {:ok, restored} = Theta.deserialize_datasketches(binary)
      assert Theta.estimate(restored) == Theta.estimate(sketch)
    end
  end
end
