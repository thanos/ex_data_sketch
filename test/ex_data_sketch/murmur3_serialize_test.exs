defmodule ExDataSketch.Murmur3SerializeTest do
  @moduledoc """
  Regression tests for issue #238: `:murmur3` HLL/ULL/Theta/CMS sketches
  crash on `serialize/1`.

  v0.7.x froze the sketch-local hash-strategy params byte at:

      0 = :phash2
      1 = :xxhash3
      2 = :custom

  v0.8.0 introduced `:murmur3` as a first-class strategy at the
  `HLL.new/1` / `ULL.new/1` / `Theta.new/1` / `CMS.new/1` level, but the
  per-sketch `hash_strategy_byte/1` was never updated to encode it.
  Calling `serialize/1` on a Murmur3 sketch raised:

      ** (CaseClauseError) no case clause matching: :murmur3

  The fix assigns byte `3` to `:murmur3` in every sketch's params
  binary, leaving byte `2 = :custom` intact for v0.7.x backward
  compatibility. These tests lock the contract.

  See `plans/0.8.0-review.md` Critical Finding #1.
  """

  use ExUnit.Case, async: true

  alias ExDataSketch.{CMS, HLL, Theta, ULL}

  @items for i <- 1..200, do: "murmur3_item_#{i}"

  describe "HLL Murmur3 serialize/deserialize round-trip" do
    test "serialize/1 does not crash" do
      sketch = HLL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      bin = HLL.serialize(sketch)
      assert is_binary(bin)
      assert <<"EXSK", 2, _rest::binary>> = bin
    end

    test "deserialize/1 preserves :murmur3 in opts" do
      sketch = HLL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      bin = HLL.serialize(sketch)
      assert {:ok, restored} = HLL.deserialize(bin)
      assert restored.opts[:hash_strategy] == :murmur3
      assert restored.opts[:p] == 10
    end

    test "round-trip preserves state byte-for-byte" do
      sketch = HLL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      assert {:ok, restored} = HLL.deserialize(HLL.serialize(sketch))
      assert restored.state == sketch.state
    end

    test "round-trip preserves estimate" do
      sketch = HLL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      assert {:ok, restored} = HLL.deserialize(HLL.serialize(sketch))
      assert_in_delta HLL.estimate(restored), HLL.estimate(sketch), 1.0e-9
    end
  end

  describe "ULL Murmur3 serialize/deserialize round-trip" do
    test "serialize/1 does not crash" do
      sketch = ULL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      bin = ULL.serialize(sketch)
      assert is_binary(bin)
      assert <<"EXSK", 2, _rest::binary>> = bin
    end

    test "deserialize/1 preserves :murmur3 in opts" do
      sketch = ULL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      bin = ULL.serialize(sketch)
      assert {:ok, restored} = ULL.deserialize(bin)
      assert restored.opts[:hash_strategy] == :murmur3
      assert restored.opts[:p] == 10
    end

    test "round-trip preserves state byte-for-byte" do
      sketch = ULL.from_enumerable(@items, p: 10, hash_strategy: :murmur3)
      assert {:ok, restored} = ULL.deserialize(ULL.serialize(sketch))
      assert restored.state == sketch.state
    end
  end

  describe "Theta Murmur3 serialize/deserialize round-trip" do
    test "serialize/1 does not crash" do
      sketch = Theta.from_enumerable(@items, k: 1024, hash_strategy: :murmur3)
      bin = Theta.serialize(sketch)
      assert is_binary(bin)
      assert <<"EXSK", 2, _rest::binary>> = bin
    end

    test "deserialize/1 preserves :murmur3 in opts" do
      sketch = Theta.from_enumerable(@items, k: 1024, hash_strategy: :murmur3)
      bin = Theta.serialize(sketch)
      assert {:ok, restored} = Theta.deserialize(bin)
      assert restored.opts[:hash_strategy] == :murmur3
      assert restored.opts[:k] == 1024
    end

    test "round-trip preserves state byte-for-byte" do
      sketch = Theta.from_enumerable(@items, k: 1024, hash_strategy: :murmur3)
      assert {:ok, restored} = Theta.deserialize(Theta.serialize(sketch))
      assert restored.state == sketch.state
    end
  end

  describe "CMS Murmur3 serialize/deserialize round-trip" do
    test "serialize/1 does not crash" do
      sketch =
        CMS.from_enumerable(@items,
          width: 1024,
          depth: 5,
          counter_width: 32,
          hash_strategy: :murmur3
        )

      bin = CMS.serialize(sketch)
      assert is_binary(bin)
      assert <<"EXSK", 2, _rest::binary>> = bin
    end

    test "deserialize/1 preserves :murmur3 in opts" do
      sketch =
        CMS.from_enumerable(@items,
          width: 1024,
          depth: 5,
          counter_width: 32,
          hash_strategy: :murmur3
        )

      bin = CMS.serialize(sketch)
      assert {:ok, restored} = CMS.deserialize(bin)
      assert restored.opts[:hash_strategy] == :murmur3
      assert restored.opts[:width] == 1024
      assert restored.opts[:depth] == 5
      assert restored.opts[:counter_width] == 32
    end

    test "round-trip preserves state byte-for-byte" do
      sketch =
        CMS.from_enumerable(@items,
          width: 1024,
          depth: 5,
          counter_width: 32,
          hash_strategy: :murmur3
        )

      assert {:ok, restored} = CMS.deserialize(CMS.serialize(sketch))
      assert restored.state == sketch.state
    end

    test "round-trip preserves estimate for inserted items" do
      sketch =
        CMS.from_enumerable(@items,
          width: 1024,
          depth: 5,
          counter_width: 32,
          hash_strategy: :murmur3
        )

      assert {:ok, restored} = CMS.deserialize(CMS.serialize(sketch))

      for item <- Enum.take(@items, 20) do
        assert CMS.estimate(restored, item) == CMS.estimate(sketch, item)
      end
    end
  end

  describe "Murmur3 wire-byte stability" do
    # The sketch-local params binary must carry byte 3 for :murmur3.
    # This locks the wire-byte assignment so a future refactor cannot
    # silently relocate it.

    test "HLL Murmur3 params binary contains byte 3 in the hash strategy slot" do
      sketch = HLL.new(p: 10, hash_strategy: :murmur3)
      bin = HLL.serialize(sketch)
      assert {:ok, decoded} = ExDataSketch.Binary.decode(bin)
      # HLL params = <<p::u8, hash_strategy::u8>>
      assert <<_p::unsigned-8, 3::unsigned-8>> = decoded.params
    end

    test "ULL Murmur3 params binary contains byte 3 in the hash strategy slot" do
      sketch = ULL.new(p: 10, hash_strategy: :murmur3)
      bin = ULL.serialize(sketch)
      assert {:ok, decoded} = ExDataSketch.Binary.decode(bin)
      assert <<_p::unsigned-8, 3::unsigned-8>> = decoded.params
    end

    test "Theta Murmur3 params binary contains byte 3 in the hash strategy slot" do
      sketch = Theta.new(k: 1024, hash_strategy: :murmur3)
      bin = Theta.serialize(sketch)
      assert {:ok, decoded} = ExDataSketch.Binary.decode(bin)
      # Theta params = <<k::u32-le, hash_strategy::u8>>
      assert <<_k::unsigned-little-32, 3::unsigned-8>> = decoded.params
    end

    test "CMS Murmur3 params binary contains byte 3 in the hash strategy slot" do
      sketch = CMS.new(width: 1024, depth: 5, counter_width: 32, hash_strategy: :murmur3)
      bin = CMS.serialize(sketch)
      assert {:ok, decoded} = ExDataSketch.Binary.decode(bin)
      # CMS params = <<w::u32-le, d::u16-le, cw::u8, hash_strategy::u8>>
      assert <<_w::unsigned-little-32, _d::unsigned-little-16, _cw::unsigned-8, 3::unsigned-8>> =
               decoded.params
    end
  end

  describe "backward compatibility: :custom byte (2) is preserved for v0.7.x" do
    # Byte 2 = :custom must remain reserved so v0.7.x-serialized sketches
    # with :custom can still be detected and rejected with a clear error.

    test "HLL deserialize of v0.7.x :custom frame returns the legacy custom error" do
      # Manually construct a v1 frame with custom strategy byte 2.
      # HLL state size for p=10 is 4 (header) + 1024 (registers) = 1028 bytes.
      state = :binary.copy(<<0>>, 1028)
      v1 = ExDataSketch.Codec.encode(ExDataSketch.Codec.sketch_id_hll(), 1, <<10, 2>>, state)

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               HLL.deserialize(v1)

      assert msg =~ "custom"
    end
  end
end
