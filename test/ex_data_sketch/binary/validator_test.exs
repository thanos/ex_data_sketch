defmodule ExDataSketch.Binary.ValidatorTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.Binary.{Header, Validator}
  alias ExDataSketch.Errors.DeserializationError
  alias ExDataSketch.Hash.Metadata

  doctest ExDataSketch.Binary.Validator

  describe "check_minimum_v2_size/1" do
    test "passes when binary is >= 32 bytes" do
      assert :ok = Validator.check_minimum_v2_size(:binary.copy(<<0>>, 32))
      assert :ok = Validator.check_minimum_v2_size(:binary.copy(<<0>>, 100))
    end

    test "fails when binary is too short" do
      assert {:error, %DeserializationError{}} = Validator.check_minimum_v2_size(<<>>)
      assert {:error, %DeserializationError{}} = Validator.check_minimum_v2_size(<<1, 2, 3>>)
    end
  end

  describe "check_magic/1" do
    test "passes on correct magic" do
      assert :ok = Validator.check_magic("EXSK" <> <<0, 0>>)
    end

    test "fails on wrong magic" do
      assert {:error, %DeserializationError{message: msg}} = Validator.check_magic("BAAD")
      assert msg =~ "invalid magic"
    end

    test "fails on truncated magic" do
      assert {:error, %DeserializationError{}} = Validator.check_magic(<<>>)
      assert {:error, %DeserializationError{}} = Validator.check_magic("EX")
    end
  end

  describe "check_version/2" do
    test "passes on expected version" do
      assert :ok = Validator.check_version("EXSK" <> <<2>>, 2)
      assert :ok = Validator.check_version("EXSK" <> <<1, 99, 99>>, 1)
    end

    test "fails on mismatching version" do
      assert {:error, %DeserializationError{message: msg}} =
               Validator.check_version("EXSK" <> <<1>>, 2)

      assert msg =~ "unsupported EXSK frame version 1"
    end

    test "fails when binary is too short" do
      assert {:error, %DeserializationError{}} = Validator.check_version(<<>>, 2)
    end
  end

  describe "check_crc/1" do
    test "passes on a valid v2 frame" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<1, 2, 3>>)
      assert :ok = Validator.check_crc(bin)
    end

    test "fails when CRC trailer is flipped" do
      meta = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      bin = Header.encode(meta, <<10, 20, 30>>)
      <<body::binary-size(byte_size(bin) - 4), crc::unsigned-little-32>> = bin
      bad = <<body::binary, crc + 1::unsigned-little-32>>
      assert {:error, %DeserializationError{message: msg}} = Validator.check_crc(bad)
      assert msg =~ "checksum mismatch"
    end

    test "fails on a frame that is too short to contain a CRC" do
      assert {:error, %DeserializationError{}} = Validator.check_crc(<<>>)
      assert {:error, %DeserializationError{}} = Validator.check_crc(<<1, 2, 3>>)
    end
  end
end
