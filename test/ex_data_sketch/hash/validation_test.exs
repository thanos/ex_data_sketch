defmodule ExDataSketch.Hash.ValidationTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.Errors.IncompatibleSketchesError
  alias ExDataSketch.Hash.Metadata
  alias ExDataSketch.Hash.Validation

  doctest ExDataSketch.Hash.Validation

  describe "validate_options!/3" do
    test "accepts identical configurations" do
      assert Validation.validate_options!(
               [hash_strategy: :xxhash3, seed: 0],
               [hash_strategy: :xxhash3, seed: 0],
               "HLL"
             ) == :ok
    end

    test "accepts default-vs-explicit phash2/0" do
      assert Validation.validate_options!([], [hash_strategy: :phash2, seed: 0], "HLL") == :ok
    end

    test "rejects strategy mismatch" do
      assert_raise IncompatibleSketchesError, ~r/hash strategy mismatch/, fn ->
        Validation.validate_options!(
          [hash_strategy: :xxhash3],
          [hash_strategy: :phash2],
          "HLL"
        )
      end
    end

    test "rejects seed mismatch" do
      assert_raise IncompatibleSketchesError, ~r/seed mismatch/, fn ->
        Validation.validate_options!(
          [hash_strategy: :xxhash3, seed: 1],
          [hash_strategy: :xxhash3, seed: 2],
          "HLL"
        )
      end
    end

    test "rejects :custom on either side" do
      assert_raise IncompatibleSketchesError, ~r/custom/, fn ->
        Validation.validate_options!(
          [hash_strategy: :custom],
          [hash_strategy: :xxhash3],
          "HLL"
        )
      end

      assert_raise IncompatibleSketchesError, ~r/custom/, fn ->
        Validation.validate_options!(
          [hash_strategy: :xxhash3],
          [hash_strategy: :custom],
          "HLL"
        )
      end
    end

    test "names the sketch type in the error" do
      try do
        Validation.validate_options!(
          [hash_strategy: :xxhash3],
          [hash_strategy: :phash2],
          "MyExoticSketch"
        )
      rescue
        e in IncompatibleSketchesError ->
          assert e.message =~ "MyExoticSketch"
      end
    end
  end

  describe "validate_metadata!/3" do
    test "accepts identical metadata (backend irrelevant)" do
      a = Metadata.new(:xxhash3, 0, 1, 1, :pure)
      b = Metadata.new(:xxhash3, 0, 1, 1, :rust)
      assert Validation.validate_metadata!(a, b, "HLL") == :ok
    end

    test "rejects algorithm mismatch" do
      a = Metadata.new(:xxhash3, 0, 1, 1, :pure)
      b = Metadata.new(:murmur3, 0, 1, 1, :pure)

      assert_raise IncompatibleSketchesError, ~r/algorithm mismatch/, fn ->
        Validation.validate_metadata!(a, b, "HLL")
      end
    end

    test "rejects seed mismatch" do
      a = Metadata.new(:xxhash3, 1, 1, 1, :pure)
      b = Metadata.new(:xxhash3, 2, 1, 1, :pure)

      assert_raise IncompatibleSketchesError, ~r/seed mismatch/, fn ->
        Validation.validate_metadata!(a, b, "HLL")
      end
    end

    test "rejects sketch_family mismatch" do
      a = Metadata.new(:xxhash3, 0, 1, 1, :pure)
      b = Metadata.new(:xxhash3, 0, 2, 1, :pure)

      assert_raise IncompatibleSketchesError, ~r/sketch_family mismatch/, fn ->
        Validation.validate_metadata!(a, b, "HLL")
      end
    end

    test "rejects sketch_family_version mismatch" do
      a = Metadata.new(:xxhash3, 0, 1, 1, :pure)
      b = Metadata.new(:xxhash3, 0, 1, 2, :pure)

      assert_raise IncompatibleSketchesError, ~r/sketch_family_version mismatch/, fn ->
        Validation.validate_metadata!(a, b, "HLL")
      end
    end

    test "rejects :custom on either side" do
      a = Metadata.new(:custom, 0, 1, 1, :pure)
      b = Metadata.new(:xxhash3, 0, 1, 1, :pure)

      assert_raise IncompatibleSketchesError, ~r/custom/, fn ->
        Validation.validate_metadata!(a, b, "HLL")
      end
    end
  end

  describe "compatible_options?/2" do
    test "returns true when validate_options! would succeed" do
      assert Validation.compatible_options?(
               [hash_strategy: :xxhash3, seed: 0],
               hash_strategy: :xxhash3,
               seed: 0
             ) == true
    end

    test "returns false when validate_options! would raise" do
      refute Validation.compatible_options?(
               [hash_strategy: :xxhash3],
               hash_strategy: :phash2
             )

      refute Validation.compatible_options?(
               [hash_strategy: :custom],
               hash_strategy: :xxhash3
             )
    end

    test "never raises" do
      # Even bogus shapes should not raise from compatible_options?/2 with
      # well-formed keyword lists.
      assert Validation.compatible_options?([], seed: 999) in [true, false]
    end
  end

  describe "no-false-negative properties" do
    property "matching strategy/seed pairs are always compatible" do
      check all(
              strategy <- StreamData.member_of([:phash2, :xxhash3, :murmur3]),
              seed <- StreamData.integer(0..0xFFFFFFFFFFFFFFFF)
            ) do
        assert Validation.compatible_options?(
                 [hash_strategy: strategy, seed: seed],
                 hash_strategy: strategy,
                 seed: seed
               )
      end
    end

    property "any :custom usage is incompatible" do
      check all(strategy <- StreamData.member_of([:phash2, :xxhash3, :murmur3, :custom])) do
        refute Validation.compatible_options?(
                 [hash_strategy: :custom],
                 hash_strategy: strategy
               )

        refute Validation.compatible_options?(
                 [hash_strategy: strategy],
                 hash_strategy: :custom
               )
      end
    end
  end

  describe "backward-compat shim" do
    test "ExDataSketch.Hash.validate_merge_hash_compat!/3 delegates to Validation" do
      # The legacy entry point should produce the same outcome as the new module.
      assert ExDataSketch.Hash.validate_merge_hash_compat!(
               [hash_strategy: :xxhash3],
               [hash_strategy: :xxhash3],
               "HLL"
             ) == :ok

      assert_raise IncompatibleSketchesError, fn ->
        ExDataSketch.Hash.validate_merge_hash_compat!(
          [hash_strategy: :xxhash3],
          [hash_strategy: :phash2],
          "HLL"
        )
      end
    end
  end
end
