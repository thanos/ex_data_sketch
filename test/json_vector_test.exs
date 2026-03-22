defmodule ExDataSketch.JsonVectorTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.TestVectors

  # Sketches whose vectors depend on the hash strategy (xxhash3 via NIF).
  # These tests are excluded when the NIF is not available because the pure
  # backend uses phash2, which produces different hashes.
  @hash_dependent_algos ~w(hll cms theta ull)

  for algo <- ["hll", "cms", "theta", "kll", "ddsketch", "frequent_items", "ull"] do
    describe "#{algo} JSON vectors" do
      for {filename, vector} <- TestVectors.load_vectors(algo) do
        @vector vector
        @filename filename
        @algo algo

        if algo in @hash_dependent_algos do
          @tag :rust_nif
        end

        test "#{filename}" do
          TestVectors.assert_vector(@vector, "#{@algo}/#{@filename}")

          if @vector["merge_inputs"] do
            TestVectors.assert_merge_vector(@vector, "#{@algo}/#{@filename} (merge)")
          end
        end
      end
    end
  end
end
