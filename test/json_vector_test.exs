defmodule ExDataSketch.JsonVectorTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.TestVectors

  for algo <- ["hll", "cms", "theta", "kll"] do
    describe "#{algo} JSON vectors" do
      for {filename, vector} <- TestVectors.load_vectors(algo) do
        @vector vector
        @filename filename
        @algo algo

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
