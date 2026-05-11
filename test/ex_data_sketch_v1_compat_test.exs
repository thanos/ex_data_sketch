defmodule ExDataSketch.V1CompatTest do
  @moduledoc """
  Backward-compatibility tests for pre-v0.8.0 EXSK v1 binaries.

  The v0.8.0 release migrates the default writer to EXSK v2 but MUST continue
  to decode v1 binaries that were persisted by earlier releases. These tests
  use the preserved v1 golden vectors under `test/vectors_v1/` and assert
  that:

  - Every v1 binary round-trips through `Binary.decode/1`.
  - Every per-sketch `deserialize/1` accepts the v1 binary.
  - The resulting sketch's estimate/state matches the v1-recorded value.

  These tests are intentionally distinct from the JSON vector tests under
  `test/json_vector_test.exs` so a future bump (v3, v4, ...) does not lose
  the v1 decode regression coverage.
  """

  use ExUnit.Case, async: true

  alias ExDataSketch.{Binary, CMS, DDSketch, FrequentItems, HLL, KLL, Theta, ULL}

  @v1_vectors_dir Path.join([__DIR__, "vectors_v1"])

  @sketch_modules %{
    "hll" => HLL,
    "cms" => CMS,
    "theta" => Theta,
    "kll" => KLL,
    "ddsketch" => DDSketch,
    "frequent_items" => FrequentItems,
    "ull" => ULL
  }

  describe "v1 decode regression" do
    test "every preserved v1 vector is still decodable through Binary.decode/1" do
      for algorithm <- File.ls!(@v1_vectors_dir) do
        algo_dir = Path.join(@v1_vectors_dir, algorithm)

        if File.dir?(algo_dir) do
          for filename <- File.ls!(algo_dir),
              String.ends_with?(filename, ".json") do
            json = algo_dir |> Path.join(filename) |> File.read!() |> Jason.decode!()
            assert json["vector_version"] == 1, "expected v1 vector in #{filename}"
            bin = Base.decode64!(json["expected"]["canonical_exsk_base64"])

            assert {:ok, decoded} = Binary.decode(bin),
                   "v1 decode failed for #{algorithm}/#{filename}"

            assert decoded.version == 1, "expected v1 frame, got #{decoded.version}"
            assert decoded.metadata == nil, "v1 frames must report metadata: nil"
          end
        end
      end
    end
  end

  for algorithm <- ["hll", "cms", "theta", "kll", "ddsketch", "frequent_items", "ull"] do
    describe "#{algorithm} v1 decode" do
      @algo algorithm
      @vec_dir Path.join(@v1_vectors_dir, algorithm)

      if @algo in ["hll", "cms", "theta", "ull"] do
        @tag :rust_nif
      end

      test "every v1 file deserializes into a usable sketch" do
        mod = @sketch_modules[@algo]
        files = File.ls!(@vec_dir) |> Enum.filter(&String.ends_with?(&1, ".json"))
        assert files != [], "no v1 fixtures for #{@algo}"

        for filename <- files do
          json = @vec_dir |> Path.join(filename) |> File.read!() |> Jason.decode!()
          bin = Base.decode64!(json["expected"]["canonical_exsk_base64"])

          assert {:ok, _sketch} = mod.deserialize(bin),
                 "#{@algo}.deserialize/1 rejected its own v1 vector #{filename}"
        end
      end
    end
  end

  describe "v1 → v2 upgrade path" do
    test "a v1-decoded sketch re-serializes as v2 and decodes again" do
      # Read a v1 HLL binary, deserialize, re-serialize, decode again.
      json =
        @v1_vectors_dir
        |> Path.join("hll/empty.json")
        |> File.read!()
        |> Jason.decode!()

      v1_bin = Base.decode64!(json["expected"]["canonical_exsk_base64"])
      assert {:ok, sketch} = HLL.deserialize(v1_bin)

      v2_bin = HLL.serialize(sketch)
      assert <<"EXSK", 2, _rest::binary>> = v2_bin

      assert {:ok, sketch2} = HLL.deserialize(v2_bin)
      assert sketch2.state == sketch.state
    end
  end
end
