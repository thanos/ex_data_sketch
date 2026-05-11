defmodule ExDataSketch.VectorGenerator do
  @moduledoc false

  # Internal helper used to (re)generate the JSON golden vectors after the
  # EXSK frame format evolves. Not part of the public API.
  #
  # Usage:
  #
  #     EX_DATA_SKETCH_BUILD=1 mix run -e 'ExDataSketch.VectorGenerator.regenerate_all!()'
  #
  # The generator preserves the existing `expected` map shape and only updates
  # `canonical_exsk_base64`, the `vector_version`, and (when present) the
  # `merge_expected.canonical_exsk_base64`.

  alias ExDataSketch.TestVectors

  @vectors_dir Path.join([__DIR__, "..", "vectors"])

  @algorithms ~w(hll cms theta kll ddsketch frequent_items ull)

  @spec regenerate_all!() :: :ok
  def regenerate_all! do
    for algo <- @algorithms do
      regenerate!(algo)
    end

    :ok
  end

  @spec regenerate!(String.t()) :: :ok
  def regenerate!(algorithm) do
    IO.puts("Regenerating #{algorithm} vectors...")

    for {filename, vector} <- TestVectors.load_vectors(algorithm) do
      new_vector = regenerate_vector(algorithm, vector)
      path = Path.join([@vectors_dir, algorithm, filename])
      File.write!(path, Jason.encode!(new_vector, pretty: true) <> "\n")
      IO.puts("  wrote #{path}")
    end

    :ok
  end

  defp regenerate_vector(algorithm, vector) do
    sketch_mod = sketch_module(algorithm)
    opts = TestVectors.normalize_opts(algorithm, vector["algorithm_opts"])

    sketch = build_sketch(sketch_mod, opts, vector["input_items"])
    serialized = sketch_mod.serialize(sketch)

    expected =
      vector["expected"]
      |> Map.put("canonical_exsk_base64", Base.encode64(serialized))

    base = %{vector | "expected" => expected, "vector_version" => 2}

    case vector do
      %{"merge_inputs" => merge_inputs, "merge_expected" => merge_expected} ->
        sketch_a = build_sketch(sketch_mod, opts, vector["input_items"])
        sketch_b = build_sketch(sketch_mod, opts, merge_inputs)
        merged = sketch_mod.merge(sketch_a, sketch_b)
        merged_bin = sketch_mod.serialize(merged)

        merged_expected =
          Map.put(merge_expected, "canonical_exsk_base64", Base.encode64(merged_bin))

        Map.put(base, "merge_expected", merged_expected)

      _ ->
        base
    end
  end

  defp sketch_module("hll"), do: ExDataSketch.HLL
  defp sketch_module("cms"), do: ExDataSketch.CMS
  defp sketch_module("theta"), do: ExDataSketch.Theta
  defp sketch_module("kll"), do: ExDataSketch.KLL
  defp sketch_module("ddsketch"), do: ExDataSketch.DDSketch
  defp sketch_module("frequent_items"), do: ExDataSketch.FrequentItems
  defp sketch_module("ull"), do: ExDataSketch.ULL

  defp build_sketch(mod, opts, []), do: mod.new(opts)

  defp build_sketch(ExDataSketch.FrequentItems, opts, items),
    do: ExDataSketch.FrequentItems.from_enumerable(items, opts)

  defp build_sketch(ExDataSketch.KLL, opts, items) do
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    ExDataSketch.KLL.from_enumerable(values, opts)
  end

  defp build_sketch(ExDataSketch.DDSketch, opts, items) do
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    ExDataSketch.DDSketch.from_enumerable(values, opts)
  end

  defp build_sketch(mod, opts, items), do: mod.from_enumerable(items, opts)
end
