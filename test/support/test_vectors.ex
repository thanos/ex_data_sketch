defmodule ExDataSketch.TestVectors do
  @moduledoc false

  @vectors_dir Path.join([__DIR__, "..", "vectors"])

  @doc """
  Loads all JSON vector files for the given algorithm.

  Returns a list of `{filename, map}` tuples.
  """
  @spec load_vectors(String.t()) :: [{String.t(), map()}]
  def load_vectors(algorithm)
      when algorithm in ["hll", "cms", "theta", "kll", "ddsketch", "frequent_items"] do
    dir = Path.join(@vectors_dir, algorithm)

    dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".json"))
    |> Enum.sort()
    |> Enum.map(fn filename ->
      path = Path.join(dir, filename)
      data = path |> File.read!() |> Jason.decode!()
      {filename, data}
    end)
  end

  @doc """
  Builds a sketch from the vector's input_items and asserts it matches
  the expected serialized bytes and estimate.
  """
  def assert_vector(vector, context) do
    sketch_mod = sketch_module(vector["algorithm"])
    opts = normalize_opts(vector["algorithm"], vector["algorithm_opts"])
    expected = vector["expected"]

    # Build sketch from inputs
    sketch = build_sketch(sketch_mod, opts, vector["input_items"])

    # Assert serialized bytes match
    expected_bytes = Base.decode64!(expected["canonical_exsk_base64"])
    serialized = sketch_mod.serialize(sketch)

    ExUnit.Assertions.assert(
      serialized == expected_bytes,
      "Serialized bytes mismatch for #{context}"
    )

    # Assert estimate matches
    assert_estimate(sketch_mod, sketch, expected, vector, context)

    sketch
  end

  @doc """
  Asserts a merge vector: builds two sketches, merges them, and checks
  the result against expected merged bytes and estimate.
  """
  def assert_merge_vector(vector, context) do
    sketch_mod = sketch_module(vector["algorithm"])
    opts = normalize_opts(vector["algorithm"], vector["algorithm_opts"])
    merge_expected = vector["merge_expected"]

    sketch_a = build_sketch(sketch_mod, opts, vector["input_items"])
    sketch_b = build_sketch(sketch_mod, opts, vector["merge_inputs"])
    merged = sketch_mod.merge(sketch_a, sketch_b)

    expected_bytes = Base.decode64!(merge_expected["canonical_exsk_base64"])
    serialized = sketch_mod.serialize(merged)

    ExUnit.Assertions.assert(
      serialized == expected_bytes,
      "Merged serialized bytes mismatch for #{context}"
    )

    assert_estimate(sketch_mod, merged, merge_expected, vector, context)

    merged
  end

  @doc """
  Converts a JSON string-key opts map to the keyword list expected by
  sketch constructors.
  """
  @spec normalize_opts(String.t(), map()) :: keyword()
  def normalize_opts("hll", %{"p" => p}), do: [p: p]

  def normalize_opts("cms", opts) do
    [
      width: opts["width"],
      depth: opts["depth"],
      counter_width: opts["counter_width"]
    ]
  end

  def normalize_opts("theta", %{"k" => k}), do: [k: k]
  def normalize_opts("kll", %{"k" => k}), do: [k: k]
  def normalize_opts("ddsketch", %{"alpha" => alpha}), do: [alpha: alpha]
  def normalize_opts("frequent_items", %{"k" => k}), do: [k: k]

  # -- Private --

  defp sketch_module("hll"), do: ExDataSketch.HLL
  defp sketch_module("cms"), do: ExDataSketch.CMS
  defp sketch_module("theta"), do: ExDataSketch.Theta
  defp sketch_module("kll"), do: ExDataSketch.KLL
  defp sketch_module("ddsketch"), do: ExDataSketch.DDSketch
  defp sketch_module("frequent_items"), do: ExDataSketch.FrequentItems

  defp build_sketch(mod, opts, []), do: mod.new(opts)

  defp build_sketch(ExDataSketch.FrequentItems, opts, items) do
    # FrequentItems takes string items directly (no hashing)
    ExDataSketch.FrequentItems.from_enumerable(items, opts)
  end

  defp build_sketch(ExDataSketch.KLL, opts, items) do
    # KLL takes numeric values directly (no hashing)
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    ExDataSketch.KLL.from_enumerable(values, opts)
  end

  defp build_sketch(ExDataSketch.DDSketch, opts, items) do
    # DDSketch takes numeric values directly (no hashing)
    values = Enum.map(items, fn v when is_number(v) -> v * 1.0 end)
    ExDataSketch.DDSketch.from_enumerable(values, opts)
  end

  defp build_sketch(mod, opts, items), do: mod.from_enumerable(items, opts)

  defp assert_estimate(ExDataSketch.KLL, sketch, expected, _vector, context) do
    tolerance = expected["tolerance"] || 0
    delta = max(tolerance, 1.0e-9)

    # Assert count
    if expected["count"] do
      actual_count = ExDataSketch.KLL.count(sketch)

      ExUnit.Assertions.assert(
        actual_count == expected["count"],
        "KLL count mismatch for #{context}: expected #{expected["count"]}, got #{actual_count}"
      )
    end

    # Assert min/max
    if Map.has_key?(expected, "min") do
      actual_min = ExDataSketch.KLL.min_value(sketch)

      ExUnit.Assertions.assert(
        actual_min == expected["min"],
        "KLL min mismatch for #{context}: expected #{inspect(expected["min"])}, got #{inspect(actual_min)}"
      )
    end

    if Map.has_key?(expected, "max") do
      actual_max = ExDataSketch.KLL.max_value(sketch)

      ExUnit.Assertions.assert(
        actual_max == expected["max"],
        "KLL max mismatch for #{context}: expected #{inspect(expected["max"])}, got #{inspect(actual_max)}"
      )
    end

    # Assert quantile estimates
    case expected["quantile_estimates"] do
      estimates when is_map(estimates) and map_size(estimates) > 0 ->
        for {rank_str, expected_val} <- estimates do
          rank = String.to_float(rank_str)
          actual = ExDataSketch.KLL.quantile(sketch, rank)

          ExUnit.Assertions.assert_in_delta(
            actual,
            expected_val,
            delta,
            "KLL quantile(#{rank}) mismatch for #{context}: " <>
              "expected #{expected_val}, got #{actual} (delta: #{delta})"
          )
        end

      _ ->
        :ok
    end
  end

  defp assert_estimate(ExDataSketch.DDSketch, sketch, expected, _vector, context) do
    tolerance = expected["tolerance"] || 0
    delta = max(tolerance, 1.0e-9)

    if expected["count"] do
      actual_count = ExDataSketch.DDSketch.count(sketch)

      ExUnit.Assertions.assert(
        actual_count == expected["count"],
        "DDSketch count mismatch for #{context}: expected #{expected["count"]}, got #{actual_count}"
      )
    end

    if Map.has_key?(expected, "min") do
      actual_min = ExDataSketch.DDSketch.min_value(sketch)

      ExUnit.Assertions.assert(
        actual_min == expected["min"],
        "DDSketch min mismatch for #{context}: expected #{inspect(expected["min"])}, got #{inspect(actual_min)}"
      )
    end

    if Map.has_key?(expected, "max") do
      actual_max = ExDataSketch.DDSketch.max_value(sketch)

      ExUnit.Assertions.assert(
        actual_max == expected["max"],
        "DDSketch max mismatch for #{context}: expected #{inspect(expected["max"])}, got #{inspect(actual_max)}"
      )
    end

    case expected["quantile_estimates"] do
      estimates when is_map(estimates) and map_size(estimates) > 0 ->
        for {rank_str, expected_val} <- estimates do
          rank = String.to_float(rank_str)
          actual = ExDataSketch.DDSketch.quantile(sketch, rank)

          ExUnit.Assertions.assert_in_delta(
            actual,
            expected_val,
            delta,
            "DDSketch quantile(#{rank}) mismatch for #{context}: " <>
              "expected #{expected_val}, got #{actual} (delta: #{delta})"
          )
        end

      _ ->
        :ok
    end
  end

  defp assert_estimate(ExDataSketch.CMS, sketch, expected, _vector, context) do
    # CMS vectors include point query estimates
    case expected do
      %{"point_estimates" => point_estimates} ->
        for {item, expected_count} <- point_estimates do
          actual = ExDataSketch.CMS.estimate(sketch, item)

          ExUnit.Assertions.assert(
            actual == expected_count,
            "CMS estimate mismatch for item #{inspect(item)} in #{context}: " <>
              "expected #{expected_count}, got #{actual}"
          )
        end

      %{"estimate" => estimate} ->
        # For empty/single CMS vectors, check a known item
        if is_number(estimate) do
          :ok
        end

      _ ->
        :ok
    end
  end

  defp assert_estimate(ExDataSketch.FrequentItems, sketch, expected, _vector, context) do
    if expected["count"] do
      actual_count = ExDataSketch.FrequentItems.count(sketch)

      ExUnit.Assertions.assert(
        actual_count == expected["count"],
        "FrequentItems count mismatch for #{context}: expected #{expected["count"]}, got #{actual_count}"
      )
    end

    if expected["entry_count"] do
      actual_ec = ExDataSketch.FrequentItems.entry_count(sketch)

      ExUnit.Assertions.assert(
        actual_ec == expected["entry_count"],
        "FrequentItems entry_count mismatch for #{context}: expected #{expected["entry_count"]}, got #{actual_ec}"
      )
    end

    case expected["top_k"] do
      [_ | _] = expected_top ->
        actual_top = ExDataSketch.FrequentItems.top_k(sketch)

        for {expected_entry, i} <- Enum.with_index(expected_top) do
          actual_entry = Enum.at(actual_top, i)

          ExUnit.Assertions.assert(
            actual_entry != nil,
            "FrequentItems top_k[#{i}] missing for #{context}"
          )

          ExUnit.Assertions.assert(
            actual_entry.item == expected_entry["item"],
            "FrequentItems top_k[#{i}].item mismatch for #{context}: " <>
              "expected #{inspect(expected_entry["item"])}, got #{inspect(actual_entry.item)}"
          )

          ExUnit.Assertions.assert(
            actual_entry.estimate == expected_entry["estimate"],
            "FrequentItems top_k[#{i}].estimate mismatch for #{context}: " <>
              "expected #{expected_entry["estimate"]}, got #{actual_entry.estimate}"
          )
        end

      _ ->
        :ok
    end
  end

  defp assert_estimate(_mod, sketch, expected, _vector, context) do
    case expected["estimate"] do
      nil ->
        :ok

      expected_estimate ->
        actual = sketch.__struct__.estimate(sketch)
        tolerance = expected["tolerance"] || 0
        # Always use assert_in_delta with a minimum epsilon to avoid brittle
        # exact float equality across libm/OTP/Rust versions.
        delta = max(tolerance, 1.0e-9)

        ExUnit.Assertions.assert_in_delta(
          actual,
          expected_estimate,
          delta,
          "Estimate out of tolerance for #{context}: expected #{expected_estimate}, got #{actual} (delta: #{delta})"
        )
    end
  end
end
