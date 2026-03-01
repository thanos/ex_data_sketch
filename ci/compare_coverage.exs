# Coverage comparison script for CI.
#
# Compares current test coverage against the baseline threshold.
# Fails if coverage drops below the configured minimum.

defmodule CI.CompareCoverage do
  @baseline_file "ci/coverage_baseline.json"
  @default_threshold 70.0
  # Maximum allowed drop from baseline (percentage points)
  @tolerance 0.5

  def run do
    threshold = read_baseline_threshold()
    coverage = read_current_coverage()

    IO.puts("Coverage: #{coverage}%")
    IO.puts("Baseline threshold: #{threshold}%")
    IO.puts("Tolerance: #{@tolerance} percentage points")

    cond do
      coverage < @default_threshold ->
        IO.puts("FAIL: Coverage #{coverage}% is below minimum #{@default_threshold}%")
        System.halt(1)

      coverage < threshold - @tolerance ->
        IO.puts("FAIL: Coverage #{coverage}% dropped below baseline #{threshold}% (tolerance: #{@tolerance})")
        System.halt(1)

      true ->
        IO.puts("PASS: Coverage #{coverage}% meets threshold")
    end
  end

  defp read_baseline_threshold do
    case File.read(@baseline_file) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, %{"threshold" => threshold}} -> threshold
          _ -> @default_threshold
        end

      {:error, _} ->
        IO.puts("No baseline file found at #{@baseline_file}, using default #{@default_threshold}%")
        @default_threshold
    end
  end

  defp read_current_coverage do
    case File.read("cover/excoveralls.json") do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, %{"source_files" => files}} ->
            {total_relevant, total_covered} =
              Enum.reduce(files, {0, 0}, fn file, {rel, cov} ->
                coverage = file["coverage"] || []
                relevant = Enum.count(coverage, &(not is_nil(&1)))
                covered = Enum.count(coverage, &(&1 != nil and &1 > 0))
                {rel + relevant, cov + covered}
              end)

            if total_relevant > 0 do
              Float.round(total_covered / total_relevant * 100, 1)
            else
              0.0
            end

          _ ->
            IO.puts("WARN: Could not parse excoveralls.json")
            0.0
        end

      {:error, _} ->
        IO.puts("WARN: No coverage report found. Run mix coveralls.json first.")
        0.0
    end
  end
end

CI.CompareCoverage.run()
