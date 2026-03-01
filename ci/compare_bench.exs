# Benchmark comparison script for CI.
#
# Compares current benchmark results against baseline.
# Warns (or fails) if performance regresses beyond threshold.

defmodule CI.CompareBench do
  @baseline_file "ci/bench_baseline.json"
  # Percentage regression that triggers failure
  @regression_threshold 15.0

  def run do
    case File.read(@baseline_file) do
      {:ok, content} ->
        case Jason.decode(content) do
          {:ok, baseline} -> compare(baseline)
          _ -> warn_no_baseline("Could not parse baseline file")
        end

      {:error, _} ->
        warn_no_baseline("No baseline file found at #{@baseline_file}")
    end
  end

  defp compare(baseline) do
    current = read_current_results()

    if map_size(current) == 0 do
      IO.puts("WARN: No current benchmark results found")
      :ok
    else
      regressions =
        Enum.reduce(baseline, [], fn {name, base_ips}, acc ->
          case Map.get(current, name) do
            nil ->
              IO.puts("WARN: Benchmark '#{name}' missing from current run")
              acc

            current_ips ->
              change = (current_ips - base_ips) / base_ips * 100

              if change < -@regression_threshold do
                IO.puts("REGRESSION: '#{name}' is #{abs(Float.round(change, 1))}% slower")
                [{name, change} | acc]
              else
                IO.puts("OK: '#{name}' change: #{Float.round(change, 1)}%")
                acc
              end
          end
        end)

      if length(regressions) > 0 do
        IO.puts("\nFAIL: #{length(regressions)} benchmark(s) regressed beyond #{@regression_threshold}%")
        System.halt(1)
      else
        IO.puts("\nPASS: No significant regressions detected")
      end
    end
  end

  defp read_current_results do
    bench_dir = "bench/output"

    case File.ls(bench_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".json"))
        |> Enum.reduce(%{}, fn file, acc ->
          path = Path.join(bench_dir, file)

          case File.read(path) do
            {:ok, content} ->
              case Jason.decode(content) do
                {:ok, data} -> extract_ips(data, acc)
                _ -> acc
              end

            _ ->
              acc
          end
        end)

      {:error, _} ->
        %{}
    end
  end

  defp extract_ips(%{"scenarios" => scenarios}, acc) when is_list(scenarios) do
    Enum.reduce(scenarios, acc, fn scenario, inner_acc ->
      name = scenario["name"] || "unknown"
      ips = get_in(scenario, ["run_time_data", "statistics", "ips"]) || 0
      Map.put(inner_acc, name, ips)
    end)
  end

  defp extract_ips(_, acc), do: acc

  defp warn_no_baseline(reason) do
    IO.puts("WARN: #{reason}")
    IO.puts("Skipping benchmark comparison. Run benchmarks on main to establish a baseline.")
    IO.puts("To create a baseline: mix run bench/hll_bench.exs && mix run bench/cms_bench.exs")
    IO.puts("Then copy bench/output/*.json to ci/bench_baseline.json")
  end
end

CI.CompareBench.run()
