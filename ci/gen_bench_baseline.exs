# Generates ci/bench_baseline.json from Benchee JSON output files.
#
# Usage:
#   MIX_ENV=dev mix run ci/gen_bench_baseline.exs [input_dir]
#
# input_dir defaults to bench/output/
# Extracts IPS values for Pure-backend scenarios only.

bench_dir = List.first(System.argv()) || "bench/output"

unless File.dir?(bench_dir) do
  IO.puts("Error: directory #{bench_dir} not found")
  System.halt(1)
end

{:ok, files} = File.ls(bench_dir)

all =
  files
  |> Enum.filter(&String.ends_with?(&1, ".json"))
  |> Enum.reduce(%{}, fn file, acc ->
    path = Path.join(bench_dir, file)
    {:ok, content} = File.read(path)
    {:ok, data} = Jason.decode(content)

    scenarios =
      case data do
        list when is_list(list) -> list
        %{"scenarios" => s} when is_list(s) -> s
        _ -> []
      end

    Enum.reduce(scenarios, acc, fn scenario, inner_acc ->
      name = Map.get(scenario, "name")

      ips =
        scenario
        |> Map.get("run_time_data", %{})
        |> Map.get("statistics", %{})
        |> Map.get("ips")

      if name && ips do
        Map.put(inner_acc, name, Float.round(ips, 2))
      else
        inner_acc
      end
    end)
  end)

# Keep only Pure-backend and non-backend-specific scenarios
baseline =
  all
  |> Enum.filter(fn {name, _} ->
    String.contains?(name, "[Pure]") or not String.contains?(name, "[")
  end)
  |> Map.new()

output = Jason.encode!(baseline, pretty: true)
File.write!("ci/bench_baseline.json", output <> "\n")
IO.puts("Wrote ci/bench_baseline.json with #{map_size(baseline)} scenarios")
