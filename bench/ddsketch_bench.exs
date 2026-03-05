# DDSketch Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/ddsketch_bench.exs

alias ExDataSketch.{Backend, DDSketch}

IO.puts("ExDataSketch DDSketch Benchmark")
IO.puts("===============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

backends =
  [{"Pure", Backend.Pure}] ++
    if(Backend.Rust.available?(), do: [{"Rust", Backend.Rust}], else: [])

# Pre-generate data outside the benchmark closure
items_1k = for i <- 1..1_000, do: i * 1.0
items_100k = for i <- 1..100_000, do: i * 1.0

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = DDSketch.new(alpha: 0.01, backend: backend)
    sketch_populated = DDSketch.from_enumerable(items_1k, alpha: 0.01, backend: backend)

    merge_a =
      DDSketch.from_enumerable(Enum.map(1..5000, &(&1 * 1.0)), alpha: 0.01, backend: backend)

    merge_b =
      DDSketch.from_enumerable(Enum.map(5001..10000, &(&1 * 1.0)), alpha: 0.01, backend: backend)

    {name,
     %{
       sketch: sketch,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"ddsketch_update_many 1k [#{name}]", fn -> DDSketch.update_many(s.sketch, items_1k) end},
      {"ddsketch_update_many 100k [#{name}]",
       fn -> DDSketch.update_many(s.sketch, items_100k) end},
      {"ddsketch_merge [#{name}]", fn -> DDSketch.merge(s.merge_a, s.merge_b) end},
      {"ddsketch_quantile p50 [#{name}]", fn -> DDSketch.quantile(s.sketch_populated, 0.5) end},
      {"ddsketch_quantile p95 [#{name}]", fn -> DDSketch.quantile(s.sketch_populated, 0.95) end},
      {"ddsketch_quantile p99 [#{name}]", fn -> DDSketch.quantile(s.sketch_populated, 0.99) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/ddsketch_bench.json"}
  ]
)
