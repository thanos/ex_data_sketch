# KLL Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/kll_bench.exs

alias ExDataSketch.{Backend, KLL}

IO.puts("ExDataSketch KLL Benchmark")
IO.puts("==========================")
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
    sketch_k200 = KLL.new(k: 200, backend: backend)
    sketch_populated = KLL.from_enumerable(items_1k, k: 200, backend: backend)
    merge_a = KLL.from_enumerable(Enum.map(1..5000, &(&1 * 1.0)), k: 200, backend: backend)
    merge_b = KLL.from_enumerable(Enum.map(5001..10000, &(&1 * 1.0)), k: 200, backend: backend)

    {name,
     %{
       sketch_k200: sketch_k200,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"kll_update k=200 [#{name}]", fn -> KLL.update(s.sketch_k200, 42.0) end},
      {"kll_update_many 1k [#{name}]", fn -> KLL.update_many(s.sketch_k200, items_1k) end},
      {"kll_update_many 100k [#{name}]", fn -> KLL.update_many(s.sketch_k200, items_100k) end},
      {"kll_merge k=200 [#{name}]", fn -> KLL.merge(s.merge_a, s.merge_b) end},
      {"kll_quantile p50 [#{name}]", fn -> KLL.quantile(s.sketch_populated, 0.5) end},
      {"kll_quantile p95 [#{name}]", fn -> KLL.quantile(s.sketch_populated, 0.95) end},
      {"kll_quantile p99 [#{name}]", fn -> KLL.quantile(s.sketch_populated, 0.99) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/kll_bench.json"}
  ]
)
