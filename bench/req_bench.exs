# REQ Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/req_bench.exs

alias ExDataSketch.{Backend, REQ}

IO.puts("ExDataSketch REQ Benchmark")
IO.puts("==========================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

# REQ has no Rust NIF acceleration, Pure only
backends = [{"Pure", Backend.Pure}]

items_1k = for i <- 1..1_000, do: i * 1.0
items_100k = for i <- 1..100_000, do: i * 1.0

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch_k12 = REQ.new(k: 12, backend: backend)
    sketch_populated = REQ.from_enumerable(items_1k, k: 12, backend: backend)
    merge_a = REQ.from_enumerable(Enum.map(1..5000, &(&1 * 1.0)), k: 12, backend: backend)
    merge_b = REQ.from_enumerable(Enum.map(5001..10000, &(&1 * 1.0)), k: 12, backend: backend)

    {name,
     %{
       sketch_k12: sketch_k12,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"req_update k=12 [#{name}]", fn -> REQ.update(s.sketch_k12, 42.0) end},
      {"req_update_many 1k [#{name}]", fn -> REQ.update_many(s.sketch_k12, items_1k) end},
      {"req_update_many 100k [#{name}]", fn -> REQ.update_many(s.sketch_k12, items_100k) end},
      {"req_merge k=12 [#{name}]", fn -> REQ.merge(s.merge_a, s.merge_b) end},
      {"req_quantile p50 [#{name}]", fn -> REQ.quantile(s.sketch_populated, 0.5) end},
      {"req_quantile p95 [#{name}]", fn -> REQ.quantile(s.sketch_populated, 0.95) end},
      {"req_rank value=500 [#{name}]", fn -> REQ.rank(s.sketch_populated, 500.0) end},
      {"req_cdf [#{name}]", fn -> REQ.cdf(s.sketch_populated, [100.0, 500.0, 900.0]) end},
      {"req_pmf [#{name}]", fn -> REQ.pmf(s.sketch_populated, [100.0, 500.0, 900.0]) end},
      {"req_serialize [#{name}]", fn -> REQ.serialize(s.sketch_populated) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/req_bench.json"}
  ]
)
