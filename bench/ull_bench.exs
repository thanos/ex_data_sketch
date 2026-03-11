# ULL Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/ull_bench.exs

alias ExDataSketch.{Backend, ULL}

IO.puts("ExDataSketch ULL Benchmark")
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
items_1k = for i <- 0..999, do: "item_#{i}"
items_100k = for i <- 0..99_999, do: "item_#{i}"

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch_p10 = ULL.new(p: 10, backend: backend)
    sketch_p14 = ULL.new(p: 14, backend: backend)
    sketch_populated = ULL.from_enumerable(items_1k, p: 14, backend: backend)
    merge_a = ULL.from_enumerable(for(i <- 0..4999, do: "a_#{i}"), p: 14, backend: backend)
    merge_b = ULL.from_enumerable(for(i <- 0..4999, do: "b_#{i}"), p: 14, backend: backend)

    {name,
     %{
       sketch_p10: sketch_p10,
       sketch_p14: sketch_p14,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"ull_update p=10 [#{name}]", fn -> ULL.update(s.sketch_p10, "bench_item") end},
      {"ull_update p=14 [#{name}]", fn -> ULL.update(s.sketch_p14, "bench_item") end},
      {"ull_update_many 1k [#{name}]", fn -> ULL.update_many(s.sketch_p14, items_1k) end},
      {"ull_update_many 100k [#{name}]", fn -> ULL.update_many(s.sketch_p14, items_100k) end},
      {"ull_merge p=14 [#{name}]", fn -> ULL.merge(s.merge_a, s.merge_b) end},
      {"ull_estimate p=14 [#{name}]", fn -> ULL.estimate(s.sketch_populated) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/ull_bench.json"}
  ]
)
