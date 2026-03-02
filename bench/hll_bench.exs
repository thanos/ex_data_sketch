# HLL Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/hll_bench.exs

alias ExDataSketch.{Backend, HLL}

IO.puts("ExDataSketch HLL Benchmark")
IO.puts("==========================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
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
    sketch_p10 = HLL.new(p: 10, backend: backend)
    sketch_p14 = HLL.new(p: 14, backend: backend)
    sketch_populated = HLL.from_enumerable(items_1k, p: 14, backend: backend)
    merge_a = HLL.from_enumerable(for(i <- 0..4999, do: "a_#{i}"), p: 14, backend: backend)
    merge_b = HLL.from_enumerable(for(i <- 0..4999, do: "b_#{i}"), p: 14, backend: backend)

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
      {"hll_update p=10 [#{name}]", fn -> HLL.update(s.sketch_p10, "bench_item") end},
      {"hll_update p=14 [#{name}]", fn -> HLL.update(s.sketch_p14, "bench_item") end},
      {"hll_update_many 1k [#{name}]", fn -> HLL.update_many(s.sketch_p14, items_1k) end},
      {"hll_update_many 100k [#{name}]", fn -> HLL.update_many(s.sketch_p14, items_100k) end},
      {"hll_merge p=14 [#{name}]", fn -> HLL.merge(s.merge_a, s.merge_b) end},
      {"hll_estimate p=14 [#{name}]", fn -> HLL.estimate(s.sketch_populated) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/hll_bench.json"}
  ]
)
