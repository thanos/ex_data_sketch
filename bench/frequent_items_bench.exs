# FrequentItems Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/frequent_items_bench.exs

alias ExDataSketch.{Backend, FrequentItems}

IO.puts("ExDataSketch FrequentItems Benchmark")
IO.puts("=====================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

backends =
  [{"Pure", Backend.Pure}] ++
    if(Backend.Rust.available?(), do: [{"Rust", Backend.Rust}], else: [])

# Pre-generate skewed data outside the benchmark closure.
# Zipf-like distribution: item i appears floor(N / i) times.
build_skewed = fn n ->
  Enum.flat_map(1..n, fn i ->
    count = max(div(n, i), 1)
    List.duplicate("item_#{i}", count)
  end)
  |> Enum.shuffle()
end

items_1k = build_skewed.(1_000)
items_100k = build_skewed.(100_000)

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = FrequentItems.new(k: 128, backend: backend)
    sketch_populated = FrequentItems.from_enumerable(items_1k, k: 128, backend: backend)

    merge_a =
      FrequentItems.from_enumerable(Enum.take(items_1k, 500), k: 128, backend: backend)

    merge_b =
      FrequentItems.from_enumerable(Enum.drop(items_1k, 500), k: 128, backend: backend)

    {name,
     %{
       sketch: sketch,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

File.mkdir_p!("bench/output")

bench_opts = [warmup: 1, time: 3, formatters: [Benchee.Formatters.Console]]

groups = [
  {"fi_update_many 1k", fn s -> fn -> FrequentItems.update_many(s.sketch, items_1k) end end},
  {"fi_update_many 100k", fn s -> fn -> FrequentItems.update_many(s.sketch, items_100k) end end},
  {"fi_merge k=128", fn s -> fn -> FrequentItems.merge(s.merge_a, s.merge_b) end end},
  {"fi_top_k", fn s -> fn -> FrequentItems.top_k(s.sketch_populated) end end}
]

for {label, bench_fn} <- groups do
  IO.puts("--- #{label} ---")

  benches =
    for {name, s} <- scenarios, into: %{} do
      {"#{label} [#{name}]", bench_fn.(s)}
    end

  Benchee.run(benches, bench_opts)
  IO.puts("")
end
