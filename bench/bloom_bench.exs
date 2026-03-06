# Bloom Filter Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/bloom_bench.exs

alias ExDataSketch.{Backend, Bloom}

IO.puts("ExDataSketch Bloom Benchmark")
IO.puts("============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

backends =
  [{"Pure", Backend.Pure}] ++
    if(Backend.Rust.available?(), do: [{"Rust", Backend.Rust}], else: [])

# Pre-generate deterministic item lists
items_1k = Enum.map(1..1_000, &"bloom_bench_#{&1}")
items_100k = Enum.map(1..100_000, &"bloom_bench_#{&1}")

# Pre-generate lookup items: half inserted, half not
lookup_items = Enum.map(1..10_000, &"bloom_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"bloom_bench_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = Bloom.new(capacity: 100_000, backend: backend)
    sketch_populated = Bloom.from_enumerable(items_1k, capacity: 100_000, backend: backend)

    merge_a =
      Bloom.from_enumerable(Enum.take(items_1k, 500), capacity: 100_000, backend: backend)

    merge_b =
      Bloom.from_enumerable(Enum.drop(items_1k, 500), capacity: 100_000, backend: backend)

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
  {"bloom_put_many 1k",
   fn s -> fn -> Bloom.put_many(s.sketch, items_1k) end end},
  {"bloom_put_many 100k",
   fn s -> fn -> Bloom.put_many(s.sketch, items_100k) end end},
  {"bloom_merge",
   fn s -> fn -> Bloom.merge(s.merge_a, s.merge_b) end end},
  {"bloom_member? (hit)",
   fn s ->
     fn -> Enum.each(lookup_items, &Bloom.member?(s.sketch_populated, &1)) end
   end},
  {"bloom_member? (miss)",
   fn s ->
     fn -> Enum.each(lookup_miss, &Bloom.member?(s.sketch_populated, &1)) end
   end}
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
