# Bloom Filter Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/bloom_bench.exs

alias ExDataSketch.{Backend, Bloom, Hash}

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

# Phase 6 moved item hashing for put_many from Elixir into the Rust NIF
# itself (see guides/filter_performance.md). This reproduces the old
# pre-hash-in-Elixir-then-NIF path directly against the backend, bypassing
# Bloom.put_many's now-automatic raw dispatch, as the "before" baseline.
legacy_put_many = fn items ->
  if Backend.Rust.available?() do
    sketch = Bloom.new(capacity: 100_000, backend: Backend.Rust)
    seed = Keyword.get(sketch.opts, :seed, 0)

    fn ->
      hashes = Enum.map(items, &Hash.hash64(&1, seed: seed))
      Backend.Rust.bloom_put_many(sketch.state, hashes, sketch.opts)
    end
  end
end

groups = [
  {"bloom_put_many 1k", fn s -> fn -> Bloom.put_many(s.sketch, items_1k) end end},
  {"bloom_put_many 100k", fn s -> fn -> Bloom.put_many(s.sketch, items_100k) end end},
  {"bloom_merge", fn s -> fn -> Bloom.merge(s.merge_a, s.merge_b) end end},
  {"bloom_member? (hit)",
   fn s ->
     fn -> Enum.each(lookup_items, &Bloom.member?(s.sketch_populated, &1)) end
   end},
  {"bloom_member? (miss)",
   fn s ->
     fn -> Enum.each(lookup_miss, &Bloom.member?(s.sketch_populated, &1)) end
   end}
]

legacy_by_label = %{
  "bloom_put_many 1k" => legacy_put_many.(items_1k),
  "bloom_put_many 100k" => legacy_put_many.(items_100k)
}

for {label, bench_fn} <- groups do
  IO.puts("--- #{label} ---")

  benches =
    for {name, s} <- scenarios, into: %{} do
      {"#{label} [#{name}]", bench_fn.(s)}
    end

  benches =
    case Map.get(legacy_by_label, label) do
      nil -> benches
      legacy_fn -> Map.put(benches, "#{label} [Rust (pre-hashed, legacy)]", legacy_fn)
    end

  Benchee.run(benches, bench_opts)
  IO.puts("")
end
