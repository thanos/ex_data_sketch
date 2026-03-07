# Quotient Filter Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/quotient_bench.exs

alias ExDataSketch.{Backend, Quotient}

IO.puts("ExDataSketch Quotient Benchmark")
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
items_1k = Enum.map(1..1_000, &"quotient_bench_#{&1}")
items_100k = Enum.map(1..100_000, &"quotient_bench_#{&1}")
lookup_items = Enum.map(1..10_000, &"quotient_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"quotient_bench_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = Quotient.new(q: 16, r: 8, backend: backend)
    sketch_populated = Quotient.from_enumerable(items_1k, q: 16, r: 8, backend: backend)

    merge_a =
      Quotient.from_enumerable(Enum.take(items_1k, 500), q: 16, r: 8, backend: backend)

    merge_b =
      Quotient.from_enumerable(Enum.drop(items_1k, 500), q: 16, r: 8, backend: backend)

    binary = Quotient.serialize(sketch_populated)

    {name,
     %{
       sketch: sketch,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b,
       binary: binary
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"quotient_put_many 1k [#{name}]", fn -> Quotient.put_many(s.sketch, items_1k) end},
      {"quotient_put_many 100k [#{name}]", fn -> Quotient.put_many(s.sketch, items_100k) end},
      {"quotient_member? (hit) [#{name}]",
       fn -> Enum.each(lookup_items, &Quotient.member?(s.sketch_populated, &1)) end},
      {"quotient_member? (miss) [#{name}]",
       fn -> Enum.each(lookup_miss, &Quotient.member?(s.sketch_populated, &1)) end},
      {"quotient_delete [#{name}]",
       fn -> Quotient.delete(s.sketch_populated, "quotient_bench_1") end},
      {"quotient_merge [#{name}]", fn -> Quotient.merge(s.merge_a, s.merge_b) end},
      {"quotient_serialize [#{name}]", fn -> Quotient.serialize(s.sketch_populated) end},
      {"quotient_deserialize [#{name}]", fn -> {:ok, _} = Quotient.deserialize(s.binary) end},
      {"quotient_from_enumerable 1k [#{name}]",
       fn -> Quotient.from_enumerable(items_1k, q: 16, r: 8, backend: s.sketch.backend) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/quotient_bench.json"}
  ]
)
