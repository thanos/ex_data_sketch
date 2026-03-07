# Cuckoo Filter Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/cuckoo_bench.exs

alias ExDataSketch.{Backend, Cuckoo}

IO.puts("ExDataSketch Cuckoo Benchmark")
IO.puts("=============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

backends =
  [{"Pure", Backend.Pure}] ++
    if(Backend.Rust.available?(), do: [{"Rust", Backend.Rust}], else: [])

# Pre-generate data outside the benchmark closure
items_1k = Enum.map(1..1_000, &"cuckoo_bench_#{&1}")
items_100k = Enum.map(1..100_000, &"cuckoo_bench_#{&1}")
lookup_items = Enum.map(1..10_000, &"cuckoo_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"cuckoo_bench_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = Cuckoo.new(capacity: 200_000, backend: backend)

    {:ok, sketch_populated} =
      Cuckoo.from_enumerable(items_1k, capacity: 200_000, backend: backend)

    binary = Cuckoo.serialize(sketch_populated)

    {name,
     %{
       sketch: sketch,
       sketch_populated: sketch_populated,
       binary: binary
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"cuckoo_put_many 1k [#{name}]", fn -> {:ok, _} = Cuckoo.put_many(s.sketch, items_1k) end},
      {"cuckoo_put_many 100k [#{name}]",
       fn -> {:ok, _} = Cuckoo.put_many(s.sketch, items_100k) end},
      {"cuckoo_member? (hit) [#{name}]",
       fn -> Enum.each(lookup_items, &Cuckoo.member?(s.sketch_populated, &1)) end},
      {"cuckoo_member? (miss) [#{name}]",
       fn -> Enum.each(lookup_miss, &Cuckoo.member?(s.sketch_populated, &1)) end},
      {"cuckoo_delete [#{name}]",
       fn -> {:ok, _} = Cuckoo.delete(s.sketch_populated, "cuckoo_bench_1") end},
      {"cuckoo_serialize [#{name}]", fn -> Cuckoo.serialize(s.sketch_populated) end},
      {"cuckoo_deserialize [#{name}]", fn -> {:ok, _} = Cuckoo.deserialize(s.binary) end},
      {"cuckoo_from_enumerable 1k [#{name}]",
       fn ->
         {:ok, _} = Cuckoo.from_enumerable(items_1k, capacity: 200_000, backend: s.sketch.backend)
       end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/cuckoo_bench.json"}
  ]
)
