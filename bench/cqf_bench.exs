# CQF (Counting Quotient Filter) Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/cqf_bench.exs

alias ExDataSketch.{Backend, CQF}

IO.puts("ExDataSketch CQF Benchmark")
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
items_1k = Enum.map(1..1_000, &"cqf_bench_#{&1}")
items_100k = Enum.map(1..100_000, &"cqf_bench_#{&1}")
lookup_items = Enum.map(1..10_000, &"cqf_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"cqf_bench_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = CQF.new(q: 16, r: 8, backend: backend)
    sketch_populated = CQF.from_enumerable(items_1k, q: 16, r: 8, backend: backend)

    # For estimate_count: insert same item 100 times for multiplicity
    sketch_with_counts =
      Enum.reduce(1..100, sketch_populated, fn _, acc -> CQF.put(acc, "cqf_bench_1") end)

    merge_a =
      CQF.from_enumerable(Enum.take(items_1k, 500), q: 16, r: 8, backend: backend)

    merge_b =
      CQF.from_enumerable(Enum.drop(items_1k, 500), q: 16, r: 8, backend: backend)

    binary = CQF.serialize(sketch_populated)

    {name,
     %{
       sketch: sketch,
       sketch_populated: sketch_populated,
       sketch_with_counts: sketch_with_counts,
       merge_a: merge_a,
       merge_b: merge_b,
       binary: binary
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"cqf_put_many 1k [#{name}]", fn -> CQF.put_many(s.sketch, items_1k) end},
      {"cqf_put_many 100k [#{name}]", fn -> CQF.put_many(s.sketch, items_100k) end},
      {"cqf_member? (hit) [#{name}]",
       fn -> Enum.each(lookup_items, &CQF.member?(s.sketch_populated, &1)) end},
      {"cqf_member? (miss) [#{name}]",
       fn -> Enum.each(lookup_miss, &CQF.member?(s.sketch_populated, &1)) end},
      {"cqf_estimate_count [#{name}]",
       fn -> CQF.estimate_count(s.sketch_with_counts, "cqf_bench_1") end},
      {"cqf_delete [#{name}]", fn -> CQF.delete(s.sketch_populated, "cqf_bench_1") end},
      {"cqf_merge [#{name}]", fn -> CQF.merge(s.merge_a, s.merge_b) end},
      {"cqf_serialize [#{name}]", fn -> CQF.serialize(s.sketch_populated) end},
      {"cqf_deserialize [#{name}]", fn -> {:ok, _} = CQF.deserialize(s.binary) end},
      {"cqf_from_enumerable 1k [#{name}]",
       fn -> CQF.from_enumerable(items_1k, q: 16, r: 8, backend: s.sketch.backend) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/cqf_bench.json"}
  ]
)
