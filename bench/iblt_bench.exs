# IBLT (Invertible Bloom Lookup Table) Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/iblt_bench.exs

alias ExDataSketch.{Backend, IBLT}

IO.puts("ExDataSketch IBLT Benchmark")
IO.puts("===========================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

backends =
  [{"Pure", Backend.Pure}] ++
    if(Backend.Rust.available?(), do: [{"Rust", Backend.Rust}], else: [])

# Pre-generate data outside the benchmark closure
items_1k = Enum.map(1..1_000, &"iblt_bench_#{&1}")
items_100k = Enum.map(1..100_000, &"iblt_bench_#{&1}")
lookup_items = Enum.map(1..10_000, &"iblt_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"iblt_bench_#{&1}")

# For subtract/list_entries: overlapping sets with small difference
set_a_items = Enum.map(1..600, &"iblt_bench_#{&1}")
set_b_items = Enum.map(1..500, &"iblt_bench_#{&1}") ++ Enum.map(601..700, &"iblt_bench_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = IBLT.new(cell_count: 2000, backend: backend)
    sketch_100k = IBLT.new(cell_count: 200_000, backend: backend)
    sketch_populated = IBLT.from_enumerable(items_1k, cell_count: 2000, backend: backend)

    iblt_a = IBLT.from_enumerable(set_a_items, cell_count: 2000, backend: backend)
    iblt_b = IBLT.from_enumerable(set_b_items, cell_count: 2000, backend: backend)
    diff = IBLT.subtract(iblt_a, iblt_b)

    merge_a =
      IBLT.from_enumerable(Enum.take(items_1k, 500), cell_count: 2000, backend: backend)

    merge_b =
      IBLT.from_enumerable(Enum.drop(items_1k, 500), cell_count: 2000, backend: backend)

    binary = IBLT.serialize(sketch_populated)

    {name,
     %{
       sketch: sketch,
       sketch_100k: sketch_100k,
       sketch_populated: sketch_populated,
       iblt_a: iblt_a,
       iblt_b: iblt_b,
       diff: diff,
       merge_a: merge_a,
       merge_b: merge_b,
       binary: binary
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"iblt_put_many 1k [#{name}]", fn -> IBLT.put_many(s.sketch, items_1k) end},
      {"iblt_put_many 100k [#{name}]", fn -> IBLT.put_many(s.sketch_100k, items_100k) end},
      {"iblt_member? (hit) [#{name}]",
       fn -> Enum.each(lookup_items, &IBLT.member?(s.sketch_populated, &1)) end},
      {"iblt_member? (miss) [#{name}]",
       fn -> Enum.each(lookup_miss, &IBLT.member?(s.sketch_populated, &1)) end},
      {"iblt_delete [#{name}]", fn -> IBLT.delete(s.sketch_populated, "iblt_bench_1") end},
      {"iblt_subtract [#{name}]", fn -> IBLT.subtract(s.iblt_a, s.iblt_b) end},
      {"iblt_list_entries [#{name}]", fn -> IBLT.list_entries(s.diff) end},
      {"iblt_merge [#{name}]", fn -> IBLT.merge(s.merge_a, s.merge_b) end},
      {"iblt_serialize [#{name}]", fn -> IBLT.serialize(s.sketch_populated) end},
      {"iblt_deserialize [#{name}]", fn -> {:ok, _} = IBLT.deserialize(s.binary) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/iblt_bench.json"}
  ]
)
