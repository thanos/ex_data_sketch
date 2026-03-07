# FilterChain Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/filter_chain_bench.exs

alias ExDataSketch.{Cuckoo, FilterChain, Quotient, XorFilter}

IO.puts("ExDataSketch FilterChain Benchmark")
IO.puts("===================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{ExDataSketch.Backend.Rust.available?()}")
IO.puts("")

# Pre-generate data outside the benchmark closure
items_1k = Enum.map(1..1_000, &"chain_bench_#{&1}")
lookup_items = Enum.map(1..10_000, &"chain_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"chain_bench_#{&1}")

# Build populated stages
{:ok, cuckoo_pop} = Cuckoo.from_enumerable(items_1k, capacity: 200_000)
quotient_pop = Quotient.from_enumerable(items_1k, q: 16, r: 8)
{:ok, xor_pop} = XorFilter.build(items_1k)

# Chain 1: Cuckoo-only (baseline for dispatch overhead)
chain_cuckoo = FilterChain.new() |> FilterChain.add_stage(cuckoo_pop)

# Chain 2: Cuckoo + Quotient (multi-stage dynamic)
chain_cuckoo_quotient =
  FilterChain.new()
  |> FilterChain.add_stage(cuckoo_pop)
  |> FilterChain.add_stage(quotient_pop)

# Chain 3: Cuckoo + XorFilter (dynamic + static terminal)
chain_cuckoo_xor =
  FilterChain.new()
  |> FilterChain.add_stage(cuckoo_pop)
  |> FilterChain.add_stage(xor_pop)

# Empty chains for put benchmarks
chain_cuckoo_empty =
  FilterChain.new() |> FilterChain.add_stage(Cuckoo.new(capacity: 200_000))

chain_cuckoo_quotient_empty =
  FilterChain.new()
  |> FilterChain.add_stage(Cuckoo.new(capacity: 200_000))
  |> FilterChain.add_stage(Quotient.new(q: 16, r: 8))

# Pre-serialize for deserialization benchmarks
chain_cuckoo_quotient_bin = FilterChain.serialize(chain_cuckoo_quotient)
chain_cuckoo_xor_bin = FilterChain.serialize(chain_cuckoo_xor)

benches = %{
  "chain_member? (hit) [Cuckoo]" => fn ->
    Enum.each(lookup_items, &FilterChain.member?(chain_cuckoo, &1))
  end,
  "chain_member? (hit) [Cuckoo+Quotient]" => fn ->
    Enum.each(lookup_items, &FilterChain.member?(chain_cuckoo_quotient, &1))
  end,
  "chain_member? (hit) [Cuckoo+XorFilter]" => fn ->
    Enum.each(lookup_items, &FilterChain.member?(chain_cuckoo_xor, &1))
  end,
  "chain_member? (miss) [Cuckoo+Quotient]" => fn ->
    Enum.each(lookup_miss, &FilterChain.member?(chain_cuckoo_quotient, &1))
  end,
  "chain_put [Cuckoo]" => fn -> {:ok, _} = FilterChain.put(chain_cuckoo_empty, "bench_item") end,
  "chain_put [Cuckoo+Quotient]" => fn ->
    {:ok, _} = FilterChain.put(chain_cuckoo_quotient_empty, "bench_item")
  end,
  "chain_serialize [Cuckoo+Quotient]" => fn -> FilterChain.serialize(chain_cuckoo_quotient) end,
  "chain_deserialize [Cuckoo+Quotient]" => fn ->
    {:ok, _} = FilterChain.deserialize(chain_cuckoo_quotient_bin)
  end,
  "chain_serialize [Cuckoo+XorFilter]" => fn -> FilterChain.serialize(chain_cuckoo_xor) end
}

File.mkdir_p!("bench/output")

Benchee.run(
  benches,
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/filter_chain_bench.json"}
  ]
)
