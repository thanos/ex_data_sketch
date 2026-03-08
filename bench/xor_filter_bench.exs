# XorFilter Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/xor_filter_bench.exs

alias ExDataSketch.{Backend, XorFilter}

IO.puts("ExDataSketch XorFilter Benchmark")
IO.puts("================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

backends =
  [{"Pure", Backend.Pure}] ++
    if(Backend.Rust.available?(), do: [{"Rust", Backend.Rust}], else: [])

# Pre-generate data outside the benchmark closure
items_1k = Enum.map(1..1_000, &"xor_bench_#{&1}")
items_100k = Enum.map(1..100_000, &"xor_bench_#{&1}")
lookup_items = Enum.map(1..10_000, &"xor_bench_#{&1}")
lookup_miss = Enum.map(100_001..110_000, &"xor_bench_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    {:ok, filter_8} = XorFilter.build(items_1k, fingerprint_bits: 8, backend: backend)
    {:ok, filter_16} = XorFilter.build(items_1k, fingerprint_bits: 16, backend: backend)
    binary_8 = XorFilter.serialize(filter_8)

    {name,
     %{
       filter_8: filter_8,
       filter_16: filter_16,
       binary_8: binary_8
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    backend = if name == "Pure", do: Backend.Pure, else: Backend.Rust

    [
      {"xor_build 1k xor8 [#{name}]",
       fn -> {:ok, _} = XorFilter.build(items_1k, fingerprint_bits: 8, backend: backend) end},
      {"xor_build 100k xor8 [#{name}]",
       fn -> {:ok, _} = XorFilter.build(items_100k, fingerprint_bits: 8, backend: backend) end},
      {"xor_build 1k xor16 [#{name}]",
       fn -> {:ok, _} = XorFilter.build(items_1k, fingerprint_bits: 16, backend: backend) end},
      {"xor_member? (hit) xor8 [#{name}]",
       fn -> Enum.each(lookup_items, &XorFilter.member?(s.filter_8, &1)) end},
      {"xor_member? (miss) xor8 [#{name}]",
       fn -> Enum.each(lookup_miss, &XorFilter.member?(s.filter_8, &1)) end},
      {"xor_member? (hit) xor16 [#{name}]",
       fn -> Enum.each(lookup_items, &XorFilter.member?(s.filter_16, &1)) end},
      {"xor_serialize xor8 [#{name}]", fn -> XorFilter.serialize(s.filter_8) end},
      {"xor_deserialize xor8 [#{name}]", fn -> {:ok, _} = XorFilter.deserialize(s.binary_8) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/xor_filter_bench.json"}
  ]
)
