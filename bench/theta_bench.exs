# Theta Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/theta_bench.exs

alias ExDataSketch.{Backend, Theta}

IO.puts("ExDataSketch Theta Benchmark")
IO.puts("============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
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
    sketch_k1024 = Theta.new(k: 1024, backend: backend)
    sketch_k4096 = Theta.new(k: 4096, backend: backend)
    sketch_populated = Theta.from_enumerable(items_1k, k: 4096, backend: backend)

    sketch_est =
      Theta.from_enumerable(for(i <- 0..9999, do: "item_#{i}"), k: 4096, backend: backend)

    merge_a = Theta.from_enumerable(for(i <- 0..4999, do: "a_#{i}"), k: 4096, backend: backend)
    merge_b = Theta.from_enumerable(for(i <- 0..4999, do: "b_#{i}"), k: 4096, backend: backend)

    {name,
     %{
       sketch_k1024: sketch_k1024,
       sketch_k4096: sketch_k4096,
       sketch_populated: sketch_populated,
       sketch_est: sketch_est,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

# DataSketches serialization (Pure-only, format-level)
sketch_for_ds = Theta.from_enumerable(for(i <- 0..9999, do: "item_#{i}"), k: 4096)
ds_binary = Theta.serialize_datasketches(sketch_for_ds)

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"theta_update k=1024 [#{name}]", fn -> Theta.update(s.sketch_k1024, "bench_item") end},
      {"theta_update k=4096 [#{name}]", fn -> Theta.update(s.sketch_k4096, "bench_item") end},
      {"theta_update_many 1k [#{name}]", fn -> Theta.update_many(s.sketch_k4096, items_1k) end},
      {"theta_update_many 100k [#{name}]",
       fn -> Theta.update_many(s.sketch_k4096, items_100k) end},
      {"theta_merge k=4096 [#{name}]", fn -> Theta.merge(s.merge_a, s.merge_b) end},
      {"theta_estimate exact [#{name}]", fn -> Theta.estimate(s.sketch_populated) end},
      {"theta_estimate estimation [#{name}]", fn -> Theta.estimate(s.sketch_est) end},
      {"theta_compact estimation [#{name}]", fn -> Theta.compact(s.sketch_est) end}
    ]
  end)

# Add DataSketches format benches (not backend-parameterized)
ds_benches = [
  {"theta_serialize_datasketches", fn -> Theta.serialize_datasketches(sketch_for_ds) end},
  {"theta_deserialize_datasketches", fn -> Theta.deserialize_datasketches(ds_binary) end}
]

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches ++ ds_benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/theta_bench.json"}
  ]
)
