# CMS Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/cms_bench.exs

alias ExDataSketch.{Backend, CMS}

IO.puts("ExDataSketch CMS Benchmark")
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
items_1k = for i <- 0..999, do: "item_#{i}"
items_100k = for i <- 0..99_999, do: "item_#{i}"

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch = CMS.new(backend: backend)
    sketch_populated = CMS.from_enumerable(items_1k, backend: backend)
    merge_a = CMS.from_enumerable(for(i <- 0..4999, do: "a_#{i}"), backend: backend)
    merge_b = CMS.from_enumerable(for(i <- 0..4999, do: "b_#{i}"), backend: backend)

    {name,
     %{
       sketch: sketch,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"cms_update single [#{name}]", fn -> CMS.update(s.sketch, "bench_item") end},
      {"cms_update_many 1k [#{name}]", fn -> CMS.update_many(s.sketch, items_1k) end},
      {"cms_update_many 100k [#{name}]", fn -> CMS.update_many(s.sketch, items_100k) end},
      {"cms_merge [#{name}]", fn -> CMS.merge(s.merge_a, s.merge_b) end},
      {"cms_estimate [#{name}]", fn -> CMS.estimate(s.sketch_populated, "item_42") end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/cms_bench.json"}
  ]
)
