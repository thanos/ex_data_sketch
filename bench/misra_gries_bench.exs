# MisraGries Benchmark Suite
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/misra_gries_bench.exs

alias ExDataSketch.{Backend, MisraGries}

IO.puts("ExDataSketch MisraGries Benchmark")
IO.puts("==================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

# MisraGries has no Rust NIF acceleration, Pure only
backends = [{"Pure", Backend.Pure}]

# Zipf-like distribution: frequent items + tail
items_1k =
  List.duplicate("a", 100) ++
    List.duplicate("b", 60) ++
    List.duplicate("c", 30) ++
    List.duplicate("d", 10) ++
    Enum.map(1..800, &"tail_#{&1}")

items_10k =
  List.duplicate("a", 1000) ++
    List.duplicate("b", 600) ++
    List.duplicate("c", 300) ++
    List.duplicate("d", 100) ++
    Enum.map(1..8000, &"tail_#{&1}")

scenarios =
  for {name, backend} <- backends, into: %{} do
    sketch_k10 = MisraGries.new(k: 10, backend: backend)
    sketch_populated = MisraGries.new(k: 10, backend: backend) |> MisraGries.update_many(items_1k)

    merge_a =
      MisraGries.new(k: 10, backend: backend) |> MisraGries.update_many(Enum.take(items_1k, 500))

    merge_b =
      MisraGries.new(k: 10, backend: backend) |> MisraGries.update_many(Enum.drop(items_1k, 500))

    {name,
     %{
       sketch_k10: sketch_k10,
       sketch_populated: sketch_populated,
       merge_a: merge_a,
       merge_b: merge_b
     }}
  end

benches =
  Enum.flat_map(scenarios, fn {name, s} ->
    [
      {"mg_update k=10 [#{name}]", fn -> MisraGries.update(s.sketch_k10, "x") end},
      {"mg_update_many 1k [#{name}]", fn -> MisraGries.update_many(s.sketch_k10, items_1k) end},
      {"mg_update_many 10k [#{name}]", fn -> MisraGries.update_many(s.sketch_k10, items_10k) end},
      {"mg_merge k=10 [#{name}]", fn -> MisraGries.merge(s.merge_a, s.merge_b) end},
      {"mg_estimate [#{name}]", fn -> MisraGries.estimate(s.sketch_populated, "a") end},
      {"mg_top_k [#{name}]", fn -> MisraGries.top_k(s.sketch_populated) end},
      {"mg_serialize [#{name}]", fn -> MisraGries.serialize(s.sketch_populated) end}
    ]
  end)

File.mkdir_p!("bench/output")

Benchee.run(
  Map.new(benches),
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/misra_gries_bench.json"}
  ]
)
