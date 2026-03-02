# HLL Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/hll_bench.exs

alias ExDataSketch.HLL

IO.puts("ExDataSketch HLL Benchmark")
IO.puts("==========================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("")

# Pre-generate sketches outside the benchmark closure
sketch_p10 = HLL.new(p: 10)
sketch_p14 = HLL.new(p: 14)

sketch_p14_populated =
  HLL.from_enumerable(for(i <- 0..999, do: "item_#{i}"), p: 14)

sketch_p14_for_merge_a =
  HLL.from_enumerable(for(i <- 0..4999, do: "a_#{i}"), p: 14)

sketch_p14_for_merge_b =
  HLL.from_enumerable(for(i <- 0..4999, do: "b_#{i}"), p: 14)

File.mkdir_p!("bench/output")

Benchee.run(
  %{
    "hll_update (p=10)" => fn ->
      HLL.update(sketch_p10, "bench_item")
    end,
    "hll_update (p=14)" => fn ->
      HLL.update(sketch_p14, "bench_item")
    end,
    "hll_update_many (p=14, 1k items)" => fn ->
      HLL.update_many(sketch_p14, for(i <- 0..999, do: "item_#{i}"))
    end,
    "hll_update_many (p=14, 100k items)" => fn ->
      HLL.update_many(sketch_p14, for(i <- 0..99_999, do: "item_#{i}"))
    end,
    "hll_merge (p=14)" => fn ->
      HLL.merge(sketch_p14_for_merge_a, sketch_p14_for_merge_b)
    end,
    "hll_estimate (p=14, populated)" => fn ->
      HLL.estimate(sketch_p14_populated)
    end
  },
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/hll_bench.json"}
  ]
)
