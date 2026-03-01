# CMS Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/cms_bench.exs

alias ExDataSketch.CMS

IO.puts("ExDataSketch CMS Benchmark")
IO.puts("==========================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("")

# Pre-generate data outside the benchmark closure
sketch_default = CMS.new()

sketch_populated =
  CMS.from_enumerable(for(i <- 0..999, do: "item_#{i}"))

sketch_for_merge_a =
  CMS.from_enumerable(for(i <- 0..4999, do: "a_#{i}"))

sketch_for_merge_b =
  CMS.from_enumerable(for(i <- 0..4999, do: "b_#{i}"))

File.mkdir_p!("bench/output")

Benchee.run(
  %{
    "cms_update (single item)" => fn ->
      CMS.update(sketch_default, "bench_item")
    end,
    "cms_update_many (1k items)" => fn ->
      CMS.update_many(sketch_default, for(i <- 0..999, do: "item_#{i}"))
    end,
    "cms_update_many (100k items)" => fn ->
      CMS.update_many(sketch_default, for(i <- 0..99_999, do: "item_#{i}"))
    end,
    "cms_merge" => fn ->
      CMS.merge(sketch_for_merge_a, sketch_for_merge_b)
    end,
    "cms_estimate" => fn ->
      CMS.estimate(sketch_populated, "item_42")
    end
  },
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/cms_bench.json"}
  ]
)
