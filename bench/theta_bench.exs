# Theta Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/theta_bench.exs

alias ExDataSketch.Theta

IO.puts("ExDataSketch Theta Benchmark")
IO.puts("============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("")

# Pre-generate sketches outside the benchmark closure
sketch_k1024 = Theta.new(k: 1024)
sketch_k4096 = Theta.new(k: 4096)

sketch_k4096_populated =
  Theta.from_enumerable(for(i <- 0..999, do: "item_#{i}"), k: 4096)

sketch_k4096_est =
  Theta.from_enumerable(for(i <- 0..9999, do: "item_#{i}"), k: 4096)

sketch_for_merge_a =
  Theta.from_enumerable(for(i <- 0..4999, do: "a_#{i}"), k: 4096)

sketch_for_merge_b =
  Theta.from_enumerable(for(i <- 0..4999, do: "b_#{i}"), k: 4096)

ds_binary = Theta.serialize_datasketches(sketch_k4096_est)

File.mkdir_p!("bench/output")

Benchee.run(
  %{
    "theta_update (k=1024)" => fn ->
      Theta.update(sketch_k1024, "bench_item")
    end,
    "theta_update (k=4096)" => fn ->
      Theta.update(sketch_k4096, "bench_item")
    end,
    "theta_update_many (k=4096, 1k items)" => fn ->
      Theta.update_many(sketch_k4096, for(i <- 0..999, do: "item_#{i}"))
    end,
    "theta_update_many (k=4096, 100k items)" => fn ->
      Theta.update_many(sketch_k4096, for(i <- 0..99_999, do: "item_#{i}"))
    end,
    "theta_merge (k=4096)" => fn ->
      Theta.merge(sketch_for_merge_a, sketch_for_merge_b)
    end,
    "theta_estimate (k=4096, exact)" => fn ->
      Theta.estimate(sketch_k4096_populated)
    end,
    "theta_estimate (k=4096, estimation)" => fn ->
      Theta.estimate(sketch_k4096_est)
    end,
    "theta_compact (k=4096, estimation)" => fn ->
      Theta.compact(sketch_k4096_est)
    end,
    "theta_serialize_datasketches (k=4096)" => fn ->
      Theta.serialize_datasketches(sketch_k4096_est)
    end,
    "theta_deserialize_datasketches (k=4096)" => fn ->
      Theta.deserialize_datasketches(ds_binary)
    end
  },
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/theta_bench.json"}
  ]
)
