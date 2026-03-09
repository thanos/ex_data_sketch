# XXHash3 Benchmark Suite
#
# Compares XXHash3 NIF throughput vs phash2 at various data sizes.
#
# Run with: EX_DATA_SKETCH_BUILD=true mix run bench/xxhash3_bench.exs

alias ExDataSketch.Hash

IO.puts("ExDataSketch XXHash3 Benchmark")
IO.puts("==============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("")

data_1b = :crypto.strong_rand_bytes(1)
data_100b = :crypto.strong_rand_bytes(100)
data_1kb = :crypto.strong_rand_bytes(1024)
data_10kb = :crypto.strong_rand_bytes(10240)

benches = %{
  "xxhash3_64 1B" => fn -> Hash.xxhash3_64(data_1b) end,
  "xxhash3_64 100B" => fn -> Hash.xxhash3_64(data_100b) end,
  "xxhash3_64 1KB" => fn -> Hash.xxhash3_64(data_1kb) end,
  "xxhash3_64 10KB" => fn -> Hash.xxhash3_64(data_10kb) end,
  "phash2 1B" => fn -> :erlang.phash2(data_1b, 1 <<< 32) end,
  "phash2 100B" => fn -> :erlang.phash2(data_100b, 1 <<< 32) end,
  "phash2 1KB" => fn -> :erlang.phash2(data_1kb, 1 <<< 32) end,
  "phash2 10KB" => fn -> :erlang.phash2(data_10kb, 1 <<< 32) end
}

File.mkdir_p!("bench/output")

Benchee.run(
  benches,
  warmup: 1,
  time: 3,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/xxhash3_bench.json"}
  ]
)
