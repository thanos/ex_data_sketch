# Update Many Chunk Size Benchmark
#
# Run with: MIX_ENV=dev mix run bench/update_many_chunk_bench.exs
#
# Benchmarks the impact of configurable chunk_size on HLL.update_many/2.

alias ExDataSketch.HLL

IO.puts("ExDataSketch Chunk Size Benchmark")
IO.puts("==================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Schedulers: #{System.schedulers_online()}")
IO.puts("")

items_100k = for i <- 0..99_999, do: "item_#{i}"

chunk_sizes = [100, 1_000, 5_000, 10_000, 50_000]

for chunk_size <- chunk_sizes do
  IO.puts("")
  IO.puts("--- HLL.update_many 100k items, chunk_size=#{chunk_size} ---")

  Benchee.run(
    %{
      "update_many (chunk=#{chunk_size})" => fn ->
        HLL.new(p: 14, update_many_chunk_size: chunk_size)
        |> HLL.update_many(items_100k)
      end
    },
    time: 3,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

IO.puts("--- HLL.update_many 100k items, default chunk_size=10000 ---")

Benchee.run(
  %{
    "update_many (default)" => fn ->
      HLL.new(p: 14) |> HLL.update_many(items_100k)
    end
  },
  time: 3,
  memory_time: 1,
  print: [configuration: false, benchmarking: false]
)

IO.puts("")
IO.puts("Chunk size benchmark complete.")
