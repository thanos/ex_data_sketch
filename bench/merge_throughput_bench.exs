# Merge Throughput Benchmark
#
# Run with: MIX_ENV=dev mix run bench/merge_throughput_bench.exs

alias ExDataSketch.{HLL, ULL, CMS, Theta}

IO.puts("ExDataSketch Merge Throughput Benchmark")
IO.puts("=========================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Schedulers: #{System.schedulers_online()}")
IO.puts("")

merge_counts = [2, 4, 8, 16]

for count <- merge_counts do
  sketches =
    for partition <- 0..(count - 1) do
      offset = partition * 1000
      items = for i <- offset..(offset + 999), do: "item_#{i}"
      HLL.from_enumerable(items, p: 14)
    end

  IO.puts("")
  IO.puts("--- HLL merge_many #{count} sketches (1000 items each) ---")

  Benchee.run(
    %{
      "merge_many (#{count} partitions)" => fn ->
        HLL.merge_many(sketches)
      end,
      "sequential merge (#{count} partitions)" => fn ->
        Enum.reduce(sketches, fn s, acc -> HLL.merge(acc, s) end)
      end
    },
    time: 2,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

for count <- merge_counts do
  sketches =
    for partition <- 0..(count - 1) do
      offset = partition * 1000
      items = for i <- offset..(offset + 999), do: "item_#{i}"
      ULL.from_enumerable(items, p: 14)
    end

  IO.puts("--- ULL merge_many #{count} sketches ---")

  Benchee.run(
    %{
      "merge_many (#{count} partitions)" => fn ->
        ULL.merge_many(sketches)
      end
    },
    time: 2,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

for count <- merge_counts do
  sketches =
    for partition <- 0..(count - 1) do
      offset = partition * 1000
      items = for i <- offset..(offset + 999), do: "item_#{i}"
      CMS.from_enumerable(items, width: 128, depth: 5)
    end

  IO.puts("--- CMS merge_many #{count} sketches ---")

  Benchee.run(
    %{
      "merge_many (#{count} partitions)" => fn ->
        CMS.merge_many(sketches)
      end
    },
    time: 2,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")
IO.puts("Merge throughput benchmark complete.")
