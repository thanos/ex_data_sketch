# Stream Ingestion Benchmark Suite
#
# Run with: MIX_ENV=dev mix run bench/stream_ingestion_bench.exs

alias ExDataSketch.{HLL, CMS, Bloom}
alias ExDataSketch.Stream, as: S

IO.puts("ExDataSketch Stream Ingestion Benchmark")
IO.puts("=========================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Schedulers: #{System.schedulers_online()}")
IO.puts("")

sizes = [1_000, 10_000, 100_000]

for size <- sizes do
  items = for i <- 0..(size - 1), do: "item_#{i}"

  IO.puts("")
  IO.puts("--- #{size} items ---")

  Benchee.run(
    %{
      "HLL.from_enumerable/2" => fn -> HLL.from_enumerable(items, p: 12) end,
      "Stream.hll/2" => fn -> S.hll(items, p: 12) end,
      "Enum.into/2 (Collectable)" => fn -> Enum.into(items, HLL.new(p: 12)) end,
      "Stream.reduce_into/3" => fn -> S.reduce_into(items, HLL, p: 12) end
    },
    time: 3,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

for size <- sizes do
  items = for i <- 0..(size - 1), do: "item_#{i}"

  IO.puts("")
  IO.puts("--- CMS #{size} items ---")

  Benchee.run(
    %{
      "CMS.from_enumerable/2" => fn -> CMS.from_enumerable(items, width: 128, depth: 3) end,
      "Stream.cms/2" => fn -> S.cms(items, width: 128, depth: 3) end,
      "Enum.into/2 (Collectable)" => fn -> Enum.into(items, CMS.new(width: 128, depth: 3)) end
    },
    time: 3,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

for size <- sizes do
  items = for i <- 0..(size - 1), do: "item_#{i}"

  IO.puts("")
  IO.puts("--- Bloom #{size} items ---")

  Benchee.run(
    %{
      "Bloom.from_enumerable/2" => fn -> Bloom.from_enumerable(items, capacity: size * 2) end,
      "Stream.bloom/2" => fn -> S.bloom(items, capacity: size * 2) end,
      "Enum.into/2 (Collectable)" => fn -> Enum.into(items, Bloom.new(capacity: size * 2)) end
    },
    time: 3,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")

items_partition = for i <- 0..99_999, do: "item_#{i}"

IO.puts("--- Partitioned HLL 100k items ---")

Benchee.run(
  %{
    "HLL.from_enumerable (single pass)" => fn ->
      HLL.from_enumerable(items_partition, p: 12)
    end,
    "Stream.reduce_partitioned (4 partitions)" => fn ->
      S.reduce_partitioned(items_partition, HLL, partitions: 4, p: 12)
    end,
    "Stream.reduce_partitioned (8 partitions)" => fn ->
      S.reduce_partitioned(items_partition, HLL, partitions: 8, p: 12)
    end,
    "Stream.reduce_partitioned (16 partitions)" => fn ->
      S.reduce_partitioned(items_partition, HLL, partitions: 16, p: 12)
    end
  },
  time: 5,
  memory_time: 2,
  print: [configuration: false, benchmarking: false]
)
