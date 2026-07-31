# Serialization Overhead Benchmark
#
# Run with: MIX_ENV=dev mix run bench/serialization_bench.exs

alias ExDataSketch.{HLL, ULL, CMS, Theta, Bloom}

IO.puts("ExDataSketch Serialization Benchmark")
IO.puts("======================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Schedulers: #{System.schedulers_online()}")
IO.puts("")

items_1k = for i <- 0..999, do: "item_#{i}"
items_10k = for i <- 0..9999, do: "item_#{i}"

sketches = %{
  "HLL p=10 (1k items)" => HLL.from_enumerable(items_1k, p: 10),
  "HLL p=14 (1k items)" => HLL.from_enumerable(items_1k, p: 14),
  "HLL p=14 (10k items)" => HLL.from_enumerable(items_10k, p: 14),
  "ULL p=14 (1k items)" => ULL.from_enumerable(items_1k, p: 14),
  "ULL p=14 (10k items)" => ULL.from_enumerable(items_10k, p: 14),
  "CMS (1k items)" => CMS.from_enumerable(items_1k, width: 128, depth: 5),
  "Bloom (1k items)" => Bloom.from_enumerable(items_1k, capacity: 2000)
}

for {label, sketch} <- sketches do
  mod = sketch.__struct__
  serialized = mod.serialize(sketch)
  size_bytes = byte_size(serialized)
  IO.puts("")
  IO.puts("--- #{label} (#{size_bytes} bytes) ---")

  Benchee.run(
    %{
      "serialize" => fn -> mod.serialize(sketch) end,
      "deserialize" => fn -> mod.deserialize(serialized) end
    },
    time: 2,
    memory_time: 1,
    print: [configuration: false, benchmarking: false]
  )
end

IO.puts("")
IO.puts("Serialization benchmark complete.")
