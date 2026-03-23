alias ExDataSketch.HLL

# HLL vs MapSet -- Cardinality Estimation Benchmark
#
# IMPORTANT: Understanding the memory numbers
# --------------------------------------------
# Benchee's "Memory usage" measures TOTAL HEAP ALLOCATION during the function
# call, not peak or resident memory. This includes all transient garbage that
# is immediately collectible (short-lived binary copies, list cons cells, etc.).
#
# The Pure backend's mix64 is fixnum-safe (all intermediates stay under 60
# bits), so hashing itself does not allocate transient bigints. The remaining
# per-item allocation comes from :erlang.term_to_binary conversions and the
# unavoidable 64-bit hash return values that exceed the BEAM fixnum range.
#
# The real HLL advantage is RESULT SIZE: the final sketch is a fixed 4 KB
# (at p=12) regardless of whether you inserted 1K or 100M items, while MapSet
# must store every unique element. This is demonstrated by the
# "Result Memory" benchmark below, which compares :erlang.external_size/1 of
# the finished data structures.
#
# For production use the Rust backend (ExDataSketch.Backend.Rust) is
# recommended; it moves hashing into the NIF batch call, eliminating per-item
# Elixir heap allocation entirely (94.6% memory reduction at 10M items).

defmodule TaxiData do
  def stream_ids(limit),
    do: Stream.repeatedly(fn -> :rand.uniform(5_000_000) end) |> Stream.take(limit)
end

defmodule Format do
  def bytes(n) when n < 1_024, do: "#{n} B"
  def bytes(n) when n < 1_048_576, do: "#{Float.round(n / 1_024, 1)} KB"
  def bytes(n), do: "#{Float.round(n / 1_048_576, 2)} MB"
end

inputs = %{
  "100k Rows" => 100_000,
  "1m Rows" => 1_000_000,
  "10m Rows" => 10_000_000
}

# -- Benchmark 1: Throughput & total allocation (Benchee) ---------------------
IO.puts("\n=== Throughput & Total Allocation (Benchee) ===\n")

Benchee.run(
  %{
    "Naive: MapSet (Uniq)" => {
      fn data -> data |> Enum.into(MapSet.new()) |> MapSet.size() end,
      before_each: fn limit -> TaxiData.stream_ids(limit) |> Enum.to_list() end
    },
    "Sketch: HLL (Uniq)" => {
      fn items ->
        HLL.new(p: 12, backend: ExDataSketch.Backend.Rust)
        |> HLL.update_many(items)
        |> HLL.estimate()
      end,
      before_each: fn limit ->
        TaxiData.stream_ids(limit) |> Enum.map(&Integer.to_string/1)
      end
    }
  },
  inputs: inputs,
  memory_time: 2,
  time: 5
)

# -- Benchmark 2: Result memory (the real HLL advantage) ---------------------
IO.puts("\n=== Result Memory: Final Data Structure Size ===\n")

for {label, limit} <- Enum.sort_by(inputs, fn {_, v} -> v end) do
  data = TaxiData.stream_ids(limit) |> Enum.to_list()
  items = Enum.map(data, &Integer.to_string/1)

  mapset = Enum.into(data, MapSet.new())
  hll = HLL.new(p: 12) |> HLL.update_many(items)

  mapset_bytes = :erlang.external_size(mapset)
  hll_bytes = :erlang.external_size(hll.state)

  IO.puts("#{label}:")
  IO.puts("  MapSet result size:  #{Format.bytes(mapset_bytes)}")
  IO.puts("  HLL result size:     #{Format.bytes(hll_bytes)}")
  IO.puts("  Ratio:               MapSet is #{Float.round(mapset_bytes / hll_bytes, 1)}x larger")
  IO.puts("  MapSet unique count: #{MapSet.size(mapset)}")
  IO.puts("  HLL estimate:        #{Float.round(HLL.estimate(hll), 0)}")
  IO.puts("")
end
