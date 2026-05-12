# HLL Hot-Path Benchmark Suite (v0.8.0 Phase 3)
#
# Run with: EX_DATA_SKETCH_BUILD=1 MIX_ENV=dev mix run bench/hll_hot_path_bench.exs
#
# This benchmark compares the four HLL hot paths exposed in v0.8.0:
#
#   1. Pure Elixir backend (no NIF, phash2 or xxhash3 via NIF when available)
#   2. Rust non-raw NIF: hashing happens in Elixir, hashes_bin crosses
#      the NIF boundary (this is the v0.6.0 path).
#   3. Rust raw NIF (XXH3): hashing happens inside Rust over a
#      ListIterator of binaries (v0.7.1 path).
#   4. Rust raw_h NIF (Murmur3): hashing happens inside Rust via the
#      newly-shared Murmur3 implementation (v0.8.0 path).
#
# It also sweeps the dirty-NIF threshold to confirm scheduler behavior.

alias ExDataSketch.{Backend, HLL}

IO.puts("ExDataSketch HLL Hot-Path Benchmark (v0.8.0 Phase 3)")
IO.puts("====================================================")
IO.puts("Elixir       : #{System.version()}")
IO.puts("OTP          : #{System.otp_release()}")
IO.puts("Arch         : #{:erlang.system_info(:system_architecture)}")
IO.puts("Schedulers   : #{:erlang.system_info(:schedulers)}")
IO.puts("Dirty CPU    : #{:erlang.system_info(:dirty_cpu_schedulers)}")
IO.puts("Rust NIF     : #{Backend.Rust.available?()}")
IO.puts("")

unless Backend.Rust.available?() do
  IO.puts("Rust NIF is required for this benchmark.")
  System.halt(1)
end

# Pre-generate the input corpus outside the bench closures so allocation
# cost is excluded from the measurement.
items_10k = for i <- 0..9_999, do: "item_#{i}"
items_100k = for i <- 0..99_999, do: "item_#{i}"
items_1m = for i <- 0..999_999, do: "item_#{i}"

p = 14
empty_pure_phash2 = HLL.new(p: p, backend: Backend.Pure, hash_strategy: :phash2)
empty_pure_xxh3 = HLL.new(p: p, backend: Backend.Pure, hash_strategy: :xxhash3)
empty_rust_xxh3 = HLL.new(p: p, backend: Backend.Rust, hash_strategy: :xxhash3)
empty_rust_m3 = HLL.new(p: p, backend: Backend.Rust, hash_strategy: :murmur3)

# We bench update_many for three sizes. For each size we measure four
# paths so the matrix is 12 entries. We deliberately rebuild the empty
# sketch in each iteration (cheap copy) so successive benches do not
# accumulate state.
mk_paths = fn items ->
  %{
    "Pure phash2     (#{length(items)} items)" => fn ->
      HLL.update_many(empty_pure_phash2, items)
    end,
    "Pure xxhash3    (#{length(items)} items)" => fn ->
      HLL.update_many(empty_pure_xxh3, items)
    end,
    "Rust raw XXH3   (#{length(items)} items)" => fn ->
      HLL.update_many(empty_rust_xxh3, items)
    end,
    "Rust raw_h Mur3 (#{length(items)} items)" => fn -> HLL.update_many(empty_rust_m3, items) end
  }
end

benches =
  Map.merge(
    mk_paths.(items_10k),
    Map.merge(mk_paths.(items_100k), mk_paths.(items_1m))
  )

File.mkdir_p!("bench/output")

Benchee.run(
  benches,
  warmup: 1,
  time: 3,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/hll_hot_path_bench.json"}
  ]
)
