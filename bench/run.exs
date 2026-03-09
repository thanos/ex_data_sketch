# Benchmark Runner
#
# Runs all benchmark suites in sequence and writes JSON output to bench/output/.
#
# Usage: EX_DATA_SKETCH_BUILD=true mix run bench/run.exs

IO.puts("ExDataSketch Benchmark Runner")
IO.puts("=============================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{ExDataSketch.Backend.Rust.available?()}")
IO.puts("")

scripts = [
  "bench/hll_bench.exs",
  "bench/cms_bench.exs",
  "bench/theta_bench.exs",
  "bench/kll_bench.exs",
  "bench/ddsketch_bench.exs",
  "bench/frequent_items_bench.exs",
  "bench/bloom_bench.exs",
  "bench/cuckoo_bench.exs",
  "bench/quotient_bench.exs",
  "bench/cqf_bench.exs",
  "bench/xor_filter_bench.exs",
  "bench/iblt_bench.exs",
  "bench/filter_chain_bench.exs",
  "bench/req_bench.exs",
  "bench/misra_gries_bench.exs",
  "bench/xxhash3_bench.exs"
]

for script <- scripts do
  IO.puts(">>> Running #{script} ...")
  IO.puts("")
  Code.eval_file(script)
  IO.puts("")
end

IO.puts("All benchmarks complete. JSON output in bench/output/")
