# Phase 6 raw-NIF-hashing before/after benchmark.
#
# Measures Pure vs Rust (pre-hashed, legacy) vs Rust (raw, current) for
# each of the six membership-filter families' batch insert/build
# operation. See guides/filter_performance.md.
#
# Run with: MIX_ENV=dev mix run bench/filter_raw_hashing_bench.exs

alias ExDataSketch.{Backend, Bloom, CQF, Cuckoo, Hash, IBLT, Quotient, XorFilter}

IO.puts("ExDataSketch Filter Raw-Hashing Benchmark (Phase 6)")
IO.puts("=====================================================")
IO.puts("Elixir: #{System.version()}")
IO.puts("OTP: #{System.otp_release()}")
IO.puts("Arch: #{:erlang.system_info(:system_architecture)}")
IO.puts("Rust NIF: #{Backend.Rust.available?()}")
IO.puts("")

items = Enum.map(1..10_000, &"filter_bench_#{&1}")

bench_opts = [warmup: 0.5, time: 2, formatters: [Benchee.Formatters.Console]]

benches = %{
  "bloom [Pure]" => fn ->
    Bloom.new(capacity: 20_000, backend: Backend.Pure) |> Bloom.put_many(items)
  end,
  "bloom [Rust raw]" => fn ->
    Bloom.new(capacity: 20_000, backend: Backend.Rust) |> Bloom.put_many(items)
  end,
  "bloom [Rust pre-hashed, legacy]" => fn ->
    sketch = Bloom.new(capacity: 20_000, backend: Backend.Rust)
    seed = Keyword.get(sketch.opts, :seed, 0)
    hashes = Enum.map(items, &Hash.hash64(&1, seed: seed))
    Backend.Rust.bloom_put_many(sketch.state, hashes, sketch.opts)
  end,
  "cuckoo [Pure]" => fn ->
    {:ok, _} = Cuckoo.new(capacity: 20_000, backend: Backend.Pure) |> Cuckoo.put_many(items)
  end,
  "cuckoo [Rust raw]" => fn ->
    {:ok, _} = Cuckoo.new(capacity: 20_000, backend: Backend.Rust) |> Cuckoo.put_many(items)
  end,
  "cuckoo [Rust pre-hashed, legacy]" => fn ->
    sketch = Cuckoo.new(capacity: 20_000, backend: Backend.Rust)
    seed = Keyword.get(sketch.opts, :seed, 0)
    hashes = Enum.map(items, &Hash.hash64(&1, seed: seed))
    {:ok, _} = Backend.Rust.cuckoo_put_many(sketch.state, hashes, sketch.opts)
  end,
  "quotient [Pure]" => fn ->
    Quotient.new(q: 16, r: 8, backend: Backend.Pure) |> Quotient.put_many(items)
  end,
  "quotient [Rust raw]" => fn ->
    Quotient.new(q: 16, r: 8, backend: Backend.Rust) |> Quotient.put_many(items)
  end,
  "quotient [Rust pre-hashed, legacy]" => fn ->
    sketch = Quotient.new(q: 16, r: 8, backend: Backend.Rust)
    seed = Keyword.get(sketch.opts, :seed, 0)
    hashes = Enum.map(items, &Hash.hash64(&1, seed: seed))
    Backend.Rust.quotient_put_many(sketch.state, hashes, sketch.opts)
  end,
  "cqf [Pure]" => fn -> CQF.new(q: 16, r: 8, backend: Backend.Pure) |> CQF.put_many(items) end,
  "cqf [Rust raw]" => fn -> CQF.new(q: 16, r: 8, backend: Backend.Rust) |> CQF.put_many(items) end,
  "cqf [Rust pre-hashed, legacy]" => fn ->
    sketch = CQF.new(q: 16, r: 8, backend: Backend.Rust)
    seed = Keyword.get(sketch.opts, :seed, 0)
    hashes = Enum.map(items, &Hash.hash64(&1, seed: seed))
    Backend.Rust.cqf_put_many(sketch.state, hashes, sketch.opts)
  end,
  "iblt [Pure]" => fn ->
    IBLT.new(cell_count: 20_000, backend: Backend.Pure) |> IBLT.put_many(items)
  end,
  "iblt [Rust raw]" => fn ->
    IBLT.new(cell_count: 20_000, backend: Backend.Rust) |> IBLT.put_many(items)
  end,
  "iblt [Rust pre-hashed, legacy]" => fn ->
    sketch = IBLT.new(cell_count: 20_000, backend: Backend.Rust)
    seed = Keyword.get(sketch.opts, :seed, 0)
    pairs = Enum.map(items, fn item -> {Hash.hash64(item, seed: seed), 0} end)
    Backend.Rust.iblt_put_many(sketch.state, pairs, sketch.opts)
  end,
  "xor_filter [Pure]" => fn -> {:ok, _} = XorFilter.build(items, backend: Backend.Pure) end,
  "xor_filter [Rust raw]" => fn -> {:ok, _} = XorFilter.build(items, backend: Backend.Rust) end,
  "xor_filter [Rust pre-hashed, legacy]" => fn ->
    opts = [fingerprint_bits: 8, seed: 0]
    hashes = items |> Enum.map(&Hash.hash64(&1, seed: 0)) |> Enum.uniq()
    {:ok, _} = Backend.Rust.xor_build(hashes, opts)
  end
}

File.mkdir_p!("bench/output")

Benchee.run(
  benches,
  Keyword.put(bench_opts, :formatters, [
    Benchee.Formatters.Console,
    {Benchee.Formatters.JSON, file: "bench/output/filter_raw_hashing_bench.json"}
  ])
)
