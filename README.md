# ExDataSketch



<img width="500"  alt="ex_data_sketch" src="https://github.com/user-attachments/assets/d7e25c7c-db05-4cc4-b39a-92d08ee87312" alt="chipmunk full of Elixir, using a very rusty nail, to carve data sketches" />


Production-grade streaming data sketching algorithms for Elixir.

ExDataSketch provides probabilistic data structures for approximate counting,
frequency estimation, quantile computation, heavy-hitter detection, membership
testing with deletion, and set reconciliation on streaming data. Stream-native
integration with Elixir's `Collectable`, `GenStage`, `Broadway`, and `Flow`,
plus `:telemetry`/OpenTelemetry instrumentation, persistence backends (ETS,
DETS, CubDB, Mnesia, Ecto), and 23 production-oriented Livebooks (7
cross-cutting integration guides plus a per-family tutorial for every
sketch).

[![CI](https://github.com/thanos/ex_data_sketch/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/ex_data_sketch/actions/workflows/ci.yml)
[![Hex version](https://img.shields.io/hexpm/v/ex_data_sketch.svg)](https://hex.pm/packages/ex_data_sketch)
[![Hex docs](https://img.shields.io/badge/docs-hexdocs.pm-blue)](https://hexdocs.pm/ex_data_sketch)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/thanos/ex_data_sketch/badge.svg?branch=main)](https://coveralls.io/github/thanos/ex_data_sketch?branch=main)



## Supported Algorithms

| Algorithm | Purpose | Status |
|-----------|---------|--------|
| HyperLogLog (HLL) | Cardinality estimation | Implemented (Pure + Rust) |
| Count-Min Sketch (CMS) | Frequency estimation | Implemented (Pure + Rust) |
| Theta Sketch | Set operations on cardinalities | Implemented (Pure + Rust) |
| KLL Quantiles | Rank and quantile estimation | Implemented (Pure + Rust) |
| DDSketch | Relative-error quantile estimation | Implemented (Pure + Rust) |
| FrequentItems (SpaceSaving) | Heavy-hitter / top-k detection | Implemented (Pure + Rust) |
| Bloom Filter | Probabilistic membership testing | Implemented (Pure + Rust) |
| Cuckoo Filter | Membership testing with deletion | Implemented (Pure + Rust) |
| Quotient Filter | Membership with deletion and merge | Implemented (Pure + Rust) |
| CQF (Counting Quotient) | Multiset membership with counting | Implemented (Pure + Rust) |
| XorFilter | Static immutable membership testing | Implemented (Pure + Rust) |
| IBLT | Set reconciliation | Implemented (Pure + Rust) |
| REQ Sketch | Relative-error quantile estimation | Implemented (Pure) |
| Misra-Gries | Deterministic heavy-hitter detection | Implemented (Pure) |
| UltraLogLog (ULL) | Improved cardinality estimation | Implemented (Pure + Rust) |

### Capability Matrix

| Structure | insert | delete | merge | count | serialize | static | reconciliation |
|-----------|--------|--------|-------|-------|-----------|--------|----------------|
| Bloom | yes | -- | yes | yes* | yes | -- | -- |
| Cuckoo | yes | yes | -- | yes | yes | -- | -- |
| Quotient | yes | yes | yes | yes | yes | -- | -- |
| CQF | yes | yes | yes | yes | yes | -- | -- |
| XorFilter | -- | -- | -- | yes | yes | yes | -- |
| IBLT | yes | yes | yes | yes | yes | -- | yes |

*Bloom count is a popcount-based cardinality estimate.

### When to Choose

- **Bloom** -- Default choice for membership testing. No deletion needed, mergeable.
- **Cuckoo** -- Need to delete items. Better space efficiency than Bloom at low FPR.
- **Quotient** -- Need deletion and merge. Good when filter resizing may be needed.
- **CQF** -- Need to count how many times each item was inserted (multiset).
- **XorFilter** -- Static set known at build time. Smallest footprint, fastest lookups.
- **IBLT** -- Need to find differences between two sets (reconciliation).

## Installation

Add `ex_data_sketch` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_data_sketch, "~> 0.10.2"}
  ]
end
```

## Quick Start

```elixir
# HLL: count distinct elements
hll = ExDataSketch.HLL.new() |> ExDataSketch.HLL.update_many(1..100_000)
ExDataSketch.HLL.estimate(hll)  # ~100_000

# Stream API: build from lazy enumerables
sketch = 1..100_000 |> Stream.map(&to_string/1) |> ExDataSketch.Stream.hll(p: 14)

# Collectable: Enum.into works with any mergeable sketch
sketch = Enum.into(1..50_000, ExDataSketch.ULL.new(p: 14))

# Partitioned parallel reduction
sketch = ExDataSketch.Stream.reduce_partitioned(1..1_000_000, ExDataSketch.HLL, partitions: 8, p: 14)

# Persistence: save and load from ETS
:ets.new(:sketches, [:set, :public, :named_table])
ExDataSketch.Storage.ETS.save(sketch, :sketches, "daily:2024-01-15")
{:ok, loaded} = ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :sketches, "daily:2024-01-15")

# KLL: quantile estimation
kll = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100_000)
ExDataSketch.KLL.quantile(kll, 0.5)   # approximate median (~50_000)
ExDataSketch.KLL.quantile(kll, 0.99)  # 99th percentile (~99_000)

# Bloom: membership testing
bloom = ExDataSketch.Bloom.new(capacity: 100_000)
bloom = ExDataSketch.Bloom.put_many(bloom, 1..50_000)
ExDataSketch.Bloom.member?(bloom, 42)      # true
ExDataSketch.Bloom.member?(bloom, 99_999)  # false (probably)
```

See the [Quick Start Guide](guides/quick_start.md) for more examples.

## Livebooks

23 production-oriented Livebooks demonstrate real-world patterns: a
per-family tutorial for all 16 sketches (each generating and caching its
own sample data), plus 7 cross-cutting guides covering stream
consumption, distributed merge semantics, framework integration, and a
1-billion-row-style case study. See [Livebooks Guide](guides/livebooks.md)
for the recommended reading order and what each Livebook teaches.

## Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/ex_data_sketch).

## Architecture

- **Binary state**: All sketch state is canonical Elixir binaries. No opaque NIF resources.
- **Backend system**: Pure Elixir reference implementation with optional Rust NIF acceleration. The Rust backend falls back to Pure automatically when unavailable.
- **Streaming native**: `ExDataSketch.Stream`, `Collectable`, and `from_enumerable/2` for lazy stream consumption. Broadway, GenStage, and Flow integration for production pipelines.
- **Persistence**: ETS, DETS, CubDB, Mnesia, and Ecto backends with atomic `save/load/merge/delete` operations. All storage uses EXSK v2 binary format with CRC32C checksum.
- **Observability**: Structured `:telemetry` events at batch boundaries, optional OpenTelemetry span bridge, category-based enable/disable.
- **Serialization**: ExDataSketch-native format (EXSK) for all sketches, plus Apache DataSketches CompactSketch interop for Theta. v1 format escape hatch for backward compatibility.
- **Deterministic hashing**: Stable 64-bit hash (`ExDataSketch.Hash`) for reproducible results.
- **Backend parity**: Both backends produce byte-identical serialized output for the same inputs.

## Compatibility and Stability

The following guarantees apply within the v0.x release series:

- **EXSK serialization**: The ExDataSketch-native binary format is stable. Binaries produced by any v0.x release can be deserialized by any other v0.x release, with one exception: see the ULL v1/v2 state-format note below.
- **Pure vs Rust parity**: Given identical inputs, both backends produce byte-identical serialized state and identical estimates.
- **Deterministic output**: The same input sequence always produces the same sketch state and estimate, regardless of backend.
- **Backward compatibility**: v0.7.x EXSK v1 frames remain decodable. `HLL.serialize(sketch, format: :v1)` (and, as of v0.10.0, every other family except `FilterChain`) produces backward-compatible v0.7.x-style output (requires `:phash2` hash strategy where applicable).

Not guaranteed:

- **ULL binary format (v0.10.1+)**: `ExDataSketch.ULL`'s register encoding and estimator were rewritten in v0.10.1 to fix a correctness bug (the previous implementation was an HLL-derived approximation, not real UltraLogLog, and overestimated cardinality by orders of magnitude once a sketch's registers filled up). Its internal `ULL1` state format version was bumped 1 -> 2 as part of the fix; sketches serialized by v0.10.0 or earlier fail to decode on v0.10.1+ with a clear error instead of being silently misread. Rebuild any persisted ULL sketches from source data after upgrading. No other family is affected.
- **ULL estimate stability**: beyond the v0.10.1 binary-format break above, ULL estimates also shifted at low cardinalities between v0.8.0 and v0.9.0 due to an earlier accuracy correction.
- **Cross-language interop**: `Theta` and `KLL` (as of v0.10.0) support Apache DataSketches compact binary formats. HLL and CMS DataSketches interop is not implemented.
- **Performance stability**: Benchmark results may vary across hardware and OTP versions.
- **EXSK format across major versions**: The binary format may change in future major releases.

## Development

```bash
# Get dependencies
mix deps.get

# Run tests with coverage
mix test --cover

# Run lints
mix lint

# Run benchmarks
mix bench

# Generate docs
mix docs
```

## Roadmap

| Version | Focus | Status |
|---------|-------|--------|
| v0.1.0 | Core sketches (HLL, CMS, Theta) + Rust NIFs | Released |
| v0.2.0 | KLL quantiles | Released |
| v0.2.1 | DDSketch relative-error quantiles | Released |
| v0.3.0 | FrequentItems (SpaceSaving) | Released |
| v0.4.0 | Bloom filter (membership testing) | Released |
| v0.5.0 | Advanced membership filters (Cuckoo, Quotient, CQF, XorFilter, IBLT, FilterChain) | Released |
| v0.6.0 | REQ sketch, Misra-Gries, XXHash3, Rust NIF parity for all membership filters | Released |
| v0.7.0 | ULL (UltraLogLog) -- improved cardinality estimation with Pure + Rust NIF | Released |
| v0.7.1 | NIF batch hashing, hash customization, quotient filter fix, merge safety | Released |
| v0.8.0 | Deterministic Foundations -- pluggable hash registry (XXHash3 + Murmur3), binary stability and corruption detection, HLL hot-path optimization, precompiled NIFs, property-based validation | Released |
| v0.9.0 | Streaming Integrations -- Stream/Collectable API, Broadway/GenStage/Flow integration, persistence (ETS/DETS/CubDB/Mnesia/Ecto), telemetry + OpenTelemetry, ULL accuracy fix, v1 serialization escape hatch | Released |
| v0.10.0 | Production Ergonomics -- unified sketch contract & facade dispatch, storage behaviour, windowing, supervised sketches (Server/Sketches), Telemetry.Metrics + LiveDashboard, filter NIF raw-hashing, Apache KLL interop, v1 serialization escape hatch for every family | Released |
| v0.10.1 | Correctness and polish -- `ULL` rewritten to the real UltraLogLog algorithm (was an HLL-derived approximation; binary format bumped v1->v2), `KLL` compaction weight-invariant fix, `HLL` precision range widened to p=4..26, 16 new per-family tutorial Livebooks, `phoenix_demo/` sample app, plus the original post-review fixes (Server graceful-shutdown snapshotting, storage merge crash-safety, filter `:hash_strategy` build/round-trip fix, hardened `opencode.yml` workflow) | Released |
| v0.10.2 | Application-config-driven per-family defaults (`ExDataSketch.Config`), `FilterChain.put_many/2` batching (with a simplified `delete/2` return), plus correctness fixes found via manual livebook verification -- `Cuckoo` eviction-slot cycling, `Quotient`/`CQF` O(table size) lookups, `CQF` silent overflow, and a `REQ` compaction weight-invariant bug matching v0.10.1's `KLL` fix | Released |
| v0.11.0 | Apache HLL Interoperability & New Sketch Families -- full cross-language HLL exchange, CPC (Compressed Probabilistic Counting), Tuple Sketch (weighted distinct counting) | Planned |
| v0.12.0 | Similarity & Sampling -- MinHash, Weighted MinHash, VarOpt sampling | Planned |
| v1.0.0 | Stable Binary Contract -- locked EXSK format, full benchmark suite, Nx / Arrow ecosystem integrations | Planned |


## License

MIT License. See [LICENSE](LICENSE) for details.
