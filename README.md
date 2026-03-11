# ExDataSketch

Production-grade streaming data sketching algorithms for Elixir.

ExDataSketch provides probabilistic data structures for approximate counting,
frequency estimation, quantile computation, heavy-hitter detection, membership
testing with deletion, and set reconciliation on streaming data. All sketch
state is stored as Elixir-owned binaries, enabling straightforward
serialization, distribution, and persistence.

[![CI](https://github.com/thanos/ex_data_sketch/actions/workflows/ci.yml/badge.svg)](https://github.com/thanos/ex_data_sketch/actions/workflows/ci.yml)
[![Hex version](https://img.shields.io/hexpm/v/ex_data_sketch.svg)](https://hex.pm/packages/ex_data_sketch)
[![Hex docs](https://img.shields.io/badge/docs-hexdocs.pm-blue)](https://hexdocs.pm/ex_data_sketch)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)


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
    {:ex_data_sketch, "~> 0.6.0"}
  ]
end
```

## Quick Start

```elixir
# HLL: count distinct elements
hll = ExDataSketch.HLL.new() |> ExDataSketch.HLL.update_many(1..100_000)
ExDataSketch.HLL.estimate(hll)  # ~100_000

# KLL: quantile estimation
kll = ExDataSketch.KLL.new() |> ExDataSketch.KLL.update_many(1..100_000)
ExDataSketch.KLL.quantile(kll, 0.5)   # approximate median (~50_000)
ExDataSketch.KLL.quantile(kll, 0.99)  # 99th percentile (~99_000)

# Bloom: membership testing
bloom = ExDataSketch.Bloom.new(capacity: 100_000)
bloom = ExDataSketch.Bloom.put_many(bloom, 1..50_000)
ExDataSketch.Bloom.member?(bloom, 42)      # true
ExDataSketch.Bloom.member?(bloom, 99_999)  # false (probably)

# Cuckoo: membership testing with deletion
cuckoo = ExDataSketch.Cuckoo.new(capacity: 100_000)
{:ok, cuckoo} = ExDataSketch.Cuckoo.put(cuckoo, "user_42")
ExDataSketch.Cuckoo.member?(cuckoo, "user_42")  # true
{:ok, cuckoo} = ExDataSketch.Cuckoo.delete(cuckoo, "user_42")
ExDataSketch.Cuckoo.member?(cuckoo, "user_42")  # false
```

See the [Quick Start Guide](guides/quick_start.md) for more examples.

## Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/ex_data_sketch).

## Architecture

- **Binary state**: All sketch state is canonical Elixir binaries. No opaque NIF resources.
- **Backend system**: Pure Elixir reference implementation with optional Rust NIF acceleration. The Rust backend falls back to Pure automatically when unavailable.
- **Serialization**: ExDataSketch-native format (EXSK) for all sketches, plus Apache DataSketches CompactSketch interop for Theta.
- **Deterministic hashing**: Stable 64-bit hash (`ExDataSketch.Hash`) for reproducible results.
- **Backend parity**: Both backends produce byte-identical serialized output for the same inputs.

## Compatibility and Stability

The following guarantees apply within the v0.x release series:

- **EXSK serialization**: The ExDataSketch-native binary format is stable. Binaries produced by any v0.x release can be deserialized by any other v0.x release.
- **Pure vs Rust parity**: Given identical inputs, both backends produce byte-identical serialized state and identical estimates.
- **Deterministic output**: The same input sequence always produces the same sketch state and estimate, regardless of backend.

Not guaranteed:

- **Cross-language interop**: Only Theta supports Apache DataSketches CompactSketch format. HLL and CMS DataSketches interop is not implemented.
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
| v0.7.0 | ULL (UltraLogLog) - Rust NIF Parity | Planned |
| v0.8.0 | Massive Static Data & Industry Interop  - Binary Fuse Filters, Ribbon Filter Implementation. Apache DataSketches Interop. | Planned |
| v0.9.0 | dequantized HHL and Sphinx (Succinct Perfect Hash Index) | Planned |

## License

MIT License. See [LICENSE](LICENSE) for details.
