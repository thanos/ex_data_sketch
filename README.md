# ExDataSketch

Production-grade streaming data sketching algorithms for Elixir.

ExDataSketch provides probabilistic data structures for approximate counting and
frequency estimation on streaming data. All sketch state is stored as
Elixir-owned binaries, enabling straightforward serialization, distribution,
and persistence.

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

## Installation

Add `ex_data_sketch` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ex_data_sketch, "~> 0.1.0"}
  ]
end
```

## Quick Start

See the [Quick Start Guide](guides/quick_start.md) for usage examples.

## Documentation

Full documentation is available at [HexDocs](https://hexdocs.pm/ex_data_sketch).

## Architecture

- **Binary state**: All sketch state is canonical Elixir binaries. No opaque NIF resources.
- **Backend system**: Pure Elixir reference implementation with optional Rust NIF acceleration. The Rust backend falls back to Pure automatically when unavailable.
- **Serialization**: ExDataSketch-native format (EXSK) for all sketches, plus Apache DataSketches CompactSketch interop for Theta.
- **Deterministic hashing**: Stable 64-bit hash (`ExDataSketch.Hash`) for reproducible results.
- **Backend parity**: Both backends produce byte-identical serialized output for the same inputs.

## Compatibility and Stability

The following guarantees apply within the v0.1.x release series:

- **EXSK serialization**: The ExDataSketch-native binary format is stable. Binaries produced by any v0.1.x release can be deserialized by any other v0.1.x release.
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

## License

MIT License. See [LICENSE](LICENSE) for details.
