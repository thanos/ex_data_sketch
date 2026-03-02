# ExDataSketch

Production-grade streaming data sketching algorithms for Elixir.

ExDataSketch provides probabilistic data structures for approximate counting and
frequency estimation on streaming data. All sketch state is stored as
Elixir-owned binaries, enabling straightforward serialization, distribution,
and persistence.

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

- **Binary state**: All sketch state is canonical Elixir binaries.
- **Backend system**: Pure Elixir reference implementation with optional Rust NIF acceleration.
- **Serialization**: ExDataSketch-native format (EXSK) plus Apache DataSketches CompactSketch interop for Theta.
- **Deterministic hashing**: Stable 64-bit hash for reproducible results.
- **Backend parity**: Both backends produce byte-identical output for the same inputs.

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
