# Usage Guide

This guide covers the full API, configuration options, backend system,
serialization formats, and error handling for ExDataSketch.

## Options

### HLL Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:p` | integer | 14 | Precision parameter. Valid range: 4..16. Higher values use more memory but give better accuracy. Register count = 2^p. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Memory usage: `2^p` bytes for registers (e.g., p=14 uses 16 KiB).

Relative error: approximately `1.04 / sqrt(2^p)`.

### CMS Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:width` | pos_integer | 2048 | Number of counters per row. Higher values reduce error. |
| `:depth` | pos_integer | 5 | Number of hash functions (rows). Higher values reduce failure probability. |
| `:counter_width` | 32 or 64 | 32 | Bit width of each counter. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Memory usage: `width * depth * (counter_width / 8)` bytes.

Error bound: `e * total_count / width` with probability `1 - (1/2)^depth`,
where `e` is Euler's number.

### Theta Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:k` | pos_integer | 4096 | Nominal number of entries. Controls accuracy. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

## Backend System

ExDataSketch uses a backend system to allow swapping computation engines
without changing the public API.

### Available Backends

- `ExDataSketch.Backend.Pure` -- Pure Elixir implementation. Always available.
  Default backend.
- `ExDataSketch.Backend.Rust` -- Optional Rust NIF acceleration (Phase 2).
  Requires Rustler and Rust toolchain.

### Selecting a Backend

Per-sketch:

```elixir
sketch = ExDataSketch.HLL.new(backend: ExDataSketch.Backend.Rust)
```

Global default (in config):

```elixir
config :ex_data_sketch, backend: ExDataSketch.Backend.Pure
```

The per-sketch option always takes precedence over the global config.

### Backend Guarantees

- Both backends produce identical results for the same inputs.
- Serialized state is identical regardless of which backend produced it.
- The public API does not change between backends.

## Serialization

### ExDataSketch-Native Format (EXSK)

All sketches support the native binary format:

```elixir
binary = ExDataSketch.HLL.serialize(sketch)
{:ok, sketch} = ExDataSketch.HLL.deserialize(binary)
```

The EXSK format structure:

| Field | Size | Description |
|-------|------|-------------|
| Magic | 4 bytes | `"EXSK"` |
| Version | 1 byte | Format version (currently 1) |
| Sketch ID | 1 byte | Identifies sketch type (HLL=1, CMS=2, Theta=3) |
| Params length | 4 bytes | Little-endian u32, byte length of params |
| Params | variable | Sketch-specific parameters |
| State length | 4 bytes | Little-endian u32, byte length of state |
| State | variable | Raw sketch state |

### DataSketches Interop

Selected sketch types support the Apache DataSketches binary format for
cross-language interoperability:

```elixir
# Theta sketch interop (priority target)
binary = ExDataSketch.Theta.serialize_datasketches(theta_sketch)
{:ok, sketch} = ExDataSketch.Theta.deserialize_datasketches(binary)
```

Interop priority order: Theta (CompactSketch), then HLL, then KLL.

## Hashing

ExDataSketch uses a deterministic 64-bit hash function for all sketch operations.
The hash module provides:

```elixir
# Hash any Elixir term
ExDataSketch.Hash.hash64("hello")

# Hash raw binary data
ExDataSketch.Hash.hash64_binary(<<1, 2, 3>>)
```

The default hash implementation uses `:erlang.phash2/2` combined with
additional mixing to produce a full 64-bit output. This is deterministic
within the same BEAM instance.

## Error Handling

ExDataSketch uses tagged tuples for recoverable errors:

```elixir
{:error, %ExDataSketch.Errors.InvalidOption{message: "p must be between 4 and 16"}}
{:error, %ExDataSketch.Errors.DeserializationError{message: "invalid magic bytes"}}
```

Functions that cannot fail return values directly (no `:ok` tuple wrapping).
Functions that validate external input (deserialization, option parsing) return
`{:ok, result} | {:error, error}` tuples.

## Merging in Distributed Systems

Sketches are designed for distributed aggregation. A common pattern:

1. Each node maintains a local sketch.
2. Periodically serialize and send sketches to an aggregator.
3. The aggregator deserializes and merges all sketches.
4. Query the merged sketch for global estimates.

Requirements for merging:
- Both sketches must be the same type (e.g., both HLL).
- Both sketches must have the same parameters (e.g., same `p` value for HLL).
- Attempting to merge incompatible sketches returns an error.
