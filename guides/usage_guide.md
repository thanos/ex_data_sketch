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

### KLL Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:k` | integer | 200 | Accuracy parameter. Valid range: 8..65535. Higher values use more memory but give better accuracy. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Rank error: approximately `1.65 / k`.

### DDSketch Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:alpha` | float | 0.01 | Relative accuracy parameter. Controls bucket width. Must be in (0.0, 1.0). |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Value-relative error: the returned quantile value v satisfies
`v_true * (1 - alpha) <= v <= v_true * (1 + alpha)`.

| alpha | Relative Error | Bucket Count (for 1ms..10s range) |
|-------|---------------|-----------------------------------|
| 0.05  | 5%            | ~185 |
| 0.01  | 1%            | ~920 |
| 0.005 | 0.5%          | ~1840 |
| 0.001 | 0.1%          | ~9200 |

### FrequentItems Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:k` | pos_integer | 10 | Maximum number of counters. Controls how many distinct items can be tracked. Higher values give more accurate frequency estimates. |
| `:key_encoding` | atom/tuple | `:binary` | Key encoding policy: `:binary` (raw binaries), `:int` (64-bit LE integers), or `{:term, :external}` (Erlang external term format). |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

FrequentItems uses the SpaceSaving algorithm to track the top-k most
frequent items in a data stream. It maintains at most `k` counters, each
storing an item, its estimated count, and a maximum overcount error.

When a new item arrives and all k slots are full, the item with the minimum
count is evicted (ties broken by lexicographically smallest key). The new
item inherits the evicted count plus one, and the evicted count is recorded
as the error bound.

Frequency estimates are always upper bounds -- the true frequency may be
lower, but never higher. The `lower` field gives a guaranteed lower bound:
`max(estimate - error, 0)`.

### Bloom Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:capacity` | pos_integer | 10,000 | Expected number of items to insert. Used to derive optimal bit_count and hash_count. |
| `:false_positive_rate` | float | 0.01 | Target false positive probability. Must be in (0.0, 1.0). |
| `:seed` | non_neg_integer | 0 | Hash seed. Filters with different seeds cannot be merged. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Bloom filters provide probabilistic membership testing: `member?/2` returns
`true` if an item was probably inserted, or `false` if it was definitely not.
False positives are possible; false negatives are not.

Parameters are derived automatically:
- `bit_count = ceil(-capacity * ln(fpr) / ln(2)^2)`
- `hash_count = max(1, round(bit_count / capacity * ln(2)))`, clamped to 1..30

For capacity=100,000 and fpr=0.01: bit_count=958,506, hash_count=7, ~117 KB.

### KLL vs DDSketch

Both are quantile sketches available through `ExDataSketch.Quantiles`, but they
provide different accuracy guarantees:

| Property | KLL | DDSketch |
|----------|-----|----------|
| Error type | Rank error | Value-relative error |
| Best for | General rank queries | Latency percentiles, SLOs |
| Parameter | `k` (default 200) | `alpha` (default 0.01) |
| Guarantee | True rank within ~1.65/k | True value within factor (1 +/- alpha) |
| Negative values | Supported | Rejected |
| Memory | Fixed by k | Grows with log(max/min) |

Use KLL when you need rank accuracy (e.g., "what fraction of values are below X?").
Use DDSketch when you need value accuracy (e.g., "p99 latency is 142ms +/- 1%").

### Quantiles Facade

The `ExDataSketch.Quantiles` module provides a type-dispatched facade:

```elixir
# KLL (default)
sketch = ExDataSketch.Quantiles.new(type: :kll, k: 200)
sketch = ExDataSketch.Quantiles.update_many(sketch, 1..1000)
ExDataSketch.Quantiles.quantile(sketch, 0.5)  # approximate median

# DDSketch
sketch = ExDataSketch.Quantiles.new(type: :ddsketch, alpha: 0.01)
sketch = ExDataSketch.Quantiles.update_many(sketch, latency_samples)
ExDataSketch.Quantiles.quantile(sketch, 0.99)  # p99 with relative accuracy
```

## Backend System

ExDataSketch uses a backend system to allow swapping computation engines
without changing the public API.

### Available Backends

- `ExDataSketch.Backend.Pure` -- Pure Elixir implementation. Always available.
  Default backend.
- `ExDataSketch.Backend.Rust` -- Rust NIF acceleration. Precompiled binaries
  are downloaded automatically on supported platforms. Falls back to Pure if
  the NIF is not available.

### Precompiled NIF Binaries

The Rust NIF is distributed as precompiled binaries for the following platforms:

| Platform | Architecture |
|----------|-------------|
| macOS | ARM64 (Apple Silicon), x86_64 |
| Linux (glibc) | x86_64, aarch64 |
| Linux (musl) | x86_64, aarch64 |

On these platforms, `mix compile` automatically downloads the correct binary.
No Rust toolchain is required.

To force compilation from source (requires Rust):

```bash
EX_DATA_SKETCH_BUILD=1 mix compile
```

On unsupported platforms, the library operates in pure Elixir mode
automatically. No configuration is needed.

Check availability at runtime:

```elixir
ExDataSketch.Backend.Rust.available?()
# => true or false
```

### Selecting a Backend

Per-sketch:

```elixir
sketch = ExDataSketch.HLL.new(backend: ExDataSketch.Backend.Rust)
```

Global default (in config):

```elixir
config :ex_data_sketch, backend: ExDataSketch.Backend.Rust
```

The per-sketch option always takes precedence over the global config.
If `Backend.Rust` is configured but the NIF is not available, it
automatically falls back to `Backend.Pure`.

### Rust Backend Details

The Rust backend accelerates batch and traversal operations via NIFs:

| Rust NIF | Pure fallback |
|----------|---------------|
| `hll_update_many`, `hll_merge`, `hll_estimate` | `hll_new`, `hll_update` |
| `cms_update_many`, `cms_merge` | `cms_new`, `cms_update`, `cms_estimate` |
| `theta_update_many`, `theta_merge` | `theta_new`, `theta_update`, `theta_compact`, `theta_estimate` |
| `kll_update_many`, `kll_merge` | `kll_new`, `kll_update`, `kll_quantile`, `kll_rank`, `kll_count`, `kll_min`, `kll_max` |

#### Dirty Scheduler Thresholds

Batch operations automatically use dirty CPU schedulers when input size
exceeds configurable thresholds:

| Operation | Default threshold |
|-----------|-------------------|
| `hll_update_many` | 10,000 hashes |
| `cms_update_many` | 10,000 pairs |
| `theta_update_many` | 10,000 hashes |
| `kll_update_many` | 10,000 values |
| `cms_merge` | 100,000 total counters |
| `theta_merge` | 50,000 combined entries |
| `kll_merge` | 50,000 combined items |

Override globally:

```elixir
config :ex_data_sketch, :dirty_thresholds, %{
  hll_update_many: 5_000,
  cms_update_many: 20_000
}
```

Or per-call via opts:

```elixir
HLL.update_many(sketch, items, dirty_threshold: 5_000)
```

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
| Sketch ID | 1 byte | Identifies sketch type (HLL=1, CMS=2, Theta=3, KLL=4, DDSketch=5, FrequentItems=6, Bloom=7) |
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
KLL DataSketches interop is stubbed but not yet implemented.

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
{:error, %ExDataSketch.Errors.InvalidOptionError{message: "p must be between 4 and 16"}}
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
