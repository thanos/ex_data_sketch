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

### ULL Options

UltraLogLog (Ertl, 2023) provides approximately 20% better accuracy than HLL
at the same memory footprint. It uses the same register array layout but
stores a different value per register and applies the FGRA estimator.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:p` | integer | 14 | Precision parameter. Valid range: 4..26. Higher values use more memory but give better accuracy. Register count = 2^p. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Memory usage: `8 + 2^p` bytes (e.g., p=14 uses ~16 KiB).

Relative error: approximately `0.835 / sqrt(2^p)`.

#### HLL vs ULL Comparison

| p  | Memory  | HLL Error | ULL Error | ULL Improvement |
|----|---------|-----------|-----------|-----------------|
| 10 | ~1 KiB  | 3.25%     | 2.61%     | ~20%            |
| 12 | ~4 KiB  | 1.63%     | 1.30%     | ~20%            |
| 14 | ~16 KiB | 0.81%     | 0.65%     | ~20%            |
| 16 | ~64 KiB | 0.41%     | 0.33%     | ~20%            |

```elixir
sketch = ExDataSketch.ULL.new(p: 14)
sketch = ExDataSketch.ULL.update_many(sketch, items)
ExDataSketch.ULL.estimate(sketch)

# Merge two sketches
merged = ExDataSketch.ULL.merge(sketch_a, sketch_b)

# Build from enumerable
sketch = ExDataSketch.ULL.from_enumerable(stream, p: 14)
```

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

### Cuckoo Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:capacity` | pos_integer | 10,000 | Expected number of items. Used to derive bucket count. |
| `:fingerprint_size` | integer | 8 | Fingerprint width in bits. Valid values: 8, 12, 16. Larger fingerprints reduce false positive rate. |
| `:bucket_size` | integer | 4 | Slots per bucket. Valid values: 2, 4. Bucket size 4 gives better space efficiency. |
| `:max_kicks` | integer | 500 | Maximum relocation attempts before reporting full. Valid range: 100..2000. |
| `:seed` | non_neg_integer | 0 | Hash seed. Filters with different seeds are not compatible. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Cuckoo filters provide membership testing with deletion support. Unlike Bloom
filters, items can be removed after insertion without introducing false
negatives. Uses partial-key cuckoo hashing where fingerprints are stored in
one of two candidate buckets.

`put/2` returns `{:ok, cuckoo}` on success or `{:error, :full}` when the
filter cannot relocate fingerprints after `max_kicks` attempts. `delete/2`
returns `{:ok, cuckoo}` on success or `{:error, :not_found}` if the
fingerprint is not present.

False positive rate: approximately `2 * bucket_size / 2^fingerprint_size`.
For fingerprint_size=8, bucket_size=4: ~3.1%. For fingerprint_size=16,
bucket_size=4: ~0.012%.

### Quotient Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:q` | integer | 16 | Quotient bits. Determines number of slots (2^q). Valid range: 1..28. |
| `:r` | integer | 8 | Remainder bits stored per slot. Valid range: 1..32. Constraint: q + r <= 64. |
| `:seed` | non_neg_integer | 0 | Hash seed. Filters with different seeds cannot be merged. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

Quotient filters split each fingerprint into a quotient (slot index) and
remainder (stored value). Three metadata bits per slot (is_occupied,
is_continuation, is_shifted) enable linear probing with cluster tracking.

Supports safe deletion and merge without re-hashing. Merge combines two
filters with matching parameters by iterating runs from both filters.

False positive rate: approximately `1 / 2^r`. For r=8: ~0.4%. For r=16: ~0.0015%.

Memory usage: `2^q * (r + 3) / 8` bytes (3 metadata bits per slot).

### CQF Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:q` | integer | 16 | Quotient bits. Determines number of slots (2^q). Valid range: 1..28. |
| `:r` | integer | 8 | Remainder bits stored per slot. Valid range: 1..32. Constraint: q + r <= 64. |
| `:seed` | non_neg_integer | 0 | Hash seed. Filters with different seeds cannot be merged. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

CQF extends the quotient filter with variable-length counter encoding to
track item multiplicities. Use `estimate_count/2` to query how many times an
item has been inserted. Counts are approximate -- never underestimated but may
be overestimated.

Merge sums counts across filters. Deletion decrements the count for an item
(safe no-op if the item is not present).

```elixir
cqf = ExDataSketch.CQF.new(q: 16, r: 8)
cqf = cqf |> ExDataSketch.CQF.put("event") |> ExDataSketch.CQF.put("event")
ExDataSketch.CQF.estimate_count(cqf, "event")  # >= 2
```

### XorFilter Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:fingerprint_bits` | integer | 8 | Fingerprint width. Valid values: 8, 16. |
| `:seed` | non_neg_integer | 0 | Hash seed. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

XorFilter is a static, immutable membership filter. All items must be
provided at construction time via `build/2`. After construction, only
`member?/2` queries are supported -- no insertion, deletion, or merge.

XorFilter offers the smallest memory footprint and fastest lookups of all
membership filters in this library. Use it when the set is known ahead of
time and will not change.

```elixir
items = MapSet.new(["alice", "bob", "carol"])
{:ok, xor} = ExDataSketch.XorFilter.build(items, fingerprint_bits: 8)
ExDataSketch.XorFilter.member?(xor, "alice")  # true
ExDataSketch.XorFilter.member?(xor, "dave")   # false (probably)
```

False positive rate: approximately `1 / 2^fingerprint_bits`. For 8-bit: ~0.4%.
For 16-bit: ~0.0015%.

### IBLT Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:cell_count` | pos_integer | 1,000 | Number of cells. Valid range: 1..16,777,216. More cells improve decode success rate. |
| `:hash_count` | pos_integer | 3 | Number of hash functions. Valid range: 1..10. |
| `:seed` | non_neg_integer | 0 | Hash seed. IBLTs with different seeds cannot be subtracted or merged. |
| `:backend` | module | `ExDataSketch.Backend.Pure` | Backend module for computation. |

IBLT (Invertible Bloom Lookup Table) extends Bloom filters with the ability
to list entries and find set differences. Two parties build IBLTs from their
respective sets, exchange them, and subtract to discover items present in one
set but not the other.

Supports two modes:
- **Set mode**: `put/2` and `delete/2` operate on items (value_hash = 0).
- **Key-value mode**: `put/3` and `delete/3` operate on key-value pairs.

`subtract/2` performs cell-wise subtraction. `list_entries/1` peels the
resulting IBLT to recover entries, returning `{:ok, %{positive: [...],
negative: [...]}}` or `{:error, :decode_failed}` if the difference is too
large for the cell count.

```elixir
iblt_a = ExDataSketch.IBLT.new(cell_count: 1000) |> ExDataSketch.IBLT.put_many(set_a)
iblt_b = ExDataSketch.IBLT.new(cell_count: 1000) |> ExDataSketch.IBLT.put_many(set_b)

diff = ExDataSketch.IBLT.subtract(iblt_a, iblt_b)
{:ok, %{positive: only_in_a, negative: only_in_b}} = ExDataSketch.IBLT.list_entries(diff)
```

Rule of thumb: set `cell_count` to at least 3x the expected number of
differences for reliable decoding.

### FilterChain Options

FilterChain composes multiple membership filters into a pipeline with
capability-aware stage validation.

```elixir
chain =
  ExDataSketch.FilterChain.new()
  |> ExDataSketch.FilterChain.add_stage(ExDataSketch.Cuckoo.new(capacity: 10_000))
  |> ExDataSketch.FilterChain.add_stage(ExDataSketch.Bloom.new(capacity: 100_000))
```

**Stage positions:**

| Position | Allowed types | Notes |
|----------|--------------|-------|
| Front / Middle | Bloom, Cuckoo, Quotient, CQF | Dynamic filters supporting `:put` |
| Terminal | XorFilter | Static filter, must be last stage |
| Adjunct | IBLT | Not in query path, used for reconciliation |

**Operations:**

- `put/2` -- Inserts into all stages supporting `:put` (skips XorFilter).
- `member?/2` -- Queries stages in order, short-circuits on `false`.
- `delete/2` -- Deletes from all stages. Raises `UnsupportedOperationError`
  if any stage does not support `:delete`.

**Lifecycle-tier pattern example:**

```elixir
# Hot tier (absorbs writes) -> Cold tier (compacted snapshot)
chain =
  ExDataSketch.FilterChain.new()
  |> ExDataSketch.FilterChain.add_stage(ExDataSketch.Cuckoo.new(capacity: 50_000))

# Later, compact hot tier into a static XorFilter
{:ok, xor} = ExDataSketch.XorFilter.build(compacted_items, fingerprint_bits: 16)
chain = ExDataSketch.FilterChain.add_stage(chain, xor)
```

### Membership Filter Comparison

| Property | Bloom | Cuckoo | Quotient | CQF | XorFilter | IBLT |
|----------|-------|--------|----------|-----|-----------|------|
| Insert | yes | yes | yes | yes | build-only | yes |
| Delete | no | yes | yes | yes | no | yes |
| Merge | yes | no | yes | yes | no | yes |
| Count items | yes | yes | yes | yes (multiset) | yes | yes |
| Static | no | no | no | no | yes | no |
| Reconciliation | no | no | no | no | no | yes |
| Space efficiency | good | better at low FPR | moderate | moderate | best | depends on diff size |

### REQ Options

The Relative Error Quantile (REQ) sketch provides relative-error rank accuracy.
Unlike KLL (rank error) or DDSketch (value-relative error), REQ concentrates
accuracy near the extremes of the distribution.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `k` | integer | 12 | Accuracy parameter. Higher k means more accuracy but more memory |
| `hra` | boolean | true | High Rank Accuracy. When true, accuracy is better at high ranks (p99, p999) |
| `backend` | module | configured | Backend module |

```elixir
sketch = ExDataSketch.REQ.new(k: 12, hra: true)
sketch = ExDataSketch.REQ.update_many(sketch, Enum.map(1..10_000, &(&1 * 1.0)))
ExDataSketch.REQ.quantile(sketch, 0.99)
ExDataSketch.REQ.rank(sketch, 9500.0)
ExDataSketch.REQ.cdf(sketch, [1000.0, 5000.0, 9000.0])
ExDataSketch.REQ.pmf(sketch, [1000.0, 5000.0, 9000.0])
```

### MisraGries Options

The Misra-Gries algorithm provides deterministic heavy-hitter detection.
Unlike FrequentItems (SpaceSaving), Misra-Gries makes no estimate when
an item is not tracked, returning 0 instead.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `:k` | integer | 10 | Number of counters. Detects items with frequency > n/k. |
| `:key_encoding` | atom/tuple | `:binary` | Key encoding policy: `:binary` (raw binaries), `:int` (signed 64-bit LE integers), or `{:term, :external}` (Erlang external term format). |
| `:backend` | module | configured | Backend module. |

```elixir
# Default binary keys
sketch = ExDataSketch.MisraGries.new(k: 10)
sketch = ExDataSketch.MisraGries.update_many(sketch, items)
ExDataSketch.MisraGries.estimate(sketch, "frequent_item")
ExDataSketch.MisraGries.top_k(sketch, 10)

# Integer keys
sketch = ExDataSketch.MisraGries.new(k: 10, key_encoding: :int)
sketch = ExDataSketch.MisraGries.update_many(sketch, [100, 200, 100, 300, 100])
ExDataSketch.MisraGries.top_k(sketch, 3)
# => [{100, 3}, {200, 1}, {300, 1}]

# Arbitrary Erlang terms
sketch = ExDataSketch.MisraGries.new(k: 10, key_encoding: {:term, :external})
sketch = ExDataSketch.MisraGries.update_many(sketch, [{:user, 1}, {:user, 2}, {:user, 1}])
ExDataSketch.MisraGries.estimate(sketch, {:user, 1})
# => 2
```

### XXHash3

ExDataSketch includes a Rust NIF implementation of XXHash3 for high-quality
64-bit hashing. This is used internally by the membership filters and is
also available directly.

```elixir
ExDataSketch.Hash.xxhash3_64("some data")
ExDataSketch.Hash.xxhash3_64("some data", 42)
```

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

### Auto-detection

`hash64/2` automatically selects the best available hash implementation:

- **XXHash3 (NIF)**: When the Rust NIF is loaded, `hash64/2` uses XXHash3 via
  NIF, producing native 64-bit hashes with zero Elixir-side overhead. XXHash3
  output is stable across platforms.

- **phash2 + fixnum-safe mix64 (pure)**: When the NIF is not available,
  `hash64/2` falls back to `:erlang.phash2/2` with a 64-bit mixer that uses
  16-bit partial products to avoid bigint heap allocations. Every intermediate
  value stays under 35 bits (well within the BEAM's 60-bit fixnum limit),
  eliminating the 3+ transient bigint allocations per hash call that a naive
  64-bit multiply would incur.

The NIF availability check is performed once and cached in `:persistent_term`
for zero-cost subsequent lookups.

### Why not 32-bit hashes

32-bit hashes degrade sketch accuracy above approximately 10M items due to
birthday-paradox collisions. The fixnum-safe mixer preserves full 64-bit output
quality at the cost of roughly 20 fixnum operations per hash -- still faster
than 3+ bigint heap allocations from naive 64-bit multiplications.

### Hash strategy tagging

Sketches record which hash function was used at creation time (`:xxhash3`,
`:phash2`, or `:custom`). This tag is persisted through serialization in the
EXSK params section and controls runtime hash dispatch:

- `:phash2` -- always uses the pure Elixir mix64 path, even when the NIF is available.
- `:xxhash3` -- requires the Rust NIF; raises `ArgumentError` if unavailable.
- `:custom` -- cannot be deserialized (the original function is not recoverable from bytes); `deserialize/1` returns `{:error, %DeserializationError{}}`.

Merge operations validate that both sketches share the same strategy and seed.

### Pluggable hash

Users can override the default hash with a custom function:

```elixir
HLL.new(p: 14, hash_fn: fn term -> my_hash(term) end)
```

When `:hash_fn` is provided, the sketch records `hash_strategy: :custom`.

### Stability

`:erlang.phash2/2` output is not guaranteed stable across OTP major versions.
XXHash3 output is stable across platforms. For cross-version or cross-system
stability, use the NIF build (XXHash3) or supply a custom `:hash_fn`.

## Use Cases

### Algorithm Selection Guide

| Use Case | Algorithm | Key Option | What You Get |
|----------|-----------|------------|--------------|
| Count distinct users / IPs / sessions | HLL | `p: 14` | Cardinality estimate with ~0.8% error |
| Frequency of specific items (query counts, error codes) | CMS | `width: 2048, depth: 5` | Per-item count estimates (always >= true count) |
| Set intersection / union cardinality | Theta | `k: 4096` | Jaccard similarity, union/intersection sizes |
| Percentiles and rank queries (general) | KLL | `k: 200` | Median, P95, P99 with rank-error guarantees |
| Latency percentiles and SLO monitoring | DDSketch | `alpha: 0.01` | P99 with value-relative error (e.g., 142ms +/- 1%) |
| Top-k / heavy hitter detection | FrequentItems | `k: 64` | Most frequent items with count and error bounds |
| Membership testing (seen before?) | Bloom | `capacity: 100_000` | "Probably yes" or "definitely no" with tunable FPR |
| Membership with deletion | Cuckoo | `capacity: 100_000` | Bloom-like but supports delete |
| Membership with deletion + merge | Quotient | `q: 16, r: 8` | Mergeable filter with safe deletion |
| Multiset counting (how many times?) | CQF | `q: 16, r: 8` | Approximate per-item multiplicity |
| Static set membership | XorFilter | `fingerprint_bits: 8` | Smallest footprint, fastest lookup |
| Set reconciliation (what's different?) | IBLT | `cell_count: 1000` | Find symmetric difference between sets |
| Improved cardinality estimation | ULL | `p: 14` | ~20% better accuracy than HLL at the same memory |

### HLL: Real-time unique visitor counting

A web analytics service needs to count unique visitors per page per day
without storing every visitor ID. HLL provides bounded-memory cardinality
estimation that can be merged across time windows and server instances.

```elixir
defmodule Analytics.UniqueVisitors do
  alias ExDataSketch.HLL

  # Each page gets its own HLL sketch, stored in ETS or Redis
  def record_visit(page_id, visitor_id) do
    sketch = get_or_create_sketch(page_id)
    updated = HLL.update(sketch, visitor_id)
    store_sketch(page_id, updated)
  end

  def unique_count(page_id) do
    page_id
    |> get_or_create_sketch()
    |> HLL.estimate()
    |> round()
  end

  # Merge hourly sketches into a daily rollup
  def daily_rollup(page_id, date) do
    0..23
    |> Enum.map(&get_hourly_sketch(page_id, date, &1))
    |> Enum.reject(&is_nil/1)
    |> HLL.merge_many()
  end

  # Compare unique visitors across two pages
  def overlap_estimate(page_a, page_b) do
    sketch_a = get_or_create_sketch(page_a)
    sketch_b = get_or_create_sketch(page_b)
    merged = HLL.merge(sketch_a, sketch_b)

    count_a = HLL.estimate(sketch_a)
    count_b = HLL.estimate(sketch_b)
    count_union = HLL.estimate(merged)

    # Inclusion-exclusion: |A intersect B| = |A| + |B| - |A union B|
    overlap = max(count_a + count_b - count_union, 0)
    %{page_a: round(count_a), page_b: round(count_b), overlap: round(overlap)}
  end

  defp get_or_create_sketch(page_id) do
    case :ets.lookup(:hll_sketches, page_id) do
      [{_, bin}] ->
        {:ok, sketch} = HLL.deserialize(bin)
        sketch
      [] ->
        HLL.new(p: 14)
    end
  end

  defp store_sketch(page_id, sketch) do
    :ets.insert(:hll_sketches, {page_id, HLL.serialize(sketch)})
  end

  defp get_hourly_sketch(page_id, date, hour) do
    key = {page_id, date, hour}
    case :ets.lookup(:hll_hourly, key) do
      [{_, bin}] -> {:ok, s} = HLL.deserialize(bin); s
      [] -> nil
    end
  end
end
```

### CMS: API rate limiting with frequency tracking

An API gateway needs to track request counts per API key to enforce rate
limits. CMS provides constant-memory frequency estimation -- counts are
always at least the true count, so rate limits are never under-enforced.

```elixir
defmodule Gateway.RateLimiter do
  use GenServer

  alias ExDataSketch.CMS

  @rate_limit 1000  # max requests per window
  @window_ms 60_000  # 1-minute windows

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def check_and_record(api_key) do
    GenServer.call(__MODULE__, {:check, api_key})
  end

  def top_consumers do
    GenServer.call(__MODULE__, :top_consumers)
  end

  @impl true
  def init(_opts) do
    schedule_rotation()
    {:ok, %{sketch: CMS.new(width: 4096, depth: 5), keys_seen: MapSet.new()}}
  end

  @impl true
  def handle_call({:check, api_key}, _from, state) do
    current_count = CMS.estimate(state.sketch, api_key)

    if current_count >= @rate_limit do
      {:reply, {:error, :rate_limited, current_count}, state}
    else
      updated_sketch = CMS.update(state.sketch, api_key)
      updated_keys = MapSet.put(state.keys_seen, api_key)
      {:reply, {:ok, current_count + 1}, %{state | sketch: updated_sketch, keys_seen: updated_keys}}
    end
  end

  @impl true
  def handle_call(:top_consumers, _from, state) do
    # Check counts for all observed keys
    top =
      state.keys_seen
      |> Enum.map(fn key -> {key, CMS.estimate(state.sketch, key)} end)
      |> Enum.sort_by(fn {_k, count} -> -count end)
      |> Enum.take(10)

    {:reply, top, state}
  end

  @impl true
  def handle_info(:rotate, _state) do
    schedule_rotation()
    {:noreply, %{sketch: CMS.new(width: 4096, depth: 5), keys_seen: MapSet.new()}}
  end

  defp schedule_rotation, do: Process.send_after(self(), :rotate, @window_ms)
end
```

### Theta: A/B test audience overlap analysis

A product team needs to measure how much overlap exists between users
exposed to experiment A vs experiment B. Theta sketches support set
operations (union, intersection) on cardinalities, giving overlap estimates
without storing every user ID.

```elixir
defmodule Experiments.OverlapAnalysis do
  alias ExDataSketch.Theta

  def analyze_overlap(experiment_a_users, experiment_b_users) do
    sketch_a = Theta.from_enumerable(experiment_a_users, k: 4096)
    sketch_b = Theta.from_enumerable(experiment_b_users, k: 4096)

    union = Theta.merge(sketch_a, sketch_b)

    count_a = Theta.estimate(sketch_a)
    count_b = Theta.estimate(sketch_b)
    count_union = Theta.estimate(union)

    # Inclusion-exclusion principle
    count_intersection = max(count_a + count_b - count_union, 0.0)

    jaccard = if count_union > 0, do: count_intersection / count_union, else: 0.0

    %{
      group_a_size: round(count_a),
      group_b_size: round(count_b),
      union_size: round(count_union),
      intersection_size: round(count_intersection),
      jaccard_similarity: Float.round(jaccard, 4),
      overlap_pct_of_a: Float.round(count_intersection / max(count_a, 1) * 100, 1),
      overlap_pct_of_b: Float.round(count_intersection / max(count_b, 1) * 100, 1)
    }
  end

  # Compare overlap across multiple experiment cohorts
  def pairwise_overlap(cohorts) when is_map(cohorts) do
    sketches =
      Map.new(cohorts, fn {name, users} ->
        {name, Theta.from_enumerable(users, k: 4096)}
      end)

    names = Map.keys(sketches)

    for a <- names, b <- names, a < b do
      union = Theta.merge(sketches[a], sketches[b])
      est_a = Theta.estimate(sketches[a])
      est_b = Theta.estimate(sketches[b])
      est_union = Theta.estimate(union)
      intersection = max(est_a + est_b - est_union, 0.0)

      {a, b, round(intersection)}
    end
  end
end
```

### KLL: Monitoring response size distributions

A CDN needs to track the distribution of response body sizes to plan
cache shard capacity. KLL provides rank-accurate quantiles -- useful when
you care about "what fraction of responses are below X bytes?"

```elixir
defmodule CDN.ResponseSizeMonitor do
  use GenServer

  alias ExDataSketch.KLL

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_response(size_bytes) do
    GenServer.cast(__MODULE__, {:record, size_bytes})
  end

  def record_batch(sizes) do
    GenServer.cast(__MODULE__, {:record_batch, sizes})
  end

  def distribution_report do
    GenServer.call(__MODULE__, :report)
  end

  @impl true
  def init(_opts) do
    {:ok, %{sketch: KLL.new(k: 200)}}
  end

  @impl true
  def handle_cast({:record, size}, state) do
    {:noreply, %{state | sketch: KLL.update(state.sketch, size)}}
  end

  @impl true
  def handle_cast({:record_batch, sizes}, state) do
    {:noreply, %{state | sketch: KLL.update_many(state.sketch, sizes)}}
  end

  @impl true
  def handle_call(:report, _from, state) do
    sketch = state.sketch
    n = KLL.count(sketch)

    report =
      if n > 0 do
        %{
          count: n,
          min: KLL.min(sketch),
          p25: KLL.quantile(sketch, 0.25),
          median: KLL.quantile(sketch, 0.5),
          p75: KLL.quantile(sketch, 0.75),
          p90: KLL.quantile(sketch, 0.90),
          p99: KLL.quantile(sketch, 0.99),
          max: KLL.max(sketch),
          # What fraction of responses are under 1 MB?
          pct_under_1mb: Float.round(KLL.rank(sketch, 1_048_576) * 100, 1)
        }
      else
        %{count: 0}
      end

    {:reply, report, state}
  end
end

# Usage:
# CDN.ResponseSizeMonitor.record_batch(response_sizes)
# CDN.ResponseSizeMonitor.distribution_report()
# => %{count: 1_500_000, median: 24576, p99: 4_194_304, pct_under_1mb: 92.3, ...}
```

### DDSketch: SLO compliance monitoring

A platform team needs to track API latency percentiles for SLO reporting.
DDSketch provides value-relative error -- when the SLO says "P99 < 200ms",
DDSketch guarantees the reported P99 is within 1% of the true value, not
just within a rank tolerance.

```elixir
defmodule Platform.SLOMonitor do
  use GenServer

  alias ExDataSketch.DDSketch

  @slo_rules [
    {:p50, 0.5, 50},    # P50 < 50ms
    {:p95, 0.95, 150},  # P95 < 150ms
    {:p99, 0.99, 200}   # P99 < 200ms
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_latency(endpoint, latency_ms) do
    GenServer.cast(__MODULE__, {:record, endpoint, latency_ms})
  end

  def record_latencies(endpoint, latencies) do
    GenServer.cast(__MODULE__, {:record_batch, endpoint, latencies})
  end

  def slo_report(endpoint) do
    GenServer.call(__MODULE__, {:report, endpoint})
  end

  @impl true
  def init(_opts) do
    {:ok, %{sketches: %{}}}
  end

  @impl true
  def handle_cast({:record, endpoint, latency_ms}, state) do
    sketch = Map.get(state.sketches, endpoint, DDSketch.new(alpha: 0.01))
    updated = DDSketch.update(sketch, latency_ms)
    {:noreply, %{state | sketches: Map.put(state.sketches, endpoint, updated)}}
  end

  @impl true
  def handle_cast({:record_batch, endpoint, latencies}, state) do
    sketch = Map.get(state.sketches, endpoint, DDSketch.new(alpha: 0.01))
    updated = DDSketch.update_many(sketch, latencies)
    {:noreply, %{state | sketches: Map.put(state.sketches, endpoint, updated)}}
  end

  @impl true
  def handle_call({:report, endpoint}, _from, state) do
    case Map.fetch(state.sketches, endpoint) do
      {:ok, sketch} ->
        n = DDSketch.count(sketch)

        checks =
          Enum.map(@slo_rules, fn {label, quantile, threshold_ms} ->
            actual = DDSketch.quantile(sketch, quantile)
            %{
              metric: label,
              threshold_ms: threshold_ms,
              actual_ms: Float.round(actual, 2),
              passing: actual <= threshold_ms
            }
          end)

        all_passing = Enum.all?(checks, & &1.passing)

        report = %{
          endpoint: endpoint,
          sample_count: n,
          min_ms: Float.round(DDSketch.min(sketch), 2),
          max_ms: Float.round(DDSketch.max(sketch), 2),
          slo_checks: checks,
          compliant: all_passing
        }

        {:reply, {:ok, report}, state}

      :error ->
        {:reply, {:error, :no_data}, state}
    end
  end
end

# Usage:
# Platform.SLOMonitor.record_latencies("/api/search", latency_samples)
# Platform.SLOMonitor.slo_report("/api/search")
# => {:ok, %{compliant: true, slo_checks: [
#      %{metric: :p99, threshold_ms: 200, actual_ms: 142.37, passing: true}, ...]}}
```

### FrequentItems: Trending search queries

A search engine needs to identify the most popular queries in real time
to populate autocomplete suggestions and trending lists. FrequentItems
(SpaceSaving) tracks the top-k items with bounded memory, providing count
estimates and error bounds.

```elixir
defmodule Search.TrendingQueries do
  use GenServer

  alias ExDataSketch.FrequentItems

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def record_query(query) do
    GenServer.cast(__MODULE__, {:query, query})
  end

  def record_queries(queries) do
    GenServer.cast(__MODULE__, {:queries, queries})
  end

  def trending(limit \\ 10) do
    GenServer.call(__MODULE__, {:trending, limit})
  end

  def query_estimate(query) do
    GenServer.call(__MODULE__, {:estimate, query})
  end

  @impl true
  def init(_opts) do
    {:ok, %{sketch: FrequentItems.new(k: 128)}}
  end

  @impl true
  def handle_cast({:query, query}, state) do
    normalized = query |> String.downcase() |> String.trim()
    {:noreply, %{state | sketch: FrequentItems.update(state.sketch, normalized)}}
  end

  @impl true
  def handle_cast({:queries, queries}, state) do
    normalized = Enum.map(queries, &(&1 |> String.downcase() |> String.trim()))
    {:noreply, %{state | sketch: FrequentItems.update_many(state.sketch, normalized)}}
  end

  @impl true
  def handle_call({:trending, limit}, _from, state) do
    top =
      state.sketch
      |> FrequentItems.top_k(limit: limit)
      |> Enum.map(fn entry ->
        %{
          query: entry.item,
          estimated_count: entry.count,
          min_count: entry.lower,
          max_count: entry.upper
        }
      end)

    {:reply, top, state}
  end

  @impl true
  def handle_call({:estimate, query}, _from, state) do
    normalized = query |> String.downcase() |> String.trim()
    result = FrequentItems.estimate(state.sketch, normalized)
    {:reply, result, state}
  end
end

# Usage:
# Search.TrendingQueries.record_queries(batch_of_queries)
# Search.TrendingQueries.trending(5)
# => [%{query: "elixir genserver", estimated_count: 4821, min_count: 4750, max_count: 4821},
#     %{query: "phoenix liveview", estimated_count: 3102, min_count: 3010, max_count: 3102}, ...]
```

### ULL: Multi-node distributed cardinality with improved accuracy

An ad-tech platform counts unique ad impressions per campaign across a cluster.
Each node maintains a local ULL sketch, serializes it, and sends to a central
aggregator that merges and reports campaign reach. ULL is chosen over HLL for
its ~20% better accuracy at the same memory -- significant when reporting to
advertisers who pay per unique impression.

```elixir
defmodule AdTech.CampaignReach do
  @moduledoc """
  Distributed unique impression counting per campaign using ULL sketches.

  Each application node tracks impressions locally and periodically
  flushes serialized sketches to a central aggregator for merge.
  """

  alias ExDataSketch.ULL

  # --- Per-node: local impression tracking ---

  def new_tracker(precision \\ 14) do
    %{sketches: %{}, precision: precision}
  end

  def record_impression(tracker, campaign_id, user_id) do
    sketch =
      Map.get_lazy(tracker.sketches, campaign_id, fn ->
        ULL.new(p: tracker.precision)
      end)

    updated = ULL.update(sketch, user_id)
    put_in(tracker.sketches[campaign_id], updated)
  end

  def record_impressions(tracker, campaign_id, user_ids) do
    sketch =
      Map.get_lazy(tracker.sketches, campaign_id, fn ->
        ULL.new(p: tracker.precision)
      end)

    updated = ULL.update_many(sketch, user_ids)
    put_in(tracker.sketches[campaign_id], updated)
  end

  @doc "Serialize all campaign sketches for network transport."
  def flush(tracker) do
    payloads =
      Map.new(tracker.sketches, fn {campaign_id, sketch} ->
        {campaign_id, ULL.serialize(sketch)}
      end)

    {payloads, %{tracker | sketches: %{}}}
  end

  # --- Central aggregator: merge from all nodes ---

  def new_aggregator, do: %{sketches: %{}}

  def ingest(aggregator, payloads) do
    Enum.reduce(payloads, aggregator, fn {campaign_id, binary}, acc ->
      {:ok, remote} = ULL.deserialize(binary)

      merged =
        case Map.fetch(acc.sketches, campaign_id) do
          {:ok, existing} -> ULL.merge(existing, remote)
          :error -> remote
        end

      put_in(acc.sketches[campaign_id], merged)
    end)
  end

  @doc "Report unique reach per campaign."
  def report(aggregator) do
    Map.new(aggregator.sketches, fn {campaign_id, sketch} ->
      {campaign_id, %{
        unique_impressions: ULL.estimate(sketch),
        sketch_bytes: ULL.size_bytes(sketch)
      }}
    end)
  end

  @doc "Merge hourly aggregators into a daily rollup."
  def rollup(hourly_aggregators) do
    Enum.reduce(hourly_aggregators, new_aggregator(), fn hourly, daily ->
      Enum.reduce(hourly.sketches, daily, fn {campaign_id, sketch}, acc ->
        merged =
          case Map.fetch(acc.sketches, campaign_id) do
            {:ok, existing} -> ULL.merge(existing, sketch)
            :error -> sketch
          end

        put_in(acc.sketches[campaign_id], merged)
      end)
    end)
  end
end

# Usage:
# Node A records impressions
# tracker = AdTech.CampaignReach.new_tracker(14)
# tracker = AdTech.CampaignReach.record_impressions(tracker, "camp_42", user_ids)
# {payloads, tracker} = AdTech.CampaignReach.flush(tracker)
# send(aggregator_node, {:impressions, payloads})
#
# Aggregator merges from all nodes
# agg = AdTech.CampaignReach.new_aggregator()
# agg = AdTech.CampaignReach.ingest(agg, payloads_from_node_a)
# agg = AdTech.CampaignReach.ingest(agg, payloads_from_node_b)
# AdTech.CampaignReach.report(agg)
# => %{"camp_42" => %{unique_impressions: 1_247_823, sketch_bytes: 16392}}
#
# ULL at p=14 gives ~0.65% relative error vs HLL's ~0.81%
# -- a meaningful improvement when billing per unique impression.
```

### Bloom: Deduplication in event processing

An event pipeline receives millions of events per hour and needs to filter
duplicates without storing every event ID. A Bloom filter provides
space-efficient "have I seen this before?" checks -- false positives mean
an occasional duplicate slips through, but no event is ever wrongly dropped.

```elixir
defmodule Events.Deduplicator do
  use GenServer

  alias ExDataSketch.Bloom

  @capacity 10_000_000  # expected events per window
  @fpr 0.001            # 0.1% false positive rate

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Returns :new if not seen before, :duplicate if probably seen
  def check_and_insert(event_id) do
    GenServer.call(__MODULE__, {:check, event_id})
  end

  # Batch check for Broadway/Flow pipelines
  def filter_duplicates(event_ids) do
    GenServer.call(__MODULE__, {:filter_batch, event_ids})
  end

  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @impl true
  def init(_opts) do
    {:ok, %{
      bloom: Bloom.new(capacity: @capacity, false_positive_rate: @fpr),
      checked: 0,
      duplicates: 0
    }}
  end

  @impl true
  def handle_call({:check, event_id}, _from, state) do
    if Bloom.member?(state.bloom, event_id) do
      {:reply, :duplicate, %{state | checked: state.checked + 1, duplicates: state.duplicates + 1}}
    else
      updated_bloom = Bloom.put(state.bloom, event_id)
      {:reply, :new, %{state | bloom: updated_bloom, checked: state.checked + 1}}
    end
  end

  @impl true
  def handle_call({:filter_batch, event_ids}, _from, state) do
    {new_ids, bloom, dup_count} =
      Enum.reduce(event_ids, {[], state.bloom, 0}, fn id, {acc, bloom, dups} ->
        if Bloom.member?(bloom, id) do
          {acc, bloom, dups + 1}
        else
          {[id | acc], Bloom.put(bloom, id), dups}
        end
      end)

    new_state = %{state |
      bloom: bloom,
      checked: state.checked + length(event_ids),
      duplicates: state.duplicates + dup_count
    }

    {:reply, Enum.reverse(new_ids), new_state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    reply = %{
      checked: state.checked,
      duplicates: state.duplicates,
      approximate_unique: Bloom.count(state.bloom),
      filter_size_bytes: Bloom.size_bytes(state.bloom),
      configured_fpr: Bloom.error_rate(state.bloom)
    }

    {:reply, reply, state}
  end
end

# Usage in a Broadway pipeline:
defmodule MyPipeline do
  use Broadway

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    ids = Enum.map(messages, & &1.data.event_id)
    new_ids = Events.Deduplicator.filter_duplicates(ids)
    new_id_set = MapSet.new(new_ids)

    messages
    |> Enum.filter(fn msg -> MapSet.member?(new_id_set, msg.data.event_id) end)
    |> Enum.each(&process_event/1)

    messages
  end
end
```

### Cuckoo: Session revocation cache

A session management system needs to maintain a revoked-session cache that
supports both addition and removal of session IDs. Cuckoo filters support
deletion, making them suitable for caches where items expire or are reinstated.

```elixir
defmodule Auth.RevocationCache do
  alias ExDataSketch.Cuckoo

  def new_cache(capacity \\ 100_000) do
    Cuckoo.new(capacity: capacity, fingerprint_size: 16)
  end

  def revoke(cache, session_id) do
    case Cuckoo.put(cache, session_id) do
      {:ok, updated} -> {:ok, updated}
      {:error, :full} -> {:error, :cache_full}
    end
  end

  def reinstate(cache, session_id) do
    case Cuckoo.delete(cache, session_id) do
      {:ok, updated} -> {:ok, updated}
      {:error, :not_found} -> {:ok, cache}
    end
  end

  def revoked?(cache, session_id) do
    Cuckoo.member?(cache, session_id)
  end
end
```

### IBLT: Replica set reconciliation

Two database replicas need to synchronize their key sets without exchanging
full inventories. Each node builds an IBLT from its keys, they exchange the
compact IBLTs, and subtract to find only the differing keys.

```elixir
defmodule Sync.Reconciler do
  alias ExDataSketch.IBLT

  # Each node builds an IBLT from its local keys
  def build_digest(keys, cell_count \\ 3000) do
    iblt = IBLT.new(cell_count: cell_count, hash_count: 3)
    IBLT.put_many(iblt, keys)
  end

  # Find keys that differ between two nodes
  def find_differences(local_digest, remote_digest) do
    diff = IBLT.subtract(local_digest, remote_digest)

    case IBLT.list_entries(diff) do
      {:ok, %{positive: only_local, negative: only_remote}} ->
        {:ok, %{push: only_local, pull: only_remote}}

      {:error, :decode_failed} ->
        {:error, :too_many_differences}
    end
  end
end
```

### FilterChain: Hot/cold lifecycle tiers

A membership service uses a hot Cuckoo filter for active writes and periodic
compaction into a cold XorFilter for archived snapshots.

```elixir
defmodule Membership.TieredFilter do
  alias ExDataSketch.{FilterChain, Cuckoo, XorFilter}

  def new_hot(capacity \\ 50_000) do
    FilterChain.new()
    |> FilterChain.add_stage(Cuckoo.new(capacity: capacity))
  end

  def ingest(chain, items) do
    Enum.reduce(items, chain, fn item, acc ->
      {:ok, updated} = FilterChain.put(acc, item)
      updated
    end)
  end

  def compact(chain, archived_items) do
    {:ok, xor} = XorFilter.build(archived_items, fingerprint_bits: 16)
    FilterChain.add_stage(chain, xor)
  end

  def seen?(chain, item) do
    FilterChain.member?(chain, item)
  end
end
```

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

## Benchmarking

Run all benchmarks with `mix bench`, or individual benchmark files with
`mix run bench/<name>.exs`.

### Interpreting Benchee Memory Numbers

Benchee's "Memory usage" metric measures **total heap allocation** during the
benchmarked function call. This is the sum of every byte allocated on the
process heap, including transient garbage that is immediately collectible
(intermediate values, short-lived binary copies, list cons cells, etc.). It is
**not** peak memory, resident memory, or the size of the final data structure.

This distinction matters when benchmarking probabilistic sketches against exact
data structures like MapSet:

- **Processing allocation is O(n) for both approaches.** Every item must be
  processed, so both MapSet and HLL/ULL allocate proportionally to the number
  of items inserted. The pure-Elixir hash mixer uses 16-bit partial products
  that stay within the BEAM's fixnum limit, avoiding the transient bigint
  allocations that earlier versions incurred. When the Rust NIF is available,
  hashing moves entirely into native code with zero Elixir heap allocation
  per item.

- **Result size is where sketches win.** An HLL sketch at p=12 occupies a
  fixed 4 KB regardless of whether 1,000 or 100,000,000 items were inserted.
  A MapSet must store every unique element and grows linearly with cardinality.

To measure the actual memory advantage, compare the size of the finished data
structures rather than Benchee's total allocation metric:

```elixir
mapset = Enum.into(data, MapSet.new())
hll = HLL.new(p: 12) |> HLL.update_many(items)

:erlang.external_size(mapset)   # grows with cardinality
:erlang.external_size(hll.state) # fixed at ~4 KB for p=12
```

See `bench/hhl_v_naive.exs` for a complete example that includes both a
Benchee throughput/allocation comparison and a result-size comparison showing
the fixed sketch footprint.

### Backend Considerations

The **Rust backend** (`ExDataSketch.Backend.Rust`) moves batch sketch operations
(update_many, merge, estimate) into a NIF for maximum throughput. When the NIF
is loaded, `hash64/2` also automatically uses XXHash3 for hashing, so the full
pipeline runs in native code. When benchmarking, specify the backend explicitly
so results are reproducible:

```elixir
HLL.new(p: 12, backend: ExDataSketch.Backend.Rust)
```

The **Pure backend** processes items one at a time in `update_many`, avoiding
intermediate list allocation but performing all arithmetic on the Elixir heap.
The fixnum-safe hash mixer eliminates bigint overhead that earlier versions
incurred, keeping per-item allocation to a single unavoidable 64-bit return
value. The pure backend is useful for environments where the Rust NIF is not
available.
