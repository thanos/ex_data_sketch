# ExDataSketch v0.4.0 Release Notes

**Release date**: 2026-03-08

## Summary

v0.4.0 adds **Bloom filters** for probabilistic membership testing --
the seventh algorithm in ExDataSketch. This release also includes v0.3.0
changes (FrequentItems / SpaceSaving for heavy-hitter detection) which
were developed but not previously published.

ExDataSketch now covers five categories of streaming data sketches:

| Category | Algorithms |
|----------|-----------|
| Cardinality | HyperLogLog (HLL) |
| Frequency | Count-Min Sketch (CMS) |
| Set operations | Theta Sketch |
| Quantiles | KLL, DDSketch |
| Frequency ranking | FrequentItems (SpaceSaving) |
| Membership | Bloom Filter |

## What's new in v0.4.0

### Bloom Filter (`ExDataSketch.Bloom`)

A space-efficient probabilistic data structure for membership testing.
`member?/2` returns `false` if an item was definitely not inserted, or
`true` if it was probably inserted. False positives are possible; false
negatives are not.

**Key features:**

- Automatic parameter derivation from `capacity` and `false_positive_rate`
- Double hashing (Kirsch-Mitzenmacher) for efficient multi-probe from a
  single 64-bit hash
- Merge via bitwise OR (commutative and associative)
- BLM1 binary state format (40-byte header + LSB-first packed bitset)
- EXSK serialization (sketch ID 7)
- Capacity overflow validation (rejects configurations that would exceed
  u32 limits in the binary format)

**Quick start:**

```elixir
# Create a filter expecting 100k items at 1% false positive rate
bloom = ExDataSketch.Bloom.new(capacity: 100_000, false_positive_rate: 0.01)

# Insert items
bloom = ExDataSketch.Bloom.put_many(bloom, known_user_ids)

# Test membership
ExDataSketch.Bloom.member?(bloom, "user_42")    # true (if inserted)
ExDataSketch.Bloom.member?(bloom, "unknown_99") # false (definitely not inserted)

# Merge filters from different partitions
merged = ExDataSketch.Bloom.merge(bloom_a, bloom_b)
```

**Benchmarks:**

| Operation | Dataset | Approximate time |
|-----------|---------|-----------------|
| `put_many` | 1,000 items | ~46 ms |
| `put_many` | 100,000 items | ~4.6 s |
| `merge` | two 1,000-item filters | ~0.5 ms |
| `member?` | 1,000 lookups | ~0.8 ms |

### Testing

- 40 unit tests, 5 property tests, 2 statistical validation tests
- Property tests: no false negatives, merge commutativity/associativity/identity,
  serialization round-trip
- Statistical: observed FPR within 2x of target after inserting N = capacity items
- Merge law properties added to shared merge_laws_test.exs
- Parity test stubs prepared for future Rust NIF backend

## What's new in v0.3.0

### FrequentItems (`ExDataSketch.FrequentItems`)

Heavy-hitter / top-k detection using the SpaceSaving algorithm. Tracks the
most frequent items in a data stream with bounded memory (at most k counters),
providing estimated counts with error bounds.

**Key features:**

- SpaceSaving algorithm with deterministic tie-breaking (lexicographic order)
- Batch optimization via pre-aggregation with weighted updates
- Three key encoding policies: `:binary`, `:int`, `{:term, :external}`
- Commutative merge via additive count combination
- FI1 binary state format (32-byte header + sorted variable-length entries)
- EXSK serialization (sketch ID 6)
- Rust NIF acceleration for `fi_update_many` and `fi_merge`

**Quick start:**

```elixir
sketch = ExDataSketch.FrequentItems.new(k: 64)
sketch = ExDataSketch.FrequentItems.update_many(sketch, search_queries)

# Top 10 most frequent items
ExDataSketch.FrequentItems.top_k(sketch, limit: 10)
# => [%{item: "elixir genserver", count: 4821, error: 71, lower: 4750, upper: 4821}, ...]

# Estimate for a specific item
ExDataSketch.FrequentItems.estimate(sketch, "phoenix liveview")
# => {:ok, %{estimate: 3102, error: 92, lower: 3010, upper: 3102}}
```

## Algorithm matrix

| Algorithm | Purpose | EXSK ID | Backend |
|-----------|---------|---------|---------|
| HLL | Cardinality estimation | 1 | Pure + Rust |
| CMS | Frequency estimation | 2 | Pure + Rust |
| Theta | Set operations | 3 | Pure + Rust |
| KLL | Rank/quantile estimation | 4 | Pure + Rust |
| DDSketch | Value-relative quantiles | 5 | Pure + Rust |
| FrequentItems | Heavy-hitter detection | 6 | Pure + Rust |
| Bloom | Membership testing | 7 | Pure |

## Installation

```elixir
def deps do
  [
    {:ex_data_sketch, "~> 0.4.0"}
  ]
end
```

Precompiled Rust NIF binaries are downloaded automatically on supported
platforms (macOS ARM64/x86_64, Linux x86_64/aarch64 glibc/musl). No Rust
toolchain required. The library works in pure Elixir mode on all other
platforms.

## Upgrade notes

- No breaking changes from v0.2.1.
- EXSK binaries produced by earlier v0.x releases remain fully compatible.
- The `ExDataSketch.Backend` behaviour now includes 14 additional callbacks
  (8 for FrequentItems, 6 for Bloom). Custom backend implementations must
  add these callbacks.

## What's next

v0.5.0 will add Cuckoo filters -- membership testing with support for
deletion, which Bloom filters do not provide.

## Links

- [HexDocs](https://hexdocs.pm/ex_data_sketch)
- [GitHub](https://github.com/thanos/ex_data_sketch)
- [Changelog](https://github.com/thanos/ex_data_sketch/blob/main/CHANGELOG.md)
