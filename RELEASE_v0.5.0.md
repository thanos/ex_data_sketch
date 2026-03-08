# ExDataSketch v0.5.0 Release Notes

**Release date**: 2026-03-10

## Summary

v0.5.0 adds **six new structures** for advanced membership testing and set
reconciliation, plus **FilterChain** for composing filters into lifecycle-tier
pipelines. This is the largest feature release yet, bringing ExDataSketch to
13 sketch types across seven categories.

ExDataSketch now covers:

| Category | Algorithms |
|----------|-----------|
| Cardinality | HyperLogLog (HLL) |
| Frequency | Count-Min Sketch (CMS) |
| Set operations | Theta Sketch |
| Quantiles | KLL, DDSketch |
| Frequency ranking | FrequentItems (SpaceSaving) |
| Membership | Bloom, Cuckoo, Quotient, CQF, XorFilter |
| Set reconciliation | IBLT |
| Composition | FilterChain |

## What's new in v0.5.0

### Cuckoo Filter (`ExDataSketch.Cuckoo`)

Membership testing with deletion support using partial-key cuckoo hashing.
Unlike Bloom filters, Cuckoo filters support safe deletion of previously
inserted items. Better space efficiency than Bloom at low false positive rates.

**Key features:**

- Partial-key cuckoo hashing with configurable fingerprint size (8, 12, 16 bits)
- Configurable bucket size (2 or 4 slots) and max kicks (100..2000)
- Safe deletion without false negatives on subsequent queries
- CKO1 binary state format
- EXSK serialization (sketch ID 8)

**Quick start:**

```elixir
cuckoo = ExDataSketch.Cuckoo.new(capacity: 100_000, fingerprint_size: 8)

{:ok, cuckoo} = ExDataSketch.Cuckoo.put(cuckoo, "user_42")
ExDataSketch.Cuckoo.member?(cuckoo, "user_42")  # true

{:ok, cuckoo} = ExDataSketch.Cuckoo.delete(cuckoo, "user_42")
ExDataSketch.Cuckoo.member?(cuckoo, "user_42")  # false
```

### Quotient Filter (`ExDataSketch.Quotient`)

Membership testing with safe deletion and merge. Splits fingerprints into
quotient (slot index) and remainder (stored value) with metadata bits for
cluster tracking. Supports merge without re-hashing.

**Key features:**

- Quotient/remainder fingerprint splitting with linear probing
- Metadata bits (is_occupied, is_continuation, is_shifted) for cluster tracking
- Safe deletion and merge support
- QOT1 binary state format
- EXSK serialization (sketch ID 9)

**Quick start:**

```elixir
qf = ExDataSketch.Quotient.new(q: 16, r: 8)
qf = ExDataSketch.Quotient.put(qf, "item_a")
ExDataSketch.Quotient.member?(qf, "item_a")  # true

# Merge two quotient filters
merged = ExDataSketch.Quotient.merge(qf_a, qf_b)
```

### Counting Quotient Filter (`ExDataSketch.CQF`)

Extends the quotient filter with variable-length counter encoding to answer
not just "is this present?" but "how many times was this inserted?" Counts are
approximate (never underestimated).

**Key features:**

- Variable-length counter encoding within runs
- `estimate_count/2` for approximate multiplicity queries
- Safe deletion (decrements count)
- Merge sums counts across filters
- CQF1 binary state format
- EXSK serialization (sketch ID 10)

**Quick start:**

```elixir
cqf = ExDataSketch.CQF.new(q: 16, r: 8)
cqf = ExDataSketch.CQF.put(cqf, "event_x")
cqf = ExDataSketch.CQF.put(cqf, "event_x")
cqf = ExDataSketch.CQF.put(cqf, "event_x")

ExDataSketch.CQF.estimate_count(cqf, "event_x")  # >= 3
ExDataSketch.CQF.member?(cqf, "event_x")          # true
```

### XorFilter (`ExDataSketch.XorFilter`)

Static, immutable membership filter with the smallest footprint and fastest
lookups. All items must be provided at construction time via `build/2`.
No insertion, deletion, or merge -- query only.

**Key features:**

- Build-once immutable construction via `build/2`
- 8-bit or 16-bit fingerprints (configurable false positive rate)
- Smallest memory footprint of all membership filters
- XOR1 binary state format
- EXSK serialization (sketch ID 11)

**Quick start:**

```elixir
items = MapSet.new(1..100_000)
{:ok, xor} = ExDataSketch.XorFilter.build(items, fingerprint_bits: 8)

ExDataSketch.XorFilter.member?(xor, 42)      # true
ExDataSketch.XorFilter.member?(xor, 999_999) # false (probably)
```

### IBLT (`ExDataSketch.IBLT`)

Invertible Bloom Lookup Table for set reconciliation. Two parties each build
an IBLT from their sets, exchange them, and subtract to discover only the
differing items -- without transmitting the full sets.

**Key features:**

- Set mode (items only) and key-value mode
- `subtract/2` for cell-wise difference
- `list_entries/1` for peeling decoded entries (positive/negative sets)
- Merge via cell-wise addition
- IBL1 binary state format
- EXSK serialization (sketch ID 12)

**Quick start:**

```elixir
# Set reconciliation between two nodes
iblt_a = ExDataSketch.IBLT.new(cell_count: 1000) |> ExDataSketch.IBLT.put_many(set_a)
iblt_b = ExDataSketch.IBLT.new(cell_count: 1000) |> ExDataSketch.IBLT.put_many(set_b)

diff = ExDataSketch.IBLT.subtract(iblt_a, iblt_b)
{:ok, %{positive: only_in_a, negative: only_in_b}} = ExDataSketch.IBLT.list_entries(diff)
```

### FilterChain (`ExDataSketch.FilterChain`)

Capability-aware composition framework for chaining membership filters into
lifecycle-tier pipelines. Enforces valid chain positions based on each
filter's capabilities.

**Key features:**

- `add_stage/2` with position validation (front/middle/terminal/adjunct)
- `put/2` fans out to all stages supporting insertion (skips static stages)
- `member?/2` queries stages in order, short-circuits on false
- Lifecycle-tier patterns: hot Cuckoo (writes) -> cold XorFilter (snapshots)
- IBLT stages placed as adjuncts (not in query path)
- FCN1 binary state format

**Quick start:**

```elixir
chain =
  ExDataSketch.FilterChain.new()
  |> ExDataSketch.FilterChain.add_stage(ExDataSketch.Cuckoo.new(capacity: 10_000))
  |> ExDataSketch.FilterChain.add_stage(ExDataSketch.Bloom.new(capacity: 100_000))

{:ok, chain} = ExDataSketch.FilterChain.put(chain, "item_1")
ExDataSketch.FilterChain.member?(chain, "item_1")  # true
```

### Benchmarks

Benchmark suites added for all new structures:

- `bench/cuckoo_bench.exs`
- `bench/quotient_bench.exs`
- `bench/cqf_bench.exs`
- `bench/xor_filter_bench.exs`
- `bench/iblt_bench.exs`
- `bench/filter_chain_bench.exs`

Run all benchmarks with `mix bench`.

## Algorithm Matrix

| Algorithm | Purpose | EXSK ID | Backend |
|-----------|---------|---------|---------|
| HLL | Cardinality estimation | 1 | Pure + Rust |
| CMS | Frequency estimation | 2 | Pure + Rust |
| Theta | Set operations | 3 | Pure + Rust |
| KLL | Rank/quantile estimation | 4 | Pure + Rust |
| DDSketch | Value-relative quantiles | 5 | Pure + Rust |
| FrequentItems | Heavy-hitter detection | 6 | Pure + Rust |
| Bloom | Membership testing | 7 | Pure |
| Cuckoo | Membership with deletion | 8 | Pure |
| Quotient | Membership with deletion/merge | 9 | Pure |
| CQF | Multiset membership/counting | 10 | Pure |
| XorFilter | Static membership testing | 11 | Pure |
| IBLT | Set reconciliation | 12 | Pure |
| FilterChain | Filter composition | -- | Pure |

## Installation

```elixir
def deps do
  [
    {:ex_data_sketch, "~> 0.5.0"}
  ]
end
```

Precompiled Rust NIF binaries are downloaded automatically on supported
platforms (macOS ARM64/x86_64, Linux x86_64/aarch64 glibc/musl). No Rust
toolchain required. The library works in pure Elixir mode on all other
platforms.

## Upgrade Notes

- No breaking changes from v0.4.0.
- EXSK binaries produced by earlier v0.x releases remain fully compatible.
- The `ExDataSketch.Backend` behaviour now includes additional callbacks for
  Cuckoo, Quotient, CQF, XorFilter, IBLT, and FilterChain. Custom backend
  implementations must add these callbacks.
- New error types: `UnsupportedOperationError` and `InvalidChainCompositionError`.
- All membership filter modules now export `capabilities/0` returning a MapSet
  of supported operations.

## What's Next

v0.6.0 scope is under discussion. Potential directions include Rust NIF
acceleration for v0.5.0 structures, additional composition patterns, or
new sketch types.

## Links

- [HexDocs](https://hexdocs.pm/ex_data_sketch)
- [GitHub](https://github.com/thanos/ex_data_sketch)
- [Changelog](https://github.com/thanos/ex_data_sketch/blob/main/CHANGELOG.md)
