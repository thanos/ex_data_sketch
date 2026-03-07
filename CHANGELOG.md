# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-03-07

### Added

- Cuckoo filter (`ExDataSketch.Cuckoo`) with Pure Elixir backend. CKO1 binary state format. Partial-key cuckoo hashing with configurable fingerprint size, bucket size, and max kicks. Supports insertion, safe deletion, and membership testing. EXSK serialization (sketch ID 8).
- Quotient filter (`ExDataSketch.Quotient`) with Pure Elixir backend. QOT1 binary state format. Quotient/remainder fingerprint splitting with linear probing and metadata bits (is_occupied, is_continuation, is_shifted). Supports insertion, safe deletion, merge, and membership testing. EXSK serialization (sketch ID 9).
- Counting Quotient Filter (`ExDataSketch.CQF`) with Pure Elixir backend. CQF1 binary state format. Extends quotient filter with variable-length counter encoding for multiset membership and approximate counting via `estimate_count/2`. Supports insertion, deletion, merge. EXSK serialization (sketch ID 10).
- XorFilter (`ExDataSketch.XorFilter`) with Pure Elixir backend. XOR1 binary state format. Static build-once immutable filter constructed via `build/2` with 8-bit or 16-bit fingerprints. Supports membership testing only. EXSK serialization (sketch ID 11).
- IBLT (`ExDataSketch.IBLT`) with Pure Elixir backend. IBL1 binary state format. Invertible Bloom Lookup Table for set reconciliation via `subtract/2` and `list_entries/1`. Supports set mode and key-value mode, insertion, deletion, merge. EXSK serialization (sketch ID 12).
- FilterChain (`ExDataSketch.FilterChain`) for capability-aware membership filter composition. FCN1 binary state format. Lifecycle-tier patterns (hot/warm/cold) with stage position enforcement. Supports `add_stage/2`, `put/2`, `member?/2`, `delete/2`. Serializes all stages in order.
- Benchmark suites for Cuckoo, Quotient, CQF, XorFilter, IBLT, and FilterChain (`bench/*.exs`).
- `UnsupportedOperationError` for operations not supported by a structure (used by FilterChain).
- `InvalidChainCompositionError` for invalid FilterChain stage composition.
- `capabilities/0` function on Bloom, Cuckoo, Quotient, CQF, XorFilter, IBLT, and FilterChain modules.
- Cuckoo, Quotient, CQF, XorFilter, IBLT, and FilterChain backend callbacks on `ExDataSketch.Backend`.

## [0.4.0] - 2026-03-06

### Added

- Bloom filter (`ExDataSketch.Bloom`) with Pure Elixir backend.
- BLM1 binary state format (40-byte header + LSB-first packed bitset).
- Double hashing (Kirsch-Mitzenmacher) deriving k bit positions from a single 64-bit hash.
- Bloom backend callbacks: `bloom_new/1`, `bloom_put/3`, `bloom_put_many/3`, `bloom_member?/3`, `bloom_merge/3`, `bloom_count/2`.
- Automatic parameter derivation from capacity and false_positive_rate options.
- Bloom merge via bitwise OR with validation of matching bit_count, hash_count, and seed.
- Bloom popcount-based cardinality estimation.
- Bloom serialization via EXSK envelope (sketch ID 7).
- Bloom property tests (no false negatives, merge commutativity/associativity/identity, serialization round-trip).
- Bloom statistical validation tests (observed FPR within 2x of target).
- Bloom merge law properties in merge_laws_test.exs.
- Bloom parity test stubs for future Rust NIF backend.
- Bloom benchmark suite (`bench/bloom_bench.exs`).
- Bloom options section in usage guide.

## [0.3.0] - 2026-03-06

### Added

- FrequentItems sketch (`ExDataSketch.FrequentItems`) using the SpaceSaving algorithm with Pure Elixir and Rust NIF backends.
- FI1 binary state format (32-byte header + variable-length sorted entries).
- FrequentItems backend callbacks: `fi_new/1`, `fi_update/3`, `fi_update_many/3`, `fi_merge/3`, `fi_estimate/3`, `fi_top_k/3`, `fi_count/2`, `fi_entry_count/2`.
- Batch optimization via pre-aggregation (`Enum.frequencies/1`) with weighted updates.
- Deterministic tie-breaking on eviction (lexicographically smallest item_bytes).
- Key encoding policies: `:binary`, `:int` (signed 64-bit LE), `{:term, :external}`.
- Commutative merge via additive count combination and canonical replay.
- Rust NIF acceleration for `fi_update_many` and `fi_merge` with dirty scheduler support.
- FrequentItems support in `ExDataSketch.update_many/2` facade.
- FrequentItems merge law properties (commutativity, identity, count conservation).
- FrequentItems golden vector test fixtures.
- FrequentItems parity tests ensuring byte-identical output between Pure and Rust backends.
- FrequentItems benchmark suite (`bench/frequent_items_bench.exs`).
- EXSK codec sketch ID 6 for FrequentItems.
- FrequentItems usage documentation in usage guide with SpaceSaving algorithm overview.
- Mox test dependency for backend contract testing.
- Theta `compact/1` function for explicit compaction.

## [0.2.1] - 2026-03-05

### Added

- DDSketch quantiles sketch (`ExDataSketch.DDSketch`) with Pure Elixir and Rust NIF backends.
- DDSketch backend callbacks: `ddsketch_new/1`, `ddsketch_update/3`, `ddsketch_update_many/3`, `ddsketch_merge/3`, `ddsketch_quantile/3`, `ddsketch_count/2`, `ddsketch_min/2`, `ddsketch_max/2`.
- Rust NIF acceleration for `ddsketch_update_many` and `ddsketch_merge` with dirty scheduler support.
- DDSketch support in `ExDataSketch.Quantiles` facade (`type: :ddsketch`).
- DDSketch merge law properties (commutativity, identity, count additivity, min/max preservation).
- DDSketch golden vector test fixtures (empty, single, small_set, merge, zeros).
- DDSketch parity tests ensuring byte-identical output between Pure and Rust backends.
- DDSketch benchmark suite (`bench/ddsketch_bench.exs`).
- EXSK codec sketch ID 5 for DDSketch.
- DDSketch usage documentation in usage guide with KLL vs DDSketch comparison table.

## [0.2.0] - 2026-03-04

### Added

- KLL quantiles sketch (`ExDataSketch.KLL`) with Pure Elixir and Rust NIF backends.
- `ExDataSketch.Quantiles` facade module for type-dispatched quantile sketch access.
- KLL backend callbacks: `kll_new/1`, `kll_update/3`, `kll_update_many/3`, `kll_merge/3`, `kll_quantile/3`, `kll_rank/3`, `kll_count/2`, `kll_min/2`, `kll_max/2`.
- Rust NIF acceleration for `kll_update_many` and `kll_merge` with dirty scheduler support.
- KLL merge law properties (associativity, commutativity, identity, count additivity, min/max preservation).
- KLL golden vector test fixtures (empty, single, small_set, merge).
- KLL parity tests ensuring byte-identical output between Pure and Rust backends.
- KLL benchmark suite (`bench/kll_bench.exs`).
- EXSK codec sketch ID 4 for KLL.

## [0.1.1] - 2026-03-04

### Added

- Deterministic golden vector test fixtures for HLL, CMS, and Theta (JSON format with versioned schema).
- Pure vs Rust parity test suite ensuring byte-identical serialization and estimates.
- Merge-law property tests (associativity, commutativity, identity, chunking equivalence) for all sketch types.
- Compatibility and Stability section in README documenting serialization and parity guarantees.
- CI regression tracking for coverage and benchmark baselines.

### Changed

- Stabilized benchmark scripts with deterministic datasets and JSON output.
- Clarified HLL and CMS DataSketches interop stubs as intentionally unimplemented (not "future").
- Removed stale "Phase 2" language from module documentation.

## [0.1.0] - 2026-03-02

### Added

- Precompiled Rust NIF binaries for macOS (ARM64, x86_64) and Linux (x86_64 gnu/musl, aarch64 gnu/musl).
- Optional Rust NIF acceleration backend (`ExDataSketch.Backend.Rust`).
- Rust NIFs for HLL (update_many, merge, estimate), CMS (update_many, merge), and Theta (update_many, merge).
- Normal and dirty CPU scheduler NIF variants with configurable thresholds.
- Automatic fallback to Pure Elixir backend when Rust NIF is unavailable.
- Cross-backend parity tests ensuring byte-identical output between Pure and Rust.
- Side-by-side Pure vs Rust benchmark scenarios.
- CI jobs for Rust NIF compilation and testing (`test-rust`, `bench-rust`).
- Pure Elixir Theta sketch implementation (new, update, update_many, compact, merge, estimate).
- Apache DataSketches CompactSketch codec (serialize/deserialize) for Theta interop.
- MurmurHash3 seed hash computation for DataSketches compatibility.
- Deterministic test vectors for Theta sketch.
- Cross-language vector harness specification for Java interop testing.
- Theta Benchee benchmarks.
- Pure Elixir HLL implementation (new, update, update_many, merge, estimate).
- Pure Elixir CMS implementation (new, update, update_many, merge, estimate).
- Deterministic test vectors for HLL and CMS.
- Real Benchee benchmarks for HLL and CMS.
- Project skeleton with directory structure and dependencies.
- Public API stubs for HLL, CMS, and Theta sketch modules.
- ExDataSketch-native binary codec (EXSK format).
- Hash module with stable 64-bit hash interface.
- Backend behaviour with Pure Elixir stub implementation.
- Quick Start and Usage Guide documentation.
- GitHub Actions CI workflow.
- Integration convenience functions (`from_enumerable/2`, `merge_many/1`, `reducer/1`, `merger/1`) on all sketch modules.
- Integration guide with ecosystem examples (Flow, Broadway, Explorer, Nx, ex_arrow/ExZarr).
- Documented merge properties (associativity, commutativity) for HLL, CMS, and Theta.
