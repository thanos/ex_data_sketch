# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
