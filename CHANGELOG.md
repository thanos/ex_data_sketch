# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] - 2026-05-19

Release theme: **Streaming Integrations.** Transforms ex_data_sketch from a collection of probabilistic algorithms into a BEAM-native streaming approximate analytics infrastructure layer. Stream/Collectable integration, Broadway/GenStage/Flow pipelines, five persistence backends, production-grade telemetry + OpenTelemetry, ULL accuracy fixes, and comprehensive educational materials.

### Added

- **Stream and Collectable integration (Phase 1).**
  - `ExDataSketch.Stream` -- terminal stream consumers (`hll/2`, `cms/2`, `theta/2`, `ull/2`, `kll/2`, `ddsketch/2`, `req/2`, `bloom/2`, `quotient/2`, `cqf/2`, `iblt/2`, `frequent_items/2`, `misra_gries//2`).
  - `ExDataSketch.Stream.reduce_into/3` -- reduce an enumerable into a module or existing sketch.
  - `ExDataSketch.Stream.reduce_partitioned/3` -- partitioned parallel reduction with merge.
  - `Collectable` protocol for all mergeable sketches -- `Enum.into/2` and `for` comprehensions.
  - `from_enumerable/2` on all 13 mergeable sketch modules.
  - `reducer/1` and `merger/1` on all mergeable sketch modules for `Enum.reduce/3` and `Flow.reduce/3` ergonomics.

- **Broadway, GenStage, and Flow integration (Phase 2).**
  - `ExDataSketch.Broadway.accumulate/3` and `accumulate_into/4` -- build sketches from Broadway message batches.
  - `ExDataSketch.Broadway.PeriodicAggregator` -- GenServer that accumulates sketches and flushes on a timer with optional callback.
  - `ExDataSketch.GenStage.SketchConsumer` -- GenStage consumer that accumulates events into a sketch, supports periodic flush.
  - `ExDataSketch.GenStage.SketchProducer` -- GenStage producer that emits accumulated sketches on demand.
  - `ExDataSketch.GenStage.SketchStage` -- combined producer-consumer that accumulates and emits.
  - `ExDataSketch.Flow.reduce/3` and `merge/2` -- parallel partition-local reduction with merge for Flow pipelines.
  - All integration modules are optional and gated behind dependency availability checks (`ExDataSketch.Integration`).

- **Persistence surfaces (Phase 3).**
  - `ExDataSketch.Storage.ETS` -- in-memory persistence with `save/3`, `load/3`, `merge/3`, `delete/2`.
  - `ExDataSketch.Storage.DETS` -- disk-backed persistence with same API.
  - `ExDataSketch.Storage.CubDB` -- CubDB persistence for atomic key-value storage.
  - `ExDataSketch.Storage.Mnesia` -- distributed persistence for multi-node scenarios.
  - `ExDataSketch.Storage.Ecto` -- SQL database persistence with schema and migration helpers.
  - `ExDataSketch.Storage.Ecto.Schema` and `ExDataSketch.Storage.Ecto.Migration` -- Ecto schema and migration for sketch storage.
  - `ExDataSketch.Storage` -- shared behaviour documentation and types for all backends.
  - All backends serialize via EXSK v2 binary format with CRC32C checksum; no raw state is ever stored.
  - Configuration-driven backend availability via `config :ex_data_sketch, :persistence_backends`.

- **Telemetry and observability (Phase 4).**
  - `ExDataSketch.Telemetry` -- structured telemetry event emission at batch/compound operation boundaries (not per-update).
  - Four event categories: `:sketch` (create, ingest, merge, serialize, deserialize), `:persistence` (save, load, merge, delete), `:stream` (reduce, partition_merge), `:pipeline` (accumulate, periodic_flush).
  - `Telemetry.execute/4`, `Telemetry.span/5`, `Telemetry.span_with_result/6` -- timing wrappers with category-based enable/disable.
  - `Telemetry.event_name/2` and `all_event_names/0` for programmatic handler attachment.
  - `ExDataSketch.Telemetry.OpenTelemetry` -- OTEL span bridge (requires `:opentelemetry_api ~> 1.0`).
  - Configuration: `config :ex_data_sketch, telemetry_enabled: false` or per-category `config :ex_data_sketch, :telemetry, sketch: true, persistence: false`.
  - Telemetry events instrumented in all 13 sketch modules, all 5 storage backends, `Stream`, and `Broadway`/`GenStage`/`Flow`.

- **ULL accuracy correction (Phase 5).**
  - ULL linear counting correction: `zeros > 0` threshold (not HLL-style `raw_estimate <= 2.5*m && zeros > 0`). Empirical validation shows linear counting always more accurate for ULL when empty registers exist.
  - ULL large range correction: bias correction for very high cardinality estimates, matching Ertl 2023.
  - Both Pure Elixir and Rust NIF backends updated; property tests updated with tiered accuracy bounds (35%/25%/15% at p=8).

- **Configurable `update_many` chunk size (Phase 5).**
  - `update_many_chunk_size` option on HLL, ULL, CMS, and Theta (via `new/1` opts). Default 10,000 (backward compatible).

- **EXSK v1 serialization escape hatch (Phase 5).**
  - `HLL.serialize(sketch, format: :v1)` produces a backward-compatible v0.7.x binary (requires `:phash2` hash strategy, raises `ArgumentError` for other strategies).
  - `Binary.encode_v1/4` utility for custom v1 encoding.
  - v0.7.x binaries remain decodable via `Binary.decode/1` (version sniffing).

- **Generalized corruption propagation properties (Phase 5).**
  - HLL, ULL, and CMS bit-flip properties in `property_guarantees_test.exs` asserting that corrupted frames either fail CRC or produce estimates within 10x of the truthful estimate (never silently catastrophic).
  - Quotient filter delete property: count reduction (not `member?` becomes false).

- **Benchmarks and property tests (Phase 6).**
  - `bench/persistence_bench.exs` -- ETS save/load/merge overhead.
  - `bench/serialization_bench.exs` -- serialize/deserialize throughput.
  - `bench/merge_throughput_bench.exs` -- HLL/ULL/CMS `merge_many` benchmarks.
  - `bench/update_many_chunk_bench.exs` -- configurable chunk size impact on throughput.
  - `bench/stream_ingestion_bench.exs` -- stream ingestion latency and throughput.
  - `test/ex_data_sketch_serialization_stability_test.exs` -- 7 round-trip properties (HLL v2/v1, ULL, CMS, Theta, Bloom, v1-v2 cross-version).
  - Expanded stream properties: ULL stream equivalence, ULL partition merge, ULL merge associativity, Theta stream equivalence, CMS merge associativity.
  - Expanded storage properties: DETS save/load, DETS merge.

- **Educational materials.**
  - `guides/aggregation_wall.md` (188 lines) -- why exact aggregation breaks at scale, BEAM's natural fit, common patterns.
  - `guides/distributed_merge_semantics.md` (328 lines) -- associativity/commutativity proofs, fan-in/tree/partition patterns, anti-patterns.
  - `guides/livebooks.md` -- Livebook catalogue with recommended order and learning objectives.
  - Updated `guides/telemetry.md` -- pipeline/stream event tables, `all_event_names/0` reference.
  - Updated `guides/streaming_sketches.md` -- Stream API, Collectable, partitioned reduction.
  - Updated `guides/broadway_integration.md` -- `accumulate/3`, `accumulate_into/4`, `PeriodicAggregator`.
  - Updated `guides/genstage_integration.md` -- `SketchConsumer`, `SketchProducer`, `SketchStage`.
  - Updated `guides/persistence.md` -- all 5 backends, configuration, EXSK v2 storage contract.
  - Updated `guides/observability.md` -- telemetry categories, event names, OTEL bridge.

- **Livebooks.**
  - `livebooks/streaming_cardinality.livemd` -- Stream API, precision tradeoffs, ULL vs HLL.
  - `livebooks/broadway_integration.livemd` -- accumulate, PeriodicAggregator, partition handling.
  - `livebooks/genstage_aggregation.livemd` -- SketchConsumer, SketchProducer, flush patterns.
  - `livebooks/rolling_telemetry.livemd` -- time-windowed sketches, ETS persistence.
  - `livebooks/distributed_merges.livemd` -- associativity, tree aggregation, ETS sharding.
  - `livebooks/persistence_snapshots.livemd` -- ETS/DETS, serialization, multi-backend strategy.
  - `livebooks/livedashboard_integration.livemd` -- telemetry wiring, custom dashboard pages.
  - `livebooks/ai_token_analytics.livemd` -- LLM workload multi-dimensional sketch dashboard.
  - `livebooks/phoenix_observability.livemd` -- DAU, latency, rate limiting, ETS persistence.

### Changed

- **`ExDataSketch.HLL.new/1`** now accepts `update_many_chunk_size` option (default 10,000).
- **`ExDataSketch.ULL.new/1`** now accepts `update_many_chunk_size` option (default 10,000).
- **`ExDataSketch.CMS.new/1`** now accepts `update_many_chunk_size` option (default 10,000).
- **`ExDataSketch.Theta.new/1`** now accepts `update_many_chunk_size` option (default 10,000).
- **ULL accuracy** at low cardinalities significantly improved via linear counting + large range correction. Users may see different estimates for sketches with very few items; the new estimates are more accurate.
- **`ExDataSketch.ULL` moduledoc** updated with estimation strategy description and p>=12 recommendation.

### Fixed

- **ULL low-precision accuracy**: p=8 with n=1000 improved from ~62.5% relative error to ~0.8% via linear counting correction.
- **ETS merge test tolerance**: 60% tolerance for cardinality < 5 (HLL at p=10 has high relative error at tiny cardinalities).
- **Quotient filter delete property**: corrected from asserting `member?` becomes false (not guaranteed) to asserting count reduction.
- **DETS API**: corrected `close_file` to `:dets.close/1` in property tests.
- **`PeriodicAggregator` telemetry metadata**: uses `sketch_type` only (removed non-existent `state.id`).
- **OTEL handler IDs**: tuples `{"ex_data_sketch_opentelemetry", event_name}`, not strings.

### Migration

See `guides/v0.8.0_migration_notes.md` for the v0.7.x to v0.8.0 migration guide. For v0.8.0 to v0.9.0:

- **No code changes required for most users.** All new modules are additive; existing APIs are backward compatible.
- **ULL estimates may change** at very low cardinalities (p < 12, n < 500). The new estimates are more accurate. If you depend on exact numeric values in tests, add tolerance for small cardinalities.
- **`update_many_chunk_size`** defaults to 10,000 (matching v0.8.0 behavior). No change needed unless you want to tune batch throughput.
- **v1 serialization** is an opt-in escape hatch via `format: :v1`. Default serialization remains EXSK v2.
- **Telemetry** is enabled by default. Disable with `config :ex_data_sketch, telemetry_enabled: false`.
- **Persistence backends** are enabled by default when their runtime dependencies are available. Disable individuals via `config :ex_data_sketch, :persistence_backends, ets: [enabled: false]`.

### Stats

- **+21 new modules**: `Stream`, `Broadway`, `Broadway.PeriodicAggregator`, `Flow`, `GenStage`, `GenStage.SketchConsumer`, `GenStage.SketchProducer`, `GenStage.SketchStage`, `Storage`, `Storage.ETS`, `Storage.DETS`, `Storage.CubDB`, `Storage.Mnesia`, `Storage.Ecto`, `Storage.Ecto.Schema`, `Storage.Ecto.Migration`, `Telemetry`, `Telemetry.OpenTelemetry`, `Integration`, `Binary`, `Binary.encode_v1/4` utility.
- **1558 tests, 204 doctests, 199 properties, 0 failures** (NIF on).
- **9 Livebooks**, **20 guides** (3 new educational guides + 6 updated + `livebooks.md` index).
- **5 new benchmark suites**, **7 new property test groups**.
- **`:telemetry ~> 1.0`** required dependency; **`:opentelemetry_api ~> 1.0`**, **`:broadway`**, **`:flow`**, **`:cubdb`**, **`:ecto_sql`**, **`:mnesia`** optional dependencies.

## [0.8.0] - 2026-05-12

Release theme: **Deterministic Foundations.** Transforms ex_data_sketch from a collection of probabilistic algorithms into a production-grade probabilistic runtime for the BEAM. Focus: deterministic hashing, binary stability, corruption detection, hot-path performance, and installation reliability.

### Added

- **Deterministic hashing infrastructure (Phase 1).**
  - `ExDataSketch.Hash.XXH3` — focused XXHash3-64 wrapper. Raises `ArgumentError` when the Rust NIF is unavailable so hash drift cannot occur silently.
  - `ExDataSketch.Hash.Murmur3` — full `MurmurHash3_x64_128` returning the high 64 bits (Apache DataSketches convention). Pure Elixir and Rust NIF implementations are byte-identical, verified by property-based parity (200 random inputs per CI run) and against canonical Python `mmh3` regression vectors.
  - `ExDataSketch.Hash.Metadata` — 16-byte versioned binary block recording `(algorithm, seed, sketch_family, sketch_family_version, backend)` with a forward-compatible extension trailer. The building block for the EXSK v2 binary header.
  - `ExDataSketch.Hash.Validation` — centralized merge-compatibility checks (`validate_options!/3`, `validate_metadata!/3`, `compatible_options?/2`).
  - Public registry API on `ExDataSketch.Hash`: `default_algorithm/0`, `supported_algorithms/0`, `algorithm_info/1`.
  - `ExDataSketch.Hash.resolve_strategy/1` — single source of truth for sketch constructors. Honors a user-supplied `:hash_strategy` or falls back to `default_algorithm/0`.
  - HLL, ULL, Theta, CMS `new/1` now respect user-supplied `:hash_strategy` (`:xxhash3 | :murmur3 | :phash2`). The option was silently overridden in v0.7.x.

- **Binary stability and corruption detection (Phase 2).**
  - `ExDataSketch.Binary` — public facade for the EXSK v2 frame (`encode/3`, `decode/1`, `peek_version/1`, `build_payload/2`, `metadata_from_opts/3`).
  - `ExDataSketch.Binary.Header` — EXSK v2 frame encoder/decoder. Layout: magic + version + sketch_family + family_version + flags + header_size + `Hash.Metadata` block + payload_size + payload + CRC32C trailer.
  - `ExDataSketch.Binary.Validator` — discrete defensive check helpers (`check_minimum_v2_size`, `check_magic`, `check_version`, `check_crc`).
  - `ExDataSketch.Binary.CRC` — CRC32C (Castagnoli polynomial, reflected, init `0xFFFFFFFF`, final XOR `0xFFFFFFFF`). Pure Elixir and Rust NIF implementations are byte-identical, verified against the standard `"123456789" -> 0xE3069283` check vector and Python `crc32c` regression vectors.
  - Rust `crc32c_nif` — table-driven CRC32C, ~1 GB/s on commodity hardware.

- **HLL hot-path generalization (Phase 3).**
  - 8 new Rust NIFs: `{hll, ull, theta, cms}_update_many_raw_h_nif/_dirty_nif`. Each accepts an `algorithm: u8` parameter (`1 = xxhash3`, `2 = murmur3`) and shares a single `Murmur3_x64_128` implementation via `pub(crate)` export from `hash.rs`.
  - End-to-end Murmur3 acceleration: `:murmur3` callers now hit the in-Rust hashing fast path instead of falling off to the Elixir-side hash.
  - `bench/hll_hot_path_bench.exs` — comprehensive benchmark across Pure phash2 / Pure xxhash3 / Rust raw XXH3 / Rust raw_h Murmur3 at 10k / 100k / 1M items.

- **Precompiled NIF platform matrix (Phase 4).**
  - Two Windows targets added: `x86_64-pc-windows-msvc` and `aarch64-pc-windows-msvc`. Release matrix is now 8 targets × 2 NIF versions = 16 artifacts per tagged release.
  - `mix test.nif_on` and `mix test.nif_off` aliases automatically reset the per-env `rustler_precompiled :force_build` state between local NIF-mode flips.
  - `test/ex_data_sketch/nif_availability_test.exs` — 18 contract tests asserting `Hash.nif_available?/0` stability, default-algorithm reflection, registry availability flags, `XXH3.hash/2` failure mode, Murmur3 NIF-less fallback, checksum file shape, and `nif.ex` ↔ `release.yml` target-list alignment.

- **Property-based validation (Phase 5).**
  - `test/property_guarantees_test.exs` — 14 new properties locking:
    - HLL / ULL cardinality monotonicity and error bounds within published RSE.
    - KLL / REQ rank monotonicity and quantile/rank inversion within published epsilon.
    - CMS overestimation-only (`estimate(item) >= true_count(item)`).
    - Bloom / XorFilter / Cuckoo no-false-negative guarantees.
    - Binary v2 bit-flip corruption never silently propagates to a sketch.

- **User-facing release guides** (shipped to HexDocs):
  - `guides/v0.8.0_migration_notes.md` — v0.7.x to v0.8.0 upgrade guide.
  - `guides/v0.8.0_architecture.md` — layered architecture overview.
  - `guides/serialization_compatibility.md` — the v0.x EXSK stability contract.
  - `guides/hash_strategies.md` — choosing between phash2, XXH3, Murmur3, and custom.
  - `guides/hll_performance.md` — HLL hot-path architecture, benchmark numbers, and external-library context.
  - `guides/precompiled_nifs.md` — platform matrix, release pipeline, and source-build fallback.
  - `guides/roadmap.md` — v0.9.0 preview.
- **Internal plans and reviewer checklists** (repo-only, not packaged):
  - [`plans/hash_binary_contract.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/hash_binary_contract.md)
  - [`plans/binary_contract.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/binary_contract.md), [`plans/corruption_detection.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/corruption_detection.md)
  - [`plans/hll_scheduler_safety.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/hll_scheduler_safety.md)
  - [`plans/property_testing.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/property_testing.md)
  - [`plans/0.8.0_implementation_plan.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/0.8.0_implementation_plan.md), Phase 1-5 reviewer checklists
  - [`plans/0.8.0-risks.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/0.8.0-risks.md) (31-risk consolidated register)
  - [`plans/0.8.0-review.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/0.8.0-review.md) (pre-release code review)

### Changed

- **EXSK serialization format bumped to v2.** Every sketch's `serialize/1` now produces an EXSK v2 frame (magic + version 2 + sketch family + family version + flags + header_size + 16-byte hash metadata block + payload size + payload + CRC32C trailer). v0.7.x EXSK v1 frames remain decodable via `Binary.decode/1`'s version sniffing; `ExDataSketch.Codec` is preserved as the legacy v1 path.
- **Golden vectors regenerated as v2** under `test/vectors/`. The previous v1 vectors are preserved under `test/vectors_v1/` and exercised by `test/ex_data_sketch_v1_compat_test.exs` as a permanent regression guard.
- **`README.md` roadmap rewritten** to match the strategic roadmap in [`plans/next_steps.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/next_steps.md): v0.8.0 = Deterministic Foundations; v0.9.0 = Streaming Integrations; v0.10.0 = Apache Interoperability; v0.11.0 = New Sketch Families (CPC, Tuple); v0.12.0 = Similarity & Sampling (MinHash, VarOpt); v1.0.0 = Stable Binary Contract.
- **`ExDataSketch.Hash.validate_merge_hash_compat!/3`** is preserved as a backward-compatible shim that delegates to `ExDataSketch.Hash.Validation.validate_options!/3`.

### Fixed

- `ExDataSketch.Hash.XXH3` doctests are now NIF-safe: they exercise the success path when the NIF is available and explicitly verify the documented `ArgumentError` contract when the NIF is unavailable, removing a CI failure on the `EX_DATA_SKETCH_SKIP_NIF=true` lane.
- `test/ex_data_sketch/nif_availability_test.exs` checksum-file assertions softened to "if present, parses as a map" so fresh checkouts (where `checksum-Elixir.ExDataSketch.Nif.exs` has not been populated by the release pipeline) pass CI.

### Migration

See `guides/v0.8.0_migration_notes.md` (shipped in HexDocs) for the full v0.7.x -> v0.8.0 migration guide. Key points:

- **No code changes required for most users.** EXSK v1 frames are still decoded; the `serialize/1` output format changes but downstream code that uses round-trip serialization sees no API difference.
- **One-way upgrade for persisted sketches.** v0.7.x cannot read v0.8.0-produced binaries. Stage your rollout: deploy v0.8.0 readers first, then producers.
- **Opt-in Murmur3.** New `:murmur3` strategy is opt-in via `hash_strategy: :murmur3` at sketch construction. Default remains `:xxhash3`.

### Documentation

User-facing guides (shipped to HexDocs and the Hex package):

- `guides/v0.8.0_architecture.md` — consolidated Phase 1-5 design overview.
- `guides/serialization_compatibility.md` — the v0.x stability contract.
- `guides/roadmap.md` — preview of the next release's streaming-integration scope.

Internal documentation (repo-only; linked from the user guides):

- [`plans/0.8.0-risks.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/0.8.0-risks.md) — open risk register at release time.
- [`plans/0.8.0-review.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/0.8.0-review.md) — pre-release code review.

### Stats

- **+10 new modules**: `Hash.XXH3`, `Hash.Murmur3`, `Hash.Metadata`, `Hash.Validation`, `Binary`, `Binary.Header`, `Binary.Validator`, `Binary.CRC` (Elixir); 2 Rust NIF entry points (`hash`, `crc`).
- **+11 Rust NIFs**: `murmur3_x64_128_nif`, `murmur3_x64_128_full_nif`, `crc32c_nif`, 4 × `*_update_many_raw_h_nif` + 4 × `*_dirty_nif`.
- **+92 tests, +33 doctests, +19 properties** since v0.7.1.
- **Full suite (NIF on)**: 1,317 tests, 202 doctests, 171 properties, 0 failures.
- **Full suite (NIF off)**: 1,088 tests, 202 doctests, 128 properties, 0 failures.
- **Coverage**: 92.7% line coverage (target was 70%).
- **HLL throughput**: 25-34 M items/sec at p=14 (XXH3, Rust raw); ~15x faster than the Pure path.

## [0.7.1] - 2026-03-22

### Added

- Move hashing into NIF batch calls: `update_many` for HLL, ULL, Theta, and CMS now sends raw items to Rust and hashes inside the NIF, eliminating per-item Elixir heap allocations (94.6% memory reduction at 10M items). (#202)
- Wire `:hash_fn` and `:seed` options through HLL, ULL, Theta, and CMS, enabling custom hash functions and reproducible seeded hashing. (#198)
- Merge hash-compatibility validation: `merge/2` on HLL, ULL, Theta, and CMS now raises `IncompatibleSketchesError` when hash strategy or seed differs between sketches. (#205)
- Pure backend `hll_update_many` optimization: pre-aggregate map with sorted binary splice replaces tuple-based per-hash full-tuple copies, reducing transient allocation from O(n * m) to O(n + m).
- ListIterator-based NIF item decoding for zero-copy Erlang list iteration in Rust.
- Test infrastructure: configurable coverage baselines, Rust CI coverage reporting, 39 new tests covering deserialization edge cases, custom hash_fn paths, helper functions, and merge validation.

### Fixed

- Quotient filter wrap-around bug: `extract_all` in both Pure and Rust backends failed when a cluster wrapped from slot N-1 to slot 0, producing nil quotients (Pure crash) or silent corruption (Rust). (#203, #204)
- CMS merge validation: replaced flawed `Keyword.delete(opts, :hash_strategy)` comparison with explicit width/depth/counter_width checks.
- Pure backend `update_many` regression: restored chunk + batch `*_update_many` path for HLL, ULL, and Theta (was incorrectly using per-item `*_update`).
- Seed clamping: all raw NIF functions now clamp seed values to u64 range before passing to Rust.
- Hash-dependent vector tests tagged with `@tag :rust_nif` to prevent failures in pure-only CI (vectors were generated with xxhash3).

## [0.7.0] - 2026-03-11

### Added

- UltraLogLog sketch (`ExDataSketch.ULL`) for improved cardinality estimation with ~20% lower relative error than HLL at the same memory footprint. ULL1 binary state format with 8-byte header + 2^p registers. EXSK serialization (sketch ID 15).
- ULL register encoding from Ertl 2023: `register_value = 2 * geometric_rank - sub_bit` doubles the information per register compared to HLL.
- FGRA estimator (Ertl 2017 sigma/tau convergence) for ULL cardinality estimation.
- Rust NIF acceleration for ULL: `update_many`, `merge`, and `estimate` operations with dirty scheduler thresholds.
- Precision parameter `p` supports range 4..26 (vs 4..16 for HLL), allowing higher accuracy at larger memory budgets.
- Full API: `new`, `update`, `update_many`, `merge`, `estimate`, `count`, `serialize`, `deserialize`, `from_enumerable`, `merge_many`, `reducer`, `merger`, `size_bytes`.
- ULL test vectors, parity tests, merge law property tests, and benchmark suite.

## [0.6.0] - 2026-03-11

### Added

- REQ sketch (`ExDataSketch.REQ`) for relative-error quantile estimation with configurable high-rank accuracy. REQ1 binary state format. EXSK serialization (sketch ID 13).
- Misra-Gries sketch (`ExDataSketch.MisraGries`) for deterministic heavy-hitter detection with configurable key encoding (`:binary`, `:int`, `{:term, :external}`). MG01 binary state format. EXSK serialization (sketch ID 14).
- XXHash3 NIF integration (`ExDataSketch.Hash.xxhash3_64/1,2`) for fast, cross-platform stable hashing via Rust NIF with phash2-based fallback.
- KLL `cdf/2` and `pmf/2` for cumulative distribution and probability mass functions.
- DDSketch `rank/2` for normalized rank queries.
- Rust NIF acceleration for all membership filters: Bloom, Cuckoo, Quotient, CQF, XorFilter, and IBLT. Batch operations (`put_many`, `merge`, `build`) automatically use compiled Rust NIFs when available, with dirty scheduler thresholds for large inputs.
- Parity tests verifying byte-identical serialization between Pure Elixir and Rust NIF backends for all sketch algorithms.
- Benchmark suites for REQ sketch, Misra-Gries, and XXHash3 NIF throughput.
- `Quantiles` facade for unified quantile sketch API across KLL and DDSketch.

## [0.5.0] - 2026-03-10

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
