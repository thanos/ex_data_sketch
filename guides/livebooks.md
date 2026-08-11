# Livebooks

ExDataSketch ships with production-oriented Livebooks that demonstrate
real-world usage patterns. Each Livebook is self-contained and executable,
requiring only `Mix.install([{:ex_data_sketch, "~> 0.10"}])` plus any
integration dependencies noted below.

## Per-Family Tutorials (`livebooks/sketches/`)

One tutorial livebook per sketch family -- the fastest way to learn a
specific family's API, accuracy properties, and operational guidance in
isolation, before moving on to the cross-cutting Livebooks below. Each
generates its own sample data on first run and caches it under
`System.tmp_dir!()`, so re-running a tutorial (or all 16 in sequence)
after the first pass is fast.

| Livebook | Family | Key Concept |
|----------|--------|-------------|
| `sketches/hll.livemd` | HLL | Cardinality estimation, precision/memory tradeoff, merge |
| `sketches/ull.livemd` | ULL | HLL alternative with ~30% better accuracy at equal memory |
| `sketches/cms.livemd` | CMS | Frequency estimation, over-estimation-only guarantee |
| `sketches/theta.livemd` | Theta | Set operations (union/intersection/difference) via inclusion-exclusion |
| `sketches/kll.livemd` | KLL | Quantiles, rank, CDF/PMF |
| `sketches/ddsketch.livemd` | DDSketch | Value-relative accuracy across orders of magnitude |
| `sketches/req.livemd` | REQ | HRA vs LRA tail-accuracy tradeoff |
| `sketches/frequent_items.livemd` | FrequentItems | SpaceSaving top-K with lower/upper error bounds |
| `sketches/misra_gries.livemd` | MisraGries | Deterministic heavy hitters, undercount guarantee |
| `sketches/bloom.livemd` | Bloom | Membership testing, no false negatives, measured FPR |
| `sketches/cuckoo.livemd` | Cuckoo | Membership with deletion, `FilterFullError` |
| `sketches/quotient.livemd` | Quotient | Membership with safe deletion and merge |
| `sketches/cqf.livemd` | CQF | Approximate multiset counting |
| `sketches/xor_filter.livemd` | XorFilter | Static, build-once membership filter |
| `sketches/iblt.livemd` | IBLT | Set reconciliation (symmetric difference recovery) |
| `sketches/filter_chain.livemd` | FilterChain | Composing multiple filters into one query pipeline |

## Recommended Order

The Livebooks are designed to build knowledge incrementally. The recommended
reading order is:

1. **Streaming Cardinality** -- Start here. Covers `from_enumerable/2`,
   `ExDataSketch.Stream`, `Collectable`, precision tradeoffs, and ULL vs HLL
   comparison. No extra dependencies.

2. **Persistence Snapshots** -- How to save, load, and merge sketches using
   ETS, DETS, and binary serialization. No extra dependencies.

3. **Distributed Merges** -- Associativity, commutativity, tree aggregation,
   and ETS-sharded patterns. No extra dependencies.

4. **Broadway Integration** -- `accumulate/3`, `accumulate_into/3`,
   `PeriodicAggregator`. Requires `:broadway`.

5. **GenStage Aggregation** -- `SketchConsumer`, `SketchProducer`, flushing,
   and callbacks. No extra dependencies.

6. **AI Token Stream Analytics** -- Multi-dimensional dashboard combining HLL,
   ULL, DDSketch, MisraGries, CMS, and Bloom for LLM workload monitoring.
   No extra dependencies.

7. **Sketching One Billion Rows** -- Capstone case study. Streams a 1BRC-style
   (`station;temperature`) dataset without materializing it, combining exact
   per-station accumulators with `ExDataSketch.KLL` (quantiles),
   `ExDataSketch.HLL` (distinct stations), and `ExDataSketch.FrequentItems`
   (heavy hitters) into one `WeatherSketch` summary; covers partition-local
   merge equivalence, bounded concurrency via `Task.async_stream/3`, and
   measuring approximation error against the exact accumulator. No extra
   dependencies beyond `:gen_stage` and `:ecto` (compile-time only, pulled in
   by ExDataSketch's integration modules).

For `ExDataSketch.Window` (time-windowed sketch accumulation), see
`guides/windowing.md` -- it covers everything the former
`rolling_telemetry.livemd` Livebook demonstrated (basic usage,
deterministic testing, persistence) in more depth, in prose plus doctested
examples rather than a Livebook that could drift out of sync.

For Phoenix LiveDashboard and telemetry specifically, see `phoenix_demo/`
at the repository root instead of a Livebook -- a minimal, real, runnable
Phoenix app with a mounted `ExDataSketch.LiveDashboard.Page`,
`ExDataSketch.Telemetry.Metrics.all/1` wired into its own telemetry
module, and a live `/` page backed by two supervised `ExDataSketch.Server`
instances (one of them windowed, so `[:ex_data_sketch, :window, :roll]`
telemetry -- the former `rolling_telemetry.livemd`'s other topic -- shows
up live on the dashboard too). It replaces this project's former
`livedashboard_integration.livemd`, `phoenix_observability.livemd`, and
`rolling_telemetry.livemd` Livebooks. See `phoenix_demo/README.md`.

## What Each Livebook Teaches

| Livebook | Core API | Key Concept |
|----------|----------|-------------|
| Streaming Cardinality | `Stream.hll`, `reduce_into`, `reduce_partitioned`, `Collectable` | Lazy stream consumption, precision/memory tradeoff |
| Persistence Snapshots | `Storage.ETS.save/load/merge`, `Storage.DETS`, `serialize/1` | Durability hierarchy, EXSK v2 binary format |
| Distributed Merges | `merge_many/1`, `merge/2`, `Storage.ETS.merge/3` | Associativity, commutativity, tree aggregation |
| Broadway Integration | `ExDataSketch.Broadway.accumulate/3`, `ExDataSketch.Broadway.PeriodicAggregator` | Batch aggregation, periodic flush, partition handling |
| GenStage Aggregation | `SketchConsumer`, `SketchProducer`, `flush/1` | Back-pressure, push-based accumulation, callbacks |
| AI Token Analytics | HLL, ULL, DDSketch, KLL, MisraGries, CMS, Bloom | Multi-dimensional sketch dashboard |
| Sketching One Billion Rows | `KLL`, `HLL`, `FrequentItems`, `Task.async_stream/3` | Streaming a large dataset, partition merge equivalence, accuracy-vs-size tradeoff |

## Running a Livebook

```bash
# From the project root
livebook open livebooks/streaming_cardinality.livemd

# Or start Livebook and navigate to the livebooks/ directory
livebook server
```

Each Livebook begins with a `Mix.install` cell that fetches the required
dependencies. The Broadway Livebook additionally installs `:broadway`;
Sketching One Billion Rows additionally installs `:gen_stage` and `:ecto`
(compile-time dependencies of ExDataSketch's integration modules).

## Livebook Listing

| File | Topic | Lines |
|------|-------|-------|
| `streaming_cardinality.livemd` | Stream/Collectable API, precision, ULL vs HLL | 126 |
| `persistence_snapshots.livemd` | ETS, DETS, serialization, multi-backend | 152 |
| `distributed_merges.livemd` | Associativity, tree aggregation, ETS sharding | 121 |
| `broadway_integration.livemd` | Batch accumulation, PeriodicAggregator | 130 |
| `genstage_aggregation.livemd` | SketchConsumer, SketchProducer, flushing | 172 |
| `ai_token_analytics.livemd` | LLM workload monitoring, multi-sketch dashboard | 192 |
| `sketching_one_billion_rows.livemd` | 1BRC-style streaming case study, KLL/HLL/FrequentItems combined | 589 |