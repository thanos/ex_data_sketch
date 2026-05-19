# Livebooks

ExDataSketch ships with production-oriented Livebooks that demonstrate
real-world usage patterns. Each Livebook is self-contained and executable,
requiring only `Mix.install([{:ex_data_sketch, "~> 0.9.0"}])` plus any
integration dependencies noted below.

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

4. **Rolling Telemetry** -- Time-windowed sketch accumulation, persistence, and
   `:telemetry` event attachment. No extra dependencies.

5. **Broadway Integration** -- `accumulate/3`, `accumulate_into/4`,
   `PeriodicAggregator`. Requires `:broadway`.

6. **GenStage Aggregation** -- `SketchConsumer`, `SketchProducer`, flushing,
   and callbacks. No extra dependencies.

7. **LiveDashboard Integration** -- Wiring `:telemetry` events into Phoenix
   LiveDashboard metrics. No extra dependencies (Phoenix patterns are
   documented in comments).

8. **Phoenix Observability** -- Endpoint cardinality, latency percentiles,
   Theta set difference, ETS persistence, memory comparison. No extra
   dependencies.

9. **AI Token Stream Analytics** -- Multi-dimensional dashboard combining HLL,
   ULL, DDSketch, MisraGries, CMS, and Bloom for LLM workload monitoring.
   No extra dependencies.

## What Each Livebook Teaches

| Livebook | Core API | Key Concept |
|----------|----------|-------------|
| Streaming Cardinality | `Stream.hll`, `reduce_into`, `reduce_partitioned`, `Collectable` | Lazy stream consumption, precision/memory tradeoff |
| Persistence Snapshots | `Storage.ETS.save/load/merge`, `Storage.DETS`, `serialize/1` | Durability hierarchy, EXSK v2 binary format |
| Distributed Merges | `merge_many/1`, `merge/2`, `Storage.ETS.merge/3` | Associativity, commutativity, tree aggregation |
| Rolling Telemetry | `Telemetry.execute`, `Telemetry.span`, `GenServer` timer | Time-windowed aggregation, periodic flush |
| Broadway Integration | `ExDataSketch.Broadway.accumulate/3`, `PeriodicAggregator` | Batch aggregation, periodic flush, partition handling |
| GenStage Aggregation | `SketchConsumer`, `SketchProducer`, `flush/1` | Back-pressure, push-based accumulation, callbacks |
| LiveDashboard Integration | `Telemetry.event_name`, `all_event_names`, `:telemetry.attach` | Phoenix metrics wiring, custom dashboard pages |
| Phoenix Observability | HLL, REQ, CMS, Theta, ETS | Per-endpoint DAU, latency distributions, rate limiting |
| AI Token Analytics | HLL, ULL, DDSketch, KLL, MisraGries, CMS, Bloom | Multi-dimensional sketch dashboard |

## Running a Livebook

```bash
# From the project root
livebook open livebooks/streaming_cardinality.livemd

# Or start Livebook and navigate to the livebooks/ directory
livebook server
```

Each Livebook begins with a `Mix.install` cell that fetches the required
dependencies. The Broadway Livebook additionally installs `:broadway`.

## Livebook Listing

| File | Topic | Lines |
|------|-------|-------|
| `streaming_cardinality.livemd` | Stream/Collectable API, precision, ULL vs HLL | 126 |
| `persistence_snapshots.livemd` | ETS, DETS, serialization, multi-backend | 152 |
| `distributed_merges.livemd` | Associativity, tree aggregation, ETS sharding | 121 |
| `rolling_telemetry.livemd` | Time windows, GenServer, telemetry events | 119 |
| `broadway_integration.livemd` | Batch accumulation, PeriodicAggregator | 130 |
| `genstage_aggregation.livemd` | SketchConsumer, SketchProducer, flushing | 172 |
| `livedashboard_integration.livemd` | Telemetry wiring, custom pages, events | 190 |
| `phoenix_observability.livemd` | DAU, latency, rate limiting, ETS persistence | 263 |
| `ai_token_analytics.livemd` | LLM workload monitoring, multi-sketch dashboard | 192 |