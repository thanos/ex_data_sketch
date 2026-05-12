# v0.9.0 Roadmap Preview

This document is the **planning stub** for the v0.9.0 release. It is
intentionally not a commitment — actual scope will be locked when a
v0.9.0 prompt is authored. The purpose here is to:

1. Inform users and contributors what the v0.9.0 work surface will
   likely look like.
2. Park v0.8.0 follow-up items so they are not lost.
3. Give the next prompt-author a starting point.

For the strategic context see [`plans/next_steps.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/next_steps.md). For the
just-shipped release see `CHANGELOG.md` `[0.8.0]`.

## Release theme (proposed)

**v0.9.0 — Streaming Integrations.**

Where v0.8.0 hardened the substrate (deterministic hashing, binary
stability, hot paths, precompiled NIFs, property validation), v0.9.0
opens it to the streaming and observability ecosystems that the BEAM
already excels at. The goal is to make `ExDataSketch.HLL.from_enumerable/2`
the boring, obvious choice for anyone reaching for "approximate
distinct count" inside a Broadway pipeline or a Phoenix LiveView.

## Tracks (proposed)

### Track A — Stream Integration

| Deliverable | Notes |
|-------------|-------|
| `ExDataSketch.Stream` | Wrap each sketch family as a stream sink: `Stream |> ExDataSketch.Stream.hll(p: 14) |> Enum.to_list`. Returns a sketch. |
| Reducer / collectable | Implement `Enumerable.reduce/3` adapters so any `Enum.into/2` works. |
| Partition-aware merge | A stream over an iolist of partitioned inputs should merge per-partition results correctly without manual `merge_many/1`. |

### Track B — Broadway / GenStage

| Deliverable | Notes |
|-------------|-------|
| `ExDataSketch.Broadway` | Producer / processor wrappers. Windowed sketches with periodic flush. |
| `ExDataSketch.GenStage` | Same surface, GenStage-level. |
| Partition-aware aggregation | Per-partition sketches that merge in the consumer stage. |

### Track C — Persistence

| Deliverable | Notes |
|-------------|-------|
| `ExDataSketch.Storage.ETS` | Concurrent-safe sketch table; periodic snapshot. |
| `ExDataSketch.Storage.DETS` | Disk-backed equivalent. |
| `ExDataSketch.Storage.CubDB` | High-throughput KV-store backed by CubDB. |
| Snapshot semantics | Document atomic-merge guarantees per store. |

### Track D — Observability

| Deliverable | Notes |
|-------------|-------|
| `:telemetry` events | Per-operation latency, batch size, scheduler dispatch (normal vs dirty). |
| OpenTelemetry instrumentation | Auto-link sketch operations to a parent span. |
| Suggested dashboards | A reference Grafana / LiveDashboard panel set for sketches in production. |

## Carry-forward from v0.8.0 (follow-up issues)

The following v0.8.0 risks are candidate v0.9.0 work. None of them is
guaranteed scope; the v0.9.0 prompt should choose deliberately.

### High-priority carry-forward

| ID | Title | Why v0.9.0? |
|----|-------|--------------|
| 5-R1 / X-R1 | ULL low-`p` accuracy + HLL memory profile at 10M items | Both surface the "BEAM-side chunk lifecycle interacts with sketch internals at scale" theme. v0.9.0's streaming work has to grapple with batch size and memory budget anyway. Natural place to fix. |
| 2-R1 | EXSK v2 one-way upgrade | An opt-in `serialize(sketch, format: :v1)` escape hatch would smooth multi-version rollouts. Trivial code change once the format is stable. |
| 3-R4 | Membership filter raw-NIF hot path | The 6 membership filters (Bloom / Cuckoo / Quotient / CQF / Xor / IBLT) still hash in Elixir. Closing this gap completes the "every cardinality / membership operation hashes inside Rust" promise. |

### Medium-priority carry-forward

| ID | Title | Notes |
|----|-------|-------|
| 4-R5 | `Backend.default/0` returns `Pure` regardless of NIF state | Reconsider only with data showing adoption friction. |
| 3-R7 | Benchmarks run on M1 only | CI step to run on x86_64 / Linux ARM64 release matrix. Mechanical. |
| X-R2 | README roadmap not protected by integration test | Tiny `ci/check_roadmap.exs` script. |
| 4-R4 | Cross-compile reliability | Add retry annotation to the four cross-compiled matrix entries. |
| 5-R4 | Corruption-propagation property targets HLL only | Generalize across all sketches. Mechanical. |

### Low-priority / opportunistic

| ID | Title | Notes |
|----|-------|-------|
| 5-R2 | REQ rank/quantile slack too loose | Empirically tighten. |
| 5-R3 | Cuckoo saturation not exercised by property | Add saturation-specific property. |
| 5-R5 | Property `max_runs` bounded; nightly deep run | Optional CI workflow. |
| X-R3 | `prompts/benchmark_comparisons.md` is empty | Decide: populate or delete. |

### Explicitly deferred (NOT v0.9.0)

| ID | Title | Target release |
|----|-------|----------------|
| 3-R5 | 6-bit register packing | v1.0 |
| 3-R6 | SIMD intrinsics for HLL | v1.0 |
| 3-R3 | Remove legacy `_raw_nif` family | v1.0 (binary-stability break) |
| 1-R4 | Deprecate `:phash2` | v0.10+ (data-driven) |

## Out-of-scope guardrails

The v0.9.0 release should NOT:

- Add new sketch families (CPC, Tuple, MinHash, VarOpt are v0.11+).
- Break the v0.x serialization compatibility contract documented in
  `serialization_compatibility.md`.
- Default any opt-out path (e.g., flipping `Backend.default/0`
  silently). Such changes are v1.0 work.
- Add Rust dependencies that pull in a C compiler at NIF-build time
  (slows down `EX_DATA_SKETCH_BUILD=1` users).

## Suggested v0.9.0 prompt outline

When authoring the v0.9.0 prompt, the structure of the v0.8.0 prompt
worked well and should be reused:

1. Release theme banner.
2. IMPORTANT EXECUTION RULES (architectural, Elixir design philosophy).
3. PROJECT GOALS.
4. RELEASE SCOPE — explicit INCLUDES and DOES NOT INCLUDE lists.
5. Phase-by-phase breakdown with STOP conditions.
6. FINAL RELEASE REQUIREMENTS.
7. FINAL OUTPUT REQUIREMENTS.
8. Next-release preview (v0.10.0).

Phase count for v0.9.0 will likely be similar (4-6 phases). Likely
phase breakdown:

| Phase | Theme | Primary modules |
|-------|-------|------------------|
| 1 | Stream + Collectable | `ExDataSketch.Stream`, `Enumerable` adapters |
| 2 | Broadway / GenStage | `ExDataSketch.Broadway`, `ExDataSketch.GenStage` |
| 3 | Persistence | `ExDataSketch.Storage.{ETS, DETS, CubDB}` |
| 4 | Telemetry / OpenTelemetry | event names + reference dashboards |
| 5 | Carry-forward from v0.8.0 (high-priority risks) | per the table above |
| 6 | Property + bench expansion for new surfaces | new properties for streaming semantics |

## See also

- [`plans/next_steps.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/next_steps.md) — strategic roadmap (v0.8.0 through v1.0).
- `v0.8.0_architecture.md` — what just shipped.
- [`plans/0.8.0-risks.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/0.8.0-risks.md) — open risks at v0.8.0 release.
- [`prompts/0.8.0_prompt.md`](https://github.com/thanos/ex_data_sketch/blob/main/prompts/0.8.0_prompt.md) — the v0.8.0 prompt for reference style.
- `CHANGELOG.md` `[0.8.0]` — v0.8.0 changes.
