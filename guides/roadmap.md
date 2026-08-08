# v0.11.0 Roadmap Preview

This document is the **planning stub** for the v0.11.0 release. It is
intentionally not a commitment -- actual scope will be locked when a
v0.11.0 prompt is authored. The purpose here is to:

1. Inform users and contributors what the v0.11.0 work surface will
   likely look like.
2. Park v0.10.0 follow-up items so they are not lost.
3. Give the next prompt-author a starting point.

For the strategic context see [`plans/next_steps.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/next_steps.md). For the
just-shipped release see `CHANGELOG.md` `[0.10.0]`.

## Release theme (proposed)

**v0.11.0 -- Apache HLL Interoperability & New Sketch Families.**

Where v0.10.0 built out the operational middle layer (unified sketch
contract, storage, windowing, supervised sketches, metrics/dashboard) and
closed the first cross-language interop gap (Apache KLL, a pure
binary-layout problem since KLL doesn't hash its inputs), v0.11.0 tackles
the harder interop case -- HLL -- and starts widening the sketch-family
catalog again.

## Tracks (proposed)

### Track A -- Apache HLL Interoperability

| Deliverable | Notes |
|-------------|-------|
| Hash-function reconciliation | DataSketches HLL is a union of four internal encodings (LIST/SET/HLL_4/HLL_6/HLL_8) keyed to MurmurHash3_x64_128; `ExDataSketch.Hash.hash64/1` uses a different function. Unlike KLL, HLL's registers are *derived from* hashed items, so decoding a foreign binary requires reconciling hash semantics, not just binary layout -- this is the core research/design item. |
| `HLL.serialize_datasketches/2` / `deserialize_datasketches/2` | Replace the `not_implemented!` stubs, mirroring the `Theta`/`KLL` precedent (`ExDataSketch.DataSketches.*` codec module + `Backend.hll_from_components/N` callback). |
| Golden fixture corpus | `test/fixtures/interop/hll/`, generated from the real `datasketches` Python package (pinned version), following the pattern `test/fixtures/interop/kll/` established in v0.10.0. |
| `guides/apache_interop.md` update | Add HLL's row and hash-caveat section (HLL will need the Theta-style caveat, not KLL's hash-free one). |

### Track B -- CPC (Compressed Probabilistic Counting)

| Deliverable | Notes |
|-------------|-------|
| `ExDataSketch.CPC` | New cardinality sketch family; smaller serialized footprint than HLL at comparable accuracy. |
| Backend.Pure + Backend.Rust | Full parity from day one, following the established pattern (raw-hashing NIF path per v0.10.0 Phase 6). |
| Apache interop | Evaluate whether CPC's binary format is KLL-shaped (tractable) or HLL-shaped (hash-equality-blocked) before committing to an interop deliverable in the same release. |

### Track C -- Tuple Sketch (weighted distinct counting)

| Deliverable | Notes |
|-------------|-------|
| `ExDataSketch.TupleSketch` | Theta-family sketch carrying a summary value per retained entry (e.g. weighted counting, associative aggregation). |
| Summary operations | At minimum sum; consider min/max/last-writer-wins as a configurable summary combiner. |

## Carry-forward from v0.10.0 (follow-up issues)

The following v0.10.0 items are candidate v0.11.0 work. None of them is
guaranteed scope; the v0.11.0 prompt should choose deliberately.

### High-priority carry-forward

| ID | Title | Why v0.11.0? |
|----|-------|--------------|
| 3-R7 | Benchmarks run on a single architecture (Apple M1 Max) only | Twice-deferred (v0.8.0 and v0.9.0 carry-forward). A CI bench matrix on x86_64 and ARM64 Linux was Phase 8's item 5 in v0.10.0 and requires an actual CI infrastructure decision from the maintainer, not just code -- good candidate to finally settle. |

### Medium-priority carry-forward

| ID | Title | Notes |
|----|-------|-------|
| 1-R4 | Deprecate `:phash2` | Still data-driven -- needs real adoption telemetry showing `:xxhash3`/`:murmur3` usage before deprecating the fallback. Revisit once v0.10.0's telemetry/dashboard work has been in production for a cycle. |
| -- | `FilterChain` has no `format: :v1` escape hatch | Documented as intentional in its moduledoc (bespoke FCN1 container, not a `Codec` frame) -- revisit only if a real rolling-upgrade need surfaces for filter chains specifically. |

### Explicitly deferred (NOT v0.11.0)

| ID | Title | Target release |
|----|-------|----------------|
| 3-R5 | 6-bit register packing | v1.0 |
| 3-R6 | SIMD intrinsics for HLL | v1.0 |
| 3-R3 | Remove legacy `_raw_nif` family | v1.0 (binary-stability break) |
| -- | MinHash, Weighted MinHash, VarOpt sampling | v0.12.0 (Similarity & Sampling, per README roadmap) |

## Out-of-scope guardrails

The v0.11.0 release should NOT:

- Break the v0.x serialization compatibility contract documented in
  `serialization_compatibility.md`.
- Default any opt-out path (e.g., flipping `Backend.default/0`
  silently). Such changes are v1.0 work.
- Add Rust dependencies that pull in a C compiler at NIF-build time
  (slows down `EX_DATA_SKETCH_BUILD=1` users).
- Commit to an HLL Apache-interop deliverable before the hash-function
  reconciliation research in Track A has actually confirmed a tractable
  approach -- unlike KLL, this is not guaranteed to be a one-phase item.

## Suggested v0.11.0 prompt outline

When authoring the v0.11.0 prompt, the structure of the v0.10.0 prompt
worked well and should be reused:

1. Release theme banner.
2. IMPORTANT EXECUTION RULES (architectural, Elixir design philosophy).
3. PROJECT GOALS.
4. RELEASE SCOPE -- explicit INCLUDES and DOES NOT INCLUDE lists.
5. Phase-by-phase breakdown with STOP conditions.
6. FINAL RELEASE REQUIREMENTS.
7. FINAL OUTPUT REQUIREMENTS.
8. Next-release preview (v0.12.0).

Likely phase breakdown:

| Phase | Theme | Primary modules |
|-------|-------|------------------|
| 1 | HLL hash-function reconciliation (research + design) | `ExDataSketch.DataSketches.*`, `Backend.hll_from_components` |
| 2 | HLL Apache interop implementation + golden fixtures | `ExDataSketch.HLL`, `test/fixtures/interop/hll/` |
| 3 | CPC sketch (Pure + Rust) | `ExDataSketch.CPC` |
| 4 | Tuple Sketch | `ExDataSketch.TupleSketch` |
| 5 | Carry-forward (bench matrix, phash2 deprecation review) | per the table above |
| 6 | Carry-forward, hygiene, release | mirrors v0.10.0 Phase 8 |

## See also

- [`plans/next_steps.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/next_steps.md) -- strategic roadmap.
- `apache_interop.md` -- what interoperates today (Theta, KLL) and why HLL doesn't yet.
- `serialization_compatibility.md` -- the binary compatibility contract v0.11.0 must not break.
- `CHANGELOG.md` `[0.10.0]` -- v0.10.0 changes.
