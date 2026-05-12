# Hash Infrastructure — Phase 1 of v0.8.0

This document explains the deterministic hashing infrastructure introduced
in `ex_data_sketch` v0.8.0. It is intended for contributors and for users
who care about merge correctness, persistence durability, and cross-host
reproducibility.

## Why deterministic hashing matters

Probabilistic sketches summarize streams by mapping elements onto a small
fixed-size state through a hash function. Two sketches are mergeable only
when their hash functions are point-wise identical. If `Hash_A(x)` and
`Hash_B(x)` produce different 64-bit values for any single element `x`,
then merging them produces a corrupt result that no estimator can detect.

Consequences of silent hash drift:

- **HLL / ULL**: register indices and leading-zero counts move; the merged
  cardinality estimate becomes meaningless.
- **CMS / Bloom / Cuckoo**: collision sets diverge; counts / membership
  bits no longer overlap in the intended way.
- **Theta / KLL**: ordered statistics lose their meaning entirely.

The fix is *not* "use the same algorithm name". It is "embed the exact
hashing identity into the persisted sketch and refuse merges that disagree
on that identity". Phase 1 of v0.8.0 establishes the surface for both.

## What's new in v0.8.0

- **Registry API** on `ExDataSketch.Hash`:
  - `default_algorithm/0` — the deterministic hash chosen by default.
  - `supported_algorithms/0` — `[:phash2, :xxhash3, :murmur3, :custom]`.
  - `algorithm_info/1` — a static descriptor `%{id, name, output_bits,
    has_seed, available?, stability}` per algorithm.
- **`ExDataSketch.Hash.XXH3`** — focused wrapper around the XXHash3-64
  Rust NIF. Raises rather than silently falling back when the NIF is
  missing, so hash drift cannot occur without the caller noticing.
- **`ExDataSketch.Hash.Murmur3`** — full `MurmurHash3_x64_128` returning
  the high 64 bits (Apache DataSketches convention). Implemented in both
  pure Elixir and a Rust NIF; byte-identical parity is asserted by
  property tests against 200 random inputs per run.
- **`ExDataSketch.Hash.Metadata`** — a versioned binary block recording
  the exact hashing identity and sketch family. This is the Phase 2
  binary header's building block.
- **`ExDataSketch.Hash.Validation`** — `validate_options!/3`,
  `validate_metadata!/3`, `compatible_options?/2`. The existing
  `ExDataSketch.Hash.validate_merge_hash_compat!/3` becomes a thin shim,
  preserving backward compatibility.

## Why XXHash3 is the default

| Property                          | XXHash3-64 | MurmurHash3_x64_128 high-64 | Erlang phash2 + mix64 |
|-----------------------------------|------------|------------------------------|------------------------|
| Cross-platform output             | Yes        | Yes                          | Yes (32-bit base)      |
| Cross-OTP-major stable            | Yes (NIF)  | Yes                          | **No**                 |
| Single-threaded throughput        | Highest    | Medium                       | Medium                 |
| Output bits                       | 64         | 64 (high half)               | 64 (via mixer)         |
| BEAM-only fallback                | Pure phash2| Pure Murmur3 bundled         | Native                 |
| Used by Apache DataSketches       | No         | Yes                          | No                     |

`XXHash3` is the default because:

1. It is the fastest seedable 64-bit hash with strong statistical properties.
2. The underlying NIF (`xxhash-rust`) is widely audited and SIMD-tuned by upstream.
3. Output is stable across platforms and CPU architectures.

`Murmur3` is provided alongside XXHash3 because:

1. It is the canonical hash of the Apache DataSketches ecosystem, which is
   the long-term interop target (Track 2 in [`plans/next_steps.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/next_steps.md)).
2. A pure Elixir implementation is always available, making it the
   recommended choice when the NIF cannot be loaded but cross-OTP-major
   stability is required.
3. It exposes 128 bits of output (`hash128/2`), useful for fingerprints
   and seed_hash computations.

`phash2` remains the historical default for builds with no Rust NIF, but
is **explicitly marked `:otp_dependent`** in `algorithm_info/1` and is not
recommended for any data that crosses an OTP major version boundary.

`custom` exists so callers may pass a `:hash_fn` closure for specialized
use cases. Sketches built with `:custom` are **never merge-compatible**
with any other sketch (closures cannot be compared structurally). This
restriction is documented and enforced by `validate_options!/3`.

## Merge safety

A merge between two sketches must satisfy ALL of:

1. Same hash algorithm.
2. Same seed.
3. Neither side uses `:custom`.
4. Same sketch family (when comparing metadata blocks).
5. Same sketch family version (when comparing metadata blocks).

Backend (Pure vs Rust) is **NOT** part of the merge equivalence relation —
the whole point of byte-identical parity tests is that two backends agree
on output. Block version is also not part of the relation, so the metadata
block can be evolved without breaking merge.

When any of conditions 1–5 fail, sketch modules raise
`ExDataSketch.Errors.IncompatibleSketchesError` with a human-readable
reason. The error message names the sketch type and the field that
disagreed.

## Why distributed systems fail without explicit hash metadata

In a distributed pipeline, sketches are typically produced on many nodes,
serialized, transported, and merged by a downstream aggregator. Without an
embedded hash identity:

- A rolling cluster upgrade that changes the default hash on half the
  nodes silently corrupts every cross-version merge.
- A library author who adopts `ex_data_sketch` and a custom `:hash_fn`
  cannot detect the day a teammate forgets to pass the closure.
- Switching the BEAM-only build to use `:xxhash3` after a NIF release
  cannot be reconciled with previously-persisted sketches.

With the v0.8.0 metadata block (and the Phase 2 binary header that wraps
it), every persisted sketch carries the exact hashing identity that
produced it. Merges either succeed or fail loudly.

## Stability guarantees (v0.8.0)

- The wire bytes for hash algorithms (`0=phash2`, `1=xxhash3`, `2=murmur3`,
  `255=custom`) are **stable for all v0.x releases**.
- `Hash.Metadata` block version `1` is stable. A future block version
  will be additive (forward-compatible extension bytes only); v1 readers
  must continue to decode v1 binaries identically.
- `XXHash3-64` and `Murmur3_x64_128 high-64` output is stable across all
  platforms, architectures, OTP versions, and v0.x releases of this
  library. Regression tests against canonical Python `mmh3` lock the
  Murmur3 algorithm; the `xxhash-rust` crate version is pinned in
  `Cargo.toml` and bumped only with explicit benchmark + parity review.
- The pure-Elixir `:phash2` fallback's output is **not** guaranteed
  stable across OTP majors — use `:xxhash3` or `:murmur3` for any sketch
  that crosses an OTP version boundary.

## Reviewer Checklist (Phase 1)

Before approving Phase 1, verify:

- [ ] `ExDataSketch.Hash` exposes `default_algorithm/0`,
      `supported_algorithms/0`, `algorithm_info/1`, and they appear in
      `@spec`s and `@type`s.
- [ ] `ExDataSketch.Hash.XXH3`, `ExDataSketch.Hash.Murmur3`,
      `ExDataSketch.Hash.Metadata`, `ExDataSketch.Hash.Validation` are
      present, documented, and have doctests.
- [ ] `ExDataSketch.Hash.validate_merge_hash_compat!/3` remains and
      delegates to the new validation module (backward-compatible shim).
- [ ] The Pure vs Rust Murmur3 parity test passes for ≥200 random inputs.
- [ ] The cross-language Murmur3 regression test (`"hello"` → `0xCBD8A7B341BD9B02`)
      passes; this matches Python `mmh3.hash64(..., signed=False)[0]`.
- [ ] Existing merge-validation tests (in per-sketch test files) still pass.
- [ ] `Hash.Metadata.encode/1` + `decode/1` round-trip is asserted as a
      property.
- [ ] Forward-compatibility: a metadata binary with trailing extension
      bytes round-trips unmodified.
- [ ] Error paths (truncated header, bad algorithm byte, future block
      version, oversized extension) produce structured
      `DeserializationError` results — never crashes.
- [ ] `mix format --check-formatted` clean.
- [ ] `mix credo --strict` clean.
- [ ] `mix test` full suite green with 0 regressions.
- [ ] README roadmap reflects the new v0.8.0–v1.0.0 sequence.

## What Phase 2 will do with this

Phase 2 introduces `ExDataSketch.Binary.Header` and EXSK format v2. The
header will:

1. Embed the `Hash.Metadata` block produced here directly between the
   serialization version byte and the payload size.
2. Wrap everything in a CRC32C-checked frame for corruption detection.
3. Keep v1 EXSK binaries decodable, so existing persisted sketches and
   golden vectors remain valid.

No EXSK changes are made in Phase 1; the metadata module is intentionally
self-contained so it can be reviewed and merged on its own.
