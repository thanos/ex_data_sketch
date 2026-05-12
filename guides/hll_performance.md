# HLL Hot-Path Optimization

This document explains the HLL hot-path architecture introduced (and
extended) by `ex_data_sketch` v0.8.0 Phase 3.

## Why HLL throughput matters

HyperLogLog is the workhorse cardinality sketch. In production it is
called once per stream event, so its update path runs on every record
in the system. A 10x improvement in `update_many/2` directly translates
into a 10x improvement in the upstream pipeline's ceiling. CMS, ULL,
and Theta share the same hot-path structure and benefit from the same
optimizations.

## Why the BEAM-side hot path is slow

For an HLL with `p = 14` (16,384 registers), the steady-state per-item
work is:

1. Encode the item as a binary (`term_to_binary` if not already binary).
2. Compute a 64-bit hash.
3. Split the hash into bucket index (top `p` bits) + remaining bits.
4. Count leading zeros in the remaining bits.
5. Conditionally update one byte in the register array.

Steps 1–4 produce **per-item heap garbage** on the BEAM. Even the
"optimal" pure Elixir path allocates a binary for the hash, a tuple for
the bucket+rank pair, and a few intermediate small integers per item.
At 1 M items the GC pressure dominates everything else, and the bench
numbers in this document confirm it: Pure HLL caps at ~2 M items/sec
while Rust HLL exceeds 30 M items/sec on the same hardware.

## v0.8.0 hot-path architecture

There are four hot paths exposed by v0.8.0:

| # | Path | Backend | Hashing | Used by |
|---|------|---------|---------|---------|
| 1 | Pure Elixir | `Backend.Pure` | Elixir (`phash2 / xxh3 / murmur3`) | NIF-less builds, BEAM-only test envs |
| 2 | Rust non-raw NIF | `Backend.Rust` | Elixir → pass hashes_bin to Rust | Pre-hashed inputs, `:phash2`, `:custom` |
| 3 | Rust raw NIF (XXH3) | `Backend.Rust` | Rust XXH3 inside the NIF | Default `:xxhash3` path |
| 4 | Rust raw_h NIF (Murmur3) | `Backend.Rust` | Rust Murmur3 inside the NIF | `:murmur3` path (new in v0.8.0) |

The four sketches that benefit from this architecture are **HLL, ULL,
Theta, CMS** — the cardinality / point-count family that hashes every
input item.

### Path dispatch logic

Each of the four sketches' `update_many/2` runs this decision tree:

```text
if backend == Backend.Pure:
    # Path 1: pure Elixir, batched
    hashes = chunk |> Enum.map(&hash_item/2)
    Backend.Pure.<sketch>_update_many(state, hashes, opts)

elif backend == Backend.Rust:
    if opts[:hash_fn] != nil:
        # Custom closure: must run on the BEAM. Path 2.
        hashes = chunk |> Enum.map(&hash_item/2)
        Backend.Rust.<sketch>_update_many(state, hashes, opts)

    elif opts[:hash_strategy] == :phash2:
        # phash2 not implemented in Rust. Path 2.
        hashes = chunk |> Enum.map(&hash_item/2)
        Backend.Rust.<sketch>_update_many(state, hashes, opts)

    else:
        # :xxhash3 or :murmur3 → Rust hashing. Path 3 or 4.
        Backend.Rust.<sketch>_update_many_raw(state, chunk, opts)
```

The `<sketch>_update_many_raw/3` Elixir function then dispatches at the
Rust-call level:

- `:xxhash3` → legacy `_raw_nif` (binary-compatible with v0.7.1).
- `:murmur3` → new `_raw_h_nif` with `algorithm=2`.
- Both gain a Dirty-CPU variant once the chunk size exceeds the
  configured threshold (default 10,000 items).

### Why chunking matters

Items arrive at `update_many/2` as an arbitrary enumerable. The
implementation chunks them at 10,000 items per chunk. This serves three
purposes:

1. **Bounded per-call cost** — even with 100M items, each NIF call
   stays bounded to ~30ms wall-clock, well within Erlang's 1 ms
   reduction budget for the dirty scheduler.
2. **Steady allocation** — a 10,000-item `ListIterator` is small enough
   to fit in L2 cache on most platforms.
3. **Predictable scheduler behavior** — the chunk size is the unit
   compared against the configured dirty threshold to choose normal vs
   dirty NIF.

### Why `update_many` is the only acceleration target

The `update/2` (single-item) path is intentionally NOT NIF-accelerated.
A single NIF call has fixed overhead of ~200 ns; for a single item
update this dominates the actual register write (~30 ns). Callers that
want per-item throughput either batch with `update_many/2` or accept
the BEAM-side single-item path. This matches Apache DataSketches' own
single-item-vs-batch trade-off.

## v0.8.0 Phase 3 additions

The Phase 3 work in v0.8.0 is:

1. **Generalized in-Rust hashing.** v0.7.1 shipped a `_raw_nif` family
   that hardcoded XXH3. Phase 3 adds a parallel `_raw_h_nif` family
   that accepts an algorithm byte (`1 = XXH3`, `2 = Murmur3`). The
   legacy XXH3-only NIFs stay so v0.7.x callers see zero regression.
2. **End-to-end Murmur3 support.** `HLL.new(hash_strategy: :murmur3)`
   (and the same for ULL/Theta/CMS) now wires through to the new
   `_raw_h_nif` Rust path. Murmur3 callers no longer fall off the fast
   path. Cross-language interop with Apache DataSketches becomes a
   one-line configuration change at sketch creation time.
3. **`ExDataSketch.Hash.resolve_strategy/1`** — single source of truth
   for sketch constructors. Centralizes the user-opt → strategy mapping
   that all four sketches previously open-coded inconsistently.

What Phase 3 explicitly does NOT do (per the v0.8.0 prompt's
non-goals):

- No SIMD intrinsics (deferred to v0.9 / v1.0).
- No 6-bit register packing (deferred — would require a sketch-family
  version bump and is incompatible with the existing 8-bit-register
  state binary).
- No sparse/dense layout redesign.
- No ARM-specific tuning.
- No raw-NIF migration for membership filters (Bloom/Cuckoo/Quotient/
  CQF/XorFilter/IBLT) — those already cross the NIF boundary with
  pre-hashed integers via `put_many` and would need a separate design
  pass.

## Scheduler safety

Every hot-path NIF is paired with a Dirty-CPU variant:

```
hll_update_many_nif            (normal)   small batches
hll_update_many_dirty_nif      (dirty)    batches above threshold

hll_update_many_raw_nif        (normal)   small batches, XXH3 inside Rust
hll_update_many_raw_dirty_nif  (dirty)    batches above threshold

hll_update_many_raw_h_nif      (normal)   small batches, dispatched hash
hll_update_many_raw_h_dirty_nif (dirty)   batches above threshold
```

The dispatcher in `Backend.Rust` picks dirty when `length(chunk) >
threshold`. Defaults are listed in `lib/ex_data_sketch/backend/rust.ex`
under `@default_thresholds`. They can be overridden per-call (`opts[:dirty_threshold]`)
or globally (`config :ex_data_sketch, :dirty_thresholds, %{...}`).

See [`plans/hll_scheduler_safety.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/hll_scheduler_safety.md) for the full scheduler discussion.

## Measured throughput (Phase 3 benchmark)

The full benchmark is `bench/hll_hot_path_bench.exs`. Below is a
representative run on Apple Silicon (M1 / OTP 27 / Elixir 1.19); your
hardware will vary. Units are items per second computed from the
`ips * batch_size` column.

| Path                | 10,000 items | 100,000 items | 1,000,000 items |
|---------------------|--------------|---------------|------------------|
| Pure Elixir phash2  | ~1.67 M/s    | ~1.81 M/s     | ~2.02 M/s        |
| Pure Elixir xxhash3 | ~1.72 M/s    | ~1.89 M/s     | ~2.10 M/s        |
| Rust raw XXH3       | ~25.8 M/s    | ~29.4 M/s     | ~34.3 M/s        |
| Rust raw_h Murmur3  | ~23.8 M/s    | ~27.8 M/s     | ~31.1 M/s        |

Headline numbers:

- **Rust XXH3 vs Pure xxhash3**: ~15× throughput.
- **Rust XXH3 vs Pure phash2**: ~17× throughput.
- **Rust Murmur3 vs Rust XXH3**: ~92% throughput. The 8% slowdown is
  the cost of Murmur3's full finalizer arithmetic, which is acceptable
  given Murmur3's interop value.
- **Memory**: Rust paths allocate ~10% less than Pure paths because no
  intermediate hashes list is materialized in Elixir.

## How these numbers compare to external HLL implementations

We deliberately do NOT re-benchmark external HLL implementations in
this release. Throughput numbers from upstream documentation:

- **Apache DataSketches HLL (Java)** — ~10–20 M items/sec for `p=14`
  with Java's MurmurHash3. Reference: their JMH suite.
- **`hyperloglog-rs` (Rust)** — ~80–120 M items/sec for `p=14` with
  XxHash3 and SIMD. Reference: crate README.
- **`axiomhq/hyperloglog-go`** — ~25 M items/sec for `p=14`. Reference:
  the Axiom blog post that accompanied the library release.
- **RedisBloom HLL** — single-thread bound by the Redis event loop;
  upstream throughput is on the order of 1 M items/sec including
  network round-trip.

**Our position in the stack:** v0.8.0's Rust HLL is within 2–3× of the
fastest in-process Rust implementation (which uses SIMD and a custom
packed register layout), substantially ahead of Java and Go for the
single-threaded case, and ~30× ahead of RedisBloom when network is
included.

The 2–3× headroom against `hyperloglog-rs` is **deliberate**: closing
that gap requires SIMD-tuned register updates and 6-bit packing, both
of which are deferred to v0.9 / v1.0 per the v0.8.0 prompt's scope.

## Reproducing these numbers

```sh
EX_DATA_SKETCH_BUILD=1 MIX_ENV=dev mix run bench/hll_hot_path_bench.exs
```

Outputs JSON to `bench/output/hll_hot_path_bench.json` for further
analysis.

## References

- `lib/ex_data_sketch/hll.ex`, `lib/ex_data_sketch/ull.ex`,
  `lib/ex_data_sketch/theta.ex`, `lib/ex_data_sketch/cms.ex` — high-
  level `update_many/2` dispatch.
- `lib/ex_data_sketch/backend/rust.ex` — Rust-side dispatch
  (`hll_update_many_raw/3` and friends).
- `native/ex_data_sketch_nif/src/hll.rs` — Rust HLL implementation.
- `native/ex_data_sketch_nif/src/ull.rs`,
  `native/ex_data_sketch_nif/src/theta.rs`,
  `native/ex_data_sketch_nif/src/cms.rs` — sibling sketches.
- `bench/hll_hot_path_bench.exs` — this document's measurement source.
- [`plans/hll_scheduler_safety.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/hll_scheduler_safety.md) — companion scheduler discussion.
- Heule et al. "HyperLogLog in Practice." Google, 2013.
