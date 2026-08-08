# Membership-Filter Raw-Hashing (Phase 6)

This document explains the raw-NIF hashing path added to the six
membership-filter families -- Bloom, Cuckoo, Quotient, CQF, XorFilter,
IBLT -- in v0.10.0 Phase 6, and the measured throughput improvement it
brings.

## Why this existed as a gap

`ExDataSketch.HLL`, `CMS`, `Theta`, and `ULL` have hashed items *inside*
the Rust NIF since v0.8.0 (see `guides/hll_performance.md`): `update_many/2`
hands raw item binaries straight to Rust, which hashes them with XXH3 or
Murmur3 and updates the sketch in the same native call. The six
membership filters did not get this treatment -- `put_many/2` (or
`build/2` for XorFilter) still computed each item's `hash64` in Elixir
via `ExDataSketch.Hash.hash64/1` before crossing the NIF boundary, even
when using `Backend.Rust`. This was flagged as G6 in the v0.9.0 code
review and deferred twice (`baoulo/plans/0.9.0_phase5_carry_forward.md:188`)
because it needed a dedicated phase, not because it was hard -- the
per-family bit-setting, bucket-insertion, and peeling-construction logic
in Rust already existed and was already tested; only the hash computation
needed to move.

## What changed

Each family's `Backend.Rust` callback gained a `_raw` sibling, exactly
mirroring the shape `hll_update_many_raw/3` already established:

- `Bloom.put_many/2` -> `Backend.Rust.bloom_put_many_raw/3`
- `Cuckoo.put_many/2` -> `Backend.Rust.cuckoo_put_many_raw/3`
- `Quotient.put_many/2` -> `Backend.Rust.quotient_put_many_raw/3`
- `CQF.put_many/2` -> `Backend.Rust.cqf_put_many_raw/3`
- `IBLT.put_many/2` -> `Backend.Rust.iblt_put_many_raw/3` (set-mode only,
  matching `update_many/2`'s existing scope -- see `iblt.ex`)
- `XorFilter.build/2` -> `Backend.Rust.xor_build_raw/2` (the one
  structurally different family: construction is a batch peeling
  algorithm over the whole set, not an incremental insert, so there is no
  per-item raw "put", only a raw "build")

Each `_raw` NIF pair (`_raw_nif`/`_raw_dirty_nif` for XXH3,
`_raw_h_nif`/`_raw_h_dirty_nif` for a dispatched algorithm byte) hashes
the raw item bytes with `xxhash_rust::xxh3` or the existing Murmur3
implementation in `native/ex_data_sketch_nif/src/hash.rs`, then feeds the
resulting `hash64` into the **same, unmodified** per-item logic the
existing non-raw NIF already used (Kirsch-Mitzenmacher double-hashing for
Bloom, cuckoo kick-insertion, quotient/CQF slot insertion, IBLT cell-XOR,
xor-filter peeling). No algorithmic Rust code changed -- only where the
hash is computed.

### Path dispatch logic

Identical in shape to the cardinality families' dispatch
(`guides/hll_performance.md`'s "Path dispatch logic" section):

```text
if backend == Backend.Pure:
    hashes = items |> Enum.map(&hash_item/2)
    Backend.Pure.<family>_put_many(state, hashes, opts)

elif backend == Backend.Rust:
    if opts[:hash_fn] != nil or opts[:hash_strategy] == :phash2:
        # Custom closure or phash2: must run on the BEAM.
        hashes = items |> Enum.map(&hash_item/2)
        Backend.Rust.<family>_put_many(state, hashes, opts)
    else:
        # :xxhash3 or :murmur3 -> Rust hashing, raw path.
        Backend.Rust.<family>_put_many_raw(state, items, opts)
```

`:hash_fn` and `:hash_strategy` are honored exactly as before -- a custom
hash closure cannot run inside Rust, so it always falls back to the
pre-hash-in-Elixir path, matching Pure's behavior byte-for-byte
(`test/parity_test.exs` has a dedicated `:hash_fn` parity test per
family, added in this phase).

## Byte-identical parity

`test/parity_test.exs` already had a parity block per filter family
(asserting `Pure.serialize(...) == Rust.serialize(...)` for `put_many`/
`build`, `merge`, and `member?`) under default options -- exactly the
options the new raw dispatch activates under. Once each family's
`put_many/2`/`build/2` was rewired, these existing tests started
exercising the raw path automatically, the same way `"HLL parity"`'s
existing test already covered `hll_update_many_raw`. No new algorithmic
surface needed new correctness tests; only the `:hash_fn` fallback-path
tests were genuinely new (there was no pre-existing coverage of "does the
raw path get correctly skipped" for any raw family, cardinality or
filter).

## Measured throughput

Measured on Apple M1 Max / OTP 29 / Elixir 1.20.2, 10,000 items per
operation, via `bench/filter_raw_hashing_bench.exs`:

| Family | Pure | Rust (pre-hashed, legacy) | Rust (raw) | Raw vs legacy | Raw vs Pure |
|---|---|---|---|---|---|
| Bloom | 9.6 K/s | 4.85 M/s | 16.7 M/s | 3.4x | 1,741x |
| Cuckoo | 348 K/s | 4.94 M/s | 18.2 M/s | 3.7x | 52x |
| Quotient | 8.7 K/s | 3.29 M/s | 7.1 M/s | 2.2x | 820x |
| CQF | 9.1 K/s | 3.27 M/s | 7.2 M/s | 2.2x | 793x |
| IBLT | 6.7 K/s | 1.96 M/s | 9.0 M/s | 4.6x | 1,338x |
| XorFilter (build) | 6.9 K/s | 1.33 M/s | 17.0 M/s | 12.8x | 2,469x |

("Rust (pre-hashed, legacy)" reproduces the pre-Phase-6 path directly
against `Backend.Rust`'s pre-hashed functions, bypassing the new
automatic raw dispatch -- the six per-family `bench/*_bench.exs` files
also carry this scenario permanently, labeled `[Rust (pre-hashed,
legacy)]`, for regression tracking alongside their other operations.)

Headline numbers:

- **Raw vs legacy Rust ranges from 2.2x (Quotient/CQF) to 12.8x
  (XorFilter).** Quotient and CQF's smaller gain makes sense: their
  per-item insertion logic (run-finding, shifting) is more expensive
  relative to hashing than Bloom/Cuckoo's simpler bit/bucket writes, so
  removing the hashing step matters proportionally less. XorFilter's
  outsized gain reflects `build/2`'s legacy path paying for `Enum.uniq/1`
  and a full binary round-trip of already-hashed values in Elixir before
  Rust even starts the peeling construction; raw dispatch skips that
  entirely.
- **Cuckoo's Pure number (348 K/s) is far higher than the other five
  filters' Pure numbers (~7-10 K/s)** at this same 10,000-item, 20,000-
  capacity configuration. This is a genuine property of Cuckoo's simpler
  per-item Pure path (mostly direct binary bit-twiddling, low load factor
  so few kicks), not a benchmark artifact -- also reflected in Cuckoo
  having the smallest raw-vs-Pure ratio (52x) of the six families.
- All six families now sit in the same throughput class as HLL/CMS/
  Theta/ULL's raw paths (`guides/hll_performance.md` measured 25-34 M
  items/s for HLL specifically; the filters' per-item work is heavier
  than a single register write, so 7-18 M items/s here is the expected
  relative position, not a regression).

## Reproducing these numbers

```sh
MIX_ENV=dev mix run bench/filter_raw_hashing_bench.exs
```

For per-family detail across more operations (`merge`, `member?`,
`serialize`, and so on, at both 1k and 100k item scales), run the
individual family benchmarks, each of which now also includes the
`[Rust (pre-hashed, legacy)]` comparison scenario for its `put_many`/
`build` entries:

```sh
MIX_ENV=dev mix run bench/bloom_bench.exs
MIX_ENV=dev mix run bench/cuckoo_bench.exs
MIX_ENV=dev mix run bench/quotient_bench.exs
MIX_ENV=dev mix run bench/cqf_bench.exs
MIX_ENV=dev mix run bench/iblt_bench.exs
MIX_ENV=dev mix run bench/xor_filter_bench.exs
```

Outputs JSON to `bench/output/*.json` for further analysis.

## References

- `lib/ex_data_sketch/bloom.ex`, `cuckoo.ex`, `quotient.ex`, `cqf.ex`,
  `iblt.ex`, `xor_filter.ex` -- high-level `put_many/2`/`build/2` raw
  dispatch (mirroring `hll.ex`'s `update_many/2`).
- `lib/ex_data_sketch/backend/rust.ex` -- Rust-side dispatch
  (`bloom_put_many_raw/3` and siblings).
- `native/ex_data_sketch_nif/src/bloom.rs`, `cuckoo.rs`, `quotient.rs`,
  `cqf.rs`, `iblt.rs`, `xor_filter.rs` -- Rust implementations; each
  family's per-item logic is factored into a shared helper
  (`bloom_set_bits`, `cko_insert_one`, `iblt_insert_one`,
  `xor_build_from_hashes`, and so on) called identically by the
  pre-hashed and raw paths.
- `test/parity_test.exs` -- byte-identical Pure/Rust parity, including
  the `:hash_fn` fallback-path tests added in this phase.
- `bench/filter_raw_hashing_bench.exs` -- this document's measurement
  source.
- `guides/hll_performance.md` -- the sibling document for HLL/CMS/Theta/
  ULL's raw-hashing architecture, which this phase's design directly
  extends.
- `baoulo/plans/0.10.0_phase6_design_review.md` -- the full phase design
  review.
