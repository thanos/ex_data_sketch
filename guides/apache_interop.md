# Apache DataSketches Interop

This guide is the single reference for what interoperates with Apache
DataSketches (Java, C++, Python) today, what doesn't, and why. For the
overall serialization compatibility contract (versioning, read/write
promises, EXSK format stability), see `serialization_compatibility.md`.

## What interoperates

| Sketch | Function | Format |
|---|---|---|
| `ExDataSketch.Theta` | `serialize_datasketches/2`, `deserialize_datasketches/2` | CompactSketch |
| `ExDataSketch.KLL` | `serialize_datasketches/2`, `deserialize_datasketches/2` | Compact `KllFloatsSketch`/`KllDoublesSketch` |

## What doesn't (and why)

| Sketch | Status |
|---|---|
| `ExDataSketch.HLL` | Planned for v0.11.0. Blocked on hash-function equality: DataSketches HLL is a union of four internal encodings (LIST/SET/HLL_4/HLL_6/HLL_8) keyed to MurmurHash3, and `ExDataSketch.Hash.hash64/1` is a different hash function -- unlike KLL, HLL's registers are *derived from* hashed items, so decoding a foreign binary requires reconciling hash semantics, not just binary layout. |
| `ExDataSketch.CMS` | Not planned. Apache DataSketches does not define a standard CMS binary format at all. |
| Everything else (`Bloom`, `Cuckoo`, `Quotient`, `CQF`, `XorFilter`, `IBLT`, `FilterChain`, `REQ`, `MisraGries`, `ULL`, `DDSketch`, `Quantiles`, `FrequentItems`) | ex_data_sketch-native (EXSK) only. Most of these families either have no Apache DataSketches equivalent, or (for the membership filters) Apache DataSketches doesn't ship them at all. |

## The hash caveat (Theta) vs. no caveat (KLL)

`ExDataSketch.Theta` hashes items with `ExDataSketch.Hash.hash64/1`
(`:erlang.phash2` + Murmur finalization), while Apache DataSketches Theta
uses MurmurHash3_x64_128. These are **not** cross-compatible -- the same
input string produces different hash values under each function. Theta
interop therefore works at the *binary/estimate* level only: a serialized
Theta sketch already contains pre-computed hash values, so it can be
deserialized and merged regardless of which hash function originally
produced it, but you cannot ask "does this decoded sketch contain the
string `"hello"`?" and get a meaningful answer -- only cardinality
estimates and set-operation results are portable. See
`ExDataSketch.DataSketches.CompactSketch`'s moduledoc for the full
explanation.

`ExDataSketch.KLL` does not hash anything -- it stores the raw numeric
values it was given. This makes KLL interop a full **item-level** round
trip: a `KllDoublesSketch` built in Java and decoded by
`KLL.deserialize_datasketches/2` in Elixir answers `quantile/2`, `rank/2`,
`min_value/1`, and `max_value/1` using the literal retained values Java
selected, with no hash-equality caveat at all. This is what makes KLL
interop tractable in a single phase where HLL is not (see
`ExDataSketch.KLL`'s moduledoc, "Apache DataSketches Interop" section).

## KLL specifics

### `:variant` -- float vs. double

Apache's on-disk KLL format does not self-describe item width. Choosing
between `KllFloatsSketch` (4-byte items) and `KllDoublesSketch` (8-byte
items) is the caller's responsibility in the Java/C++/Python APIs too --
there is no `heapify()` that auto-detects. `serialize_datasketches/2` and
`deserialize_datasketches/2` take a `:variant` option (`:float` or
`:double`, default `:double`) for the same reason. Passing the wrong
variant on decode will *usually* (not always -- it depends on whether the
item-count arithmetic happens to still divide evenly under the wrong
width) surface as a `DeserializationError`, not silently produce garbage;
see `ExDataSketch.DataSketches.KLLSketch`'s moduledoc for the exact
mechanism.

### `M` restriction

Apache's KLL format has a configurable minimum-level-capacity parameter
`M` (default 8). `ExDataSketch.KLL` only ever produces or accepts the
default -- a binary with a non-default `M` is rejected with a clear
error, matching `CompactSketch`'s precedent of explicitly rejecting
unsupported structural variants rather than guessing.

### Compact only

Only the compact, read-only structure is supported (the same restriction
`CompactSketch` applies to Theta). The "updatable" KLL structure
(`SerVer == 3`) is rejected with a clear error.

### Capacity-formula compatibility on encode

Apache's compact writer positions retained items at the *tail* of a
virtual buffer sized for the theoretical full (non-compact) capacity of
`(k, M, num_levels)` -- real readers (see `datasketches-cpp`'s
`kll_helper::compute_total_capacity`) recompute that capacity from the
preamble fields alone, not from the file's byte length. `ExDataSketch.KLL`'s
own internal compaction schedule is unrelated to Apache's and can retain
more items at a given level count than Apache's formula would allow
there, so `serialize_datasketches/2` replicates Apache's capacity formula
internally and pads with empty top levels as needed before writing. This
was verified against the real `datasketches` Python package (not just
against our own decoder) across a sweep of item counts and `k` values
from 0 to 500,000 -- see the KLL interop fixture generation history for
details.

## Fixture provenance

`test/fixtures/interop/kll/` contains golden binaries produced by the
real `datasketches` PyPI package (Apache's official Python bindings,
pinned to a specific version -- see the README in that directory) rather
than hand-crafted bytes. This is what actually executes the process
`test/vectors/CROSS_LANGUAGE.md` documents for Theta but which, as of
this writing, has never been run for any family until KLL's fixtures.
