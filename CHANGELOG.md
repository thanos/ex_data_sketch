# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.2] - 2026-08-14

Started as post-release fixes found via the same manual livebook-
verification process used for v0.10.1 (`Cuckoo` eviction cycling,
`Quotient`/`CQF` full-table decode on every call, `CQF` silent
overflow, `REQ`'s KLL-shared compaction bug); grew to include
Application-config-driven per-family defaults and a `FilterChain`
batching fix.

### Added

- **`ExDataSketch.Config`** -- per-family default option overrides via a
  single flat Application config key, mirroring the existing
  `config :ex_data_sketch, backend: ...` pattern:

  ```elixir
  config :ex_data_sketch,
    defaults: [
      hll: [p: 16],
      cqf: [q: 20, r: 10],
      bloom: [capacity: 50_000]
    ]
  ```

  Every family's `new/1` (or `build/2` for `ExDataSketch.XorFilter`) now
  merges its configured defaults underneath whatever options are
  explicitly passed -- explicit options always win. `FilterChain.new/0`
  takes no options at all, so it has no corresponding `:filter_chain`
  entry. See the "Configuring Per-Family Defaults" section of
  `guides/usage_guide.md` and `ExDataSketch.Config`'s moduledoc.

- **`ExDataSketch.Quotient.member_many?/2`** -- tests membership for
  multiple items in a single pass, decoding the filter's header once
  instead of once per item (mirrors the existing `put_many/2` batch
  shape). Prefer this over `Enum.map(items, &member?(filter, &1))` when
  checking many items against the same filter, e.g. measuring
  false-positive rate over a large novel set.

- **`ExDataSketch.CQF.put!/2`** -- inserts a single item, raising
  `ExDataSketch.Errors.FilterFullError` on overflow instead of returning
  an error tuple. Added for chaining convenience
  (`new() |> put!(...) |> put!(...)`), mirroring `ExDataSketch.Cuckoo`.

- **`ExDataSketch.FilterChain.put_many/2`** -- batches an insert through
  each stage's own `put_many/2` (Rust-accelerated where that stage's
  backend supports it) instead of looping single-item `put/2` calls.
  Returns `{:ok, chain}` or `{:error, :full, partial_chain}` (stages
  before the failure fully applied). `update_many/2` now delegates to
  it (see Changed below). Added `:put_many` to `capabilities/0`.

### Changed

- **BREAKING: `ExDataSketch.FilterChain.delete/2` now returns a bare
  `t()` instead of `{:ok, t()}`.** It never had a real error case to
  report -- a per-stage "item not found" is already absorbed as a no-op
  by `delete_stage/2`, and a stage that doesn't support deletion raises
  `UnsupportedOperationError` upfront rather than returning an error
  tuple. Existing callers doing `{:ok, chain} = FilterChain.delete(...)`
  need to drop the pattern match: `chain = FilterChain.delete(...)`.
- **`ExDataSketch.FilterChain.update_many/2` now delegates to the new
  `put_many/2`** instead of reducing over single-item `update/2` calls.
  The old implementation forced every stage onto the Pure backend
  (no family has a per-item Rust NIF) and reconstructed each stage's
  entire state binary per item, making a large `update_many/2` call
  orders of magnitude slower than necessary regardless of the
  configured `:backend`; behavior (raises `FilterFullError` on overflow)
  is unchanged, only the cost.

- **BREAKING: `ExDataSketch.CQF.put/2` and `put_many/2` now return
  `{:ok, cqf}` / `{:ok, cqf} | {:error, :full, partial_cqf}` instead of
  a bare `cqf`**, mirroring `ExDataSketch.Cuckoo`'s existing contract.
  This is the other half of the fix described below -- detecting
  overflow requires a way to report it. `update/2` and `update_many/2`
  (the generic `ExDataSketch.Sketch` behaviour callbacks, which must
  return a bare sketch) now raise `ExDataSketch.Errors.FilterFullError`
  on overflow instead, matching Cuckoo's `update/2`/`update_many/2`.
  `from_enumerable/2` likewise now returns `{:ok, cqf} | {:error, :full,
  partial_cqf}`. Existing callers using `|>` chains with `put/2` or
  `put_many/2` need to switch to `put!/2` or explicit `{:ok, cqf} = ...`
  pattern matching.

### Fixed

- **`ExDataSketch.CQF` silently dropped inserts once its table filled
  up, in both backends, discovered while investigating why a livebook's
  `put_many/2` call over a 1,000,000-event dataset was taking far longer
  than expected (see the sizing fix below for that side of the
  investigation).** CQF shares its slot layout and shift-right insertion
  machinery with `ExDataSketch.Quotient`; that machinery's cascade loop
  was bounded (to `slot_count` steps, so it always terminates rather
  than looping forever on a full table) but had no way to report back
  that it hadn't found room -- it just stopped, silently leaving the
  requested item uninserted. Unlike `ExDataSketch.Cuckoo`, which already
  had `{:error, :full}` for exactly this situation, CQF (and Quotient)
  had no such signal at all. Fixed for CQF by threading a success/failure
  result through the insertion call chain in both the Pure backend
  (immutable, so a failed attempt's partial mutations are simply never
  bound and thus discarded automatically) and the Rust NIF (mutates
  `Vec<Slot>` in place, so each insertion path now checks whether its
  shift-right cascade would fit *before* applying any mutation --
  including preparatory metadata-bit changes -- rather than mutating
  first and discovering failure partway through, which would have left
  the table in a structurally inconsistent state for other entries, not
  just failed the one insert cleanly). Verified: Pure and Rust report
  `:full` at the identical point and produce byte-identical partial
  state for the same over-capacity input; every item inserted before the
  failure point remains a correctly-retrievable member; round-tripping
  the partial state through serialize/deserialize preserves it exactly.
  `ExDataSketch.Quotient`'s equivalent internal fix is present too (same
  shared machinery), but surfaces as a safe no-op (a full table simply
  stops growing) rather than a public `{:error, :full}`, since Quotient's
  `put/2`/`put_many/2` API wasn't part of this change -- previously,
  Quotient's Pure backend could genuinely hang forever inserting into an
  exactly-100%-full table (no bound existed on that loop at all); it now
  terminates safely. See `ExDataSketch.CQF`'s and
  `ExDataSketch.Quotient`'s moduledocs for details.

- **`ExDataSketch.CQF.put_many/2` (and the tutorial's other full-dataset
  cells) could take minutes to hours because the demo's `q: 18` sized
  the table for 50,000 *distinct* keys when it needed to be sized for
  1,000,000 total occurrences -- CQF spends one physical slot per
  occurrence of an item, not per distinct item (see `ExDataSketch.CQF`'s
  "Counter Encoding" and "Sizing `:q`" moduledoc sections, the latter
  also corrected here: it previously described a compact "bracketing"
  counter scheme that was never actually implemented in either backend
  -- the real representation is, and always was, one physical slot per
  duplicate occurrence).** Confirmed: `q: 18` (262,144 slots, ~25% of
  the 1,000,000 occurrences the demo dataset needs) ran for over 60
  minutes without finishing `put_many/2`; `q: 21` (2,097,152 slots, ~2x
  headroom) finishes the identical call in under a second. Fixed the
  livebook's `q` values and added a "Sizing: q must budget for total
  occurrences, not distinct keys" section explaining why, with a worked
  example.

- **`ExDataSketch.REQ` had the same compaction weight-preservation bug as
  `ExDataSketch.KLL`'s v0.10.1 fix, independently discovered via the same
  manual livebook-verification process.** `req_compact_level`'s biased
  compaction promotes half of whichever portion (upper for LRA, lower for
  HRA) is being compacted, at double the weight; that only preserves
  total weight when the portion being halved has an even length. Portion
  length is `div(n, 2)`, frequently odd, so -- exactly as with KLL --
  every odd-length compaction silently gained or lost one item's worth of
  weight, corrupting the `sum(retained_weight) == n` invariant that
  `quantile/2` depends on. Confirmed via the same weight-invariant check
  used for the KLL fix (drift growing with `n`, both HRA and LRA
  affected identically since the bug is in the shared halving step, not
  the HRA/LRA bias itself). Symptom: HRA and LRA modes could produce
  visibly wrong relative behavior (e.g. HRA -- which is supposed to bias
  accuracy toward high ranks -- performing *worse* than LRA at p99.9).
  Fixed with the same technique as KLL: hold back one item (unweighted,
  for a future compaction) whenever the portion being compacted has an
  odd length. Verified the weight invariant now holds exactly, and that
  HRA is measurably more accurate than LRA at high ranks once again
  (previously, at small `k`, the bug made the two modes nearly
  indistinguishable). No binary format change -- old serialized sketches
  still decode and work. See `ExDataSketch.REQ`'s moduledoc for why,
  similarly to KLL, *value* error at a specific query can still be large
  near a sharp change in data density even with this fixed -- that part
  is inherent to rank-approximate sketches, not a bug.

- **`ExDataSketch.Cuckoo` could spuriously return `{:error, :full, ...}`
  well below its designed ~95.5% load factor, in both the Pure and Rust
  backends, discovered via the same manual livebook-verification
  process.** The kick-eviction loop chose which slot within a bucket to
  evict with a plain `rem(fingerprint + kick_count, bucket_size)` -- a
  linear function of both inputs. With only `2^fingerprint_size` possible
  fingerprint values (256 for the default 8-bit size) shared across a much
  larger item count, two colliding fingerprints that happened to differ by
  a multiple of `bucket_size` could synchronize the kick sequence into a
  short, exactly-repeating cycle among a handful of buckets, burning
  through every remaining kick without ever finding an empty slot.
  Confirmed via direct source-level tracing: a real dataset (500,000
  sequential `"session_N"` keys into `Cuckoo.new(capacity: 500_000)`) hit
  a 4-step cycle between 3 buckets that exhausted `:max_kicks` every time,
  well below the table's intended capacity. Fixed by routing the evicted
  fingerprint and kick count through the same hash already used for
  fingerprint mixing before reducing mod `bucket_size`, which keeps slot
  choice fully deterministic (same input sequence still always produces
  the same sketch state) while eliminating the arithmetic periodicity
  that caused the cycling. Ported identically to the Rust NIF backend;
  confirmed the two backends still produce byte-identical serialized
  state for identical input. Because the eviction-slot formula changed,
  any Cuckoo filter build that exercises kick-eviction now produces a
  different (but now non-cyclic) final state than before -- this is a
  behavior change, not a binary format change; old serialized sketches
  still decode and work. See `ExDataSketch.Cuckoo`'s moduledoc for
  details.

- **`ExDataSketch.Quotient.member?/2` (and `delete/2`'s lookup phase)
  could take minutes for what should be a near-instant check, discovered
  via the same manual livebook-verification process.** Both always
  decoded the *entire* slot table (`2^q` slots) into a tuple before
  reading the one run they actually needed, regardless of which
  `:backend` was configured -- there is no per-item Rust NIF for these
  (only `put_many/2` has one), so every call paid this cost. Confirmed:
  300,000 sequential `member?/2` calls against a `q: 19` (524,288-slot)
  filter took roughly 40 minutes. Fixed by adding a lazy decode path that
  parses only the 32-byte header and reads individual slots directly
  from the binary at their byte offset on demand, so a lookup now costs
  O(run length) -- typically a handful of slots -- instead of
  O(slot_count). `delete/2` additionally now skips the encode round-trip
  entirely when the item isn't present. Confirmed: the same 300,000
  calls now take ~0.13s total. No format or behavior change -- same
  results, just fast. See `ExDataSketch.Quotient`'s moduledoc for
  details.

- **`ExDataSketch.CQF.member?/2`, `estimate_count/2`, and `delete/2` had
  the identical bug**, found while investigating the same class of issue
  for `ExDataSketch.Quotient` above (CQF shares Quotient's slot layout
  and decode/encode machinery). Confirmed: ~190ms per single `member?/2`
  call against a `q: 18` (262,144-slot) filter, purely from the
  full-table decode. Fixed the same way. See `ExDataSketch.CQF`'s
  moduledoc for details.

- `livebooks/sketches/.verify_extract.exs`, the harness that
  automatically re-runs every tutorial livebook's cells to catch
  regressions, extracted every cell except the `Mix.install` cell
  itself -- so a livebook's own `config:` block (e.g. `backend:
  ExDataSketch.Backend.Rust`) was silently never applied during
  verification, and every livebook ran on the Pure backend regardless
  of what it configured. This is how the `FilterChain.put_many/2`
  slowness above went unnoticed by the harness. Fixed by parsing the
  `Mix.install` cell's `config:` option via `Code.string_to_quoted/1`
  and applying it via `Application.put_env/3` before the remaining
  cells run.

- `ExDataSketch.REQ`'s moduledoc now documents that it has no NIF
  acceleration (`ExDataSketch.Backend.Rust`'s `req_*` functions are a
  thin pass-through to `ExDataSketch.Backend.Pure`), matching the note
  `ExDataSketch.MisraGries` already carried -- previously undocumented,
  so `:backend` silently had no effect on REQ's performance either way.

- **`ExDataSketch.Bloom.put_many/2`'s Pure backend was asymptotically
  *slower* than looping single-item `put/2`, not faster, for any batch
  small relative to the filter's bit-array size -- caught by the new
  `FilterChain.put_many/2` performance-regression test flaking on CI
  (passed locally, intermittently exceeded its bound on GitHub's slower
  runners) rather than by a functional test, since the output was
  always correct.** The implementation converted the bitset to an
  Erlang tuple with the stated intent of getting O(1) destructive
  updates via `put_elem/3` in a reduce loop -- but that optimization
  requires the tuple to have a single owner at the point of update, a
  condition `Enum.reduce/3`'s closure-based iteration does not reliably
  preserve. Each `put_elem/3` call was silently a full O(bit_array_size)
  tuple copy, making a batch of `n` items with `hash_count` hashes each
  cost O(n * hash_count * bit_array_size) instead of the intended
  O(n * hash_count). Confirmed: 1,000 items into a 500,000-capacity
  Bloom (~300,000-byte bitset) took 1.8-2.6s -- slower than the 1,000
  single-item `put/2` calls it was meant to beat (~0.25s). Fixed by
  collecting the scattered bit positions into a `byte_index => or_mask`
  map first, then applying it in a single linear pass over the bitset,
  which is genuinely O(n * hash_count + bit_array_size): the same
  1,000-item batch now takes ~80ms, both well under the single-item
  loop and, unlike the tuple approach, actually scales as intended.
  Verified byte-identical output to the pre-fix implementation and to
  an equivalent single-item `put/2` loop. This bug predates v0.10.2 (it
  shipped with the original Bloom filter in v0.4.0); it was only
  surfaced now because `FilterChain.put_many/2` (new in this release)
  is the first caller to exercise `Bloom.put_many/2` at a capacity
  large enough, with a batch small enough relative to it, to make the
  quadratic-ish cost visible in a timed test.

## [0.10.1] - 2026-08-11

Started as post-release fixes from a full code review of the v0.10.0 diff
(`baoulo/reviews/0.10.0_code_review.md`); grew to include two significant
correctness fixes found via manual livebook verification (`ULL`, `KLL`),
a precision-range widening (`HLL`), and 16 new per-family tutorial
Livebooks plus a runnable Phoenix demo app.

### Added

- `livebooks/sketches/` -- one tutorial livebook per sketch family (16
  total: `hll`, `ull`, `cms`, `theta`, `kll`, `ddsketch`, `req`,
  `frequent_items`, `misra_gries`, `bloom`, `cuckoo`, `quotient`, `cqf`,
  `xor_filter`, `iblt`, `filter_chain`). Each generates its own sample
  data and caches it under `System.tmp_dir!()`, regenerating only if the
  cache file is missing, so re-running a livebook (or all 16 in sequence)
  after the first pass is fast.
- `phoenix_demo/` -- a minimal, real, runnable Phoenix app demonstrating
  `ExDataSketch.LiveDashboard.Page` and `ExDataSketch.Telemetry.Metrics.all/1`
  wired into an actual `router.ex`/`telemetry.ex`, plus a live homepage
  backed by two supervised `ExDataSketch.Server` instances and a
  background traffic simulator. See `phoenix_demo/README.md`.
- `guides/telemetry.md`: a "Window Events" and a "Server Events" section
  (`[:ex_data_sketch, :window, :roll]` and the five `:server` events),
  missing since those categories shipped -- the guide previously only
  documented `:sketch`/`:persistence`/`:stream`/`:pipeline`. Also adds a
  `:window, :roll` handler example (migrated from the now-removed
  `rolling_telemetry.livemd`).

### Fixed

- **`ExDataSketch.KLL`'s compaction violated its own weight-preservation
  invariant, corrupting quantile/rank accuracy for any sketch that ever
  compacted an odd-length level.** `kll_compact_level` (both the Pure
  Elixir and Rust NIF backends) cleared an *entire* level and promoted
  only half its items regardless of parity; for an odd-length level, that
  either promotes `ceil(len/2)` or `floor(len/2)` items, and neither
  equals `len/2` exactly -- so the "half the items at double the weight"
  step that's supposed to exactly preserve total weight instead silently
  gains or loses one item's worth of weight (`2^level`) every time. Level
  capacities (`max(2, floor(k * (2/3)^depth) + 1)`) are frequently odd, so
  this was the common case, not an edge case: the sum of retained sample
  weights (which `quantile/2`/`rank/2` divide by to compute a target rank)
  drifted further from the true item count `n` with every such
  compaction -- confirmed to reach >10% relative drift by 1,000,000
  inserts in one measurement. The result was wildly incorrect, and
  non-monotonic in `k`, on any distribution with a sharp density change
  near the queried rank (uniform data was largely unaffected; a synthetic
  99%-base/1%-tail mixture queried at p99 showed errors from tens to
  hundreds of percent, inconsistently across `k`). Fixed by holding back
  one item (leaving it at the current level, unweighted, for a future
  compaction) whenever a level has an odd length, so the actually-compacted
  subset always has even length and total weight is preserved exactly --
  verified by checking `sum(retained_weight) == n` holds exactly across
  `n` from 1,000 to 1,000,000 (previously off by up to +104,235 at
  `n=1,000,000`), and by confirming Pure and Rust backends now produce
  identical quantile estimates. No binary format change -- old serialized
  sketches still decode and work, they just carry forward whatever
  inaccuracy was baked in at serialization time; newly-built sketches are
  correct going forward. See `ExDataSketch.KLL`'s moduledoc for why *value*
  error (as opposed to the documented *rank* error bound) can still be
  large when querying exactly at a distribution's density cliff -- that
  part is inherent to rank-approximate sketches generally, not this bug.

- **`ExDataSketch.ULL` implemented the wrong algorithm.** Both backends'
  register update/merge/estimate were an HLL-derived approximation (a
  single sub-bucket bit plus Ertl 2017's HLL sigma/tau estimator), not
  Ertl 2023's actual UltraLogLog. It matched real UltraLogLog closely at
  low cardinalities (where every prior test and property check exercised
  it, up to `n=10,000` at `p=14`) but diverged sharply once every register
  had been touched at least once (`zeros == 0`, the FGRA branch) --
  overestimating by orders of magnitude at moderate-to-large `n` relative
  to `m`, at every precision. Both backends are now a direct, numerically
  verified port of hash4j's `UltraLogLog.java` (the reference Ertl 2023
  implementation): the real pack/unpack register encoding (a compressed
  3-bit window per byte: geometric rank plus a 2-bit sub-bucket
  refinement) and the real `OptimalFGRAEstimator` (closed-form
  small-range/large-range correction terms plus a 236-entry per-register
  contribution table, combined via `sum^(-1/tau) * factor[p]`). Verified
  byte-for-byte (register state) and to within `1e-6` relative error
  (estimate) against a compiled Java reference across `p` in
  `{10,12,14,16}` and `n` up to 2,000,000, plus insertion-order
  independence and merge-vs-single-sketch equivalence checks. Measured
  accuracy is now `~0.70/sqrt(m)` RSE (previously documented as
  `~0.835/sqrt(m)`, a number derived from the wrong estimator) -- about
  30% better than HLL's `~1.04/sqrt(m)`, not the previously claimed ~20%.
  The `ULL1` state binary format version is bumped 1 -> 2; version-1
  binaries are rejected on decode with a clear error (see Migration).

- `ExDataSketch.Telemetry.Metrics.all/1`'s two `counter` metrics
  (`sketch.ingest.count`, `stream.reduce.count`) never actually fired in
  any real `Telemetry.Metrics` reporter (Phoenix LiveDashboard included).
  `event_counter/4` relied on `Telemetry.Metrics.counter/2`'s implicit
  `:measurement` default (the metric name's own last segment, `:count`),
  but no `ExDataSketch` event carries a `:count` key in its measurements
  (`sketch.ingest`'s are `:duration`/`:size_bytes`; `stream.reduce` has
  none at all) -- per `counter/2`'s own docs, "the measurement must still
  be available in the event, otherwise the event is not accounted for."
  Both counters now use an explicit constant `:measurement` function so
  they actually count every occurrence, regardless of that event's real
  measurement keys.

- **Critical:** `ExDataSketch.Server` now traps exits, so a supervisor-
  initiated shutdown (not just an explicit `GenServer.stop/2`) snapshots
  before terminating, per its documented graceful-shutdown guarantee.
- **Critical:** the `opencode` GitHub Actions workflow no longer runs
  untrusted PR-comment triggers with base-repo secrets against an
  unpinned third-party action; trigger is now restricted to
  owner/member/collaborator comments and the action is pinned to a commit
  SHA.
- `Storage.merge/3` (ETS, DETS, CubDB, Mnesia, Ecto) returns `{:error,
  _}` instead of crashing when the stored binary is corrupted or from an
  incompatible sketch version.
- `ExDataSketch.Server` no longer crashes if a snapshot backend raises;
  the failure is caught and reported via the new
  `[:ex_data_sketch, :server, :snapshot_failed]` telemetry event instead.
- `ExDataSketch.CMS.capabilities/0` no longer falsely claims `:estimate`
  (CMS only supports point queries via `CMS.estimate/2`, not the generic
  single-value `estimate/1`).
- GenStage integration modules (`SketchConsumer`, `SketchProducer`,
  `SketchStage`) now honor `config :ex_data_sketch, :integrations,
  gen_stage: false` instead of silently ignoring it.
- **`:hash_strategy` was silently dropped by `new/1`/`build/2` across all
  six membership-filter families (`Bloom`, `Cuckoo`, `Quotient`, `CQF`,
  `IBLT`, `XorFilter`)**, making the option a no-op: hashing always fell
  back to the NIF-availability default regardless of what was requested,
  and the `:v1`-format "requires `:phash2`" guard always passed
  trivially as a result. `:hash_strategy` (including `:murmur3`, used for
  Apache DataSketches interop) is now resolved via
  `ExDataSketch.Hash.resolve_strategy/1` at build time and actually
  honored, matching `ExDataSketch.HLL`'s existing behavior.
- The same six filter families now restore `:hash_strategy` from the EXSK
  v2 metadata block on `deserialize/1` (previously it was discarded, so a
  filter built with a non-default hash strategy and round-tripped through
  serialize/deserialize would silently query membership with the wrong
  algorithm afterward).
- `ExDataSketch.merge/2` on an `ExDataSketch.Window` and
  `ExDataSketch.merge_many/1` on an empty list now raise the library's
  own typed errors instead of a raw `UndefinedFunctionError`/
  `FunctionClauseError`.
- The Apache DataSketches KLL decoder (`deserialize_datasketches/2`) now
  validates that `n` is not smaller than the reconstructed retained-item
  count, and that level boundaries are monotonically non-decreasing,
  rejecting corrupted input with a `DeserializationError` instead of
  crashing or silently misreading it.
- `ExDataSketch.Cuckoo.put!/2`/`update_many/2` and
  `ExDataSketch.FilterChain.update/2` now raise
  `ExDataSketch.Errors.FilterFullError` instead of a bare `RuntimeError`
  when the underlying filter is full.
- Fixed a `Storage.resolve_backend/1` ambiguity: a bare 2-tuple ref whose
  first element isn't an atom implementing the `Storage` behaviour is now
  resolved as a whole against the configured default backend, instead of
  being misread as an explicit `{backend_module, ref}` pair.

### Removed

- `livebooks/livedashboard_integration.livemd` and
  `livebooks/phoenix_observability.livemd`, superseded by `phoenix_demo/`
  -- both consisted mostly of commented-out router/application pseudocode
  where `phoenix_demo` has working code.
- `livebooks/rolling_telemetry.livemd` -- its `ExDataSketch.Window` content
  (basic usage, deterministic testing, persistence) was already covered
  in more depth by `guides/windowing.md`; its telemetry section moved to
  `guides/telemetry.md`; its live-dashboard demonstration is superseded
  by `phoenix_demo/`.

### Changed

- **`ExDataSketch.HLL`'s maximum precision raised from `p=16` to `p=26`**,
  matching `ExDataSketch.ULL`'s range. Investigation found no algorithmic
  reason for the old `p<=16` ceiling: registers are a plain byte each (no
  bit-packing to overflow), and the `alpha(m)` bias-correction constant's
  general formula (`0.7213 / (1 + 1.079/m)`) is valid for any `m >= 128`
  -- it was simply never raised. `p>=4` remains a hard floor: `alpha(m)`
  only has defined cases for `m = 2^p in {16, 32, 64}` plus the general
  formula for `m >= 128`, which together cover `p >= 4` exactly. See
  `ExDataSketch.HLL`'s new "Precision Range" moduledoc section (and
  `ExDataSketch.ULL`'s, added for contrast -- ULL's own `p<=26` ceiling
  *is* a hard limit, bounded by a 24-entry estimator lookup table).
- `lib/ex_data_sketch/telemetry.ex`'s own moduledoc claimed `:ingest`'s
  `size_bytes` measurement was "HLL only" -- wrong in both directions: 13
  of the 14 families that emit `:ingest` report it (all but `Cuckoo`,
  whose `put_many/2` returns a tagged tuple rather than a bare sketch),
  and `XorFilter`/`FilterChain` don't emit `:ingest` at all. Corrected to
  describe actual per-family coverage.
- Raised the minimum supported Elixir version from `~> 1.15` to `~> 1.18`
  to match what CI actually tests (the 1.15-1.17 floor was never
  exercised by any CI leg).
- Corrected `ExDataSketch.Storage.DETS`'s documentation, which previously
  overstated `merge/3`'s atomicity under concurrent writers; it performs
  the same non-atomic read-modify-write cycle as
  `ExDataSketch.Storage.ETS`.
- Corrected the `ExDataSketch.Window` and `guides/windowing.md`
  explanation of tumbling-window history bounds, which had the
  start-of-slot/end-of-slot cases inverted.
- `guides/observability.md` now documents
  `ExDataSketch.Telemetry.Metrics.all/1` and
  `ExDataSketch.LiveDashboard.Page` (added in the v0.10.0 Phase 5 work)
  instead of the hand-written `:telemetry.attach`/`Telemetry.Metrics`
  snippets they were built to replace.
- Minor documentation corrections: `ExDataSketch.Sketch`'s claim about
  how `SketchConsumer` dispatches, `Storage`'s `merge/3` callback doc
  (which grouped `ExDataSketch.Storage.CubDB` with the non-atomic
  backends even though its own `merge/3` uses a `CubDB.transaction/2`),
  `Storage`'s `child_spec/1` callback doc (implied `CubDB` implements it;
  none of the five shipped backends do), and the bare-ref resolution
  error message (previously said "no backend module given" even when a
  structurally plausible but invalid one was given).

### Migration

- **`ExDataSketch.ULL` binary format bump (v1 -> v2).** Sketches
  serialized by prior releases will fail to decode with
  `"unsupported ULL state version 1, expected 2"` rather than silently
  producing the old, significantly overestimated cardinality. This is
  intentional: the register encoding itself changed (not just a wrapper),
  so there is no way to reinterpret old state correctly. If you have
  persisted ULL sketches (snapshots, ETS/DETS/CubDB/Ecto storage
  backends, `ExDataSketch.Server` snapshot files), rebuild them from
  source data after upgrading. HLL and every other family are unaffected
  -- this is a `ULL`-only, algorithm-only fix.
- **ULL estimates will change** for any existing sketch once rebuilt --
  they are now correct rather than overestimated once a sketch's
  registers fill up (`n` comparable to or larger than `m`). If you assert
  specific numeric ULL estimates in tests, expect them to shift toward
  the true cardinality.

## [0.10.0] - 2026-08-07

Release theme: **Production Ergonomics.** Closes the gap between "here is a
sketch struct" and "here is a production counter that answers questions
about the last five minutes": a unified sketch contract and facade
dispatch, a storage behaviour, windowing, supervised sketch processes
(`Server`/`Sketches`), ready-made `Telemetry.Metrics` + LiveDashboard
integration, raw-hashing NIF parity for the membership filters, Apache
KLL interop, and a `format: :v1` rolling-upgrade escape hatch on every
sketch family.

### Added

- `ExDataSketch.Sketch` -- the unified behaviour every concrete sketch
  family now implements (`serialize/1`, `deserialize/1`, `size_bytes/1`,
  `capabilities/0` required; `new/1`, `update/2`, `update_many/2`, `merge/2`
  optional where a family does not support the operation). Adds
  `ExDataSketch.Sketch.implemented?/1` to check compliance.
- `ExDataSketch.sketches/0` -- the registry mapping each family atom
  (`:hll`, `:bloom`, and so on) to its module; single source of truth for
  the capability-matrix test and `ExDataSketch.Sketch.implemented?/1`.
- Top-level facade dispatch functions on `ExDataSketch`: `new/2`, `update/2`,
  `merge/2`, `merge_many/1`, `estimate/1`, `serialize/1`, `deserialize/2`,
  `size_bytes/1`, `capabilities/1`. These are additive -- every per-family
  module's own API is unchanged and remains the documented primary
  interface. See `guides/` (forthcoming) and
  `baoulo/plans/0.10.0_phase1_stub_review.md` for the full design,
  including the small set of documented family-specific exceptions
  (`XorFilter` has no incremental `update/2`; `Cuckoo`, `XorFilter`, and
  `FilterChain` have no `merge/2`; `CMS` and the membership filters have no
  single-value `estimate/1`).
- `update/2` and `update_many/2` on `Bloom`, `Cuckoo`, `Quotient`, `CQF`,
  `IBLT`, and `FilterChain` -- additive aliases for `put/2` / `put_many/2`
  (the family-idiomatic names, unchanged) so every family satisfies
  `ExDataSketch.Sketch`'s generic callbacks.
- `capabilities/0` on `HLL`, `CMS`, `Theta`, `KLL`, `DDSketch`, `REQ`,
  `FrequentItems`, `MisraGries`, and `ULL`, matching the `MapSet.t(atom())`
  vocabulary already shipped on the membership filter modules.
- `ExDataSketch.Storage` promoted to a real behaviour (`@callback save/3`,
  `load/3`, `merge/3`, `delete/2`, optional `child_spec/1`), implemented by
  all five persistence backends (`ETS`, `DETS`, `CubDB`, `Mnesia`, `Ecto`).
  Adds `ExDataSketch.Storage.backends/0` (the atom-to-module registry) and a
  dispatching facade -- `save/3`, `load/3`, `merge/3`, `delete/2` on
  `ExDataSketch.Storage` itself -- that accepts either an explicit
  `{backend_module, ref}` pair or a bare `ref` resolved against a newly
  configurable default backend:

      config :ex_data_sketch, :storage, backend: ExDataSketch.Storage.ETS

- `ExDataSketch.Window` -- a ring of tumbling sub-sketches for "in the last
  N" questions without a hand-rolled timer:

      window = ExDataSketch.Window.new(:hll, [p: 14], every: :timer.minutes(1), keep: 5)
      window = ExDataSketch.Window.update(window, user_id)
      ExDataSketch.Window.estimate(window)

  `new/3` accepts a registry atom (`:hll`) or a sketch module directly;
  windowing requires a mergeable family (`Cuckoo`, `XorFilter`, and
  `FilterChain` are rejected with a clear error). The clock defaults to
  `System.monotonic_time/1` and is fully injectable (`:time_fn` at
  construction, an explicit `now` on `update/3` and `tick/2`), so tests
  never sleep. Adds the `:window` telemetry category and the
  `[:ex_data_sketch, :window, :roll]` event. See `guides/windowing.md` and
  `baoulo/plans/0.10.0_phase3_stub_review.md` for the full design,
  including what "the last N minutes" actually means for a tumbling
  (not exact sliding) window.
- `ExDataSketch.Server` -- a supervised, named, concurrently-updatable
  sketch process wrapping a single sketch or `ExDataSketch.Window`:

      {:ok, _pid} = ExDataSketch.Server.start_link(
        name: :uniques, sketch: :hll, sketch_opts: [p: 14]
      )
      ExDataSketch.Server.update(:uniques, user_id)
      ExDataSketch.Server.estimate(:uniques)

  `update/2` and `update_many/2` are casts, droppable under an optional
  `:max_queue` for bounded-memory backpressure (emitting
  `[:ex_data_sketch, :server, :drop]`); `update_sync/2` is a call, never
  dropped. Optional `:window` holds a `ExDataSketch.Window` instead of a
  bare sketch, with `track_all_time: true` maintaining a second
  un-windowed accumulator readable via `estimate(server, window: :all)`;
  `merge/2` is not supported on a windowed server this release (tracked in
  `baoulo/plans/plan-0.10.0.md` section 9). Optional `:snapshot` persists
  state to any `ExDataSketch.Storage` backend periodically and on graceful
  shutdown, and restores on start (crash recovery); an untrappable
  `Process.exit(pid, :kill)` bypasses the graceful-shutdown snapshot, so
  worst-case loss in that specific case is bounded by the snapshot
  interval, not zero. Optional `:flush` provides a return-and-reset-on-a-
  timer pattern. See `guides/supervised_sketches.md` and
  `baoulo/plans/0.10.0_phase4_design_review.md` for the full design.
- `ExDataSketch.Sketches` -- a supervisor for starting many
  `ExDataSketch.Server` processes at runtime, addressed by an arbitrary
  term instead of a compile-time name:

      children = [{ExDataSketch.Sketches, name: MyApp.Sketches}]
      {:ok, _pid} = ExDataSketch.Sketches.start_child(MyApp.Sketches, tenant_id, sketch: :hll, sketch_opts: [p: 14])
      ExDataSketch.Server.update(ExDataSketch.Sketches.via(MyApp.Sketches, tenant_id), user_id)

  Backed by a `Registry` and `DynamicSupervisor` per instance. Adds the
  `:server` telemetry category and the `[:ex_data_sketch, :server, :snapshot]`,
  `[:ex_data_sketch, :server, :restore]`, `[:ex_data_sketch, :server, :flush]`,
  and `[:ex_data_sketch, :server, :drop]` events. See
  `guides/supervised_sketches.md`.
- `ExDataSketch.Telemetry.Metrics.all/1` -- a ready-made `Telemetry.Metrics`
  definition for every one of the 17 events
  `ExDataSketch.Telemetry.all_event_names/0` returns, for wiring straight
  into a `Telemetry.Metrics` reporter or Phoenix LiveDashboard instead of
  hand-writing one metric per event:

      def metrics do
        ExDataSketch.Telemetry.Metrics.all() ++ [
          # ... your application's own metrics
        ]
      end

  Accepts `prefix:` to namespace metric names differently; the underlying
  `:telemetry` event listened to is unaffected. Adds `{:telemetry_metrics,
  "~> 1.0"}` as a normal (non-optional) dependency -- it defines only
  struct types, with no runtime process or side effect.
- `ExDataSketch.LiveDashboard.Page` -- an optional `Phoenix.LiveDashboard.PageBuilder`
  page listing every ExDataSketch telemetry event and the metric names
  `ExDataSketch.Telemetry.Metrics.all/1` derives from them:

      live_dashboard "/dashboard", additional_pages: [sketches: ExDataSketch.LiveDashboard.Page]

  A static reference page, not a live view of any particular running
  sketch (it cannot know which `ExDataSketch.Server` instances a host
  application started). Only exists when the new optional
  `{:phoenix_live_dashboard, "~> 0.8", optional: true}` dependency is
  loaded. See `baoulo/plans/0.10.0_phase5_stub_review.md` for the full
  design.
- `ExDataSketch.KLL.serialize_datasketches/2` and
  `deserialize_datasketches/2` -- Apache DataSketches KLL interop
  (compact `KllFloatsSketch`/`KllDoublesSketch`), replacing the
  `not_implemented!` stubs (v0.10.0 Phase 7, closing G7 from the v0.9.0
  code review). Unlike Theta, KLL does not hash its inputs, so this is a
  full item-level round trip with no hash-equality caveat -- decoding a
  Java/C++/Python-produced sketch and querying `quantile/2`/`rank/2`
  answers using the exact retained values the other implementation
  selected. Takes a `:variant` option (`:float` or `:double`, default
  `:double`); Apache's wire format doesn't self-describe item width, so
  the caller must know it in advance, same as choosing between
  `KllFloatsSketch`/`KllDoublesSketch` in Java. Backed by a new
  `ExDataSketch.DataSketches.KLLSketch` codec module (mirroring
  `CompactSketch`'s shape) and a `Backend.kll_from_components/5`
  callback (mirroring `theta_from_components/3`). Verified against the
  real `datasketches` Python package (not just against our own decoder)
  in both directions -- decoding its output and having it decode ours --
  across a sweep of item counts and `k` values from 0 to 500,000; see
  `guides/apache_interop.md`. `test/fixtures/interop/kll/` carries the
  first golden cross-language fixture corpus actually generated and
  committed for any family (the process `test/vectors/CROSS_LANGUAGE.md`
  documented for Theta was never executed until now), pinned to
  `datasketches` 5.2.0.
- `guides/apache_interop.md` -- consolidated reference for what
  interoperates with Apache DataSketches (Theta, KLL) and what doesn't
  (HLL -- now targeted at v0.11.0; CMS -- not planned, no standard format
  exists), superseding the scattered mentions previously spread across
  sketch moduledocs.
- `serialize(sketch, format: :v1)` -- the opt-in legacy EXSK v1 escape
  hatch, previously HLL-only, is now available on every sketch family
  except `FilterChain` (which has its own bespoke FCN1 container format
  with no `Codec.sketch_id` of its own -- see its moduledoc). v1 output
  is compatible with v0.7.x readers, for use during rolling upgrades.
  Families with a `:hash_strategy` option require `:phash2` for v1, same
  as HLL; families with no hash-strategy concept (KLL, DDSketch, REQ,
  FrequentItems, MisraGries) have no such restriction. `Codec.encode/4`
  (already generic across every `sketch_id_*`) needed no changes --
  every family's *state* binary is unchanged between v1 and v2 eras, the
  difference is purely the frame wrapper (v2 adds a hash-algorithm
  metadata block + CRC32C trailer; v1 has neither).
- `ci/check_roadmap.exs` -- asserts `README.md`'s roadmap table has a
  row for the version `mix.exs` currently declares, and (once that
  version has no `-dev` suffix) that the row says "Released", the
  install snippet matches, and `guides/roadmap.md` has moved on to
  previewing the next release. Closes a twice-deferred carry-forward
  item (X-R2).

### Changed

- `Bloom.put_many/2`, `Cuckoo.put_many/2`, `Quotient.put_many/2`,
  `CQF.put_many/2`, `IBLT.put_many/2`, and `XorFilter.build/2` now hash
  items inside the Rust NIF itself when using `Backend.Rust` with the
  default hash strategy (`:xxhash3`/`:murmur3`), instead of hashing on
  the BEAM and passing pre-hashed integers across the NIF boundary --
  extending the raw-hashing architecture HLL/CMS/Theta/ULL already had
  (`guides/hll_performance.md`) to the six membership filters (G6 from
  the v0.9.0 code review, deferred twice, now closed). Measured 2.2x-
  12.8x faster than the pre-hashed Rust path and up to ~2,700x faster
  than Pure Elixir, depending on family -- see `guides/filter_performance.md`
  for the full measured table. A custom `:hash_fn` or `:hash_strategy:
  :phash2` still falls back to the pre-hashed path exactly as before;
  behavior and serialized output are unchanged, only where the hash is
  computed. `test/parity_test.exs` already asserted byte-identical
  Pure/Rust output for every affected family under default options, so
  it now exercises the raw path automatically; new tests were added only
  for the `:hash_fn` fallback case (one per family) and were not
  previously covered for any raw-hashing family.
- `ExDataSketch.Broadway.PeriodicAggregator` is now a thin wrapper around a
  `:flush`-configured `ExDataSketch.Server`, delegating every call. Its
  public API (`start_link/1`, `merge/2`, `flush/1`, `get/1`, `estimate/1`)
  is unchanged. **Correction (originally published as "unchanged," which
  was wrong):** the `[:ex_data_sketch, :pipeline, :periodic_flush]`
  telemetry event's scope narrowed. In v0.9.0 it fired on every call to
  `flush/1`, manual or automatic. In v0.10.0 it fires only on the
  automatic, timer-driven path -- `Server.flush/1`'s manual call does not
  invoke the `:flush` callback that emits it (see `ExDataSketch.Server`'s
  moduledoc, which already documented this correctly; only this
  CHANGELOG entry had the wrong claim). If you relied on this event
  firing from a manual `PeriodicAggregator.flush/1` call, it no longer
  will; use the `[:ex_data_sketch, :server, :flush]` event instead, which
  fires on both paths.

- `ExDataSketch.update_many/2` now dispatches generically via the `Sketch`
  behaviour instead of 13 hand-written struct clauses, extending coverage
  to 15 of 16 families (all but the immutable `XorFilter`, which raises
  `ExDataSketch.Errors.UnsupportedOperationError`).
- `ExDataSketch.GenStage.SketchConsumer` ingests raw event batches via
  `sketch_module.update_many/2` directly instead of a `from_enumerable/2`
  capability probe followed by a merge; behavior is unchanged, with one
  fewer intermediate sketch allocation per batch.
- `ExDataSketch.FilterChain`'s internal capability checks now use
  `ExDataSketch.Sketch.implemented?/1` instead of raw `function_exported?/3`.
- Removed the unused `save_opts`, `load_opts`, `merge_opts`, and
  `delete_opts` types from `ExDataSketch.Storage` -- they described a
  `[table: atom()]` keyword-list shape no backend ever used (every backend
  takes its table/db/repo as a positional argument) and were referenced
  nowhere in `lib/` or `test/`.
- Fixed a `:telemetry.attach/4` local-function-capture performance warning
  in `ExDataSketch.Telemetry.OpenTelemetry.setup/0` (it now attaches a
  remote function capture).
- Wired up `doctest` for `DDSketch`, `REQ`, `FrequentItems`, `MisraGries`,
  and all 7 membership filter modules -- their existing `@doc` examples
  were never executed by the test suite before this release.
- `test/property_guarantees_test.exs`'s bit-flip corruption-propagation
  property now covers all 15 `Codec`-backed sketch families (previously
  HLL/ULL/CMS only) instead of a hardcoded 3-family list. Closes a
  twice-deferred carry-forward item (P5R4). Construction is factored
  into a new `test/support/sketch_fixtures.ex` (`ExDataSketch.SketchFixtures`)
  shared with the generalized v1 escape-hatch tests, which handles the
  three constructor return shapes in play (bare struct; Cuckoo's
  `{:ok, t()} | {:error, :full, t()}`; XorFilter's `build/2` returning
  `{:ok, t()} | {:error, :build_failed}` since it has no
  `from_enumerable/2`). `FilterChain` is excluded (bespoke construction
  shape wrapping sub-sketches, each already covered independently).

### Fixed

- Removed stray `erl_crash.dump` and `.DS_Store` from the repository;
  `.DS_Store` is now gitignored.

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
