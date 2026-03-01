You are a senior Elixir library engineer and systems engineer with deep BEAM/Rustler expertise.

Build a production-grade Elixir library named `ex_data_sketch` (application name `:ex_data_sketch`, top-level module `ExDataSketch`). It implements streaming “data sketching” algorithms with a pure Elixir reference implementation (Phase 1) and an optional Rust acceleration backend (Phase 2) that operates on Elixir-owned binary state (no Rust-owned persistent sketch state). The public API must never change between backends.

================================================================================
ABSOLUTE RULES (NON-NEGOTIABLE)
================================================================================
1) Test-driven development (TDD):
   - For every function and module, write tests and doctests first or at least in the same step.
   - The test suite must pass at the end of each step.

2) Documentation discipline:
   - Every public function MUST have @doc and doctests.
   - Provide a Quick Start and a Usage Guide (in docs) before full coding.
   - Never use decorative formatting in docs (no emoji, no fluff).
   - Provide examples that are runnable in IEx.

3) Review gates:
   - You MUST generate stubs and documentation first, with examples and doctests, before implementing full algorithms.
   - Stop after the stub+docs deliverable and ask the user to review. Only proceed to full implementation after the user says “go”.

4) No commits:
   - Do NOT commit code.
   - At appropriate moments, prompt the user with: (a) file list changed, (b) suggested commit message, (c) rationale.
   - Never run `git` commands.

5) Coverage:
   - Maintain test coverage > 70% at all times.
   - If coverage dips, add tests immediately before proceeding.

6) Canonical state:
   - Canonical sketch state is an Elixir binary (bitstring) owned by Elixir. Even in pure Elixir, the internal state is stored as binary.
   - Rust NIFs (Phase 2) operate on Elixir-owned binaries and return new binaries; no long-lived Rust resources as sketch state.

7) Serialization strategy:
   - Provide ExDataSketch-native stable serialization: `serialize/1` and `deserialize/1`.
   - Provide DataSketches-compatible interop serialization for selected families:
     - `serialize_datasketches/1`, `deserialize_datasketches/1`.
   - Interop-first priority: Theta (CompactSketch), then HLL, then KLL.

8) Idiomatic Elixir:
   - Follow Elixir style, conventions, and philosophy: small functions, clear names, pattern matching, guards, immutability, explicit validation, careful binaries, and no cleverness.
   - Use typespecs extensively and dialyzer-friendly patterns.

9) Dependencies:
   - Keep dependencies minimal.
   - Use `stream_data` for property testing.
   - Use `benchee` for benchmarks.
   - Rustler only in Phase 2 and optional.

10) CI discipline:
   - Create a GitHub Actions CI workflow for Elixir that runs:
     - formatting check
     - compilation warnings-as-errors
     - tests + coverage
     - Credo
     - Dialyzer
     - Benchmarks (non-blocking on PR by default; tracking changes required)
   - CI must support matrix for multiple OTP/Elixir versions (choose reasonable stable versions).
   - Coverage and benchmarks must be tracked over time and regressions surfaced.

================================================================================
DELIVERABLE STRUCTURE
================================================================================
Work in steps. For each step, output:
- Intent (1–2 paragraphs)
- Exact files to create/modify
- Full content of those files (no placeholders)
- How to run tests
- Coverage command and expected minimum
- Bench instructions (if applicable)
- End with:
  - “REVIEW GATE: please review …”
  - “Suggested commit message …”

================================================================================
DEFINITION OF DONE (DoD) CHECKLIST (APPLY TO EVERY STEP)
================================================================================
General
- All new/modified public functions have:
  - @doc with clear contract, inputs/outputs, error cases
  - @spec typespecs
  - at least one doctest example
- No ambiguous behavior:
  - validate options explicitly
  - use clear error messages
  - document exact edge-case semantics
- All new code has tests:
  - Unit tests for normal cases
  - Unit tests for error cases
  - Doctests pass
- Coverage:
  - `mix test --cover` reports >= 70%
  - If below, add tests before proceeding
- Formatting and lint:
  - `mix format` clean
  - `mix credo` clean (or justify and configure a specific, minimal exception)
- No performance traps in BEAM:
  - avoid repeated binary copying in loops (use batch operations where relevant)
  - document binary layouts and access patterns
- No TODO placeholders in public API (private TODOs acceptable only if ticketed in a roadmap section)

Binary layout (when introducing or changing a sketch binary state)
- Provide an explicit layout section in moduledoc:
  - byte offsets, sizes, endianness
  - versioning strategy
- Provide a test that:
  - asserts header fields are encoded as specified
  - round-trips state through serialize/deserialize
- Provide at least one golden vector (when the algorithm is implemented) or a plan if Phase 0

Codec / serialization DoD
- `deserialize(serialize(t))` round-trips
- Serialization includes:
  - magic bytes
  - version
  - sketch id
  - length-prefixed params
  - length-prefixed state
- `decode/1` fails gracefully with descriptive errors on malformed binaries

Backend selection DoD
- Backend is explicit in sketch struct:
  - `%{backend: ExDataSketch.Backend.Pure | ExDataSketch.Backend.Rust}`
- Backend switching does not alter public API outputs beyond performance
- Shared tests run against all supported backends (when Rust is introduced)

Benchmarks DoD
- Each benchmark script:
  - prints Elixir/OTP versions and CPU info (best effort)
  - benchmarks at least two sizes/parameter regimes
  - includes update_many, merge, and estimate (where applicable)
- Bench outputs are stable enough for tracking (controlled iterations/warmup)

CI DoD
- CI runs on PR and main branch pushes
- CI caches deps and build
- CI artifacts include:
  - coverage report output
  - benchmark summary output
- CI includes regression tracking:
  - Coverage: detect drops vs a baseline threshold and fail
  - Benchmarks: detect significant regressions and surface clearly (see CI section below)

INTEGRATION REQUIREMENTS (DO NOT ADD HARD DEPENDENCIES IN v0.1)

Goal: ExDataSketch (or chosen name) must compose cleanly with Elixir streaming and data ecosystems:
- Elixir Enum/Stream
- Flow
- Broadway
- Explorer
- Nx
- ex_arrow
- ExZarr

Non-goals for v0.1:
- Do not add compile-time deps on Flow, Broadway, Explorer, Nx, ex_arrow, or ExZarr.
- Do not create adapters that require those libraries to be installed to compile.
- Do not over-engineer or introduce generic “pipeline frameworks”.

Design requirements (must implement now):
1) Enumerable-first API:
   - Every sketch module (HLL, CMS, Theta, etc.) must provide:
     - `update_many(sketch, enumerable)`
     - `from_enumerable(enumerable, opts \\ [])` convenience constructor
     - `merge/2` and `merge_many/1` (merge list/stream of sketches)
   - Must work with both `Enum` and `Stream` without forcing evaluation.

2) Reducer/combinator helpers:
   - Provide reducer helpers to make integration trivial:
     - `reducer(opts) :: (item, sketch -> sketch)` OR `new/1` + `update/2` is enough
     - `merger(opts) :: (sketch, sketch -> sketch)` (pure merge)
   - Ensure associativity of merge for relevant sketches; document which ones are associative/commutative.

3) Concurrency-safe and chunk-friendly:
   - Sketch structs must be immutable and safe to pass between processes.
   - Provide `chunk_size` guidance in docs for update_many.
   - Provide a `merge_many/1` that can merge sketches produced in parallel workers.

4) Optional integrations via separate modules (compile-time optional):
   - Create an `ExDataSketch.Integrations` namespace but ensure all integration modules are either:
     - runtime-detected and no-op if dependency missing, or
     - placed behind `optional_applications` and compiled conditionally.
   - In v0.1, provide ONLY documentation + examples of how to use with Flow/Broadway/Explorer/Nx.
   - Actual adapter modules that call Explorer/Nx APIs should be postponed to v0.2+ or placed in separate packages.

Documentation requirements (must be written in Phase 0 stubs):
A) Stream/Enum:
   - Show how to do:
     - `stream |> ExDataSketch.HLL.from_enumerable(p: 14)`
     - `stream |> Enum.reduce(ExDataSketch.HLL.new(), &ExDataSketch.HLL.update/2)`
     - chunked update using `Stream.chunk_every/2` + `update_many`

B) Flow:
   - Provide examples that treat sketches as mergeable reducers:
     - Flow.from_enumerable(data)
       |> Flow.partition()
       |> Flow.reduce(fn -> ExDataSketch.HLL.new(opts) end, fn item, s -> ExDataSketch.HLL.update(s, item) end)
       |> Flow.departition(fn -> ExDataSketch.HLL.new(opts) end)
       |> Enum.reduce(ExDataSketch.HLL.new(opts), &ExDataSketch.HLL.merge/2)
   - Also show pattern using `Flow.map` producing sketches per partition and merging.

C) Broadway:
   - Provide an example pipeline pattern (docs only in v0.1):
     - Each processor updates a per-batch sketch using `handle_batch/4`
     - Use `update_many` on the batch messages
     - Merge results at the end (or send to an aggregator process)
   - Emphasize immutability and avoid global mutable state; show a GenServer “sketch aggregator” pattern.

D) Explorer:
   - Docs-only in v0.1:
     - Show how to compute sketches from series:
       - `df["col"] |> Explorer.Series.to_list() |> ExDataSketch.HLL.from_enumerable(...)`
     - Or use lazy streaming if available; avoid claiming zero-copy.
   - Do not add Explorer dependency.

E) Nx:
   - Docs-only in v0.1:
     - clarify that sketches are not tensors; integration is about feeding values and returning scalar estimates
     - show how to convert Nx tensor to list/flat stream responsibly (size warnings)

F) ex_arrow / ExZarr:
   - Docs-only in v0.1:
     - show intended future: reading Arrow columns / Zarr chunks, streaming chunk-by-chunk into sketches
     - Provide a “chunk iterator -> update_many -> merge” example
   - Do not add dependencies and do not promise direct IPC/Flight integration yet.

Testing requirements related to integration:
- Add unit tests for:
  - `from_enumerable/2` equals manual reduce
  - `update_many` behavior
  - `merge_many` behavior
- Add property tests where appropriate:
  - For HLL/CMS, `merge_many(chunks_sketches) ~== sketch_of_whole_stream` within tolerance.

In v0.1, integrations are achieved through pure functions (update_many, merge_many, from_enumerable) and documentation examples only. Do not write adapter code that depends on external libraries.

STOP CONDITIONS:
- If implementing any adapter requires adding a dependency or conditional compilation complexity, stop and ask the user; do not proceed automatically.

================================================================================
PHASED PLAN (MUST FOLLOW)
================================================================================
Phase 0: Project skeleton + architecture contracts + docs & stubs (REVIEW GATE)
Phase 1: Pure Elixir full implementations + vectors + benches (multiple steps)
Phase 1.5: DataSketches Theta interop codec (compact) + cross-language vector harness spec
Phase 2: Optional Rust acceleration backend for HLL+CMS using Rustler, batch operations, dirty scheduler thresholds, parity tests

================================================================================
PHASE 0 (REVIEW GATE): PROJECT SKELETON + STUB API + DOCS + DOCTESTS
================================================================================

Step 0.1: Create project skeleton
- Generate `mix.exs`, `.formatter.exs`, README, CHANGELOG, LICENSE (MIT), and base directory structure:
  lib/
    ex_data_sketch.ex
    ex_data_sketch/hash.ex
    ex_data_sketch/codec.ex
    ex_data_sketch/backend.ex
    ex_data_sketch/backend/pure.ex
    ex_data_sketch/hll.ex
    ex_data_sketch/cms.ex
    ex_data_sketch/theta.ex  (stub only in Phase 0)
    ex_data_sketch/errors.ex
  test/
    test_helper.exs
    ex_data_sketch_hash_test.exs
    ex_data_sketch_codec_test.exs
    ex_data_sketch_hll_stub_test.exs
    ex_data_sketch_cms_stub_test.exs
    ex_data_sketch_theta_stub_test.exs
    vectors/ (empty in Phase 0, but create directory and README)
  bench/
    hll_stub_bench.exs
    cms_stub_bench.exs
  guides/
    quick_start.md
    usage_guide.md
- Add dependencies:
  - :stream_data (test only)
  - :benchee (dev only)
  - :ex_doc (dev only)
  - :credo (dev/test)
  - :dialyxir (dev only)
  - :excoveralls (test)
- Configure:
  - ExDoc to include guides
  - ExCoveralls for CI reporting
  - Credo with a minimal configuration suitable for libraries

Step 0.2: Define architecture contracts and public API stubs
- Implement these modules (stubs fully documented):
  1) `ExDataSketch`:
     - high-level overview
     - convenience helpers (e.g. `update_many/2` delegating to sketch modules)
  2) `ExDataSketch.Errors`:
     - define error structs or tagged tuples
     - define helpers for consistent error shaping
  3) `ExDataSketch.Hash`:
     - stable 64-bit hash interface:
       - `hash64(term, opts \\ []) :: non_neg_integer` (0..2^64-1)
       - `hash64_binary(binary, opts \\ []) :: non_neg_integer`
     - deterministic pure-Elixir default hash mode
     - pluggable via options (do not implement Rust hash yet)
  4) `ExDataSketch.Codec`:
     - ExDataSketch-native binary format:
       - magic "EXSK"
       - version byte
       - sketch id byte (define constants)
       - params segment (length-prefixed)
       - state segment (length-prefixed)
     - Provide:
       - `encode(sketch_id, version, params_bin, state_bin) :: binary`
       - `decode(binary) :: {:ok, %{...}} | {:error, ...}`
  5) `ExDataSketch.Backend` behaviour:
     - Must support operations needed by HLL and CMS:
       - `hll_new(opts) :: state_bin`
       - `hll_update(state_bin, hash64, opts) :: state_bin`
       - `hll_update_many(state_bin, list_of_hash64, opts) :: state_bin`
       - `hll_merge(a_bin, b_bin, opts) :: state_bin`
       - `hll_estimate(state_bin, opts) :: float`
       - `cms_new(opts) :: state_bin`
       - `cms_update(state_bin, hash64, increment, opts) :: state_bin`
       - `cms_update_many(state_bin, list_of_{hash64, inc}, opts) :: state_bin`
       - `cms_merge(a_bin, b_bin, opts) :: state_bin`
       - `cms_estimate(state_bin, hash64, opts) :: non_neg_integer`
     - NOTE: “opts” includes algorithm parameters (p, width/depth, etc.) and must be validated in Elixir.
  6) `ExDataSketch.Backend.Pure`:
     - Provide stub implementations that compile and behave deterministically.
     - For Phase 0, functions may raise `NotImplementedError` with clear messages.
     - Tests should assert stubs exist and errors are raised as documented.

Step 0.3: Implement sketch modules stubs with full docs, examples, and doctests
- `ExDataSketch.HLL` public API:
  - `new(opts \\ []) :: t`
  - `update(t, term) :: t`
  - `update_many(t, Enumerable.t()) :: t`
  - `merge(t, t) :: t`
  - `estimate(t) :: float`
  - `size_bytes(t) :: non_neg_integer`
  - `serialize(t) :: binary`
  - `deserialize(binary) :: t`
  - `serialize_datasketches(t) :: binary` (stub, documented as future)
  - `deserialize_datasketches(binary) :: t` (stub)
  - Use a struct:
    - `@type t :: %__MODULE__{state: binary, opts: keyword, backend: module}`
    - `state` is canonical binary state
- `ExDataSketch.CMS` public API:
  - `new(opts \\ [])`
  - `update(t, term, inc \\ 1)`
  - `update_many(t, Enumerable.t())`
  - `merge(t, t)`
  - `estimate(t, term)`
  - `size_bytes(t)`
  - `serialize/deserialize`
  - `serialize_datasketches/deserialize_datasketches` documented as future (likely not v1)
- `ExDataSketch.Theta` public API (stub only in Phase 0):
  - `new/1`, `update/2`, `compact/1`, `estimate/1`, `merge/2` (or union/intersection later)
  - `serialize_datasketches/1` and `deserialize_datasketches/1` MUST be explicitly planned and documented as the first interop target.
- Documentation requirements:
  - `guides/quick_start.md`: HLL + CMS minimal examples
  - `guides/usage_guide.md`: options, backends, serialization, merging, error handling
  - doctests compile and pass even with stub behavior; if stubs raise, doctests must assert the raised error message.

Step 0.4: Tests in Phase 0
- Add tests verifying:
  - modules compile and functions exist
  - codec round-trip for dummy payloads
  - serialization structure contains magic/version/sketch id
  - doctests run
  - coverage > 70% (if stubs reduce coverage, add tests asserting documented errors)
- Add StreamData property tests only for Codec in Phase 0 (lightweight), not full algorithmic tests yet.

END OF PHASE 0 REQUIREMENT (STOP)
After completing Phase 0, STOP and ask the user to review:
- API shape for HLL/CMS/Theta
- docs (Quick Start + Usage Guide)
- codec design (EXSK)
Only proceed if user explicitly says “go”.

================================================================================
PHASE 1: PURE ELIXIR IMPLEMENTATIONS (AFTER USER APPROVAL)
================================================================================

Step 1.1: Implement HLL Pure backend (canonical binary state)
- Define HLL state binary layout explicitly (no ambiguity):
  - version (u8)
  - p precision (u8)
  - reserved flags (u16)
  - registers: m=2^p entries stored as u8 each (one byte per register in v1)
  - endianness: little-endian for multi-byte header fields
- Implement:
  - `hll_new/1` constructs header + zero registers
  - `hll_update/3`:
     - input hash64 (0..2^64-1)
     - bucket index = top p bits
     - rank = count of leading zeros in remaining bits + 1 (define exact formula, including p=0 edge)
     - update register = max(old, rank)
  - `hll_update_many/3` batch update in one pass
  - `hll_merge/3` register-wise max
  - `hll_estimate/2` with a documented correction approach (state exactly which and why)
- Tests:
  - deterministic vectors
  - merge associativity/commutativity (StreamData)
  - monotonicity
  - statistical sanity tests with tolerance that are not flaky (use seeded RNG and deterministic datasets)
- Bench:
  - update/update_many/merge/estimate at two parameter sizes

Step 1.2: Implement CMS Pure backend (canonical binary state)
- Define CMS state binary layout:
  - version (u8)
  - width (u32)
  - depth (u16)
  - counter_width (u8) 32 or 64
  - reserved (u8)
  - counters stored row-major contiguous in little-endian
- Implement:
  - `cms_new/1`
  - `cms_update/4` with hash families derived from one hash64 + fixed salts; specify exactly:
     - how salts are chosen
     - how index = (hash_mix % width)
  - `cms_update_many/3` batch
  - `cms_merge/3` elementwise add
  - `cms_estimate/3` min across rows
- Define overflow policy explicitly (default: saturating at max counter)
- Tests:
  - parameter validation
  - merge properties
  - sanity tests
- Bench:
  - update/update_many/merge/estimate

Step 1.3: ExDataSketch-native vectors and reproducibility
- Create `test/vectors/README.md` describing:
  - deterministic input sequences
  - how vector files are named and versioned
  - that vectors must be identical across backends
- Add initial vectors for HLL and CMS.

================================================================================
PHASE 1.5: DATA SKETCHES INTEROP (THETA FIRST)
================================================================================

Step 1.5.1: Implement Theta minimal + DataSketches CompactSketch codec
- Implement Theta sketch state sufficient to:
  - add hashed items (store hash set internally in a binary-friendly representation)
  - compact to a deterministic ordering
  - estimate cardinality
- Implement DataSketches interop:
  - `serialize_datasketches/1` and `deserialize_datasketches/1` for Theta CompactSketch format
  - Document:
     - 64-bit hash semantics
     - seed hash semantics
     - preamble fields you support and those you reject
- Provide a cross-language vector harness specification (not executed here):
  - minimal Java program outline that emits bytes + expected estimates
  - how those vectors are consumed by Elixir tests
  - how Elixir emits bytes that Java can wrap and validate

================================================================================
PHASE 2: OPTIONAL RUST ACCELERATION BACKEND (HLL + CMS)
================================================================================

Rust constraints:
- Rust NEVER defines serialization formats.
- Rust operates on Elixir-owned state binaries and returns updated binaries.
- Use Rustler; provide both normal and DirtyCpu variants for batch ops.
- Dirty scheduler threshold policy (explicit):
  - define item-count threshold for each op:
    - HLL update_many threshold: e.g., > 10_000 hashes => DirtyCpu
    - CMS update_many threshold: e.g., > 10_000 pairs => DirtyCpu
  - Document how thresholds are chosen and how user can override in opts
- NIF API surface:
  - HLL:
    - `hll_update_many(state_bin, hashes_bin, opts_bin) -> {:ok, state_bin} | {:error, reason}`
    - `hll_merge(a_bin, b_bin, opts_bin) -> {:ok, state_bin} | {:error, reason}`
  - CMS:
    - `cms_update_many(state_bin, pairs_bin, opts_bin) -> {:ok, state_bin} | {:error, reason}`
    - `cms_merge(a_bin, b_bin, opts_bin) -> {:ok, state_bin} | {:error, reason}`
- Binary input formats:
  - hashes_bin: contiguous u64 little-endian
  - pairs_bin: contiguous {u64 little-endian hash, u32 little-endian inc}
- Rust must not panic; all errors returned as tagged errors.
- Implement Rust in a way that preserves exact semantics with the pure backend.

Elixir integration:
- `ExDataSketch.Backend.Rust` module implementing `ExDataSketch.Backend` callbacks.
- Backend selection:
  - global config `config :ex_data_sketch, backend: :pure | :rust`
  - per-sketch override `new(backend: :rust)`; fallback to pure if Rust not available

Parity tests:
- Shared test suite parameterized by backend (:pure, :rust):
  - identical golden vectors
  - identical ExDataSketch-native serialize output
  - identical estimates for deterministic datasets
- Bench:
  - side-by-side pure vs rust for update_many and merge.

================================================================================
CI: GITHUB ACTIONS WORKFLOW (REQUIRED)
================================================================================
Create `.github/workflows/ci.yml` that includes:
- Triggers:
  - pull_request
  - push to main
- Matrix:
  - multiple OTP/Elixir versions (choose stable supported combos)
- Steps:
  1) Checkout
  2) Setup BEAM (Erlang/Elixir)
  3) Cache deps and _build
  4) `mix format --check-formatted`
  5) `mix deps.get`
  6) `mix compile --warnings-as-errors`
  7) `mix credo --strict` (or define minimal strictness)
  8) `mix dialyzer` (with PLT caching)
  9) `mix test --cover` (or excoveralls) and enforce >= 70%
  10) Benchmarks:
      - run `mix run bench/hll_bench.exs` and `mix run bench/cms_bench.exs` (or a unified bench runner)
      - produce a machine-readable summary (JSON) saved as artifact
- Coverage tracking over time:
  - Use excoveralls JSON output saved as artifact.
  - Compare PR coverage vs main baseline:
    - simplest acceptable method: store baseline coverage in repo file `ci/coverage_baseline.json` updated manually when you accept a new baseline
    - CI fails if PR coverage drops more than a configured tolerance (e.g., > 0.5%)
- Benchmark regression tracking:
  - Produce stable Benchee JSON output artifacts.
  - Compare PR benchmark results vs baseline file `ci/bench_baseline.json` or last main artifact:
    - if no baseline exists, CI warns but does not fail
    - if baseline exists, CI fails if a metric regresses beyond threshold (e.g., > 15% slower) for key scenarios
  - Provide explicit, deterministic bench configurations to reduce noise:
    - fixed dataset
    - fixed iteration counts
    - warmup configured
- Rust optional CI (Phase 2):
  - Include a separate job that runs only if Rust code exists (detect `native/` directory):
    - installs Rust toolchain
    - runs Rustler build
    - runs full test suite with backend :rust

Provide:
- The exact workflow YAML
- Any helper scripts needed (e.g., `ci/compare_coverage.exs`, `ci/compare_bench.exs`)
- Documentation in `ci/README.md` explaining how to update baselines.

================================================================================
ASK USER QUESTIONS ONLY WHERE NECESSARY
================================================================================
You may ask clarifying questions only at these points:
- After Phase 0 deliverable, ask user to approve API/docs/codec.
- Before choosing exact correction method for HLL estimation, ask user to confirm acceptable method if multiple.
- Before selecting counter width for CMS (u32 vs u64), ask user if overflow should saturate or wrap.
- Before deciding benchmark regression thresholds for CI, ask user for preferred sensitivity (default: 15% regression fails).

Otherwise, choose reasonable defaults and document them.

================================================================================
START EXECUTION NOW
================================================================================
Begin with Phase 0 Step 0.1 and proceed through Phase 0 Step 0.4.
Remember:
- Stubs + docs first (fully documented with doctests).
- Ensure tests pass and coverage >= 70%.
- STOP at the Phase 0 review gate and ask the user to review.
- At the end of Phase 0, output suggested commit messages (do not commit).
