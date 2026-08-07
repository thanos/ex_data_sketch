# KLL Apache DataSketches Interop Fixtures

Golden binary fixtures produced by the real Apache DataSketches Python
library, used to verify `ExDataSketch.KLL.deserialize_datasketches/2`
against an independent, canonical implementation rather than only against
our own encoder.

## Provenance

- Package: `datasketches` (official Apache DataSketches Python bindings,
  C++-backed) from PyPI.
- **Pinned version: 5.2.0.**
- Generated with:

  ```sh
  python3 -m venv /tmp/kll_fixture_venv
  source /tmp/kll_fixture_venv/bin/activate
  pip install datasketches==5.2.0
  python3 test/fixtures/interop/kll/generate.py
  ```

- Generator: `generate.py` in this directory.

## Policy

These fixtures are **immutable and additive-only**. Once committed, an
existing `.bin` file is never regenerated or overwritten -- if the format
needs new coverage, add a new named case instead. This keeps old fixtures
usable as a regression check against exactly what was true the day they
were produced, independent of any later changes to `generate.py` or to
the `datasketches` package.

The generator is **not** run as part of CI (no Python/JVM dependency is
introduced into the build). `mix test` only reads the committed `.bin`
files; regenerating the corpus is a manual, occasional maintenance step.

## Naming

`kll_ds_<variant>_<case>.bin`, where `<variant>` is `floats` or `doubles`
(matching `KllFloatsSketch`/`KllDoublesSketch`) and `<case>` is one of:

| Case | k | Items | Notes |
|---|---|---|---|
| `empty` | 200 | 0 | Compact-empty structure (8 bytes) |
| `single` | 200 | 1 | Compact-single structure |
| `small` | 200 | 50 | Well under `k`, no compaction |
| `large` | 200 | 100,000 | Forces multiple compactions/levels |
| `k64` | 64 | 5,000 | Non-default `k` |

All items are `1.0, 2.0, ..., n*1.0` (ascending integers as floats), so
expected `min`/`max`/quantile values are trivially predictable and are
also printed by `generate.py` when it runs, for manual cross-checking.

## Consuming these fixtures

See `test/ex_data_sketch_kll_datasketches_fixtures_test.exs`.
