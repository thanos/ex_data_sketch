#!/usr/bin/env python3
"""Generates the golden KLL interop fixture corpus for ex_data_sketch.

Run manually (NOT part of CI -- no Python/JVM dependency in CI):

    python3 -m venv /tmp/kll_fixture_venv
    source /tmp/kll_fixture_venv/bin/activate
    pip install datasketches==5.2.0
    python3 test/fixtures/interop/kll/generate.py

See README.md in this directory for the exact pinned version and
provenance. Fixtures are immutable and additive-only once committed --
do not regenerate/overwrite existing files, only add new ones.
"""

import datasketches

OUT_DIR = "test/fixtures/interop/kll"

CASES = [
    # (name, k, item_count)
    ("empty", 200, 0),
    ("single", 200, 1),
    ("small", 200, 50),
    ("large", 200, 100_000),
    ("k64", 64, 5_000),
]


def build(sketch_ctor, k, item_count):
    sketch = sketch_ctor(k)
    for i in range(1, item_count + 1):
        sketch.update(float(i))
    return sketch


def write_fixture(variant, name, sketch):
    path = f"{OUT_DIR}/kll_ds_{variant}_{name}.bin"
    data = sketch.serialize()
    with open(path, "wb") as f:
        f.write(data)

    n = sketch.n
    print(f"{path}: {len(data)} bytes, n={n}", end="")
    if n > 0:
        print(
            f", min={sketch.get_min_value()}, max={sketch.get_max_value()}, "
            f"median={sketch.get_quantile(0.5)}, retained={sketch.num_retained}"
        )
    else:
        print()


def main():
    for name, k, item_count in CASES:
        write_fixture("floats", name, build(datasketches.kll_floats_sketch, k, item_count))
        write_fixture("doubles", name, build(datasketches.kll_doubles_sketch, k, item_count))


if __name__ == "__main__":
    main()
