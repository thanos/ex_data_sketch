# Test Vectors

This directory contains deterministic test vectors for verifying sketch algorithm
correctness and cross-backend parity.

## Directory Structure

```
vectors/
  hll/              JSON vectors for HLL
  cms/              JSON vectors for CMS
  theta/            JSON vectors for Theta
  *.bin             Legacy binary vectors (raw sketch state)
```

## JSON Vector Format

Each `.json` file is a self-contained test case with metadata, inputs, and
expected outputs.

```json
{
  "vector_version": 1,
  "algorithm": "hll",
  "algorithm_opts": { "p": 14 },
  "input_items": ["item_0", "item_1", "item_2"],
  "expected": {
    "canonical_exsk_base64": "<Base64 of serialize/1 output>",
    "estimate": 3.000456,
    "tolerance": 0
  }
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `vector_version` | integer | Schema version (currently 1) |
| `algorithm` | string | One of `"hll"`, `"cms"`, `"theta"` |
| `algorithm_opts` | object | Constructor options (e.g. `{"p": 14}`) |
| `input_items` | array | Deterministic list of strings/integers |
| `expected.canonical_exsk_base64` | string | Base64-encoded `serialize/1` output |
| `expected.estimate` | number or null | Expected estimate value. `null` for CMS (use `point_estimates` instead) |
| `expected.tolerance` | number | _(optional)_ Allowed absolute difference. Defaults to 0; a small epsilon (1e-9) is applied for float estimates |
| `expected.point_estimates` | object | _(optional, CMS only)_ Per-item expected counts: `{"item": count}` |

### Merge Vectors

Merge vectors include additional fields:

| Field | Type | Description |
|-------|------|-------------|
| `merge_inputs` | array | Second sketch's input items |
| `merge_expected.canonical_exsk_base64` | string | Expected merged state |
| `merge_expected.estimate` | number or null | Expected merged estimate. `null` for CMS |
| `merge_expected.point_estimates` | object | _(optional, CMS only)_ Per-item expected counts after merge |

## Legacy Binary Vectors

Files named `{sketch}_{version}_{description}.bin` contain raw sketch state
binaries. These predate the JSON format and are retained for regression testing.

### HLL

- `hll_v1_empty_p14.bin` -- Empty HLL sketch, p=14
- `hll_v1_100items_p14.bin` -- HLL after inserting "item_0".."item_99", p=14
- `hll_v1_10000items_p14.bin` -- HLL after inserting "item_0".."item_9999", p=14

### CMS

- `cms_v1_empty_w2048_d5_c32.bin` -- Empty CMS, width=2048, depth=5, 32-bit counters
- `cms_v1_100items_w2048_d5_c32.bin` -- CMS after inserting "item_0".."item_99"

### Theta

- `theta_v1_empty_k4096.bin` -- Empty Theta sketch, k=4096
- `theta_v1_100items_k4096.bin` -- Theta after inserting "item_0".."item_99"
- `theta_v1_10000items_k4096.bin` -- Theta after inserting "item_0".."item_9999"

## Generation

Vectors use deterministic input sequences with `ExDataSketch.Hash.hash64/1`.
Items are strings of the form `"item_0"`, `"item_1"`, etc. The hash function
uses `:erlang.phash2/2` with Murmur3-style finalization, seeded at 0.

JSON vectors are generated using the helper at `test/support/test_vectors.ex`
and loaded/asserted by `ExDataSketch.TestVectors`.

## Versioning

`vector_version` tracks the JSON schema version. Binary vector files include
the sketch format version in their filename. When the binary layout changes,
new vectors are generated and old ones retained for backwards-compatibility
testing.
