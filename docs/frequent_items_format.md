# FI1 Binary State Format

This document specifies the FI1 binary format used by the FrequentItems
(SpaceSaving) sketch in ExDataSketch.

## Overview

The FI1 format stores a fixed-capacity counter set for approximate frequent
item tracking using the SpaceSaving algorithm. The binary is canonical:
entries are always sorted by `item_bytes` ascending, ensuring deterministic
serialization and byte-identical output for equivalent logical states.

## Binary Layout

All multi-byte integers are little-endian.

### Header (32 bytes, fixed)

    Offset  Size  Type   Field         Description
    ------  ----  ----   -----         -----------
    0       4     bytes  magic         "FI1\0" (0x46 0x49 0x31 0x00)
    4       1     u8     version       Format version (currently 1)
    5       1     u8     flags         Key encoding (see below)
    6       2     u16    reserved      Must be 0
    8       4     u32    k             Max counters capacity
    12      8     u64    n             Total observed items (sum of all weights)
    20      4     u32    entry_count   Current number of entries (<= k)
    24      4     u32    reserved2     Must be 0
    28      4     u32    reserved3     Must be 0

Total header: 32 bytes.

### Flags Byte (offset 5)

The flags byte encodes the key encoding policy used when the sketch was
created:

| Value | Encoding            | Description                                |
|-------|---------------------|--------------------------------------------|
| 0     | `:binary`           | Keys are raw binary (pass-through)         |
| 1     | `:int`              | Keys are signed 64-bit LE integers         |
| 2     | `{:term, :external}`| Keys are Erlang external term format       |

The key encoding affects how items are converted to `item_bytes` at the
public API boundary. The backend always operates on raw `item_bytes`.

### Body (variable length)

The body contains `entry_count` entries, sorted by `item_bytes` in
ascending lexicographic order:

    Per entry:
      item_len    u32 LE     Byte length of item_bytes
      item_bytes  variable   Raw key bytes (item_len bytes)
      count       u64 LE     Estimated frequency count
      error       u64 LE     Maximum overcount error

Total body size: sum of (4 + item_len + 8 + 8) for each entry.

## Canonicalization Rules

1. Entries are always sorted by `item_bytes` ascending (lexicographic byte
   comparison). This ensures that two sketches with the same logical state
   produce identical binaries.

2. During SpaceSaving eviction, when multiple entries share the minimum
   count, the entry with the lexicographically smallest `item_bytes` is
   evicted. This deterministic tie-breaking ensures reproducible behavior.

3. After any mutation (update, merge), the state is re-encoded in canonical
   sorted order.

## EXSK Codec Integration

When serialized through the EXSK envelope format, FrequentItems uses:

- Sketch ID: **6**
- Params binary (5 bytes): `<<k::unsigned-little-32, flags::unsigned-8>>`

## Merge Algebra

### Merge Operation

Given sketches A and B with the same capacity k and key_encoding:

1. Compute the union of keys from both entry maps.
2. For each key, sum counts and errors additively:
   - `count_merged[key] = count_a[key] + count_b[key]`
   - `error_merged[key] = error_a[key] + error_b[key]`
   (Missing keys contribute 0 for both count and error.)
3. If the union has more than k entries, retain only the k entries with
   the highest counts. Ties are broken by keeping the entry with the
   lexicographically smallest `item_bytes`. Dropped entries are discarded.
4. Sort retained entries by `item_bytes` ascending for canonical encoding.
5. Set `n_merged = n_a + n_b`.

### Commutativity

`merge(A, B) == merge(B, A)` because:
- Additive count/error combination is commutative (addition is commutative).
- The top-k selection sorts by `{-count, item_bytes}`, which is a
  deterministic total order independent of input order.
- Canonical encoding sorts by `item_bytes` ascending.

### Associativity

Merge is **not** exactly associative at the binary level. When an
intermediate merge drops entries to enforce the k-capacity limit, those
entries' counts are lost. A different grouping may drop different entries,
leading to different retained sets.

However, the following properties hold regardless of grouping:
- `count(merge(merge(A, B), C)) == count(merge(A, merge(B, C)))` --
  the total count `n` is always exactly additive.
- `entry_count(result) <= k` -- the capacity invariant is preserved.

### Identity

`merge(empty, S) == S` because:
- An empty sketch contributes no entries and n=0.
- The additive combination with zero is identity.
- No entries are dropped since entry_count <= k.
