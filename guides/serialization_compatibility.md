# Serialization Compatibility Contract (v0.8.0)

This document is the authoritative statement of what
`ex_data_sketch` promises about its binary serialization format
across releases. It is intended for downstream users who need to
reason about persistence durability, distributed-node compatibility,
and long-term storage.

For the byte-level layout itself, see [`plans/binary_contract.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/binary_contract.md) (v2)
and `lib/ex_data_sketch/codec.ex` (v1). For migration guidance from
v0.7.x, see `v0.8.0_migration_notes.md`.

## The promise

For every release in the v0.x series, `ex_data_sketch` promises:

1. **Read compatibility** — a v0.N reader can decode any EXSK
   binary produced by any v0.M release where `M <= N`.
2. **Magic and version stability** — the magic bytes `"EXSK"` and
   the layout of the version byte are stable across all v0.x
   releases.
3. **Hash algorithm wire-byte stability** — the byte values for
   `:phash2 = 0`, `:xxhash3 = 1`, `:murmur3 = 2`, `:custom = 255`
   are stable across all v0.x releases.
4. **Sketch family ID stability** — `Codec.sketch_id_*` constants
   (1 = HLL, 2 = CMS, ..., 15 = ULL) are stable across all v0.x
   releases. Future sketch families get new IDs (16+).
5. **No silent format changes** — bumping the serialization version
   is announced in the CHANGELOG and documented in the migration
   notes for that release.
6. **Structured failure on incompatible input** — readers MUST
   return `{:error, %DeserializationError{}}` on any input they
   cannot parse. They MUST NOT crash the BEAM, return `{:ok, _}`
   with corrupted state, or silently produce a sketch from
   malformed bytes.

## The non-promise

For the v0.x series, `ex_data_sketch` does NOT promise:

1. **Write compatibility from N back to M.** A v0.N writer is free
   to produce binaries that v0.M (where `M < N`) cannot read. v0.8.0
   exercised this: it writes EXSK v2 frames that v0.7.x cannot decode.
2. **Cross-language interoperability.** Only `ExDataSketch.Theta` has
   a documented Apache DataSketches interop path
   (`Theta.serialize_datasketches/1`,
   `Theta.deserialize_datasketches/2`). Other sketch families are
   ex_data_sketch-native only until v0.10.0's interop track.
3. **Stability of internal sketch state binaries.** A sketch's
   `state` field is internal. Only the framed EXSK output of
   `serialize/1` is stable.
4. **Stability across the v0.x to v1.0 boundary.** v1.0 is the
   designated breaking-change opportunity. v0.x readers may not
   accept v1.x binaries; v1.0 may rename / re-id sketch families
   that have not yet stabilized.
5. **Stability of error messages.** `DeserializationError.message`
   strings are intended for human consumption. They may evolve in
   any release.

## Format-by-format inventory (current state at v0.8.0)

| Format | Version byte | Used by | Status |
|--------|--------------|---------|--------|
| EXSK v1 | `1` | v0.1 through v0.7.x writers, v0.8.0 reader | Read-only in v0.8.0+ |
| EXSK v2 | `2` | v0.8.0+ writers and readers | Current default |
| Theta CompactSketch | (Apache DataSketches binary layout) | `Theta.serialize_datasketches/1` | Cross-language stable |

There is no EXSK v3 today. v3 is reserved for a future frame layout
change that cannot be expressed as either a `block_version` bump or a
metadata-block extension.

## Versioning axes

EXSK v2 has four orthogonal versioning axes. The promise above
applies to each independently.

| Axis | Byte location | Bumped when... | Reader contract |
|------|---------------|----------------|-----------------|
| `serialization_version` | EXSK frame, offset 4 | The frame layout itself changes. | Reader MUST reject unknown values with a structured error. |
| `Hash.Metadata.block_version` | metadata block, offset 0 (relative) | The metadata block layout changes. | Reader MUST reject unknown values. |
| `sketch_family_version` | EXSK frame, offset 6 (mirrored in metadata block) | A specific sketch's internal state binary layout changes. | Reader MUST reject unknown values for that sketch family. |
| Metadata `extension` bytes | metadata block, offset 16+ | Additive forward-compat fields. | Reader MUST preserve unknown extension bytes verbatim on re-encode. |

This layout supports 256 frame versions × 256 metadata block versions
× 256 family versions per sketch × up to 64 KiB of forward-compat
extension space. There is no realistic scenario in which v0.x
exhausts any of these.

## Cross-platform stability

For the supported precompiled target matrix (see
`precompiled_nifs.md`):

| Property | Guarantee |
|----------|-----------|
| Endianness | All multi-byte fields are little-endian on every supported target. |
| `Hash.XXH3` output | Byte-identical across all supported targets and OTP versions when using the NIF. |
| `Hash.Murmur3` output | Byte-identical across all targets, including the pure-Elixir fallback. Verified against Python `mmh3` regression vectors. |
| `Hash.phash2` output | NOT guaranteed across OTP major versions. Documented; non-default. |
| `Binary.CRC.crc32c` output | Byte-identical across all targets. Verified against the standard `"123456789" -> 0xE3069283` check vector and Python `crc32c` regression vectors. |
| Floating-point estimator output | Identical to within `1.0e-9` across targets (libm differences are absorbed by the documented tolerance). |

## Cross-OTP stability

| OTP version | `:phash2` hash output | XXH3 / Murmur3 / CRC32C output |
|-------------|------------------------|-------------------------------|
| 26 -> 27 | Subject to change | Stable |
| 27 -> 28 | Subject to change | Stable |
| 28 -> 29 | Subject to change | Stable |

`:phash2` instability across OTP major versions is a property of the
BEAM runtime, not of `ex_data_sketch`. The library's only mitigation
is to NOT default to `:phash2` and to mark it
`stability: :otp_dependent` in `Hash.algorithm_info/1`. Users who
persist sketches across an OTP major-version boundary MUST either:

- use `:xxhash3` (NIF, fully stable) or `:murmur3` (Pure + NIF, fully
  stable);
- or accept that their `:phash2`-based sketches are not portable
  across the boundary.

## Cross-language stability

Cross-language interop is OUT OF SCOPE for v0.8.0 except for the
preserved `ExDataSketch.Theta` Apache DataSketches CompactSketch
path.

What IS preserved as the foundation for future cross-language work:

- `Hash.Murmur3` produces output byte-identical to Apache
  DataSketches' MurmurHash3_x64_128 high-64-bit convention.
- `Hash.Metadata.algorithm_to_byte/1` exposes stable wire bytes that
  any external implementation can adopt.
- `Binary.CRC.crc32c` is the standard iSCSI/Btrfs/SCTP/Snappy CRC32C.
  Any external CRC32C implementation produces the same output.

v0.10.0 will build on these to add full KLL and HLL Apache
interoperability.

## Forward-compatibility recipes

A future v0.y release wants to add a new field to the metadata block
without breaking v0.8.0 readers. Recipe:

1. Write the new field into the metadata block's `extension` trailer.
2. Increment `Hash.Metadata.block_version` only if the new field is
   load-bearing for correctness (rare).
3. Document the new field's wire layout in [`plans/hash_binary_contract.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/hash_binary_contract.md).

A v0.8.0 reader, on encountering such a binary:

- Parses the metadata block header (16 bytes) successfully.
- Sees `extension_size = N > 0` and consumes N bytes of opaque
  extension data.
- Round-trips the extension verbatim if the sketch is re-serialized.
- Does NOT interpret the extension bytes — they are forward-compat.

This is the additive-evolution path. The vast majority of future
metadata additions should use it.

## Breaking-change recipes (escape hatches reserved for v1.0)

If a future change cannot be expressed additively:

| Change | Required version bump |
|--------|------------------------|
| Rename a sketch family | `serialization_version` (v3) AND reissue sketch ID |
| Change a sketch's internal state binary layout | `sketch_family_version` only (frame stays at v2) |
| Replace CRC32C with a different checksum algorithm | `serialization_version` (v3) |
| Drop a hash algorithm | wire-byte reservation + `block_version` bump |
| Change the EXSK magic bytes | `serialization_version` (v3) + a documented one-cycle deprecation |

For v0.x, only `sketch_family_version` bumps (which are local to a
single sketch and require no global coordination) are realistically
in play. The other escape hatches are documented for v1.0+
planning.

## Test guarantees

The compatibility contract is locked by tests:

| Contract | Lock |
|----------|------|
| v0.7.x EXSK v1 binaries decode in v0.8.0 | `test/ex_data_sketch_v1_compat_test.exs` — 9 tests over `test/vectors_v1/` corpus |
| v0.8.0 EXSK v2 binaries round-trip identically | `test/ex_data_sketch_vectors_test.exs` (regenerated) + per-sketch round-trip tests |
| Bit-flip corruption is always detected | `test/ex_data_sketch/binary/header_test.exs` — 200-mutation fuzz |
| Random binaries never crash the decoder | `test/ex_data_sketch/binary/header_test.exs` — 200 random-binary property |
| Pure Elixir and Rust produce identical XXH3 / Murmur3 / CRC32C output | `test/ex_data_sketch/hash/*_test.exs`, `test/ex_data_sketch/binary/crc_test.exs` — 200-input parity properties |
| Standard CRC32C check vector | `test/ex_data_sketch/binary/crc_test.exs` — `"123456789" -> 0xE3069283` |
| Python `crc32c` and `mmh3` regression vectors | both above |

If any of these tests fail in a future release, the compatibility
contract has been violated and the release should NOT ship until
either the bug is fixed or the violation is documented as an
intentional breaking change.

## See also

- [`plans/binary_contract.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/binary_contract.md) — v2 byte-level layout specification.
- [`plans/hash_binary_contract.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/hash_binary_contract.md) — metadata block byte-level layout.
- [`plans/corruption_detection.md`](https://github.com/thanos/ex_data_sketch/blob/main/plans/corruption_detection.md) — CRC32C rationale and error taxonomy.
- `v0.8.0_migration_notes.md` — v0.7.x to v0.8.0 upgrade guide.
- `v0.8.0_architecture.md` — layered architecture overview.
- `lib/ex_data_sketch/codec.ex` — legacy v1 codec (preserved).
- `lib/ex_data_sketch/binary.ex` — v2 public facade.
- `CHANGELOG.md` — release-by-release format changes.
