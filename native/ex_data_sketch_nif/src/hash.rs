use xxhash_rust::xxh3;

#[rustler::nif]
fn xxhash3_64_nif(data: rustler::Binary) -> u64 {
    xxh3::xxh3_64(data.as_slice())
}

#[rustler::nif]
fn xxhash3_64_seeded_nif(data: rustler::Binary, seed: u64) -> u64 {
    xxh3::xxh3_64_with_seed(data.as_slice(), seed)
}

// -- MurmurHash3_x64_128 (returning high 64 bits) --
//
// Pure Rust implementation of MurmurHash3_x64_128, matching the canonical
// Austin Appleby reference and Apache DataSketches' high-64-bit convention.
// `ExDataSketch.Hash.Murmur3.pure_hash/2` MUST produce byte-identical output
// to this function for all inputs (verified by parity tests).

const C1: u64 = 0x87c3_7b91_1142_53d5;
const C2: u64 = 0x4cf5_ad43_2745_937f;

#[inline]
fn rotl64(v: u64, n: u32) -> u64 {
    v.rotate_left(n)
}

#[inline]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

/// MurmurHash3_x64_128 — exposed for use by the hot-path raw NIFs in
/// other modules (hll, ull, theta, cms). Returns the full 128-bit
/// pair; callers take the high 64 bits for a stable 64-bit hash.
pub(crate) fn murmur3_x64_128(data: &[u8], seed: u32) -> (u64, u64) {
    let seed = seed as u64;
    let mut h1: u64 = seed;
    let mut h2: u64 = seed;
    let nblocks = data.len() / 16;

    // Body
    for i in 0..nblocks {
        let off = i * 16;
        let k1 = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        let k2 = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());

        let mut k1m = k1.wrapping_mul(C1);
        k1m = rotl64(k1m, 31);
        k1m = k1m.wrapping_mul(C2);
        h1 ^= k1m;

        h1 = rotl64(h1, 27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        let mut k2m = k2.wrapping_mul(C2);
        k2m = rotl64(k2m, 33);
        k2m = k2m.wrapping_mul(C1);
        h2 ^= k2m;

        h2 = rotl64(h2, 31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    // Tail
    let tail = &data[nblocks * 16..];
    let tail_len = tail.len();

    if tail_len > 0 {
        let (k1, k2) = if tail_len >= 9 {
            let k1 = u64::from_le_bytes(tail[0..8].try_into().unwrap());
            let mut buf = [0u8; 8];
            let extra = tail_len - 8;
            buf[..extra].copy_from_slice(&tail[8..]);
            let k2 = u64::from_le_bytes(buf);
            (k1, k2)
        } else {
            let mut buf = [0u8; 8];
            buf[..tail_len].copy_from_slice(tail);
            (u64::from_le_bytes(buf), 0u64)
        };

        if tail_len > 8 {
            let mut k2m = k2.wrapping_mul(C2);
            k2m = rotl64(k2m, 33);
            k2m = k2m.wrapping_mul(C1);
            h2 ^= k2m;
        }

        let mut k1m = k1.wrapping_mul(C1);
        k1m = rotl64(k1m, 31);
        k1m = k1m.wrapping_mul(C2);
        h1 ^= k1m;
    }

    // Finalization
    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

#[rustler::nif]
fn murmur3_x64_128_nif(data: rustler::Binary, seed: u32) -> u64 {
    let (h1, _h2) = murmur3_x64_128(data.as_slice(), seed);
    h1
}

#[rustler::nif]
fn murmur3_x64_128_full_nif(data: rustler::Binary, seed: u32) -> (u64, u64) {
    murmur3_x64_128(data.as_slice(), seed)
}
