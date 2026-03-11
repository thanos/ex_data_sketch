use rustler::{Binary, Env, Term};
use std::collections::VecDeque;

use crate::error;

const XOR_HEADER_SIZE: usize = 32;
const XOR_MAX_RETRIES: u32 = 100;

fn xor_splitmix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EB);
    x ^= x >> 31;
    x
}

fn xor_rotl64(x: u64, r: u32) -> u64 {
    (x << r) | (x >> (64 - r))
}

fn xor_fastrange(hash: u64, range: u32) -> u32 {
    ((hash as u128 * range as u128) >> 64) as u32
}

fn xor_hash_positions(hash64: u64, seed: u32, seg_size: u32) -> (u32, u32, u32) {
    let h = xor_splitmix64(hash64.wrapping_add((seed as u64).wrapping_mul(0x9E3779B97F4A7C15)));

    let h0 = xor_fastrange(h, seg_size);
    let h1 = xor_fastrange(xor_rotl64(h, 21), seg_size) + seg_size;
    let h2 = xor_fastrange(xor_rotl64(h, 42), seg_size) + seg_size * 2;

    (h0, h1, h2)
}

fn xor_fingerprint(hash64: u64, fp_bits: u8) -> u64 {
    // fp_bits is validated to be 8 or 16 before reaching here
    let fp_mask = (1u64 << fp_bits) - 1;
    let fp = hash64 & fp_mask;
    if fp == 0 { 1 } else { fp }
}

fn xor_build_impl<'a>(
    env: Env<'a>,
    hashes_bin: Binary,
    fp_bits: u8,
    seed: u32,
) -> Term<'a> {
    if fp_bits != 8 && fp_bits != 16 {
        return error::error_string(env, "fp_bits must be 8 or 16");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let raw = hashes_bin.as_slice();
    let hash_count = raw.len() / 8;

    // Deduplicate deterministically (sorted order)
    let mut unique_hashes = Vec::with_capacity(hash_count);
    for i in 0..hash_count {
        let off = i * 8;
        let h = u64::from_le_bytes(raw[off..off + 8].try_into().unwrap());
        unique_hashes.push(h);
    }
    unique_hashes.sort_unstable();
    unique_hashes.dedup();
    let n = unique_hashes.len() as u32;

    let variant: u8 = if fp_bits == 16 { 1 } else { 0 };

    if n == 0 {
        let seg_size: u32 = 1;
        let arr_len: u32 = 3;
        let fp_bytes = if fp_bits == 16 { 2 } else { 1 };
        let body = vec![0u8; arr_len as usize * fp_bytes];
        let result = xor_encode_state(0, seg_size, arr_len, seed, fp_bits, variant, &body);
        return error::ok_binary(env, &result);
    }

    let capacity = ((1.23 * n as f64).ceil() as u32) + 32;
    let seg_size = std::cmp::max((capacity + 2) / 3, 1);
    let arr_len = 3 * seg_size;

    for retry in 0..XOR_MAX_RETRIES {
        let current_seed = seed.wrapping_add(retry);

        if let Some((fingerprint_array, final_seed)) =
            xor_try_build(&unique_hashes, n, seg_size, arr_len, fp_bits, current_seed)
        {
            let body = xor_encode_body(&fingerprint_array, arr_len, fp_bits);
            let result = xor_encode_state(n, seg_size, arr_len, final_seed, fp_bits, variant, &body);
            return error::ok_binary(env, &result);
        }
    }

    error::error_string(env, "build_failed")
}

fn xor_try_build(
    hashes: &[u64],
    n: u32,
    seg_size: u32,
    arr_len: u32,
    fp_bits: u8,
    seed: u32,
) -> Option<(Vec<u64>, u32)> {
    let al = arr_len as usize;

    // Degree-count + XOR-sum representation:
    // degree[i] = number of hashes mapped to position i
    // xor_set[i] = XOR of all hash values mapped to position i
    // When degree[i] == 1, xor_set[i] is the sole hash at that position.
    let mut degrees = vec![0u32; al];
    let mut xor_set = vec![0u64; al];

    for &hash in hashes {
        let (h0, h1, h2) = xor_hash_positions(hash, seed, seg_size);
        degrees[h0 as usize] += 1;
        degrees[h1 as usize] += 1;
        degrees[h2 as usize] += 1;
        xor_set[h0 as usize] ^= hash;
        xor_set[h1 as usize] ^= hash;
        xor_set[h2 as usize] ^= hash;
    }

    // Peel: find degree-1 positions
    // Use VecDeque with pop_front/push_front to match Pure Elixir's
    // list head-take and prepend ordering for deterministic parity.
    let mut queue: VecDeque<u32> = VecDeque::new();
    for i in 0..al {
        if degrees[i] == 1 {
            queue.push_back(i as u32);
        }
    }

    let mut stack: Vec<(u64, u32)> = Vec::with_capacity(n as usize);
    let mut peeled: u32 = 0;

    while let Some(pos) = queue.pop_front() {
        if degrees[pos as usize] != 1 {
            continue;
        }

        // The sole hash at this position is recovered from xor_set
        let hash = xor_set[pos as usize];
        stack.push((hash, pos));
        peeled += 1;

        // Remove this hash from all three of its positions.
        // New degree-1 positions are prepended (push_front) to match
        // Elixir's [p | queue] prepend behavior in Enum.reduce.
        let (h0, h1, h2) = xor_hash_positions(hash, seed, seg_size);
        for &p in &[h0, h1, h2] {
            degrees[p as usize] -= 1;
            xor_set[p as usize] ^= hash;
            if degrees[p as usize] == 1 && p != pos {
                queue.push_front(p);
            }
        }
    }

    if peeled != n {
        return None;
    }

    // Assign fingerprints (process stack in reverse)
    let mut b = vec![0u64; al];

    for &(hash, peel_pos) in stack.iter().rev() {
        let (h0, h1, h2) = xor_hash_positions(hash, seed, seg_size);
        let fp = xor_fingerprint(hash, fp_bits);

        let value = match peel_pos {
            p if p == h0 => fp ^ b[h1 as usize] ^ b[h2 as usize],
            p if p == h1 => fp ^ b[h0 as usize] ^ b[h2 as usize],
            _ => fp ^ b[h0 as usize] ^ b[h1 as usize],
        };
        b[peel_pos as usize] = value;
    }

    Some((b, seed))
}

fn xor_encode_body(fingerprint_array: &[u64], arr_len: u32, fp_bits: u8) -> Vec<u8> {
    match fp_bits {
        8 => {
            let mut body = Vec::with_capacity(arr_len as usize);
            for i in 0..arr_len as usize {
                body.push(fingerprint_array[i] as u8);
            }
            body
        }
        16 => {
            let mut body = Vec::with_capacity(arr_len as usize * 2);
            for i in 0..arr_len as usize {
                body.extend_from_slice(&(fingerprint_array[i] as u16).to_le_bytes());
            }
            body
        }
        _ => unreachable!(),
    }
}

fn xor_encode_state(
    item_count: u32,
    segment_size: u32,
    array_length: u32,
    seed: u32,
    fp_bits: u8,
    variant: u8,
    body: &[u8],
) -> Vec<u8> {
    let mut result = Vec::with_capacity(XOR_HEADER_SIZE + body.len());
    result.extend_from_slice(b"XOR1");
    result.push(1); // version
    result.push(fp_bits);
    result.push(variant);
    result.push(0); // flags
    result.extend_from_slice(&item_count.to_le_bytes());
    result.extend_from_slice(&segment_size.to_le_bytes());
    result.extend_from_slice(&seed.to_le_bytes());
    result.extend_from_slice(&array_length.to_le_bytes());
    result.extend_from_slice(&[0u8; 8]); // reserved
    result.extend_from_slice(body);
    result
}

#[rustler::nif]
fn xor_build_nif<'a>(
    env: Env<'a>,
    hashes_bin: Binary,
    fp_bits: u8,
    seed: u32,
) -> Term<'a> {
    xor_build_impl(env, hashes_bin, fp_bits, seed)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn xor_build_dirty_nif<'a>(
    env: Env<'a>,
    hashes_bin: Binary,
    fp_bits: u8,
    seed: u32,
) -> Term<'a> {
    xor_build_impl(env, hashes_bin, fp_bits, seed)
}
