use rustler::{Binary, Env, ListIterator, Term};
use xxhash_rust::xxh3;

use crate::error;
use crate::hash::murmur3_x64_128;

// Hash algorithm wire bytes for the v2 raw NIF dispatch.
// MUST match `ExDataSketch.Hash.Metadata.algorithm_to_byte/1`.
const ALGO_XXH3: u8 = 1;
const ALGO_MURMUR3: u8 = 2;

const BLM_HEADER_SIZE: usize = 40;

/// Kirsch-Mitzenmacher double-hashing: set `hash_count` bits derived from
/// a single `hash64`. Shared by the pre-hashed and raw (hash-in-Rust)
/// paths so both set bits identically.
fn bloom_set_bits(bitset: &mut [u8], hash64: u64, hash_count: u16, bit_count: u32) {
    let h1 = (hash64 >> 32) as u128;
    let h2 = (hash64 & 0xFFFFFFFF) as u128;
    let bc = bit_count as u128;

    for i in 0..hash_count as u128 {
        // Use u128 to match Elixir bignum arithmetic exactly
        let pos = ((h1 + i * h2) % bc) as u32;
        let byte_idx = (pos / 8) as usize;
        let bit_idx = pos % 8;
        bitset[byte_idx] |= 1u8 << bit_idx;
    }
}

fn bloom_put_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    hash_count: u16,
    bit_count: u32,
) -> Term<'a> {
    if bit_count == 0 {
        return error::error_string(env, "bit_count must be > 0");
    }
    if hash_count == 0 {
        return error::error_string(env, "hash_count must be > 0");
    }

    let bitset_len = ((bit_count as usize) + 7) / 8;
    let expected_len = BLM_HEADER_SIZE + bitset_len;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid Bloom state length");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let state = state_bin.as_slice();
    let header = &state[..BLM_HEADER_SIZE];
    let mut bitset = state[BLM_HEADER_SIZE..].to_vec();

    let hashes = hashes_bin.as_slice();

    for chunk in hashes.chunks_exact(8) {
        let hash64 = u64::from_le_bytes(chunk.try_into().unwrap());
        bloom_set_bits(&mut bitset, hash64, hash_count, bit_count);
    }

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(header);
    result.extend_from_slice(&bitset);
    error::ok_binary(env, &result)
}

fn bloom_put_many_raw_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    hash_count: u16,
    bit_count: u32,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
    if bit_count == 0 {
        return error::error_string(env, "bit_count must be > 0");
    }
    if hash_count == 0 {
        return error::error_string(env, "hash_count must be > 0");
    }

    let bitset_len = ((bit_count as usize) + 7) / 8;
    let expected_len = BLM_HEADER_SIZE + bitset_len;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid Bloom state length");
    }

    let state = state_bin.as_slice();
    let header = &state[..BLM_HEADER_SIZE];
    let mut bitset = state[BLM_HEADER_SIZE..].to_vec();
    let m3_seed = seed as u32;

    for item_term in items {
        let bin: Binary = match item_term.decode() {
            Ok(b) => b,
            Err(_) => return error::error_string(env, "all items must be binaries"),
        };
        let hash64 = match algorithm {
            ALGO_XXH3 => xxh3::xxh3_64_with_seed(bin.as_slice(), seed),
            ALGO_MURMUR3 => murmur3_x64_128(bin.as_slice(), m3_seed).0,
            _ => {
                return error::error_string(
                    env,
                    "unsupported hash algorithm byte (expected 1=xxhash3, 2=murmur3)",
                )
            }
        };
        bloom_set_bits(&mut bitset, hash64, hash_count, bit_count);
    }

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(header);
    result.extend_from_slice(&bitset);
    error::ok_binary(env, &result)
}

fn bloom_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    if a_bin.len() != b_bin.len() {
        return error::error_string(env, "Bloom state length mismatch for merge");
    }
    if a_bin.len() < BLM_HEADER_SIZE {
        return error::error_string(env, "invalid Bloom state length");
    }

    let a = a_bin.as_slice();
    let b = b_bin.as_slice();
    let mut result = a.to_vec();

    // Bitwise OR the bitset bytes (body after header)
    for i in BLM_HEADER_SIZE..result.len() {
        result[i] |= b[i];
    }

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn bloom_put_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    hash_count: u16,
    bit_count: u32,
) -> Term<'a> {
    bloom_put_many_impl(env, state_bin, hashes_bin, hash_count, bit_count)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn bloom_put_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    hash_count: u16,
    bit_count: u32,
) -> Term<'a> {
    bloom_put_many_impl(env, state_bin, hashes_bin, hash_count, bit_count)
}

#[rustler::nif]
fn bloom_put_many_raw_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    hash_count: u16,
    bit_count: u32,
    seed: u64,
) -> Term<'a> {
    bloom_put_many_raw_impl(env, state_bin, items, hash_count, bit_count, seed, ALGO_XXH3)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn bloom_put_many_raw_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    hash_count: u16,
    bit_count: u32,
    seed: u64,
) -> Term<'a> {
    bloom_put_many_raw_impl(env, state_bin, items, hash_count, bit_count, seed, ALGO_XXH3)
}

#[rustler::nif]
fn bloom_put_many_raw_h_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    hash_count: u16,
    bit_count: u32,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
    bloom_put_many_raw_impl(env, state_bin, items, hash_count, bit_count, seed, algorithm)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn bloom_put_many_raw_h_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    hash_count: u16,
    bit_count: u32,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
    bloom_put_many_raw_impl(env, state_bin, items, hash_count, bit_count, seed, algorithm)
}

#[rustler::nif]
fn bloom_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    bloom_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn bloom_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    bloom_merge_impl(env, a_bin, b_bin)
}
