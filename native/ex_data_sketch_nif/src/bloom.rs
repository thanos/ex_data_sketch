use rustler::{Binary, Env, Term};

use crate::error;

const BLM_HEADER_SIZE: usize = 40;

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
    let bc = bit_count as u64;

    for chunk in hashes.chunks_exact(8) {
        let hash64 = u64::from_le_bytes(chunk.try_into().unwrap());
        let h1 = (hash64 >> 32) as u128;
        let h2 = (hash64 & 0xFFFFFFFF) as u128;

        for i in 0..hash_count as u128 {
            // Use u128 to match Elixir bignum arithmetic exactly
            let pos = ((h1 + i * h2) % bc as u128) as u32;
            let byte_idx = (pos / 8) as usize;
            let bit_idx = pos % 8;
            bitset[byte_idx] |= 1u8 << bit_idx;
        }
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
fn bloom_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    bloom_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn bloom_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    bloom_merge_impl(env, a_bin, b_bin)
}
