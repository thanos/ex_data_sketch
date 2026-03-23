use rustler::{Binary, Env, ListIterator, Term};
use xxhash_rust::xxh3;

use crate::error;

const HLL_HEADER_SIZE: usize = 4;
const HLL_MIN_P: u8 = 4;
const HLL_MAX_P: u8 = 16;

fn validate_p(env: Env, p: u8) -> Result<usize, Term> {
    if p < HLL_MIN_P || p > HLL_MAX_P {
        return Err(error::error_string(env, "invalid HLL precision p, must be 4..16"));
    }
    Ok(1usize << p)
}

/// Count leading zeros in the top `n` bits of `value`.
/// Matches Pure Elixir's `count_leading_zeros(value, n)` exactly.
fn clz_in_bits(value: u64, n: u32) -> u8 {
    if n == 0 {
        return 0;
    }
    if value == 0 {
        return n as u8;
    }
    let num_bits = 64 - value.leading_zeros();
    (n - num_bits) as u8
}

fn alpha(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m as f64),
    }
}

fn hll_update_many_impl<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary, p: u8) -> Term<'a> {
    let m = match validate_p(env, p) { Ok(m) => m, Err(e) => return e };
    let expected_len = HLL_HEADER_SIZE + m;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid HLL state length");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let state = state_bin.as_slice();
    let mut result = state.to_vec();
    let bits = 64 - p as u32;
    let remaining_mask: u64 = (1u64 << bits) - 1;

    let hashes = hashes_bin.as_slice();
    for chunk in hashes.chunks_exact(8) {
        let hash = u64::from_le_bytes(chunk.try_into().unwrap());
        let bucket = (hash >> bits) as usize;
        let remaining = hash & remaining_mask;
        let rank = clz_in_bits(remaining, bits) + 1;

        let reg_idx = HLL_HEADER_SIZE + bucket;
        if rank > result[reg_idx] {
            result[reg_idx] = rank;
        }
    }

    error::ok_binary(env, &result)
}

fn hll_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary, p: u8) -> Term<'a> {
    let m = match validate_p(env, p) { Ok(m) => m, Err(e) => return e };
    let expected_len = HLL_HEADER_SIZE + m;

    if a_bin.len() != expected_len || b_bin.len() != expected_len {
        return error::error_string(env, "invalid HLL state length for merge");
    }

    let a = a_bin.as_slice();
    let b = b_bin.as_slice();
    let mut result = a.to_vec();

    for i in HLL_HEADER_SIZE..expected_len {
        if b[i] > result[i] {
            result[i] = b[i];
        }
    }

    error::ok_binary(env, &result)
}

fn hll_estimate_impl<'a>(env: Env<'a>, state_bin: Binary, p: u8) -> Term<'a> {
    let m = match validate_p(env, p) { Ok(m) => m, Err(e) => return e };
    let expected_len = HLL_HEADER_SIZE + m;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid HLL state length for estimate");
    }

    let state = state_bin.as_slice();
    let alpha_val = alpha(m);
    let m_f = m as f64;

    let mut sum: f64 = 0.0;
    let mut zeros: usize = 0;

    for i in HLL_HEADER_SIZE..expected_len {
        let val = state[i];
        if val == 0 {
            zeros += 1;
        }
        sum += f64::powi(2.0, -(val as i32));
    }

    let raw_estimate = alpha_val * m_f * m_f / sum;

    let estimate = if raw_estimate <= 2.5 * m_f && zeros > 0 {
        // Small range correction with linear counting
        m_f * (m_f / zeros as f64).ln()
    } else if raw_estimate > (0x100000000000000u64 as f64) / 30.0 {
        // Large range correction
        -(0x10000000000000000u128 as f64) * (1.0 - raw_estimate / (0x10000000000000000u128 as f64)).ln()
    } else {
        raw_estimate
    };

    error::ok_float(env, estimate)
}

fn hll_update_many_raw_impl<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64) -> Term<'a> {
    let m = match validate_p(env, p) { Ok(m) => m, Err(e) => return e };
    let expected_len = HLL_HEADER_SIZE + m;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid HLL state length");
    }

    let state = state_bin.as_slice();
    let mut result = state.to_vec();
    let bits = 64 - p as u32;
    let remaining_mask: u64 = (1u64 << bits) - 1;

    for item_term in items {
        let bin: Binary = match item_term.decode() {
            Ok(b) => b,
            Err(_) => return error::error_string(env, "all items must be binaries"),
        };
        let hash = xxh3::xxh3_64_with_seed(bin.as_slice(), seed);
        let bucket = (hash >> bits) as usize;
        let remaining = hash & remaining_mask;
        let rank = clz_in_bits(remaining, bits) + 1;

        let reg_idx = HLL_HEADER_SIZE + bucket;
        if rank > result[reg_idx] {
            result[reg_idx] = rank;
        }
    }

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn hll_update_many_raw_nif<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64) -> Term<'a> {
    hll_update_many_raw_impl(env, state_bin, items, p, seed)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn hll_update_many_raw_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64) -> Term<'a> {
    hll_update_many_raw_impl(env, state_bin, items, p, seed)
}

#[rustler::nif]
fn hll_update_many_nif<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary, p: u8) -> Term<'a> {
    hll_update_many_impl(env, state_bin, hashes_bin, p)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn hll_update_many_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary, p: u8) -> Term<'a> {
    hll_update_many_impl(env, state_bin, hashes_bin, p)
}

#[rustler::nif]
fn hll_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary, p: u8) -> Term<'a> {
    hll_merge_impl(env, a_bin, b_bin, p)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn hll_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary, p: u8) -> Term<'a> {
    hll_merge_impl(env, a_bin, b_bin, p)
}

#[rustler::nif]
fn hll_estimate_nif<'a>(env: Env<'a>, state_bin: Binary, p: u8) -> Term<'a> {
    hll_estimate_impl(env, state_bin, p)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn hll_estimate_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, p: u8) -> Term<'a> {
    hll_estimate_impl(env, state_bin, p)
}
