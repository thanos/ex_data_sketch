use rustler::{Binary, Env, Term};
use xxhash_rust::xxh3;

use crate::error;

const ULL_HEADER_SIZE: usize = 8;
const ULL_MIN_P: u8 = 4;
const ULL_MAX_P: u8 = 26;

fn validate_p(env: Env, p: u8) -> Result<usize, Term> {
    if p < ULL_MIN_P || p > ULL_MAX_P {
        return Err(error::error_string(env, "invalid ULL precision p, must be 4..26"));
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

/// Compute ULL register value from a 64-bit hash.
/// register_value = 2 * geometric_rank - sub_bit
fn ull_register_value(hash64: u64, p: u8) -> u8 {
    let bits = 64 - p as u32;
    let remaining_mask: u64 = (1u64 << bits) - 1;
    let remaining = hash64 & remaining_mask;

    let geometric_rank = clz_in_bits(remaining, bits) + 1;

    let sub_bit = if (geometric_rank as u32) > bits {
        0u8
    } else {
        let bit_pos = bits - geometric_rank as u32;
        ((remaining >> bit_pos) & 1) as u8
    };

    let value = 2 * geometric_rank - sub_bit;
    value.min(255)
}

/// sigma(x) from Ertl 2017 reference implementation.
/// z = x, y = 1; loop: x = x^2, z += x*y, y *= 2; until convergence
fn sigma(x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    let mut x = x;
    let mut z = x;
    let mut y = 1.0f64;
    loop {
        let x2 = x * x;
        let z2 = z + x2 * y;
        let y2 = y + y;
        if z2 == z {
            break;
        }
        x = x2;
        z = z2;
        y = y2;
    }
    z
}

/// tau(x) from Ertl 2017 reference implementation.
/// z = 1-x, y = 1; loop: x = sqrt(x), y *= 0.5, z -= (1-x)^2 * y; return z/3
fn tau(x: f64) -> f64 {
    if x <= 0.0 || x >= 1.0 {
        return 0.0;
    }
    let mut x = x;
    let mut z = 1.0 - x;
    let mut y = 1.0f64;
    loop {
        let x2 = x.sqrt();
        let y2 = y * 0.5;
        let z2 = z - (1.0 - x2).powi(2) * y2;
        if z2 == z {
            break;
        }
        x = x2;
        z = z2;
        y = y2;
    }
    z / 3.0
}

fn ull_update_many_impl<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary, p: u8) -> Term<'a> {
    let m = match validate_p(env, p) {
        Ok(m) => m,
        Err(term) => return term,
    };
    let expected_len = ULL_HEADER_SIZE + m;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid ULL state length");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let state = state_bin.as_slice();
    let mut result = state.to_vec();

    let hashes = hashes_bin.as_slice();
    for chunk in hashes.chunks_exact(8) {
        let hash = u64::from_le_bytes(chunk.try_into().unwrap());
        let bucket = (hash >> (64 - p)) as usize;
        let reg_value = ull_register_value(hash, p);

        let reg_idx = ULL_HEADER_SIZE + bucket;
        if reg_value > result[reg_idx] {
            result[reg_idx] = reg_value;
        }
    }

    error::ok_binary(env, &result)
}

fn ull_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary, p: u8) -> Term<'a> {
    let m = match validate_p(env, p) {
        Ok(m) => m,
        Err(term) => return term,
    };
    let expected_len = ULL_HEADER_SIZE + m;

    if a_bin.len() != expected_len || b_bin.len() != expected_len {
        return error::error_string(env, "invalid ULL state length for merge");
    }

    let a = a_bin.as_slice();
    let b = b_bin.as_slice();
    let mut result = a.to_vec();

    for i in ULL_HEADER_SIZE..expected_len {
        if b[i] > result[i] {
            result[i] = b[i];
        }
    }

    error::ok_binary(env, &result)
}

fn ull_estimate_impl<'a>(env: Env<'a>, state_bin: Binary, p: u8) -> Term<'a> {
    let m = match validate_p(env, p) {
        Ok(m) => m,
        Err(term) => return term,
    };
    let expected_len = ULL_HEADER_SIZE + m;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid ULL state length for estimate");
    }

    let state = state_bin.as_slice();
    let m_f = m as f64;
    let alpha_inf: f64 = 1.0 / (2.0 * 2.0_f64.ln());

    // Count registers at each value; find q_max
    let mut counts = [0u32; 256];
    let mut q_max: usize = 0;

    for i in ULL_HEADER_SIZE..expected_len {
        let val = state[i] as usize;
        counts[val] += 1;
        if val > q_max {
            q_max = val;
        }
    }

    if q_max == 0 {
        return error::ok_float(env, 0.0);
    }

    let c0 = counts[0] as f64;
    let c_q = counts[q_max] as f64;

    // Horner scheme from Ertl 2017 Algorithm 4:
    // z = m * tau(1 - C[q]/m)
    // for k from q-1 down to 1: z = (z + C[k]) * 0.5
    // z += m * sigma(C[0]/m)
    let mut z = m_f * tau(1.0 - c_q / m_f);

    for k in (1..q_max).rev() {
        z = (z + counts[k] as f64) * 0.5;
    }

    z += m_f * sigma(c0 / m_f);

    let estimate = if z == 0.0 {
        0.0
    } else {
        alpha_inf * m_f * m_f / z
    };

    error::ok_float(env, estimate)
}

fn ull_update_many_raw_impl<'a>(env: Env<'a>, state_bin: Binary, items_bin: Binary, p: u8, seed: u64) -> Term<'a> {
    let m = match validate_p(env, p) {
        Ok(m) => m,
        Err(term) => return term,
    };
    let expected_len = ULL_HEADER_SIZE + m;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid ULL state length");
    }

    let state = state_bin.as_slice();
    let mut result = state.to_vec();

    let items = items_bin.as_slice();
    let mut offset = 0;
    while offset + 4 <= items.len() {
        let len = u32::from_le_bytes(items[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > items.len() {
            return error::error_string(env, "items_bin truncated");
        }
        let item_bytes = &items[offset..offset + len];
        offset += len;

        let hash = xxh3::xxh3_64_with_seed(item_bytes, seed);
        let bucket = (hash >> (64 - p)) as usize;
        let reg_value = ull_register_value(hash, p);

        let reg_idx = ULL_HEADER_SIZE + bucket;
        if reg_value > result[reg_idx] {
            result[reg_idx] = reg_value;
        }
    }

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn ull_update_many_raw_nif<'a>(env: Env<'a>, state_bin: Binary, items_bin: Binary, p: u8, seed: u64) -> Term<'a> {
    ull_update_many_raw_impl(env, state_bin, items_bin, p, seed)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ull_update_many_raw_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, items_bin: Binary, p: u8, seed: u64) -> Term<'a> {
    ull_update_many_raw_impl(env, state_bin, items_bin, p, seed)
}

#[rustler::nif]
fn ull_update_many_nif<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary, p: u8) -> Term<'a> {
    ull_update_many_impl(env, state_bin, hashes_bin, p)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ull_update_many_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary, p: u8) -> Term<'a> {
    ull_update_many_impl(env, state_bin, hashes_bin, p)
}

#[rustler::nif]
fn ull_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary, p: u8) -> Term<'a> {
    ull_merge_impl(env, a_bin, b_bin, p)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ull_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary, p: u8) -> Term<'a> {
    ull_merge_impl(env, a_bin, b_bin, p)
}

#[rustler::nif]
fn ull_estimate_nif<'a>(env: Env<'a>, state_bin: Binary, p: u8) -> Term<'a> {
    ull_estimate_impl(env, state_bin, p)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ull_estimate_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, p: u8) -> Term<'a> {
    ull_estimate_impl(env, state_bin, p)
}
