use rustler::{Binary, Env, ListIterator, Term};
use xxhash_rust::xxh3;

use crate::error;
use crate::hash::murmur3_x64_128;

const ALGO_XXH3: u8 = 1;
const ALGO_MURMUR3: u8 = 2;

const ULL_HEADER_SIZE: usize = 8;
const ULL_MIN_P: u8 = 4;
const ULL_MAX_P: u8 = 26;
const ULL_MIN_P_ESTIMATOR: i32 = 3;

fn validate_p(env: Env, p: u8) -> Result<usize, Term> {
    if p < ULL_MIN_P || p > ULL_MAX_P {
        return Err(error::error_string(env, "invalid ULL precision p, must be 4..26"));
    }
    Ok(1usize << p)
}

// ============================================================
// ULL (UltraLogLog) Implementation
// ============================================================
//
// Port of Ertl 2023's UltraLogLog, following the dynatrace-oss/hash4j
// reference implementation. Each register byte encodes a compressed 3-bit
// window of a per-bucket accumulator: the position of the highest bit ever
// OR-ed in (geometric rank, via `pack`/`unpack`) plus the two bits just
// below it (sub-bucket refinement). Estimation uses the
// OptimalFGRAEstimator: closed-form small-range/large-range correction
// terms plus a per-register contribution lookup table for the bulk of the
// range, combined via `sum^(-1/tau) * factor[p]`.
//
// This mirrors `ExDataSketch.Backend.Pure`'s ULL section exactly (same
// constant tables, same formulas) and has been numerically verified against
// the same compiled Java reference and against the Pure backend itself
// (see `ExDataSketch.ULLTest`'s "Pure vs Rust parity" tests).

include!("ull_tables.rs");

/// hash4j's `UltraLogLog.unpack/1`.
fn ull_unpack(register: u8) -> u64 {
    ULL_UNPACK_TABLE[register as usize]
}

/// hash4j's `UltraLogLog.pack/1`. `hash_prefix` is treated as an unsigned
/// 64-bit accumulator; the low 8 bits of the result are the register byte.
fn ull_pack(hash_prefix: u64) -> u8 {
    let nlz: u32 = if hash_prefix == 0 {
        65
    } else {
        hash_prefix.leading_zeros() + 1
    };
    let shifted = hash_prefix.wrapping_shl(nlz);
    let low2 = (shifted >> 62) as i64;
    let packed = (-(nlz as i64) * 4 + low2).rem_euclid(256);
    packed as u8
}

/// Compute the (bucket, one_hot_bit) pair for a hash under precision p.
/// Direct port of hash4j's `UltraLogLog.add/2`'s index/nlz derivation.
fn ull_bucket_and_bit(hash64: u64, p: u8) -> (usize, u64) {
    let q = 64 - p as u32;
    let bucket = (hash64 >> q) as usize;
    let complement_hv = !hash64;
    let shifted = complement_hv.wrapping_shl(p as u32);
    let complement2 = !shifted;
    let nlz = complement2.leading_zeros();
    let shift_amt = ((nlz as i64) + (p as i64) - 65).rem_euclid(64) as u32;
    (bucket, 1u64 << shift_amt)
}

fn ull_update_one(result: &mut [u8], bucket: usize, bit: u64) {
    let reg_idx = ULL_HEADER_SIZE + bucket;
    let old_val = result[reg_idx];
    let new_val = ull_pack(ull_unpack(old_val) | bit);
    if new_val != old_val {
        result[reg_idx] = new_val;
    }
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
        let (bucket, bit) = ull_bucket_and_bit(hash, p);
        ull_update_one(&mut result, bucket, bit);
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
        if b[i] != 0 {
            result[i] = ull_pack(ull_unpack(result[i]) | ull_unpack(b[i]));
        }
    }

    error::ok_binary(env, &result)
}

// -- OptimalFGRAEstimator constants (Ertl 2023 / hash4j) --

const ULL_ETA_0: f64 = 4.663135422063788;
const ULL_ETA_1: f64 = 2.1378502137958524;
const ULL_ETA_2: f64 = 2.781144650979996;
const ULL_ETA_3: f64 = 0.9824082545153715;
const ULL_TAU: f64 = 0.8194911375910897;

fn ull_psi_prime(z: f64, z_square: f64, eta23x: f64, eta13x: f64, eta3012xx: f64) -> f64 {
    (z + eta23x) * (z_square + eta13x) + eta3012xx
}

// sigma(z) from OptimalFGRAEstimator. The `z >= 1.0` branch is
// mathematically unreachable for any sketch built through ordinary
// add/merge -- `z` here is always a quadratic-root probability strictly
// less than 1 by construction -- but a large finite sentinel (matching the
// Pure Elixir backend, which cannot represent float infinity) is used
// instead of `f64::INFINITY` so the two backends agree bit-for-bit even on
// a corrupted/adversarial state that somehow reaches this branch.
#[allow(clippy::too_many_arguments)]
fn ull_sigma(z: f64, eta_3: f64, eta_x: f64, pow_2_tau: f64, eta23x: f64, eta13x: f64, eta3012xx: f64) -> f64 {
    if z <= 0.0 {
        return eta_3;
    }
    if z >= 1.0 {
        return 1.0e300;
    }

    let mut pow_z = z;
    let mut next_pow_z = z * z;
    let mut s = 0.0f64;
    let mut pow_tau = eta_x;

    loop {
        let next_next_pow_z = next_pow_z * next_pow_z;
        let new_s = s + pow_tau * (pow_z - next_pow_z) * ull_psi_prime(next_pow_z, next_next_pow_z, eta23x, eta13x, eta3012xx);

        if new_s > s {
            pow_z = next_pow_z;
            next_pow_z = next_next_pow_z;
            s = new_s;
            pow_tau *= pow_2_tau;
        } else {
            return new_s / z;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn ull_phi(
    z: f64,
    z_square: f64,
    phi_1: f64,
    p_initial: f64,
    pow_2_minus_tau: f64,
    eta23x: f64,
    eta13x: f64,
    eta3012xx: f64,
) -> f64 {
    if z <= 0.0 {
        return 0.0;
    }
    if z >= 1.0 {
        return phi_1;
    }

    let mut pow_z = z;
    let mut next_pow_z = z.sqrt();
    let mut p = p_initial / (1.0 + next_pow_z);
    let mut ps = ull_psi_prime(pow_z, z_square, eta23x, eta13x, eta3012xx);
    let mut s = next_pow_z * (ps + ps) * p;

    loop {
        let new_pow_z = next_pow_z;
        let new_next_pow_z = new_pow_z.sqrt();
        let new_ps = ull_psi_prime(new_pow_z, pow_z, eta23x, eta13x, eta3012xx);
        let new_p = p * pow_2_minus_tau / (1.0 + new_next_pow_z);
        let new_s = s + new_next_pow_z * (2.0 * new_ps - (new_pow_z + new_next_pow_z) * ps) * new_p;

        if new_s > s {
            pow_z = new_pow_z;
            next_pow_z = new_next_pow_z;
            s = new_s;
            p = new_p;
            ps = new_ps;
        } else {
            return new_s;
        }
    }
}

fn ull_small_range_estimate(c0: i64, c4: i64, c8: i64, c10: i64, m: i64) -> f64 {
    let alpha = m + 3 * (c0 + c4 + c8 + c10);
    let beta = m - c0 - c4;
    let gamma = 4 * c0 + 2 * c4 + 3 * c8 + c10;
    let quad_root_z = (((beta * beta + 4 * alpha * gamma) as f64).sqrt() - beta as f64) / (2 * alpha) as f64;
    let root_z = quad_root_z * quad_root_z;
    root_z * root_z
}

fn ull_large_range_estimate(c4w0: i64, c4w1: i64, c4w2: i64, c4w3: i64, m: i64) -> f64 {
    let alpha = m + 3 * (c4w0 + c4w1 + c4w2 + c4w3);
    let beta = c4w0 + c4w1 + 2 * (c4w2 + c4w3);
    let gamma = m + 2 * c4w0 + c4w2 - c4w3;
    ((((beta * beta + 4 * alpha * gamma) as f64).sqrt() - beta as f64) / (2 * alpha) as f64).sqrt()
}

#[allow(clippy::too_many_arguments)]
fn ull_large_range_contribution(
    c4w0: i64,
    c4w1: i64,
    c4w2: i64,
    c4w3: i64,
    m: i64,
    w: i32,
    eta_0: f64,
    eta_1: f64,
    eta_2: f64,
    eta_3: f64,
    phi_1: f64,
    p_initial: f64,
    pow_2_minus_tau: f64,
    pow2mt_eta02: f64,
    pow2mt_eta13: f64,
    pow2mt_eta2: f64,
    pow2mt_eta3: f64,
    eta23x: f64,
    eta13x: f64,
    eta3012xx: f64,
) -> f64 {
    let z = ull_large_range_estimate(c4w0, c4w1, c4w2, c4w3, m);
    let root_z = z.sqrt();
    let (c4w0f, c4w1f, c4w2f, c4w3f) = (c4w0 as f64, c4w1 as f64, c4w2 as f64, c4w3 as f64);

    let mut s = ull_phi(root_z, z, phi_1, p_initial, pow_2_minus_tau, eta23x, eta13x, eta3012xx)
        * (c4w0f + c4w1f + c4w2f + c4w3f);
    s += z * (1.0 + root_z) * (c4w0f * eta_0 + c4w1f * eta_1 + c4w2f * eta_2 + c4w3f * eta_3);
    s += root_z
        * ((c4w0f + c4w1f) * (z * pow2mt_eta02 + pow2mt_eta2)
            + (c4w2f + c4w3f) * (z * pow2mt_eta13 + pow2mt_eta3));

    s * pow_2_minus_tau.powi(w) / ((1.0 + root_z) * (1.0 + z))
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

    let pow_2_tau = 2.0f64.powf(ULL_TAU);
    let pow_2_minus_tau = 2.0f64.powf(-ULL_TAU);
    let pow_4_minus_tau = 4.0f64.powf(-ULL_TAU);
    let minus_inv_tau = -1.0 / ULL_TAU;
    let eta_x = ULL_ETA_0 - ULL_ETA_1 - ULL_ETA_2 + ULL_ETA_3;
    let eta23x = (ULL_ETA_2 - ULL_ETA_3) / eta_x;
    let eta13x = (ULL_ETA_1 - ULL_ETA_3) / eta_x;
    let eta3012xx = (ULL_ETA_3 * ULL_ETA_0 - ULL_ETA_1 * ULL_ETA_2) / (eta_x * eta_x);
    let pow4mt_eta23 = pow_4_minus_tau * (ULL_ETA_2 - ULL_ETA_3);
    let pow4mt_eta01 = pow_4_minus_tau * (ULL_ETA_0 - ULL_ETA_1);
    let pow4mt_eta3 = pow_4_minus_tau * ULL_ETA_3;
    let pow4mt_eta1 = pow_4_minus_tau * ULL_ETA_1;
    let pow2mt_eta_x = pow_2_minus_tau * eta_x;
    let phi_1 = ULL_ETA_0 / (pow_2_tau * (2.0 * pow_2_tau - 1.0));
    let p_initial = eta_x * (pow_4_minus_tau / (2.0 - pow_2_minus_tau));
    let pow2mt_eta02 = pow_2_minus_tau * (ULL_ETA_0 - ULL_ETA_2);
    let pow2mt_eta13 = pow_2_minus_tau * (ULL_ETA_1 - ULL_ETA_3);
    let pow2mt_eta2 = pow_2_minus_tau * ULL_ETA_2;
    let pow2mt_eta3 = pow_2_minus_tau * ULL_ETA_3;

    let off: i32 = ((p as i32) << 2) + 4;

    let (mut c0, mut c4, mut c8, mut c10): (i64, i64, i64, i64) = (0, 0, 0, 0);
    let (mut c4w0, mut c4w1, mut c4w2, mut c4w3): (i64, i64, i64, i64) = (0, 0, 0, 0);
    let mut sum = 0.0f64;

    for &byte in &state[ULL_HEADER_SIZE..expected_len] {
        let r = byte as i32;
        let r2 = r - off;

        if r2 < 0 {
            if r2 < -8 {
                c0 += 1;
            } else if r2 == -8 {
                c4 += 1;
            } else if r2 == -4 {
                c8 += 1;
            } else if r2 == -2 {
                c10 += 1;
            }
        } else if r < 252 {
            sum += ULL_REGISTER_CONTRIBUTIONS[r2 as usize];
        } else {
            match r {
                252 => c4w0 += 1,
                253 => c4w1 += 1,
                254 => c4w2 += 1,
                255 => c4w3 += 1,
                _ => {}
            }
        }
    }

    let m_i = m as i64;

    if c0 > 0 || c4 > 0 || c8 > 0 || c10 > 0 {
        let z = ull_small_range_estimate(c0, c4, c8, c10, m_i);

        if c0 > 0 {
            sum += (c0 as f64) * ull_sigma(z, ULL_ETA_3, eta_x, pow_2_tau, eta23x, eta13x, eta3012xx);
        }
        if c4 > 0 {
            sum += (c4 as f64) * pow2mt_eta_x * ull_psi_prime(z, z * z, eta23x, eta13x, eta3012xx);
        }
        if c8 > 0 {
            sum += (c8 as f64) * (z * pow4mt_eta01 + pow4mt_eta1);
        }
        if c10 > 0 {
            sum += (c10 as f64) * (z * pow4mt_eta23 + pow4mt_eta3);
        }
    }

    if c4w0 > 0 || c4w1 > 0 || c4w2 > 0 || c4w3 > 0 {
        sum += ull_large_range_contribution(
            c4w0,
            c4w1,
            c4w2,
            c4w3,
            m_i,
            65 - p as i32,
            ULL_ETA_0,
            ULL_ETA_1,
            ULL_ETA_2,
            ULL_ETA_3,
            phi_1,
            p_initial,
            pow_2_minus_tau,
            pow2mt_eta02,
            pow2mt_eta13,
            pow2mt_eta2,
            pow2mt_eta3,
            eta23x,
            eta13x,
            eta3012xx,
        );
    }

    let estimate = if sum <= 0.0 {
        0.0
    } else {
        ULL_ESTIMATION_FACTORS[(p as i32 - ULL_MIN_P_ESTIMATOR) as usize] * sum.powf(minus_inv_tau)
    };

    error::ok_float(env, estimate)
}

fn ull_update_many_raw_impl<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64) -> Term<'a> {
    ull_update_many_raw_impl_inner(env, state_bin, items, p, seed, ALGO_XXH3)
}

fn ull_update_many_raw_impl_inner<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    p: u8,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
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
    let m3_seed = seed as u32;

    for item_term in items {
        let bin: Binary = match item_term.decode() {
            Ok(b) => b,
            Err(_) => return error::error_string(env, "all items must be binaries"),
        };
        let hash = match algorithm {
            ALGO_XXH3 => xxh3::xxh3_64_with_seed(bin.as_slice(), seed),
            ALGO_MURMUR3 => murmur3_x64_128(bin.as_slice(), m3_seed).0,
            _ => return error::error_string(env, "unsupported hash algorithm byte (expected 1=xxhash3, 2=murmur3)"),
        };
        let (bucket, bit) = ull_bucket_and_bit(hash, p);
        ull_update_one(&mut result, bucket, bit);
    }

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn ull_update_many_raw_nif<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64) -> Term<'a> {
    ull_update_many_raw_impl(env, state_bin, items, p, seed)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ull_update_many_raw_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64) -> Term<'a> {
    ull_update_many_raw_impl(env, state_bin, items, p, seed)
}

#[rustler::nif]
fn ull_update_many_raw_h_nif<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64, algorithm: u8) -> Term<'a> {
    ull_update_many_raw_impl_inner(env, state_bin, items, p, seed, algorithm)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ull_update_many_raw_h_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, items: ListIterator<'a>, p: u8, seed: u64, algorithm: u8) -> Term<'a> {
    ull_update_many_raw_impl_inner(env, state_bin, items, p, seed, algorithm)
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
