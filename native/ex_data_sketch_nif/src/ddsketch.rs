use std::collections::HashMap;

use rustler::{Binary, Env, Term};

use crate::error;

/// DDS1 binary state layout (88-byte header + variable sparse bins):
///   0..4:   magic "DDS1"
///   4:      version (u8 = 1)
///   5:      flags (u8 = 0)
///   6..8:   reserved (u16 LE = 0)
///   8..16:  alpha (f64 LE)
///   16..24: gamma (f64 LE)
///   24..32: log_gamma (f64 LE)
///   32..40: min_indexable (f64 LE)
///   40..48: n (u64 LE)
///   48..56: zero_count (u64 LE)
///   56..64: min_value (f64 LE, NaN sentinel for empty)
///   64..72: max_value (f64 LE, NaN sentinel for empty)
///   72..76: sparse_count (u32 LE)
///   76..80: dense_min_index (i32 LE, 0 in v0.2.1)
///   80..84: dense_len (u32 LE, 0 in v0.2.1)
///   84..88: reserved2 (u32 LE = 0)
///   88..:   sparse bins (sparse_count x (i32 index + u32 count))

const DDS_HEADER_SIZE: usize = 88;
const DDS_MAGIC: &[u8; 4] = b"DDS1";
const NAN_BYTES: [u8; 8] = [0, 0, 0, 0, 0, 0, 248, 127]; // canonical NaN

struct DdsState {
    alpha: f64,
    gamma: f64,
    log_gamma: f64,
    min_indexable: f64,
    n: u64,
    zero_count: u64,
    min_value: f64, // NaN for empty
    max_value: f64, // NaN for empty
    bins: Vec<(i32, u32)>, // sorted by index ascending
}

fn decode_state(data: &[u8]) -> Option<DdsState> {
    if data.len() < DDS_HEADER_SIZE {
        return None;
    }
    if &data[0..4] != DDS_MAGIC {
        return None;
    }
    if data[4] != 1 {
        return None; // unsupported version
    }

    let alpha = f64::from_le_bytes(data[8..16].try_into().ok()?);
    let gamma = f64::from_le_bytes(data[16..24].try_into().ok()?);
    let log_gamma = f64::from_le_bytes(data[24..32].try_into().ok()?);
    let min_indexable = f64::from_le_bytes(data[32..40].try_into().ok()?);
    let n = u64::from_le_bytes(data[40..48].try_into().ok()?);
    let zero_count = u64::from_le_bytes(data[48..56].try_into().ok()?);
    let min_value = f64::from_le_bytes(data[56..64].try_into().ok()?);
    let max_value = f64::from_le_bytes(data[64..72].try_into().ok()?);
    let sparse_count = u32::from_le_bytes(data[72..76].try_into().ok()?) as usize;

    let bins_bytes = sparse_count.checked_mul(8)?;
    if data.len() < DDS_HEADER_SIZE + bins_bytes {
        return None;
    }

    let mut bins = Vec::with_capacity(sparse_count);
    let mut pos = DDS_HEADER_SIZE;
    for _ in 0..sparse_count {
        let index = i32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        let count = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().ok()?);
        bins.push((index, count));
        pos += 8;
    }

    Some(DdsState {
        alpha,
        gamma,
        log_gamma,
        min_indexable,
        n,
        zero_count,
        min_value,
        max_value,
        bins,
    })
}

fn encode_state(state: &DdsState) -> Vec<u8> {
    let sparse_count = state.bins.len() as u32;
    let size = DDS_HEADER_SIZE + (state.bins.len() * 8);
    let mut buf = Vec::with_capacity(size);

    // Header
    buf.extend_from_slice(DDS_MAGIC);
    buf.push(1u8); // version
    buf.push(0u8); // flags
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
    buf.extend_from_slice(&state.alpha.to_le_bytes());
    buf.extend_from_slice(&state.gamma.to_le_bytes());
    buf.extend_from_slice(&state.log_gamma.to_le_bytes());
    buf.extend_from_slice(&state.min_indexable.to_le_bytes());
    buf.extend_from_slice(&state.n.to_le_bytes());
    buf.extend_from_slice(&state.zero_count.to_le_bytes());

    if state.min_value.is_nan() {
        buf.extend_from_slice(&NAN_BYTES);
    } else {
        buf.extend_from_slice(&state.min_value.to_le_bytes());
    }
    if state.max_value.is_nan() {
        buf.extend_from_slice(&NAN_BYTES);
    } else {
        buf.extend_from_slice(&state.max_value.to_le_bytes());
    }

    buf.extend_from_slice(&sparse_count.to_le_bytes());
    buf.extend_from_slice(&0i32.to_le_bytes()); // dense_min_index
    buf.extend_from_slice(&0u32.to_le_bytes()); // dense_len
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved2

    // Sparse bins
    for &(index, count) in &state.bins {
        buf.extend_from_slice(&index.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
    }

    buf
}

fn compute_index(value: f64, min_indexable: f64, log_gamma: f64) -> i32 {
    if value < min_indexable {
        (min_indexable.ln() / log_gamma).floor() as i32
    } else {
        (value.ln() / log_gamma).floor() as i32
    }
}

fn update_min_max(state: &mut DdsState, value: f64) {
    if state.min_value.is_nan() {
        state.min_value = value;
        state.max_value = value;
    } else {
        if value < state.min_value {
            state.min_value = value;
        }
        if value > state.max_value {
            state.max_value = value;
        }
    }
}

fn merge_index_counts(existing: &[(i32, u32)], new_counts: &HashMap<i32, u32>) -> Vec<(i32, u32)> {
    if new_counts.is_empty() {
        return existing.to_vec();
    }

    let mut merged: HashMap<i32, u32> = existing.iter().cloned().collect();
    for (&idx, &count) in new_counts {
        *merged.entry(idx).or_insert(0) += count;
    }

    let mut result: Vec<(i32, u32)> = merged.into_iter().collect();
    result.sort_by_key(|&(idx, _)| idx);
    result
}

fn merge_sorted_bins(a: &[(i32, u32)], b: &[(i32, u32)]) -> Vec<(i32, u32)> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let mut ai = 0;
    let mut bi = 0;

    while ai < a.len() && bi < b.len() {
        let (ia, ca) = a[ai];
        let (ib, cb) = b[bi];
        if ia < ib {
            result.push((ia, ca));
            ai += 1;
        } else if ia > ib {
            result.push((ib, cb));
            bi += 1;
        } else {
            result.push((ia, ca + cb));
            ai += 1;
            bi += 1;
        }
    }

    result.extend_from_slice(&a[ai..]);
    result.extend_from_slice(&b[bi..]);
    result
}

fn ddsketch_update_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    values_bin: Binary,
) -> Term<'a> {
    if values_bin.len() % 8 != 0 {
        return error::error_string(env, "values_bin length must be a multiple of 8");
    }

    let mut state = match decode_state(state_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid DDSketch state"),
    };

    if values_bin.is_empty() {
        let result = encode_state(&state);
        return error::ok_binary(env, &result);
    }

    let values = values_bin.as_slice();
    let mut n_delta: u64 = 0;
    let mut zero_delta: u64 = 0;
    let mut index_counts: HashMap<i32, u32> = HashMap::new();

    for chunk in values.chunks_exact(8) {
        let val = f64::from_le_bytes(chunk.try_into().unwrap());

        // Validate: reject negatives, NaN, Inf
        if val < 0.0 {
            return error::error_string(env, "DDSketch does not support negative values");
        }
        if val.is_nan() || val.is_infinite() {
            return error::error_string(env, "DDSketch does not support NaN or Inf values");
        }

        update_min_max(&mut state, val);
        n_delta += 1;

        if val == 0.0 {
            zero_delta += 1;
        } else {
            let idx = compute_index(val, state.min_indexable, state.log_gamma);
            *index_counts.entry(idx).or_insert(0) += 1;
        }
    }

    state.bins = merge_index_counts(&state.bins, &index_counts);
    state.n += n_delta;
    state.zero_count += zero_delta;

    let result = encode_state(&state);
    error::ok_binary(env, &result)
}

fn ddsketch_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    let mut a = match decode_state(a_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid DDSketch state A"),
    };
    let b = match decode_state(b_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid DDSketch state B"),
    };

    // Validate alpha match (compare bytes, not floats)
    if a_bin.as_slice()[8..16] != b_bin.as_slice()[8..16] {
        return error::error_string(env, "DDSketch alpha mismatch");
    }

    // Merge min/max
    if a.min_value.is_nan() && b.min_value.is_nan() {
        // Both empty, keep NaN
    } else if a.min_value.is_nan() {
        a.min_value = b.min_value;
        a.max_value = b.max_value;
    } else if !b.min_value.is_nan() {
        if b.min_value < a.min_value {
            a.min_value = b.min_value;
        }
        if b.max_value > a.max_value {
            a.max_value = b.max_value;
        }
    }

    a.n += b.n;
    a.zero_count += b.zero_count;
    a.bins = merge_sorted_bins(&a.bins, &b.bins);

    let result = encode_state(&a);
    error::ok_binary(env, &result)
}

#[rustler::nif]
fn ddsketch_update_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    values_bin: Binary,
) -> Term<'a> {
    ddsketch_update_many_impl(env, state_bin, values_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ddsketch_update_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    values_bin: Binary,
) -> Term<'a> {
    ddsketch_update_many_impl(env, state_bin, values_bin)
}

#[rustler::nif]
fn ddsketch_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    ddsketch_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn ddsketch_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    ddsketch_merge_impl(env, a_bin, b_bin)
}
