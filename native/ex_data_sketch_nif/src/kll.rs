use rustler::{Binary, Env, Term};

use crate::error;

/// KLL binary state layout:
///   0:   version (u8 = 1)
///   1:   k (u32 LE)
///   5:   n (u64 LE)
///   13:  min_val (f64 LE, NaN = empty)
///   21:  max_val (f64 LE, NaN = empty)
///   29:  num_levels (u8)
///   30:  compaction_bits (ceil(num_levels/8) bytes)
///   30+P: level_sizes (num_levels * u32 LE)
///   30+P+L: items (sum(level_sizes) * f64 LE, level 0 first)

const KLL_NAN: [u8; 8] = [0, 0, 0, 0, 0, 0, 248, 127];

struct KllState {
    k: u32,
    n: u64,
    min_val: f64, // NaN for empty
    max_val: f64, // NaN for empty
    num_levels: usize,
    compaction_bits: Vec<u8>,
    levels: Vec<Vec<f64>>,
}

fn decode_state(data: &[u8]) -> Option<KllState> {
    if data.len() < 30 {
        return None;
    }
    if data[0] != 1 {
        return None;
    }

    let k = u32::from_le_bytes(data[1..5].try_into().ok()?);
    if k < 8 || k > 65535 {
        return None;
    }

    let n = u64::from_le_bytes(data[5..13].try_into().ok()?);
    let min_val = f64::from_le_bytes(data[13..21].try_into().ok()?);
    let max_val = f64::from_le_bytes(data[21..29].try_into().ok()?);
    let num_levels = data[29] as usize;

    if num_levels < 2 {
        return None;
    }

    let parity_bytes = (num_levels + 7) / 8;
    let mut pos = 30;

    if data.len() < pos + parity_bytes {
        return None;
    }
    let compaction_bits = data[pos..pos + parity_bytes].to_vec();
    pos += parity_bytes;

    let level_sizes_bytes = num_levels.checked_mul(4)?;
    if data.len() < pos + level_sizes_bytes {
        return None;
    }

    let mut level_sizes = Vec::with_capacity(num_levels);
    let mut total_items: usize = 0;
    for i in 0..num_levels {
        let offset = pos + i * 4;
        let sz = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?) as usize;
        total_items = total_items.checked_add(sz)?;
        level_sizes.push(sz);
    }
    pos += level_sizes_bytes;

    let total_item_bytes = total_items.checked_mul(8)?;
    if data.len() < pos + total_item_bytes {
        return None;
    }

    let mut levels = Vec::with_capacity(num_levels);
    for &sz in &level_sizes {
        let bytes_needed = sz * 8;
        let mut level = Vec::with_capacity(sz);
        for j in 0..sz {
            let offset = pos + j * 8;
            let val = f64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            level.push(val);
        }
        pos += bytes_needed;
        levels.push(level);
    }

    Some(KllState {
        k,
        n,
        min_val,
        max_val,
        num_levels,
        compaction_bits,
        levels,
    })
}

fn encode_state(state: &KllState) -> Vec<u8> {
    let parity_bytes = (state.num_levels + 7) / 8;
    let total_items: usize = state.levels.iter().map(|l| l.len()).sum();

    let size = 1 + 4 + 8 + 8 + 8 + 1 + parity_bytes
        + state.num_levels * 4
        + total_items * 8;

    let mut buf = Vec::with_capacity(size);

    buf.push(1u8); // version
    buf.extend_from_slice(&state.k.to_le_bytes());
    buf.extend_from_slice(&state.n.to_le_bytes());

    if state.min_val.is_nan() {
        buf.extend_from_slice(&KLL_NAN);
    } else {
        buf.extend_from_slice(&state.min_val.to_le_bytes());
    }
    if state.max_val.is_nan() {
        buf.extend_from_slice(&KLL_NAN);
    } else {
        buf.extend_from_slice(&state.max_val.to_le_bytes());
    }

    buf.push(state.num_levels as u8);

    // Compaction bits (padded/truncated to parity_bytes)
    for i in 0..parity_bytes {
        if i < state.compaction_bits.len() {
            buf.push(state.compaction_bits[i]);
        } else {
            buf.push(0);
        }
    }

    // Level sizes
    for level in &state.levels {
        buf.extend_from_slice(&(level.len() as u32).to_le_bytes());
    }

    // Items (level 0 first, then level 1, etc.)
    for level in &state.levels {
        for &val in level {
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    buf
}

fn level_capacity(k: u32, level: usize, num_levels: usize) -> usize {
    let depth = num_levels - 1 - level;
    let cap = (k as f64) * (2.0_f64 / 3.0).powi(depth as i32);
    std::cmp::max(2, cap.floor() as usize + 1)
}

fn get_parity(compaction_bits: &[u8], level: usize) -> usize {
    let byte_idx = level / 8;
    let bit_idx = level % 8;
    ((compaction_bits[byte_idx] >> bit_idx) & 1) as usize
}

fn flip_parity(compaction_bits: &mut [u8], level: usize) {
    let byte_idx = level / 8;
    let bit_idx = level % 8;
    compaction_bits[byte_idx] ^= 1 << bit_idx;
}

fn select_half(sorted: &[f64], parity: usize) -> Vec<f64> {
    sorted
        .iter()
        .enumerate()
        .filter(|(idx, _)| idx % 2 == parity)
        .map(|(_, &val)| val)
        .collect()
}

fn compact_if_needed(state: &mut KllState, level: usize) {
    // Top level never compacts
    if level >= state.num_levels - 1 {
        return;
    }

    let cap = level_capacity(state.k, level, state.num_levels);
    if state.levels[level].len() >= cap {
        compact_level(state, level);
    }
}

fn compact_level(state: &mut KllState, level: usize) {
    let mut sorted = state.levels[level].clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let parity = get_parity(&state.compaction_bits, level);
    let promoted = select_half(&sorted, parity);

    flip_parity(&mut state.compaction_bits, level);

    // Clear current level
    state.levels[level].clear();

    // Add promoted items to next level
    let next = level + 1;
    let mut next_level = std::mem::take(&mut state.levels[next]);
    // promoted ++ next_level (prepend promoted, matching Elixir's promoted ++ next_level)
    let mut new_next = promoted;
    new_next.append(&mut next_level);
    state.levels[next] = new_next;

    // Recursively compact if next level is now full
    compact_if_needed(state, next);
}

fn check_grow(state: &mut KllState) {
    let top = state.num_levels - 1;
    let top_cap = level_capacity(state.k, top, state.num_levels);

    if state.levels[top].len() >= top_cap {
        grow_levels(state);
    }
}

fn grow_levels(state: &mut KllState) {
    state.num_levels += 1;
    state.levels.push(Vec::new());

    // Extend compaction bits if needed
    let new_parity_bytes = (state.num_levels + 7) / 8;
    while state.compaction_bits.len() < new_parity_bytes {
        state.compaction_bits.push(0);
    }

    // Recompact from bottom up
    recompact(state, 0);

    // Check if we need to grow again
    let top = state.num_levels - 1;
    let top_cap = level_capacity(state.k, top, state.num_levels);
    if state.levels[top].len() >= top_cap {
        grow_levels(state);
    }
}

fn recompact(state: &mut KllState, start_level: usize) {
    for level in start_level..state.num_levels {
        compact_if_needed(state, level);
    }
}

fn insert_value(state: &mut KllState, value: f64) {
    // Update min/max
    if state.min_val.is_nan() {
        state.min_val = value;
        state.max_val = value;
    } else {
        if value < state.min_val {
            state.min_val = value;
        }
        if value > state.max_val {
            state.max_val = value;
        }
    }

    state.n += 1;

    // Append to level 0 (O(1) amortized). Level 0 is reversed before
    // encoding to match Elixir's prepend order. Compaction sorts first,
    // so insertion order does not affect compacted results.
    state.levels[0].push(value);

    compact_if_needed(state, 0);
    check_grow(state);
}

fn kll_update_many_impl<'a>(env: Env<'a>, state_bin: Binary, values_bin: Binary) -> Term<'a> {
    if values_bin.len() % 8 != 0 {
        return error::error_string(env, "values_bin length must be a multiple of 8");
    }

    let mut state = match decode_state(state_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid KLL state"),
    };

    let values = values_bin.as_slice();
    for chunk in values.chunks_exact(8) {
        let val = f64::from_le_bytes(chunk.try_into().unwrap());
        insert_value(&mut state, val);
    }

    // Reverse level 0 to match Elixir's prepend ([value | level0]) order.
    // Items inserted since the last compaction are in append order; reversing
    // produces the same serialization as the Pure backend.
    state.levels[0].reverse();

    let result = encode_state(&state);
    error::ok_binary(env, &result)
}

fn kll_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    let mut a = match decode_state(a_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid KLL state A"),
    };
    let b = match decode_state(b_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid KLL state B"),
    };

    // Handle min/max
    if a.min_val.is_nan() && b.min_val.is_nan() {
        // Both empty
    } else if a.min_val.is_nan() {
        a.min_val = b.min_val;
        a.max_val = b.max_val;
    } else if !b.min_val.is_nan() {
        if b.min_val < a.min_val {
            a.min_val = b.min_val;
        }
        if b.max_val > a.max_val {
            a.max_val = b.max_val;
        }
    }

    a.n += b.n;

    // Merge levels: pad to max, concatenate items
    let max_levels = std::cmp::max(a.num_levels, b.num_levels);

    // Extend A's levels to max
    while a.levels.len() < max_levels {
        a.levels.push(Vec::new());
    }

    // Merge compaction bits (OR)
    let new_parity_bytes = (max_levels + 7) / 8;
    while a.compaction_bits.len() < new_parity_bytes {
        a.compaction_bits.push(0);
    }
    for i in 0..b.compaction_bits.len().min(new_parity_bytes) {
        a.compaction_bits[i] |= b.compaction_bits[i];
    }

    // Concatenate items at each level (a_level ++ b_level)
    for (i, b_level) in b.levels.into_iter().enumerate() {
        if i < a.levels.len() {
            a.levels[i].extend(b_level);
        }
    }

    a.num_levels = max_levels;

    // Recompact from bottom up (no grow — matches Pure backend)
    recompact(&mut a, 0);

    let result = encode_state(&a);
    error::ok_binary(env, &result)
}

#[rustler::nif]
fn kll_update_many_nif<'a>(env: Env<'a>, state_bin: Binary, values_bin: Binary) -> Term<'a> {
    kll_update_many_impl(env, state_bin, values_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn kll_update_many_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, values_bin: Binary) -> Term<'a> {
    kll_update_many_impl(env, state_bin, values_bin)
}

#[rustler::nif]
fn kll_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    kll_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn kll_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    kll_merge_impl(env, a_bin, b_bin)
}
