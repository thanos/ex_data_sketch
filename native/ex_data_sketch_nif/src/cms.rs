use rustler::{Binary, Env, Term};

use crate::error;

const CMS_HEADER_SIZE: usize = 9;
const GOLDEN64: u64 = 0x9E3779B97F4A7C15;

/// Compute CMS row index matching Elixir's `cms_row_index/3` exactly.
///
/// Elixir computes: `rem(hash64 + (row * @golden64 &&& @mask64), width)`
/// The multiplication is masked to 64 bits via `&&&`, but the addition with
/// hash64 can exceed 64 bits (Elixir has arbitrary precision integers).
/// We use u128 intermediate to match.
fn cms_row_index(hash64: u64, row: u16, width: u32) -> u32 {
    let mixed = (row as u64).wrapping_mul(GOLDEN64);
    let sum = (hash64 as u128) + (mixed as u128);
    (sum % (width as u128)) as u32
}

fn cms_update_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    pairs_bin: Binary,
    width: u32,
    depth: u16,
    counter_width: u8,
) -> Term<'a> {
    let counter_bytes = (counter_width / 8) as usize;
    let total_counters = (width as usize) * (depth as usize);
    let data_size = total_counters * counter_bytes;
    let expected_len = CMS_HEADER_SIZE + data_size;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid CMS state length");
    }
    if pairs_bin.len() % 12 != 0 {
        return error::error_string(env, "pairs_bin length must be a multiple of 12");
    }

    let state = state_bin.as_slice();
    let header = &state[..CMS_HEADER_SIZE];
    let counters_data = &state[CMS_HEADER_SIZE..];

    // Decode counters into Vec<u64>
    let mut counters: Vec<u64> = Vec::with_capacity(total_counters);
    match counter_bytes {
        4 => {
            for chunk in counters_data.chunks_exact(4) {
                counters.push(u32::from_le_bytes(chunk.try_into().unwrap()) as u64);
            }
        }
        8 => {
            for chunk in counters_data.chunks_exact(8) {
                counters.push(u64::from_le_bytes(chunk.try_into().unwrap()));
            }
        }
        _ => return error::error_string(env, "unsupported counter width"),
    }

    let max_counter: u64 = if counter_width == 64 {
        u64::MAX
    } else {
        (1u64 << counter_width) - 1
    };
    let w = width as usize;

    // Process pairs
    let pairs = pairs_bin.as_slice();
    for chunk in pairs.chunks_exact(12) {
        let hash64 = u64::from_le_bytes(chunk[..8].try_into().unwrap());
        let increment = u32::from_le_bytes(chunk[8..12].try_into().unwrap()) as u64;

        for row in 0..depth {
            let col = cms_row_index(hash64, row, width) as usize;
            let idx = (row as usize) * w + col;
            let new_val = counters[idx].saturating_add(increment).min(max_counter);
            counters[idx] = new_val;
        }
    }

    // Re-encode
    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(header);
    match counter_bytes {
        4 => {
            for &val in &counters {
                result.extend_from_slice(&(val as u32).to_le_bytes());
            }
        }
        8 => {
            for &val in &counters {
                result.extend_from_slice(&val.to_le_bytes());
            }
        }
        _ => unreachable!(),
    }

    error::ok_binary(env, &result)
}

fn cms_merge_impl<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    width: u32,
    depth: u16,
    counter_width: u8,
) -> Term<'a> {
    let counter_bytes = (counter_width / 8) as usize;
    let total_counters = (width as usize) * (depth as usize);
    let data_size = total_counters * counter_bytes;
    let expected_len = CMS_HEADER_SIZE + data_size;

    if a_bin.len() != expected_len || b_bin.len() != expected_len {
        return error::error_string(env, "invalid CMS state length for merge");
    }

    let a = a_bin.as_slice();
    let b = b_bin.as_slice();
    let header = &a[..CMS_HEADER_SIZE];

    let max_counter: u64 = if counter_width == 64 {
        u64::MAX
    } else {
        (1u64 << counter_width) - 1
    };

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(header);

    let a_data = &a[CMS_HEADER_SIZE..];
    let b_data = &b[CMS_HEADER_SIZE..];

    match counter_bytes {
        4 => {
            for i in 0..total_counters {
                let off = i * 4;
                let va = u32::from_le_bytes(a_data[off..off + 4].try_into().unwrap()) as u64;
                let vb = u32::from_le_bytes(b_data[off..off + 4].try_into().unwrap()) as u64;
                let merged = va.saturating_add(vb).min(max_counter);
                result.extend_from_slice(&(merged as u32).to_le_bytes());
            }
        }
        8 => {
            for i in 0..total_counters {
                let off = i * 8;
                let va = u64::from_le_bytes(a_data[off..off + 8].try_into().unwrap());
                let vb = u64::from_le_bytes(b_data[off..off + 8].try_into().unwrap());
                let merged = va.saturating_add(vb).min(max_counter);
                result.extend_from_slice(&merged.to_le_bytes());
            }
        }
        _ => return error::error_string(env, "unsupported counter width"),
    }

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn cms_update_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    pairs_bin: Binary,
    width: u32,
    depth: u16,
    counter_width: u8,
) -> Term<'a> {
    cms_update_many_impl(env, state_bin, pairs_bin, width, depth, counter_width)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cms_update_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    pairs_bin: Binary,
    width: u32,
    depth: u16,
    counter_width: u8,
) -> Term<'a> {
    cms_update_many_impl(env, state_bin, pairs_bin, width, depth, counter_width)
}

#[rustler::nif]
fn cms_merge_nif<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    width: u32,
    depth: u16,
    counter_width: u8,
) -> Term<'a> {
    cms_merge_impl(env, a_bin, b_bin, width, depth, counter_width)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cms_merge_dirty_nif<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    width: u32,
    depth: u16,
    counter_width: u8,
) -> Term<'a> {
    cms_merge_impl(env, a_bin, b_bin, width, depth, counter_width)
}
