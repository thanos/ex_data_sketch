use std::collections::BTreeSet;

use rustler::{Binary, Env, Term};

use crate::error;

const THETA_HEADER_SIZE: usize = 17;
const THETA_MAX_U64: u64 = u64::MAX;

struct ThetaState {
    k: u32,
    theta: u64,
    entries: Vec<u64>,
}

fn parse_theta_state(data: &[u8]) -> Result<ThetaState, &'static str> {
    if data.len() < THETA_HEADER_SIZE {
        return Err("Theta state too short");
    }

    let _version = data[0];
    let k = u32::from_le_bytes(data[1..5].try_into().unwrap());
    let theta = u64::from_le_bytes(data[5..13].try_into().unwrap());
    let count = u32::from_le_bytes(data[13..17].try_into().unwrap()) as usize;

    let entries_data = &data[THETA_HEADER_SIZE..];
    if entries_data.len() < count * 8 {
        return Err("Theta state truncated");
    }

    let mut entries = Vec::with_capacity(count);
    for chunk in entries_data[..count * 8].chunks_exact(8) {
        entries.push(u64::from_le_bytes(chunk.try_into().unwrap()));
    }

    Ok(ThetaState { k, theta, entries })
}

fn encode_theta_state(k: u32, theta: u64, entries: &[u64]) -> Vec<u8> {
    let count = entries.len() as u32;
    let mut result = Vec::with_capacity(THETA_HEADER_SIZE + entries.len() * 8);

    result.push(1u8); // version
    result.extend_from_slice(&k.to_le_bytes());
    result.extend_from_slice(&theta.to_le_bytes());
    result.extend_from_slice(&count.to_le_bytes());
    for &entry in entries {
        result.extend_from_slice(&entry.to_le_bytes());
    }

    result
}

fn theta_update_many_impl<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary) -> Term<'a> {
    let state = match parse_theta_state(state_bin.as_slice()) {
        Ok(s) => s,
        Err(e) => return error::error_string(env, e),
    };

    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let k = state.k as usize;
    let mut theta = state.theta;

    // Collect existing entries into BTreeSet
    let mut set: BTreeSet<u64> = state.entries.into_iter().collect();

    // Add qualifying hashes
    let hashes = hashes_bin.as_slice();
    for chunk in hashes.chunks_exact(8) {
        let hash = u64::from_le_bytes(chunk.try_into().unwrap());
        if hash < theta {
            set.insert(hash);
        }
    }

    // Compact if needed
    let sorted: Vec<u64> = if set.len() > k {
        let mut iter = set.into_iter();
        let kept: Vec<u64> = iter.by_ref().take(k).collect();
        // The (k+1)th element becomes new theta
        theta = iter.next().unwrap_or(THETA_MAX_U64);
        kept
    } else {
        set.into_iter().collect()
    };

    let result = encode_theta_state(state.k, theta, &sorted);
    error::ok_binary(env, &result)
}

fn theta_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    let state_a = match parse_theta_state(a_bin.as_slice()) {
        Ok(s) => s,
        Err(e) => return error::error_string(env, e),
    };
    let state_b = match parse_theta_state(b_bin.as_slice()) {
        Ok(s) => s,
        Err(e) => return error::error_string(env, e),
    };

    let k = state_a.k as usize;
    let mut new_theta = state_a.theta.min(state_b.theta);

    // Union into BTreeSet (auto-dedup + sorted)
    let mut set: BTreeSet<u64> = BTreeSet::new();
    for &entry in &state_a.entries {
        if entry < new_theta {
            set.insert(entry);
        }
    }
    for &entry in &state_b.entries {
        if entry < new_theta {
            set.insert(entry);
        }
    }

    // Compact if needed
    let sorted: Vec<u64> = if set.len() > k {
        let mut iter = set.into_iter();
        let kept: Vec<u64> = iter.by_ref().take(k).collect();
        new_theta = iter.next().unwrap_or(THETA_MAX_U64);
        kept
    } else {
        set.into_iter().collect()
    };

    let result = encode_theta_state(state_a.k, new_theta, &sorted);
    error::ok_binary(env, &result)
}

#[rustler::nif]
fn theta_update_many_nif<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary) -> Term<'a> {
    theta_update_many_impl(env, state_bin, hashes_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn theta_update_many_dirty_nif<'a>(env: Env<'a>, state_bin: Binary, hashes_bin: Binary) -> Term<'a> {
    theta_update_many_impl(env, state_bin, hashes_bin)
}

#[rustler::nif]
fn theta_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    theta_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn theta_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    theta_merge_impl(env, a_bin, b_bin)
}
