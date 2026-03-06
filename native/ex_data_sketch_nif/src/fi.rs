use std::collections::HashMap;

use rustler::{Binary, Encoder, Env, Term};

use crate::error;

rustler::atoms! {
    atom_item = "item",
    atom_estimate = "estimate",
    atom_error = "error",
    atom_lower = "lower",
    atom_upper = "upper",
    not_tracked,
}

/// FI1 binary state layout (32-byte header + variable body):
///   0..4:   magic "FI1\0"
///   4:      version (u8 = 1)
///   5:      flags (u8: 0=binary, 1=int, 2=term_external)
///   6..8:   reserved (u16 LE = 0)
///   8..12:  k (u32 LE, max counters capacity)
///   12..20: n (u64 LE, total observed items)
///   20..24: entry_count (u32 LE)
///   24..28: reserved2 (u32 LE = 0)
///   28..32: reserved3 (u32 LE = 0)
///
///   Body (entry_count entries, sorted by item_bytes ascending):
///     item_len (u32 LE)
///     item_bytes (item_len bytes)
///     count (u64 LE)
///     error (u64 LE)

const FI_HEADER_SIZE: usize = 32;
const FI_MAGIC: &[u8; 4] = b"FI1\0";

struct FiEntry {
    item_bytes: Vec<u8>,
    count: u64,
    error: u64,
}

struct FiState {
    k: u32,
    flags: u8,
    n: u64,
    entries: Vec<FiEntry>,
}

fn decode_state(data: &[u8]) -> Option<FiState> {
    if data.len() < FI_HEADER_SIZE {
        return None;
    }
    if &data[0..4] != FI_MAGIC {
        return None;
    }
    if data[4] != 1 {
        return None; // unsupported version
    }

    let flags = data[5];
    let k = u32::from_le_bytes(data[8..12].try_into().ok()?);
    let n = u64::from_le_bytes(data[12..20].try_into().ok()?);
    let entry_count = u32::from_le_bytes(data[20..24].try_into().ok()?) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    let mut pos = FI_HEADER_SIZE;

    for _ in 0..entry_count {
        if pos + 4 > data.len() {
            return None;
        }
        let item_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        if pos + item_len + 16 > data.len() {
            return None;
        }
        let item_bytes = data[pos..pos + item_len].to_vec();
        pos += item_len;

        let count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let error = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        entries.push(FiEntry {
            item_bytes,
            count,
            error,
        });
    }

    Some(FiState {
        k,
        flags,
        n,
        entries,
    })
}

fn encode_state(state: &FiState) -> Vec<u8> {
    let entry_count = state.entries.len() as u32;

    // Estimate capacity
    let body_size: usize = state
        .entries
        .iter()
        .map(|e| 4 + e.item_bytes.len() + 8 + 8)
        .sum();
    let mut buf = Vec::with_capacity(FI_HEADER_SIZE + body_size);

    // Header
    buf.extend_from_slice(FI_MAGIC);
    buf.push(1u8); // version
    buf.push(state.flags);
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
    buf.extend_from_slice(&state.k.to_le_bytes());
    buf.extend_from_slice(&state.n.to_le_bytes());
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved2
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved3

    // Body: entries sorted by item_bytes ascending (caller ensures sort)
    for entry in &state.entries {
        let item_len = entry.item_bytes.len() as u32;
        buf.extend_from_slice(&item_len.to_le_bytes());
        buf.extend_from_slice(&entry.item_bytes);
        buf.extend_from_slice(&entry.count.to_le_bytes());
        buf.extend_from_slice(&entry.error.to_le_bytes());
    }

    buf
}

/// Decode packed items binary: concatenation of <<item_len::u32-le, item_bytes::item_len>>
fn decode_packed_items(data: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut items = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if pos + 4 > data.len() {
            return None;
        }
        let item_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;

        if pos + item_len > data.len() {
            return None;
        }
        items.push(data[pos..pos + item_len].to_vec());
        pos += item_len;
    }

    Some(items)
}

/// Apply a single weighted SpaceSaving update to entries (in-place via HashMap).
/// Returns entries as a HashMap for efficient repeated updates.
fn apply_weighted_update(
    entries: &mut HashMap<Vec<u8>, (u64, u64)>,
    k: usize,
    item_bytes: &[u8],
    weight: u64,
) {
    if let Some((count, _error)) = entries.get_mut(item_bytes) {
        *count += weight;
        return;
    }

    if entries.len() < k {
        entries.insert(item_bytes.to_vec(), (weight, 0));
        return;
    }

    // Evict: find entry with minimum count, ties broken by smallest key
    let evict_key = entries
        .iter()
        .min_by(|(ka, (ca, _)), (kb, (cb, _))| ca.cmp(cb).then_with(|| ka.cmp(kb)))
        .map(|(k, _)| k.clone())
        .unwrap();

    let min_count = entries[&evict_key].0;
    entries.remove(&evict_key);
    entries.insert(item_bytes.to_vec(), (min_count + weight, min_count));
}

fn fi_update_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    packed_items_bin: Binary,
) -> Term<'a> {
    let mut state = match decode_state(state_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid FrequentItems state"),
    };

    if packed_items_bin.is_empty() {
        let result = encode_state(&state);
        return error::ok_binary(env, &result);
    }

    let items = match decode_packed_items(packed_items_bin.as_slice()) {
        Some(i) => i,
        None => return error::error_string(env, "invalid packed items binary"),
    };

    let batch_n = items.len() as u64;
    let k = state.k as usize;

    // Pre-aggregate into frequency map
    let mut freq: HashMap<Vec<u8>, u64> = HashMap::new();
    for item in &items {
        *freq.entry(item.clone()).or_insert(0) += 1;
    }

    // Convert existing entries to HashMap for O(1) lookups during updates
    let mut entry_map: HashMap<Vec<u8>, (u64, u64)> = HashMap::with_capacity(state.entries.len());
    for entry in state.entries.drain(..) {
        entry_map.insert(entry.item_bytes, (entry.count, entry.error));
    }

    // Apply weighted updates in sorted key order for determinism
    let mut sorted_keys: Vec<&Vec<u8>> = freq.keys().collect();
    sorted_keys.sort();

    for key in sorted_keys {
        let weight = freq[key];
        apply_weighted_update(&mut entry_map, k, key, weight);
    }

    // Convert back to sorted entries
    let mut entries: Vec<FiEntry> = entry_map
        .into_iter()
        .map(|(item_bytes, (count, error))| FiEntry {
            item_bytes,
            count,
            error,
        })
        .collect();
    entries.sort_by(|a, b| a.item_bytes.cmp(&b.item_bytes));

    state.n += batch_n;
    state.entries = entries;

    let result = encode_state(&state);
    error::ok_binary(env, &result)
}

fn fi_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    let a = match decode_state(a_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid FrequentItems state A"),
    };
    let b = match decode_state(b_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid FrequentItems state B"),
    };

    let k = a.k as usize;

    // Combine counts additively across union of keys
    let mut combined: HashMap<Vec<u8>, (u64, u64)> = HashMap::new();
    for entry in &a.entries {
        combined.insert(entry.item_bytes.clone(), (entry.count, entry.error));
    }
    for entry in &b.entries {
        let e = combined
            .entry(entry.item_bytes.clone())
            .or_insert((0, 0));
        e.0 += entry.count;
        e.1 += entry.error;
    }

    // Keep top-k by count (ties: smallest key), then sort by key for canonical encoding
    let mut entries: Vec<FiEntry> = if combined.len() <= k {
        combined
            .into_iter()
            .map(|(item_bytes, (count, error))| FiEntry {
                item_bytes,
                count,
                error,
            })
            .collect()
    } else {
        let mut all: Vec<(Vec<u8>, (u64, u64))> = combined.into_iter().collect();
        // Sort by (-count, item_bytes) to get top-k
        all.sort_by(|(ka, (ca, _)), (kb, (cb, _))| cb.cmp(ca).then_with(|| ka.cmp(kb)));
        all.truncate(k);
        all.into_iter()
            .map(|(item_bytes, (count, error))| FiEntry {
                item_bytes,
                count,
                error,
            })
            .collect()
    };

    // Sort by item_bytes for canonical encoding
    entries.sort_by(|a, b| a.item_bytes.cmp(&b.item_bytes));

    let result = encode_state(&FiState {
        k: a.k,
        flags: a.flags,
        n: a.n + b.n,
        entries,
    });

    error::ok_binary(env, &result)
}

fn encode_item_binary<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let mut owned = rustler::types::binary::OwnedBinary::new(data.len())
        .expect("failed to allocate binary");
    owned.as_mut_slice().copy_from_slice(data);
    owned.release(env).encode(env)
}

fn make_entry_map<'a>(env: Env<'a>, item_bytes: &[u8], count: u64, err: u64) -> Term<'a> {
    let lower = count.saturating_sub(err);
    rustler::Term::map_from_pairs(
        env,
        &[
            (atom_item().encode(env), encode_item_binary(env, item_bytes)),
            (atom_estimate().encode(env), count.encode(env)),
            (atom_error().encode(env), err.encode(env)),
            (atom_lower().encode(env), lower.encode(env)),
            (atom_upper().encode(env), count.encode(env)),
        ],
    )
    .unwrap()
}

#[rustler::nif]
fn fi_new_nif<'a>(env: Env<'a>, k: u32, flags: u8) -> Term<'a> {
    let state = FiState {
        k,
        flags,
        n: 0,
        entries: Vec::new(),
    };
    let result = encode_state(&state);
    error::ok_binary(env, &result)
}

#[rustler::nif]
fn fi_count_nif<'a>(env: Env<'a>, state_bin: Binary) -> Term<'a> {
    if state_bin.len() < FI_HEADER_SIZE {
        return error::error_string(env, "invalid FrequentItems state");
    }
    let data = state_bin.as_slice();
    if &data[0..4] != FI_MAGIC || data[4] != 1 {
        return error::error_string(env, "invalid FrequentItems state");
    }
    let n = u64::from_le_bytes(data[12..20].try_into().unwrap());
    error::ok_u64(env, n)
}

#[rustler::nif]
fn fi_entry_count_nif<'a>(env: Env<'a>, state_bin: Binary) -> Term<'a> {
    if state_bin.len() < FI_HEADER_SIZE {
        return error::error_string(env, "invalid FrequentItems state");
    }
    let data = state_bin.as_slice();
    if &data[0..4] != FI_MAGIC || data[4] != 1 {
        return error::error_string(env, "invalid FrequentItems state");
    }
    let ec = u32::from_le_bytes(data[20..24].try_into().unwrap()) as u64;
    error::ok_u64(env, ec)
}

#[rustler::nif]
fn fi_estimate_nif<'a>(env: Env<'a>, state_bin: Binary, item_bytes: Binary) -> Term<'a> {
    let state = match decode_state(state_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid FrequentItems state"),
    };

    let needle = item_bytes.as_slice();
    for entry in &state.entries {
        if entry.item_bytes == needle {
            let lower = entry.count.saturating_sub(entry.error);
            let map = rustler::Term::map_from_pairs(
                env,
                &[
                    (atom_estimate().encode(env), entry.count.encode(env)),
                    (atom_error().encode(env), entry.error.encode(env)),
                    (atom_lower().encode(env), lower.encode(env)),
                    (atom_upper().encode(env), entry.count.encode(env)),
                ],
            )
            .unwrap();
            return (rustler::types::atom::ok(), map).encode(env);
        }
    }

    (rustler::types::atom::error(), not_tracked()).encode(env)
}

#[rustler::nif]
fn fi_top_k_nif<'a>(env: Env<'a>, state_bin: Binary, limit: u32) -> Term<'a> {
    let state = match decode_state(state_bin.as_slice()) {
        Some(s) => s,
        None => return error::error_string(env, "invalid FrequentItems state"),
    };

    let limit = limit as usize;

    // Sort by (-count, item_bytes)
    let mut sorted = state.entries;
    sorted.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.item_bytes.cmp(&b.item_bytes)));
    sorted.truncate(limit);

    let maps: Vec<Term<'a>> = sorted
        .iter()
        .map(|e| make_entry_map(env, &e.item_bytes, e.count, e.error))
        .collect();

    (rustler::types::atom::ok(), maps).encode(env)
}

#[rustler::nif]
fn fi_update_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    packed_items_bin: Binary,
) -> Term<'a> {
    fi_update_many_impl(env, state_bin, packed_items_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn fi_update_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    packed_items_bin: Binary,
) -> Term<'a> {
    fi_update_many_impl(env, state_bin, packed_items_bin)
}

#[rustler::nif]
fn fi_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    fi_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn fi_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    fi_merge_impl(env, a_bin, b_bin)
}
