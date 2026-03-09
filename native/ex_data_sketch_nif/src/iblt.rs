use rustler::{Binary, Env, Term};

use crate::error;

const IBLT_HEADER_SIZE: usize = 24;
const IBLT_CELL_SIZE: usize = 24;
const GOLDEN64: u64 = 0x9E3779B97F4A7C15;

fn splitmix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EB);
    x ^= x >> 31;
    x
}

fn iblt_check_hash(key_hash: u64) -> u32 {
    let mixed = splitmix64(key_hash.wrapping_mul(0x517CC1B727220A95));
    (mixed >> 32) as u32
}

fn iblt_positions(key_hash: u64, seed: u32, hash_count: u8, cell_count: u32) -> Vec<u32> {
    let mut positions: Vec<u32> = Vec::with_capacity(hash_count as usize);

    for i in 0..hash_count as u64 {
        // Match Elixir: input = (key_hash + (seed + i) * GOLDEN) &&& mask64
        let input = key_hash.wrapping_add((seed as u64).wrapping_add(i).wrapping_mul(GOLDEN64));
        let h = splitmix64(input);
        let pos = (h % cell_count as u64) as u32;

        let pos = resolve_collision(pos, &positions, h, cell_count, 1);
        positions.push(pos);
    }

    positions
}

fn resolve_collision(
    pos: u32,
    existing: &[u32],
    h: u64,
    cell_count: u32,
    attempt: u32,
) -> u32 {
    if attempt > cell_count {
        // Fallback: find first unused position
        for p in 0..cell_count {
            if !existing.contains(&p) {
                return p;
            }
        }
        return pos;
    }

    if existing.contains(&pos) {
        let new_h = splitmix64(h.wrapping_add(attempt as u64));
        let new_pos = (new_h % cell_count as u64) as u32;
        resolve_collision(new_pos, existing, new_h, cell_count, attempt + 1)
    } else {
        pos
    }
}

fn iblt_put_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    pairs_bin: Binary,
    hash_count: u8,
    cell_count: u32,
    seed: u32,
) -> Term<'a> {
    let expected_len = IBLT_HEADER_SIZE + (cell_count as usize) * IBLT_CELL_SIZE;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid IBLT state length");
    }
    if pairs_bin.len() % 16 != 0 {
        return error::error_string(env, "pairs_bin length must be a multiple of 16");
    }

    let state = state_bin.as_slice();
    let mut result = state.to_vec();

    // Read current item_count from header (offset 12, u32-LE)
    let mut item_count =
        u32::from_le_bytes(result[12..16].try_into().unwrap());

    let pairs = pairs_bin.as_slice();
    let pair_count = pairs.len() / 16;

    for p in 0..pair_count {
        let off = p * 16;
        let key_hash = u64::from_le_bytes(pairs[off..off + 8].try_into().unwrap());
        let value_hash = u64::from_le_bytes(pairs[off + 8..off + 16].try_into().unwrap());
        let check = iblt_check_hash(key_hash);
        let positions = iblt_positions(key_hash, seed, hash_count, cell_count);

        for &pos in &positions {
            let cell_off = IBLT_HEADER_SIZE + (pos as usize) * IBLT_CELL_SIZE;

            // Read cell fields
            let count = i32::from_le_bytes(
                result[cell_off..cell_off + 4].try_into().unwrap(),
            );
            let key_sum = u64::from_le_bytes(
                result[cell_off + 4..cell_off + 12].try_into().unwrap(),
            );
            let value_sum = u64::from_le_bytes(
                result[cell_off + 12..cell_off + 20].try_into().unwrap(),
            );
            let check_sum = u32::from_le_bytes(
                result[cell_off + 20..cell_off + 24].try_into().unwrap(),
            );

            // Update cell
            result[cell_off..cell_off + 4]
                .copy_from_slice(&(count + 1).to_le_bytes());
            result[cell_off + 4..cell_off + 12]
                .copy_from_slice(&(key_sum ^ key_hash).to_le_bytes());
            result[cell_off + 12..cell_off + 20]
                .copy_from_slice(&(value_sum ^ value_hash).to_le_bytes());
            result[cell_off + 20..cell_off + 24]
                .copy_from_slice(&(check_sum ^ check).to_le_bytes());
        }

        item_count += 1;
    }

    // Write updated item_count back to header
    result[12..16].copy_from_slice(&item_count.to_le_bytes());

    error::ok_binary(env, &result)
}

fn iblt_merge_impl<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    if a_bin.len() != b_bin.len() {
        return error::error_string(env, "IBLT state length mismatch for merge");
    }
    if a_bin.len() < IBLT_HEADER_SIZE {
        return error::error_string(env, "invalid IBLT state length");
    }

    let a = a_bin.as_slice();
    let b = b_bin.as_slice();
    let mut result = a.to_vec();

    // Read cell_count from header (offset 8, u32-LE)
    let cell_count =
        u32::from_le_bytes(a[8..12].try_into().unwrap()) as usize;

    let expected_len = IBLT_HEADER_SIZE + cell_count * IBLT_CELL_SIZE;
    if a_bin.len() != expected_len {
        return error::error_string(env, "IBLT state length does not match cell_count");
    }

    // Sum item_counts
    let item_count_a = u32::from_le_bytes(a[12..16].try_into().unwrap());
    let item_count_b = u32::from_le_bytes(b[12..16].try_into().unwrap());
    result[12..16].copy_from_slice(&(item_count_a + item_count_b).to_le_bytes());

    // Cell-wise merge: count += count, XOR key_sum/value_sum/check_sum
    for i in 0..cell_count {
        let off = IBLT_HEADER_SIZE + i * IBLT_CELL_SIZE;

        let ca = i32::from_le_bytes(a[off..off + 4].try_into().unwrap());
        let cb = i32::from_le_bytes(b[off..off + 4].try_into().unwrap());
        result[off..off + 4].copy_from_slice(&(ca + cb).to_le_bytes());

        let ksa = u64::from_le_bytes(a[off + 4..off + 12].try_into().unwrap());
        let ksb = u64::from_le_bytes(b[off + 4..off + 12].try_into().unwrap());
        result[off + 4..off + 12].copy_from_slice(&(ksa ^ ksb).to_le_bytes());

        let vsa = u64::from_le_bytes(a[off + 12..off + 20].try_into().unwrap());
        let vsb = u64::from_le_bytes(b[off + 12..off + 20].try_into().unwrap());
        result[off + 12..off + 20].copy_from_slice(&(vsa ^ vsb).to_le_bytes());

        let csa = u32::from_le_bytes(a[off + 20..off + 24].try_into().unwrap());
        let csb = u32::from_le_bytes(b[off + 20..off + 24].try_into().unwrap());
        result[off + 20..off + 24].copy_from_slice(&(csa ^ csb).to_le_bytes());
    }

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn iblt_put_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    pairs_bin: Binary,
    hash_count: u8,
    cell_count: u32,
    seed: u32,
) -> Term<'a> {
    iblt_put_many_impl(env, state_bin, pairs_bin, hash_count, cell_count, seed)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn iblt_put_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    pairs_bin: Binary,
    hash_count: u8,
    cell_count: u32,
    seed: u32,
) -> Term<'a> {
    iblt_put_many_impl(env, state_bin, pairs_bin, hash_count, cell_count, seed)
}

#[rustler::nif]
fn iblt_merge_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    iblt_merge_impl(env, a_bin, b_bin)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn iblt_merge_dirty_nif<'a>(env: Env<'a>, a_bin: Binary, b_bin: Binary) -> Term<'a> {
    iblt_merge_impl(env, a_bin, b_bin)
}
