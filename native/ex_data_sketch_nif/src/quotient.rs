use rustler::{Binary, Env, Term};

use crate::error;
use crate::quotient_core as qc;

const QOT_HEADER_SIZE: usize = 32;

fn quotient_put_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    let slot_bytes = ((3 + r as usize) + 7) / 8;
    let state = state_bin.as_slice();

    // Read slot_count from header (offset 8, u32-LE)
    let slot_count = u32::from_le_bytes(state[8..12].try_into().unwrap());
    let expected_len = QOT_HEADER_SIZE + (slot_count as usize) * slot_bytes;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid Quotient state length");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let mut item_count = u32::from_le_bytes(state[12..16].try_into().unwrap());

    let body = &state[QOT_HEADER_SIZE..];
    let mut slots = qc::decode_slots(body, slot_bytes, slot_count);

    let hashes = hashes_bin.as_slice();
    let hash_count = hashes.len() / 8;

    for h in 0..hash_count {
        let off = h * 8;
        let hash64 = u64::from_le_bytes(hashes[off..off + 8].try_into().unwrap());
        let (fq, fr) = qc::qot_split_hash(hash64, q, r);

        if !qc::lookup(&slots, fq, fr, slot_count) {
            qc::do_insert(&mut slots, fq, fr, slot_count);
            item_count += 1;
        }
    }

    let new_body = qc::encode_slots(&slots, slot_bytes, slot_count);

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(&state[..QOT_HEADER_SIZE]);
    result[12..16].copy_from_slice(&item_count.to_le_bytes());
    result.extend_from_slice(&new_body);

    error::ok_binary(env, &result)
}

fn quotient_merge_impl<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    let slot_bytes = ((3 + r as usize) + 7) / 8;
    let a = a_bin.as_slice();
    let b = b_bin.as_slice();

    let slot_count_a = u32::from_le_bytes(a[8..12].try_into().unwrap());
    let slot_count_b = u32::from_le_bytes(b[8..12].try_into().unwrap());

    if slot_count_a != slot_count_b {
        return error::error_string(env, "Quotient slot count mismatch for merge");
    }

    let slot_count = slot_count_a;
    let expected_len = QOT_HEADER_SIZE + (slot_count as usize) * slot_bytes;

    if a_bin.len() != expected_len || b_bin.len() != expected_len {
        return error::error_string(env, "invalid Quotient state length");
    }

    let slots_a = qc::decode_slots(&a[QOT_HEADER_SIZE..], slot_bytes, slot_count);
    let slots_b = qc::decode_slots(&b[QOT_HEADER_SIZE..], slot_bytes, slot_count);

    let fps_a = qc::extract_all(&slots_a, slot_count);
    let fps_b = qc::extract_all(&slots_b, slot_count);

    // Merge sorted unique
    let mut all = Vec::with_capacity(fps_a.len() + fps_b.len());
    let (mut ia, mut ib) = (0, 0);
    while ia < fps_a.len() && ib < fps_b.len() {
        if fps_a[ia] < fps_b[ib] {
            all.push(fps_a[ia]);
            ia += 1;
        } else if fps_a[ia] > fps_b[ib] {
            all.push(fps_b[ib]);
            ib += 1;
        } else {
            all.push(fps_a[ia]);
            ia += 1;
            ib += 1;
        }
    }
    while ia < fps_a.len() {
        all.push(fps_a[ia]);
        ia += 1;
    }
    while ib < fps_b.len() {
        all.push(fps_b[ib]);
        ib += 1;
    }

    // Insert all into fresh filter
    let mut fresh_slots = vec![(0u8, 0u64); slot_count as usize];
    let mut item_count: u32 = 0;

    for (fq, fr) in &all {
        qc::do_insert(&mut fresh_slots, *fq, *fr, slot_count);
        item_count += 1;
    }

    let new_body = qc::encode_slots(&fresh_slots, slot_bytes, slot_count);

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(&a[..QOT_HEADER_SIZE]);
    result[12..16].copy_from_slice(&item_count.to_le_bytes());
    result.extend_from_slice(&new_body);

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn quotient_put_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    quotient_put_many_impl(env, state_bin, hashes_bin, q, r)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn quotient_put_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    quotient_put_many_impl(env, state_bin, hashes_bin, q, r)
}

#[rustler::nif]
fn quotient_merge_nif<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    quotient_merge_impl(env, a_bin, b_bin, q, r)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn quotient_merge_dirty_nif<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    quotient_merge_impl(env, a_bin, b_bin, q, r)
}
