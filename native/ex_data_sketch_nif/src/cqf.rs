use rustler::{Binary, Env, Term};

use crate::error;
use crate::quotient_core as qc;
use crate::quotient_core::{Slot, QOT_CON, QOT_OCC, QOT_SHI};

const CQF_HEADER_SIZE: usize = 40;

fn cqf_find_remainder_in_run(slots: &[Slot], run_start: u32, target_fr: u64, sc: u32) -> Option<u32> {
    cqf_scan_run(slots, run_start, target_fr, sc)
}

fn cqf_scan_run(slots: &[Slot], pos: u32, target_fr: u64, sc: u32) -> Option<u32> {
    let r = qc::rem_val(slots, pos);
    if r == target_fr {
        Some(pos)
    } else if r > target_fr {
        None
    } else {
        let next_pos = cqf_skip_duplicates(slots, pos, r, sc);
        if qc::con(slots, next_pos) {
            cqf_scan_run(slots, next_pos, target_fr, sc)
        } else {
            None
        }
    }
}

fn cqf_skip_duplicates(slots: &[Slot], pos: u32, remainder_val: u64, sc: u32) -> u32 {
    let n = qc::nxt(pos, sc);
    if qc::con(slots, n) && qc::rem_val(slots, n) == remainder_val {
        cqf_skip_duplicates(slots, n, remainder_val, sc)
    } else {
        n
    }
}

fn cqf_last_copy(slots: &[Slot], pos: u32, remainder_val: u64, sc: u32) -> u32 {
    let n = qc::nxt(pos, sc);
    if qc::con(slots, n) && qc::rem_val(slots, n) == remainder_val {
        cqf_last_copy(slots, n, remainder_val, sc)
    } else {
        pos
    }
}

fn cqf_read_counter(slots: &[Slot], pos: u32, remainder_val: u64, sc: u32) -> u64 {
    let n = qc::nxt(pos, sc);
    if qc::con(slots, n) && qc::rem_val(slots, n) == remainder_val {
        1 + cqf_read_counter(slots, n, remainder_val, sc)
    } else {
        1
    }
}

fn cqf_do_insert(
    slots: &mut [Slot],
    fq: u32,
    fr: u64,
    sc: u32,
    occupied_count: &mut u32,
    total_count: &mut u64,
) {
    if qc::occ(slots, fq) {
        // Existing quotient
        let run_start = qc::find_run_start(slots, fq, sc);
        match cqf_find_remainder_in_run(slots, run_start, fr, sc) {
            None => {
                // New remainder in existing run
                insert_into_run(slots, fq, run_start, fr, sc);
                *occupied_count += 1;
                *total_count += 1;
            }
            Some(pos) => {
                // Existing remainder - add duplicate copy
                let last = cqf_last_copy(slots, pos, fr, sc);
                let n = qc::nxt(last, sc);
                qc::shift_right(slots, n, QOT_CON | QOT_SHI, fr, sc);
                *total_count += 1;
            }
        }
    } else {
        // New fingerprint
        cqf_do_insert_new(slots, fq, fr, sc, occupied_count, total_count);
    }
}

fn cqf_do_insert_new(
    slots: &mut [Slot],
    fq: u32,
    fr: u64,
    sc: u32,
    occupied_count: &mut u32,
    total_count: &mut u64,
) {
    qc::set_meta_bit(slots, fq, QOT_OCC);
    let had_entry = qc::meta(slots, fq) != QOT_OCC;

    if had_entry {
        let run_start = qc::find_run_start(slots, fq, sc);
        let m = if run_start == fq { 0 } else { QOT_SHI };
        qc::shift_right(slots, run_start, m, fr, sc);
    } else {
        slots[fq as usize] = (QOT_OCC, fr);
    }

    *occupied_count += 1;
    *total_count += 1;
}

fn insert_into_run(slots: &mut [Slot], fq: u32, run_start: u32, fr: u64, sc: u32) {
    let (pos, at_start) = sorted_pos(slots, run_start, fr, sc);

    if at_start {
        qc::set_meta_bit(slots, run_start, QOT_CON);
        let m = if pos == fq { 0 } else { QOT_SHI };
        qc::shift_right(slots, pos, m, fr, sc);
    } else {
        let m = QOT_CON | if pos == fq { 0 } else { QOT_SHI };
        qc::shift_right(slots, pos, m, fr, sc);
    }
}

fn sorted_pos(slots: &[Slot], run_start: u32, fr: u64, sc: u32) -> (u32, bool) {
    if fr < qc::rem_val(slots, run_start) {
        (run_start, true)
    } else {
        sorted_pos_cont(slots, run_start, fr, sc)
    }
}

fn sorted_pos_cont(slots: &[Slot], pos: u32, fr: u64, sc: u32) -> (u32, bool) {
    let n = qc::nxt(pos, sc);
    if qc::con(slots, n) {
        if fr < qc::rem_val(slots, n) {
            (n, false)
        } else {
            sorted_pos_cont(slots, n, fr, sc)
        }
    } else {
        (n, false)
    }
}

fn cqf_insert_with_count(
    slots: &mut [Slot],
    fq: u32,
    fr: u64,
    count: u64,
    sc: u32,
    occupied_count: &mut u32,
    total_count: &mut u64,
) {
    let was_occ = qc::occ(slots, fq);
    let had_entry = qc::meta(slots, fq) != 0;

    qc::set_meta_bit(slots, fq, QOT_OCC);

    if !was_occ && !had_entry {
        slots[fq as usize] = (QOT_OCC, fr);
    } else if was_occ {
        let run_start = qc::find_run_start(slots, fq, sc);
        insert_into_run(slots, fq, run_start, fr, sc);
    } else {
        let run_start = qc::find_run_start(slots, fq, sc);
        let m = if run_start == fq { 0 } else { QOT_SHI };
        qc::shift_right(slots, run_start, m, fr, sc);
    }

    *occupied_count += 1;
    *total_count += count;

    // Add (count-1) duplicate copies
    if count > 1 {
        let run_start = qc::find_run_start(slots, fq, sc);
        if let Some(pos) = cqf_find_remainder_in_run(slots, run_start, fr, sc) {
            for _ in 1..count {
                let last = cqf_last_copy(slots, pos, fr, sc);
                let n = qc::nxt(last, sc);
                qc::shift_right(slots, n, QOT_CON | QOT_SHI, fr, sc);
            }
        }
    }
}

// Extract all counted fingerprints as (quotient, remainder, count) triples
fn cqf_extract_all_counted(slots: &[Slot], sc: u32) -> Vec<(u32, u64, u64)> {
    let mut triples: Vec<(u32, u64, u64)> = Vec::new();

    for i in 0..sc {
        if qc::occ(slots, i) {
            let run_start = qc::find_run_start(slots, i, sc);
            cqf_extract_run_counted(slots, run_start, i, sc, &mut triples);
        }
    }

    triples.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    triples
}

fn cqf_extract_run_counted(
    slots: &[Slot],
    pos: u32,
    quotient: u32,
    sc: u32,
    triples: &mut Vec<(u32, u64, u64)>,
) {
    let mut p = pos;
    loop {
        let fr = qc::rem_val(slots, p);
        let count = cqf_read_counter(slots, p, fr, sc);
        triples.push((quotient, fr, count));

        let next_pos = cqf_skip_duplicates(slots, p, fr, sc);
        if qc::con(slots, next_pos) {
            p = next_pos;
        } else {
            break;
        }
    }
}

// Merge two sorted lists of (q, r, count) triples, summing counts for matching (q, r)
fn cqf_merge_counted(a: &[(u32, u64, u64)], b: &[(u32, u64, u64)]) -> Vec<(u32, u64, u64)> {
    let mut result = Vec::with_capacity(a.len() + b.len());
    let (mut ia, mut ib) = (0, 0);

    while ia < a.len() && ib < b.len() {
        let key_a = (a[ia].0, a[ia].1);
        let key_b = (b[ib].0, b[ib].1);
        if key_a < key_b {
            result.push(a[ia]);
            ia += 1;
        } else if key_a > key_b {
            result.push(b[ib]);
            ib += 1;
        } else {
            result.push((a[ia].0, a[ia].1, a[ia].2 + b[ib].2));
            ia += 1;
            ib += 1;
        }
    }
    while ia < a.len() {
        result.push(a[ia]);
        ia += 1;
    }
    while ib < b.len() {
        result.push(b[ib]);
        ib += 1;
    }
    result
}

fn cqf_put_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    if state_bin.len() < CQF_HEADER_SIZE {
        return error::error_string(env, "CQF state too short for header");
    }

    let slot_bytes = ((3 + r as usize) + 7) / 8;
    let state = state_bin.as_slice();

    let slot_count = u32::from_le_bytes(state[8..12].try_into().unwrap());
    let expected_len = CQF_HEADER_SIZE + (slot_count as usize) * slot_bytes;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid CQF state length");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let mut occupied_count = u32::from_le_bytes(state[12..16].try_into().unwrap());
    let mut total_count = u64::from_le_bytes(state[16..24].try_into().unwrap());

    let body = &state[CQF_HEADER_SIZE..];
    let mut slots = qc::decode_slots(body, slot_bytes, slot_count);

    let hashes = hashes_bin.as_slice();
    let hash_count = hashes.len() / 8;

    for h in 0..hash_count {
        let off = h * 8;
        let hash64 = u64::from_le_bytes(hashes[off..off + 8].try_into().unwrap());
        let (fq, fr) = qc::qot_split_hash(hash64, q, r);
        cqf_do_insert(&mut slots, fq, fr, slot_count, &mut occupied_count, &mut total_count);
    }

    let new_body = qc::encode_slots(&slots, slot_bytes, slot_count);

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(&state[..CQF_HEADER_SIZE]);
    result[12..16].copy_from_slice(&occupied_count.to_le_bytes());
    result[16..24].copy_from_slice(&total_count.to_le_bytes());
    result.extend_from_slice(&new_body);

    error::ok_binary(env, &result)
}

fn cqf_merge_impl<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    if a_bin.len() < CQF_HEADER_SIZE || b_bin.len() < CQF_HEADER_SIZE {
        return error::error_string(env, "CQF state too short for header");
    }

    let slot_bytes = ((3 + r as usize) + 7) / 8;
    let a = a_bin.as_slice();
    let b = b_bin.as_slice();

    let slot_count_a = u32::from_le_bytes(a[8..12].try_into().unwrap());
    let slot_count_b = u32::from_le_bytes(b[8..12].try_into().unwrap());

    if slot_count_a != slot_count_b {
        return error::error_string(env, "CQF slot count mismatch for merge");
    }

    let slot_count = slot_count_a;
    let expected_len = CQF_HEADER_SIZE + (slot_count as usize) * slot_bytes;

    if a_bin.len() != expected_len || b_bin.len() != expected_len {
        return error::error_string(env, "invalid CQF state length");
    }

    let slots_a = qc::decode_slots(&a[CQF_HEADER_SIZE..], slot_bytes, slot_count);
    let slots_b = qc::decode_slots(&b[CQF_HEADER_SIZE..], slot_bytes, slot_count);

    let triples_a = cqf_extract_all_counted(&slots_a, slot_count);
    let triples_b = cqf_extract_all_counted(&slots_b, slot_count);

    let all = cqf_merge_counted(&triples_a, &triples_b);

    let mut fresh_slots = vec![(0u8, 0u64); slot_count as usize];
    let mut occupied_count: u32 = 0;
    let mut total_count: u64 = 0;

    for (fq, fr, count) in &all {
        cqf_insert_with_count(
            &mut fresh_slots, *fq, *fr, *count, slot_count,
            &mut occupied_count, &mut total_count,
        );
    }

    let new_body = qc::encode_slots(&fresh_slots, slot_bytes, slot_count);

    let mut result = Vec::with_capacity(expected_len);
    result.extend_from_slice(&a[..CQF_HEADER_SIZE]);
    result[12..16].copy_from_slice(&occupied_count.to_le_bytes());
    result[16..24].copy_from_slice(&total_count.to_le_bytes());
    result.extend_from_slice(&new_body);

    error::ok_binary(env, &result)
}

#[rustler::nif]
fn cqf_put_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    cqf_put_many_impl(env, state_bin, hashes_bin, q, r)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cqf_put_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    cqf_put_many_impl(env, state_bin, hashes_bin, q, r)
}

#[rustler::nif]
fn cqf_merge_nif<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    cqf_merge_impl(env, a_bin, b_bin, q, r)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cqf_merge_dirty_nif<'a>(
    env: Env<'a>,
    a_bin: Binary,
    b_bin: Binary,
    q: u8,
    r: u8,
) -> Term<'a> {
    cqf_merge_impl(env, a_bin, b_bin, q, r)
}
