use rustler::{Binary, Env, Term};

use crate::error;

const CKO_HEADER_SIZE: usize = 32;

fn cko_fp_hash(fingerprint: u32) -> u32 {
    let mut h = fingerprint.wrapping_mul(0x5BD1E995);
    h ^= h >> 13;
    h.wrapping_mul(0x5BD1E995)
}

fn cko_alt_index(index: u32, fingerprint: u32, bucket_count: u32) -> u32 {
    (index ^ cko_fp_hash(fingerprint)) & (bucket_count - 1)
}

fn cko_slot_offset(bucket_idx: u32, slot_idx: u32, bucket_size: u8, fp_bytes: u8) -> usize {
    (bucket_idx as usize) * (bucket_size as usize) * (fp_bytes as usize)
        + (slot_idx as usize) * (fp_bytes as usize)
}

fn cko_read_slot(body: &[u8], bucket_idx: u32, slot_idx: u32, bucket_size: u8, fp_bytes: u8) -> u32 {
    let off = cko_slot_offset(bucket_idx, slot_idx, bucket_size, fp_bytes);
    match fp_bytes {
        1 => body[off] as u32,
        2 => u16::from_le_bytes(body[off..off + 2].try_into().unwrap()) as u32,
        _ => unreachable!(),
    }
}

fn cko_write_slot(body: &mut [u8], bucket_idx: u32, slot_idx: u32, bucket_size: u8, fp_bytes: u8, value: u32) {
    let off = cko_slot_offset(bucket_idx, slot_idx, bucket_size, fp_bytes);
    match fp_bytes {
        1 => body[off] = value as u8,
        2 => body[off..off + 2].copy_from_slice(&(value as u16).to_le_bytes()),
        _ => unreachable!(),
    }
}

fn cko_find_empty_slot(body: &[u8], bucket_idx: u32, bucket_size: u8, fp_bytes: u8) -> Option<u32> {
    for slot in 0..bucket_size as u32 {
        if cko_read_slot(body, bucket_idx, slot, bucket_size, fp_bytes) == 0 {
            return Some(slot);
        }
    }
    None
}

fn cuckoo_put_many_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    _seed: u32,
) -> Term<'a> {
    let fp_bytes = (fp_bits as usize + 7) / 8;
    let expected_len = CKO_HEADER_SIZE + (bucket_count as usize) * (bucket_size as usize) * fp_bytes;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid Cuckoo state length");
    }
    if hashes_bin.len() % 8 != 0 {
        return error::error_string(env, "hashes_bin length must be a multiple of 8");
    }

    let state = state_bin.as_slice();
    let mut result = state.to_vec();

    // Read current item_count from header (offset 12, u32-LE)
    // Header layout: magic(4) + version(1) + fp_bits(1) + bucket_size(1) + flags(1) +
    //   bucket_count(4) + item_count(4) + seed(4) + max_kicks(4) + reserved(8)
    let mut item_count = u32::from_le_bytes(result[12..16].try_into().unwrap());

    let fp_mask = (1u64 << fp_bits) - 1;
    let fp_b = fp_bytes as u8;

    let hashes = hashes_bin.as_slice();
    let hash_count = hashes.len() / 8;

    for h in 0..hash_count {
        let off = h * 8;
        let hash64 = u64::from_le_bytes(hashes[off..off + 8].try_into().unwrap());

        let i1 = (hash64 & (bucket_count as u64 - 1)) as u32;
        let mut fp = ((hash64 >> 32) & fp_mask) as u32;
        if fp == 0 {
            fp = 1;
        }
        let i2 = cko_alt_index(i1, fp, bucket_count);

        let body = &mut result[CKO_HEADER_SIZE..];

        // Try i1
        if let Some(slot) = cko_find_empty_slot(body, i1, bucket_size, fp_b) {
            cko_write_slot(body, i1, slot, bucket_size, fp_b, fp);
            item_count += 1;
            continue;
        }

        // Try i2
        if let Some(slot) = cko_find_empty_slot(body, i2, bucket_size, fp_b) {
            cko_write_slot(body, i2, slot, bucket_size, fp_b, fp);
            item_count += 1;
            continue;
        }

        // Kick loop
        let evict_bucket = if fp % 2 == 0 { i1 } else { i2 };
        let kick_result = cko_kick_loop(
            &mut result[CKO_HEADER_SIZE..],
            evict_bucket,
            fp,
            bucket_size,
            fp_b,
            bucket_count,
            max_kicks,
        );

        match kick_result {
            Ok(()) => {
                item_count += 1;
            }
            Err(()) => {
                // Filter is full - write item_count back and return error with current state
                result[12..16].copy_from_slice(&item_count.to_le_bytes());
                return error::error_full_binary(env, &result);
            }
        }
    }

    // Write updated item_count back to header
    result[12..16].copy_from_slice(&item_count.to_le_bytes());

    error::ok_binary(env, &result)
}

fn cko_kick_loop(
    body: &mut [u8],
    start_bucket: u32,
    start_fp: u32,
    bucket_size: u8,
    fp_bytes: u8,
    bucket_count: u32,
    max_kicks: u32,
) -> Result<(), ()> {
    let mut bucket = start_bucket;
    let mut fp = start_fp;

    for kick_count in 0..max_kicks {
        let evict_slot = (fp + kick_count) % bucket_size as u32;
        let old_fp = cko_read_slot(body, bucket, evict_slot, bucket_size, fp_bytes);
        cko_write_slot(body, bucket, evict_slot, bucket_size, fp_bytes, fp);

        let alt_bucket = cko_alt_index(bucket, old_fp, bucket_count);

        if let Some(slot) = cko_find_empty_slot(body, alt_bucket, bucket_size, fp_bytes) {
            cko_write_slot(body, alt_bucket, slot, bucket_size, fp_bytes, old_fp);
            return Ok(());
        }

        bucket = alt_bucket;
        fp = old_fp;
    }

    Err(())
}

#[rustler::nif]
fn cuckoo_put_many_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u32,
) -> Term<'a> {
    cuckoo_put_many_impl(env, state_bin, hashes_bin, fp_bits, bucket_size, bucket_count, max_kicks, seed)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cuckoo_put_many_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    hashes_bin: Binary,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u32,
) -> Term<'a> {
    cuckoo_put_many_impl(env, state_bin, hashes_bin, fp_bits, bucket_size, bucket_count, max_kicks, seed)
}
