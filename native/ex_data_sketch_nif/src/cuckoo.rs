use rustler::{Binary, Env, ListIterator, Term};
use xxhash_rust::xxh3;

use crate::error;
use crate::hash::murmur3_x64_128;

// Hash algorithm wire bytes for the v2 raw NIF dispatch.
// MUST match `ExDataSketch.Hash.Metadata.algorithm_to_byte/1`.
const ALGO_XXH3: u8 = 1;
const ALGO_MURMUR3: u8 = 2;

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

/// Inserts a single already-derived `hash64` into the filter, trying `i1`,
/// then `i2`, then the kick loop. Shared by the pre-hashed and raw
/// (hash-in-Rust) paths so both insert identically. `Err(())` means the
/// filter is full.
fn cko_insert_one(
    result: &mut [u8],
    hash64: u64,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
) -> Result<(), ()> {
    let fp_bytes = (fp_bits as usize + 7) / 8;
    let fp_mask = (1u64 << fp_bits) - 1;
    let fp_b = fp_bytes as u8;

    let i1 = (hash64 & (bucket_count as u64 - 1)) as u32;
    let mut fp = ((hash64 >> 32) & fp_mask) as u32;
    if fp == 0 {
        fp = 1;
    }
    let i2 = cko_alt_index(i1, fp, bucket_count);

    let body = &mut result[CKO_HEADER_SIZE..];

    if let Some(slot) = cko_find_empty_slot(body, i1, bucket_size, fp_b) {
        cko_write_slot(body, i1, slot, bucket_size, fp_b, fp);
        return Ok(());
    }

    if let Some(slot) = cko_find_empty_slot(body, i2, bucket_size, fp_b) {
        cko_write_slot(body, i2, slot, bucket_size, fp_b, fp);
        return Ok(());
    }

    let evict_bucket = if fp % 2 == 0 { i1 } else { i2 };
    cko_kick_loop(body, evict_bucket, fp, bucket_size, fp_b, bucket_count, max_kicks)
}

fn cko_validate<'a>(
    env: Env<'a>,
    bucket_count: u32,
    fp_bits: u8,
) -> Result<usize, Term<'a>> {
    if bucket_count == 0 {
        return Err(error::error_string(env, "bucket_count must be > 0"));
    }
    if fp_bits == 0 || fp_bits > 32 {
        return Err(error::error_string(env, "fp_bits must be 1..32"));
    }
    Ok((fp_bits as usize + 7) / 8)
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
    let fp_bytes = match cko_validate(env, bucket_count, fp_bits) {
        Ok(fb) => fb,
        Err(e) => return e,
    };
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

    let hashes = hashes_bin.as_slice();
    let hash_count = hashes.len() / 8;

    for h in 0..hash_count {
        let off = h * 8;
        let hash64 = u64::from_le_bytes(hashes[off..off + 8].try_into().unwrap());

        match cko_insert_one(&mut result, hash64, fp_bits, bucket_size, bucket_count, max_kicks) {
            Ok(()) => item_count += 1,
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

fn cuckoo_put_many_raw_impl<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
    let fp_bytes = match cko_validate(env, bucket_count, fp_bits) {
        Ok(fb) => fb,
        Err(e) => return e,
    };
    let expected_len = CKO_HEADER_SIZE + (bucket_count as usize) * (bucket_size as usize) * fp_bytes;

    if state_bin.len() != expected_len {
        return error::error_string(env, "invalid Cuckoo state length");
    }

    let mut result = state_bin.as_slice().to_vec();
    let mut item_count = u32::from_le_bytes(result[12..16].try_into().unwrap());
    let m3_seed = seed as u32;

    for item_term in items {
        let bin: Binary = match item_term.decode() {
            Ok(b) => b,
            Err(_) => return error::error_string(env, "all items must be binaries"),
        };
        let hash64 = match algorithm {
            ALGO_XXH3 => xxh3::xxh3_64_with_seed(bin.as_slice(), seed),
            ALGO_MURMUR3 => murmur3_x64_128(bin.as_slice(), m3_seed).0,
            _ => {
                return error::error_string(
                    env,
                    "unsupported hash algorithm byte (expected 1=xxhash3, 2=murmur3)",
                )
            }
        };

        match cko_insert_one(&mut result, hash64, fp_bits, bucket_size, bucket_count, max_kicks) {
            Ok(()) => item_count += 1,
            Err(()) => {
                result[12..16].copy_from_slice(&item_count.to_le_bytes());
                return error::error_full_binary(env, &result);
            }
        }
    }

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

#[rustler::nif]
fn cuckoo_put_many_raw_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u64,
) -> Term<'a> {
    cuckoo_put_many_raw_impl(
        env, state_bin, items, fp_bits, bucket_size, bucket_count, max_kicks, seed, ALGO_XXH3,
    )
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cuckoo_put_many_raw_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u64,
) -> Term<'a> {
    cuckoo_put_many_raw_impl(
        env, state_bin, items, fp_bits, bucket_size, bucket_count, max_kicks, seed, ALGO_XXH3,
    )
}

#[rustler::nif]
fn cuckoo_put_many_raw_h_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
    cuckoo_put_many_raw_impl(
        env, state_bin, items, fp_bits, bucket_size, bucket_count, max_kicks, seed, algorithm,
    )
}

#[rustler::nif(schedule = "DirtyCpu")]
fn cuckoo_put_many_raw_h_dirty_nif<'a>(
    env: Env<'a>,
    state_bin: Binary,
    items: ListIterator<'a>,
    fp_bits: u8,
    bucket_size: u8,
    bucket_count: u32,
    max_kicks: u32,
    seed: u64,
    algorithm: u8,
) -> Term<'a> {
    cuckoo_put_many_raw_impl(
        env, state_bin, items, fp_bits, bucket_size, bucket_count, max_kicks, seed, algorithm,
    )
}
