/// Shared quotient filter slot arithmetic used by both Quotient and CQF NIFs.

pub const QOT_OCC: u8 = 1;
pub const QOT_CON: u8 = 2;
pub const QOT_SHI: u8 = 4;

pub type Slot = (u8, u64); // (meta_bits, remainder)

pub fn qot_split_hash(hash64: u64, q: u8, r: u8) -> (u32, u64) {
    let quotient = (hash64 >> (64 - q as u32)) & ((1u64 << q) - 1);
    let remainder = (hash64 >> (64 - q as u32 - r as u32)) & ((1u64 << r) - 1);
    (quotient as u32, remainder)
}

#[inline]
pub fn meta(slots: &[Slot], i: u32) -> u8 {
    slots[i as usize].0
}

#[inline]
pub fn rem_val(slots: &[Slot], i: u32) -> u64 {
    slots[i as usize].1
}

#[inline]
pub fn occ(slots: &[Slot], i: u32) -> bool {
    slots[i as usize].0 & QOT_OCC != 0
}

#[inline]
pub fn con(slots: &[Slot], i: u32) -> bool {
    slots[i as usize].0 & QOT_CON != 0
}

#[inline]
pub fn shi(slots: &[Slot], i: u32) -> bool {
    slots[i as usize].0 & QOT_SHI != 0
}

#[inline]
pub fn nxt(i: u32, sc: u32) -> u32 {
    (i + 1) % sc
}

#[inline]
pub fn prv(i: u32, sc: u32) -> u32 {
    (i + sc - 1) % sc
}

pub fn set_meta_bit(slots: &mut [Slot], i: u32, bit: u8) {
    slots[i as usize].0 |= bit;
}

pub fn clr_meta_bit(slots: &mut [Slot], i: u32, bit: u8) {
    slots[i as usize].0 &= !bit;
}

pub fn decode_slots(body: &[u8], slot_bytes: usize, slot_count: u32) -> Vec<Slot> {
    let mut slots = Vec::with_capacity(slot_count as usize);
    for i in 0..slot_count as usize {
        let off = i * slot_bytes;
        let mut raw: u64 = 0;
        for j in 0..slot_bytes {
            raw |= (body[off + j] as u64) << (j * 8);
        }
        let m = (raw & 0x7) as u8;
        let r = raw >> 3;
        slots.push((m, r));
    }
    slots
}

pub fn encode_slots(slots: &[Slot], slot_bytes: usize, slot_count: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(slot_count as usize * slot_bytes);
    for i in 0..slot_count as usize {
        let (m, r) = slots[i];
        let raw = (m as u64 & 0x7) | (r << 3);
        for j in 0..slot_bytes {
            body.push(((raw >> (j * 8)) & 0xFF) as u8);
        }
    }
    body
}

pub fn find_run_start(slots: &[Slot], fq: u32, sc: u32) -> u32 {
    if shi(slots, fq) {
        let cs = walk_back(slots, fq, sc);
        let n = count_occupied_range(slots, cs, fq, sc);
        skip_runs_fwd(slots, cs, n, sc)
    } else {
        fq
    }
}

fn walk_back(slots: &[Slot], i: u32, sc: u32) -> u32 {
    let p = prv(i, sc);
    if shi(slots, p) {
        walk_back(slots, p, sc)
    } else {
        p
    }
}

fn count_occupied_range(slots: &[Slot], from: u32, to: u32, sc: u32) -> u32 {
    if from == to {
        return 0;
    }
    let add = if occ(slots, from) { 1 } else { 0 };
    add + count_occupied_range(slots, nxt(from, sc), to, sc)
}

fn skip_runs_fwd(slots: &[Slot], pos: u32, n: u32, sc: u32) -> u32 {
    if n == 0 {
        return pos;
    }
    let mut p = nxt(pos, sc);
    p = skip_continuations(slots, p, sc);
    skip_runs_fwd(slots, p, n - 1, sc)
}

fn skip_continuations(slots: &[Slot], pos: u32, sc: u32) -> u32 {
    if con(slots, pos) {
        skip_continuations(slots, nxt(pos, sc), sc)
    } else {
        pos
    }
}

pub fn lookup(slots: &[Slot], fq: u32, fr: u64, sc: u32) -> bool {
    if !occ(slots, fq) {
        return false;
    }
    let rs = find_run_start(slots, fq, sc);
    scan_run(slots, rs, fr, sc)
}

fn scan_run(slots: &[Slot], pos: u32, fr: u64, sc: u32) -> bool {
    let r = rem_val(slots, pos);
    if r == fr {
        true
    } else if r > fr {
        false
    } else {
        let n = nxt(pos, sc);
        if con(slots, n) {
            scan_run(slots, n, fr, sc)
        } else {
            false
        }
    }
}

pub fn do_insert(slots: &mut [Slot], fq: u32, fr: u64, sc: u32) {
    let was_occ = occ(slots, fq);
    let had_entry = meta(slots, fq) != 0;

    set_meta_bit(slots, fq, QOT_OCC);

    if !was_occ && !had_entry {
        // Fast path: canonical slot was completely empty
        slots[fq as usize] = (QOT_OCC, fr);
    } else if was_occ {
        let run_start = find_run_start(slots, fq, sc);
        insert_into_run(slots, fq, run_start, fr, sc);
    } else {
        // New run: insert first entry at run_start
        let run_start = find_run_start(slots, fq, sc);
        let m = if run_start == fq { 0 } else { QOT_SHI };
        shift_right(slots, run_start, m, fr, sc);
    }
}

fn insert_into_run(slots: &mut [Slot], fq: u32, run_start: u32, fr: u64, sc: u32) {
    let (pos, at_start) = sorted_pos(slots, run_start, fr, sc);

    if at_start {
        set_meta_bit(slots, run_start, QOT_CON);
        let m = if pos == fq { 0 } else { QOT_SHI };
        shift_right(slots, pos, m, fr, sc);
    } else {
        let m = QOT_CON | if pos == fq { 0 } else { QOT_SHI };
        shift_right(slots, pos, m, fr, sc);
    }
}

fn sorted_pos(slots: &[Slot], run_start: u32, fr: u64, sc: u32) -> (u32, bool) {
    if fr < rem_val(slots, run_start) {
        (run_start, true)
    } else {
        sorted_pos_cont(slots, run_start, fr, sc)
    }
}

fn sorted_pos_cont(slots: &[Slot], pos: u32, fr: u64, sc: u32) -> (u32, bool) {
    let n = nxt(pos, sc);
    if con(slots, n) {
        if fr < rem_val(slots, n) {
            (n, false)
        } else {
            sorted_pos_cont(slots, n, fr, sc)
        }
    } else {
        (n, false)
    }
}

pub fn shift_right(slots: &mut [Slot], pos: u32, new_meta: u8, new_rem: u64, sc: u32) {
    let (old_meta, old_rem) = slots[pos as usize];
    let occ_here = old_meta & QOT_OCC;
    slots[pos as usize] = (new_meta | occ_here, new_rem);

    if old_meta == 0 {
        return;
    }

    let entry_meta = (old_meta & !QOT_OCC) | QOT_SHI;
    shift_chain(slots, nxt(pos, sc), entry_meta, old_rem, sc);
}

fn shift_chain(slots: &mut [Slot], pos: u32, m: u8, remainder: u64, sc: u32) {
    let (old_meta, old_rem) = slots[pos as usize];
    let occ_here = old_meta & QOT_OCC;
    slots[pos as usize] = (m | occ_here, remainder);

    if old_meta == 0 {
        return;
    }

    let entry_meta = (old_meta & !QOT_OCC) | QOT_SHI;
    shift_chain(slots, nxt(pos, sc), entry_meta, old_rem, sc);
}

pub fn extract_all(slots: &[Slot], sc: u32) -> Vec<(u32, u64)> {
    let mut fps: Vec<(u32, u64)> = Vec::new();
    let mut cur_q: Option<u32> = None;

    for i in 0..sc {
        let m = meta(slots, i);
        if m == 0 {
            cur_q = None;
        } else {
            let q = resolve_quotient(slots, i, m, cur_q, sc);
            fps.push((q, rem_val(slots, i)));
            cur_q = Some(q);
        }
    }

    fps.sort();
    fps.dedup();
    fps
}

fn resolve_quotient(slots: &[Slot], i: u32, m: u8, cur_q: Option<u32>, sc: u32) -> u32 {
    let is_con = m & QOT_CON != 0;
    let is_shi = m & QOT_SHI != 0;

    if is_con {
        cur_q.unwrap_or(i)
    } else if !is_shi {
        i
    } else {
        trace_quotient_for(slots, i, sc)
    }
}

fn trace_quotient_for(slots: &[Slot], slot_idx: u32, sc: u32) -> u32 {
    let cs = walk_back_to_start(slots, slot_idx, sc);
    trace_walk(slots, cs, cs, slot_idx, sc)
}

fn walk_back_to_start(slots: &[Slot], i: u32, sc: u32) -> u32 {
    if shi(slots, i) {
        walk_back_to_start(slots, prv(i, sc), sc)
    } else {
        i
    }
}

fn trace_walk(slots: &[Slot], pos: u32, cur_q: u32, target: u32, sc: u32) -> u32 {
    if pos == target {
        return cur_q;
    }
    let n = nxt(pos, sc);
    let new_q = if con(slots, n) {
        cur_q
    } else {
        next_occ_canonical(slots, cur_q, sc)
    };
    trace_walk(slots, n, new_q, target, sc)
}

fn next_occ_canonical(slots: &[Slot], cur_q: u32, sc: u32) -> u32 {
    find_occ(slots, nxt(cur_q, sc), sc, sc)
}

fn find_occ(slots: &[Slot], pos: u32, sc: u32, remaining: u32) -> u32 {
    if remaining == 0 {
        return 0;
    }
    if occ(slots, pos) {
        pos
    } else {
        find_occ(slots, nxt(pos, sc), sc, remaining - 1)
    }
}

// -- Delete support (needed for CQF merge) --

pub fn do_delete(slots: &mut [Slot], fq: u32, slot_idx: u32, sc: u32) {
    let run_start = find_run_start(slots, fq, sc);
    let n = nxt(slot_idx, sc);
    let is_first = slot_idx == run_start;
    let nxt_is_con = con(slots, n);
    let is_only = is_first && !nxt_is_con;

    if is_only {
        clr_meta_bit(slots, fq, QOT_OCC);
    }

    if is_first && nxt_is_con {
        clr_meta_bit(slots, n, QOT_CON);
    }

    shift_left(slots, slot_idx, sc);
}

fn shift_left(slots: &mut [Slot], pos: u32, sc: u32) {
    let n = nxt(pos, sc);
    let nxt_meta = meta(slots, n);
    let occ_here = meta(slots, pos) & QOT_OCC;

    if nxt_meta == 0 {
        slots[pos as usize] = (occ_here, 0);
    } else if nxt_meta & QOT_SHI == 0 {
        slots[pos as usize] = (occ_here, 0);
    } else {
        let entry_meta = nxt_meta & !QOT_OCC;
        let nxt_rem = rem_val(slots, n);
        slots[pos as usize] = (occ_here | entry_meta, nxt_rem);
        shift_left(slots, n, sc);
    }
}

// Find the slot index of fr in fq's run, or None.
#[allow(dead_code)]
pub fn find_slot(slots: &[Slot], fq: u32, fr: u64, sc: u32) -> Option<u32> {
    if !occ(slots, fq) {
        return None;
    }
    let rs = find_run_start(slots, fq, sc);
    scan_run_idx(slots, rs, fr, sc)
}

fn scan_run_idx(slots: &[Slot], pos: u32, fr: u64, sc: u32) -> Option<u32> {
    let r = rem_val(slots, pos);
    if r == fr {
        Some(pos)
    } else if r > fr {
        None
    } else {
        let n = nxt(pos, sc);
        if con(slots, n) {
            scan_run_idx(slots, n, fr, sc)
        } else {
            None
        }
    }
}
