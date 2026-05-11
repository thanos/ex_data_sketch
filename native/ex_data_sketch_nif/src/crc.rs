// CRC32C (Castagnoli, reflected, init 0xFFFFFFFF, final XOR 0xFFFFFFFF).
//
// Pure software implementation using a precomputed 256-entry table. This
// gives ~1 GB/s on commodity hardware without needing the platform-specific
// CRC32 instructions. The pure-Elixir implementation in
// `ExDataSketch.Binary.CRC` is property-tested to produce byte-identical
// output for all inputs.

const POLY_REFLECTED: u32 = 0x82F6_3B78;

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut byte = 0u32;
    while byte < 256 {
        let mut crc = byte;
        let mut i = 0;
        while i < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY_REFLECTED;
            } else {
                crc >>= 1;
            }
            i += 1;
        }
        table[byte as usize] = crc;
        byte += 1;
    }
    table
}

const TABLE: [u32; 256] = build_table();

fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    crc ^ 0xFFFF_FFFF
}

#[rustler::nif]
fn crc32c_nif(data: rustler::Binary) -> u32 {
    crc32c(data.as_slice())
}
