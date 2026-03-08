use xxhash_rust::xxh3;

#[rustler::nif]
fn xxhash3_64_nif(data: rustler::Binary) -> u64 {
    xxh3::xxh3_64(data.as_slice())
}

#[rustler::nif]
fn xxhash3_64_seeded_nif(data: rustler::Binary, seed: u64) -> u64 {
    xxh3::xxh3_64_with_seed(data.as_slice(), seed)
}
