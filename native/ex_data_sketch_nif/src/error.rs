use rustler::{Encoder, Env, Term};

pub fn ok_binary<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    let binary = rustler::types::binary::OwnedBinary::new(data.len())
        .map(|mut b| {
            b.as_mut_slice().copy_from_slice(data);
            b
        })
        .expect("failed to allocate binary");
    (rustler::types::atom::ok(), binary.release(env)).encode(env)
}

pub fn ok_float<'a>(env: Env<'a>, val: f64) -> Term<'a> {
    (rustler::types::atom::ok(), val).encode(env)
}

pub fn error_string<'a>(env: Env<'a>, reason: &str) -> Term<'a> {
    (rustler::types::atom::error(), reason).encode(env)
}
