use rustler::{Encoder, Env, Term};

pub fn ok_binary<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    match rustler::types::binary::OwnedBinary::new(data.len()) {
        Some(mut b) => {
            b.as_mut_slice().copy_from_slice(data);
            (rustler::types::atom::ok(), b.release(env)).encode(env)
        }
        None => error_string(env, "failed to allocate binary"),
    }
}

pub fn ok_u64<'a>(env: Env<'a>, val: u64) -> Term<'a> {
    (rustler::types::atom::ok(), val).encode(env)
}

pub fn ok_float<'a>(env: Env<'a>, val: f64) -> Term<'a> {
    (rustler::types::atom::ok(), val).encode(env)
}

pub fn error_string<'a>(env: Env<'a>, reason: &str) -> Term<'a> {
    (rustler::types::atom::error(), reason).encode(env)
}

pub fn error_full_binary<'a>(env: Env<'a>, data: &[u8]) -> Term<'a> {
    match rustler::types::binary::OwnedBinary::new(data.len()) {
        Some(mut b) => {
            b.as_mut_slice().copy_from_slice(data);
            (rustler::types::atom::error(), "full", b.release(env)).encode(env)
        }
        None => error_string(env, "failed to allocate binary"),
    }
}
