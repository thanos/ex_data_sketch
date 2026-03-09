mod bloom;
mod cms;
mod cqf;
mod cuckoo;
mod iblt;
mod quotient;
mod quotient_core;
mod xor_filter;
mod ddsketch;
mod error;
mod fi;
mod hash;
mod hll;
mod kll;
mod theta;

rustler::atoms! {
    ok,
}

#[rustler::nif]
fn nif_loaded() -> rustler::Atom {
    ok()
}

rustler::init!("Elixir.ExDataSketch.Nif");
