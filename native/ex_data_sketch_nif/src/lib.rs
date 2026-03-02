mod cms;
mod error;
mod hll;
mod theta;

rustler::atoms! {
    ok,
}

#[rustler::nif]
fn nif_loaded() -> rustler::Atom {
    ok()
}

rustler::init!("Elixir.ExDataSketch.Nif");
