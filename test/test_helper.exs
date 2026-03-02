exclude =
  if ExDataSketch.Backend.Rust.available?() do
    [:no_rust_nif]
  else
    [:rust_nif]
  end

ExUnit.start(exclude: exclude)
