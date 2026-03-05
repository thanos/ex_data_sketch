exclude =
  if ExDataSketch.Backend.Rust.available?() do
    [:no_rust_nif]
  else
    [:rust_nif]
  end

Mox.defmock(ExDataSketch.MockBackend, for: ExDataSketch.Backend)

ExUnit.start(exclude: exclude)
