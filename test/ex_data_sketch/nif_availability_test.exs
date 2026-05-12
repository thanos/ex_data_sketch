defmodule ExDataSketch.NifAvailabilityTest do
  @moduledoc """
  Phase 4 NIF-availability contract tests.

  These tests assert the documented contract between the precompiled
  `RustlerPrecompiled` setup and the user-facing API:

  - When the NIF is available, `ExDataSketch.Hash.nif_available?/0` returns
    `true`, every NIF-backed sketch operation works, and the fast paths are
    selected.
  - When the NIF is unavailable (either because `EX_DATA_SKETCH_SKIP_NIF`
    was set at compile time or because the precompiled artifact was not
    downloaded), `ExDataSketch.Hash.nif_available?/0` returns `false`, the
    Pure backend is the default, and operations that strictly require the
    NIF (e.g., `ExDataSketch.Hash.XXH3.hash/2`) raise `ArgumentError`.

  The same test module covers both modes — it branches on the real-time
  return of `Hash.nif_available?/0`. CI runs the suite twice (once with
  `EX_DATA_SKETCH_SKIP_NIF=true`, once with `EX_DATA_SKETCH_BUILD=true`)
  so both branches are exercised.

  This is intentionally distinct from the per-sketch hot-path tests
  (which use `@moduletag :rust_nif` to skip when the NIF is absent).
  """

  use ExUnit.Case, async: true

  alias ExDataSketch.Backend
  alias ExDataSketch.Hash
  alias ExDataSketch.Hash.XXH3
  alias ExDataSketch.HLL

  describe "Hash.nif_available?/0 contract" do
    test "returns a boolean" do
      assert is_boolean(Hash.nif_available?())
    end

    test "is cached in :persistent_term after the first call" do
      _ = Hash.nif_available?()
      key = {Hash, :nif_available}
      assert :persistent_term.get(key, :unset) in [true, false]
    end

    test "result is stable across calls (does not flap)" do
      first = Hash.nif_available?()

      for _ <- 1..10 do
        assert Hash.nif_available?() == first
      end
    end
  end

  describe "Hash.default_algorithm/0 reflects NIF availability" do
    test "is :xxhash3 when NIF is available, :phash2 otherwise" do
      if Hash.nif_available?() do
        assert Hash.default_algorithm() == :xxhash3
      else
        assert Hash.default_algorithm() == :phash2
      end
    end
  end

  describe "Hash.algorithm_info/1 availability flags" do
    test ":xxhash3 availability matches NIF availability" do
      info = Hash.algorithm_info(:xxhash3)
      assert info.available? == Hash.nif_available?()
    end

    test ":murmur3 is always available (pure Elixir fallback bundled)" do
      assert Hash.algorithm_info(:murmur3).available? == true
    end

    test ":phash2 is always available" do
      assert Hash.algorithm_info(:phash2).available? == true
    end
  end

  describe "Backend.resolve/1 default selection" do
    test "default backend is Pure unless app config opts into Rust" do
      # Backend.default/0 returns Pure unless the application has been
      # explicitly configured to use the Rust backend. This is by design:
      # users opt into the NIF, the NIF is never silently selected.
      assert Backend.default() == Backend.Pure
    end

    test "explicit :backend opt always wins" do
      sketch_pure = HLL.new(p: 4, backend: Backend.Pure)
      assert sketch_pure.backend == Backend.Pure
    end

    test "Backend.Rust.available?/0 reflects NIF availability" do
      # This is the right knob to inspect runtime NIF state. The default
      # backend is a separate, user-controlled choice.
      assert Backend.Rust.available?() == Hash.nif_available?()
    end
  end

  describe "ExDataSketch.Hash.XXH3.hash/2 contract" do
    test "behavior matches NIF availability" do
      if XXH3.available?() do
        h = XXH3.hash("hello", 0)
        assert is_integer(h)
        assert h >= 0
        assert h <= 0xFFFFFFFFFFFFFFFF
      else
        # Documented contract: raises ArgumentError when NIF is unavailable.
        assert_raise ArgumentError, ~r/requires the Rust NIF/, fn ->
          XXH3.hash("hello", 0)
        end
      end
    end
  end

  describe "HLL works in both modes" do
    test "Pure backend always works" do
      items = for i <- 1..100, do: "item_#{i}"
      sketch = HLL.from_enumerable(items, p: 10, backend: Backend.Pure)
      assert sketch.backend == Backend.Pure
      assert HLL.estimate(sketch) > 0
    end

    test "default backend produces a usable sketch in either mode" do
      items = for i <- 1..100, do: "item_#{i}"
      # No explicit :backend — let the default win. The default is Pure
      # unless app config opts into Rust; here we only check that the
      # resulting sketch is usable, not the specific backend.
      sketch = HLL.from_enumerable(items, p: 10)
      assert sketch.backend in [Backend.Pure, Backend.Rust]

      # Estimate must be in a sane range (1.04/sqrt(2^10) ≈ 3.25% RSE; with
      # 100 distinct items, the absolute error is well under 50).
      assert_in_delta HLL.estimate(sketch), 100.0, 50.0
    end

    test "explicit Rust backend works when NIF is available" do
      if Hash.nif_available?() do
        items = for i <- 1..100, do: "item_#{i}"
        sketch = HLL.from_enumerable(items, p: 10, backend: Backend.Rust)
        assert sketch.backend == Backend.Rust
        assert_in_delta HLL.estimate(sketch), 100.0, 50.0
      end
    end

    test "Murmur3 strategy works without the NIF (pure-Elixir fallback)" do
      items = for i <- 1..100, do: "item_#{i}"

      sketch =
        HLL.from_enumerable(items,
          p: 10,
          backend: Backend.Pure,
          hash_strategy: :murmur3
        )

      assert sketch.opts[:hash_strategy] == :murmur3
      assert_in_delta HLL.estimate(sketch), 100.0, 50.0
    end
  end

  describe "checksum file (release packaging precondition)" do
    # The checksum-Elixir.ExDataSketch.Nif.exs file is populated by the
    # `publish_hex` step of .github/workflows/release.yml via
    # `mix rustler_precompiled.download --all --print`. Pre-release the
    # file may be absent (fresh checkouts where no release artifacts exist
    # yet) OR present-but-empty (`%{}`) OR present-and-populated. All
    # three states are valid mid-development; only the release pipeline
    # asserts the file is populated at publish time.
    #
    # We therefore test the WEAKER contract here: "if the file exists,
    # it parses as a map". Strict presence is enforced by Hex at
    # `mix hex.publish` time, not by the unit-test suite.

    @checksum_path Path.join(__DIR__, "../../checksum-Elixir.ExDataSketch.Nif.exs")

    test "if present, the checksum file parses as a map" do
      if File.exists?(@checksum_path) do
        {term, _binding} = @checksum_path |> File.read!() |> Code.eval_string()

        assert is_map(term),
               "checksum file at #{@checksum_path} must contain a map " <>
                 "(empty %{} is allowed pre-release; populated map post-release)"
      else
        # Fresh checkout without any release-published artifacts. The
        # release pipeline regenerates this file before `mix hex.publish`.
        :ok
      end
    end

    test "if populated, every value is a {algorithm, hex_digest} pair" do
      with true <- File.exists?(@checksum_path),
           {term, _binding} when is_map(term) <-
             @checksum_path |> File.read!() |> Code.eval_string() do
        for {key, value} <- term do
          assert is_binary(key), "checksum key must be a binary, got: #{inspect(key)}"

          assert is_binary(value),
                 "checksum value must be a hex digest binary, got: #{inspect(value)}"

          assert String.starts_with?(value, "sha256:") or
                   String.match?(value, ~r/\A[0-9a-f]+\z/),
                 "checksum value must look like a hex digest (with or without sha256: prefix), got: #{inspect(value)}"
        end
      else
        _ -> :ok
      end
    end
  end

  describe "RustlerPrecompiled target matrix declared in nif.ex" do
    test "supported precompiled targets are documented in source" do
      # This is a developer-facing assertion that the targets stay aligned
      # between nif.ex and the release.yml CI matrix. We read the source
      # rather than the loaded module (the targets list is consumed at
      # compile time by the RustlerPrecompiled macro and is not available
      # via reflection).
      source = File.read!(Path.join(__DIR__, "../../lib/ex_data_sketch/nif.ex"))

      expected_targets = [
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-gnu",
        "aarch64-unknown-linux-musl",
        "x86_64-pc-windows-msvc",
        "aarch64-pc-windows-msvc"
      ]

      for target <- expected_targets do
        assert String.contains?(source, target),
               "missing precompiled target #{target} in lib/ex_data_sketch/nif.ex"
      end
    end
  end
end
