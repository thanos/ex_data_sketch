defmodule ExDataSketch.MixProject do
  use Mix.Project

  @version "0.9.0"
  @source_url "https://github.com/thanos/ex_data_sketch"

  def project do
    [
      app: :ex_data_sketch,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      package: package(),
      description:
        "Production-grade streaming data sketching algorithms (HLL, ULL, CMS, Theta, KLL, DDSketch, REQ, FrequentItems, MisraGries, Bloom, Cuckoo, Quotient, CQF, XorFilter, IBLT, FilterChain) with optional Rust NIF acceleration and XXHash3.",

      # Docs
      name: "ExDataSketch",
      source_url: @source_url,
      docs: docs(),

      # Test coverage
      test_coverage: [tool: ExCoveralls],

      # Dialyzer
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:mix, :ex_unit]
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "test.nif_on": :test,
        "test.nif_off": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:rustler, "~> 0.36", optional: true, runtime: false},
      {:rustler_precompiled, "~> 0.8"},
      {:stream_data, "~> 1.0", only: [:test], runtime: false},
      {:benchee, "~> 1.0", only: :dev, runtime: false},
      {:benchee_json, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:jason, "~> 1.4"},
      {:excoveralls, "~> 0.18", only: :test, runtime: false},
      {:mox, "~> 1.0", only: :test},
      {:ex_slop, "~> 0.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      description: "Production-grade streaming data sketching algorithms for Elixir.",
      licenses: ["MIT"],
      maintainers: ["Thanos Vassilakis"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: [
        "lib",
        "native/ex_data_sketch_nif/src",
        "native/ex_data_sketch_nif/Cargo.toml",
        "native/ex_data_sketch_nif/Cargo.lock",
        "native/ex_data_sketch_nif/.cargo",
        "checksum-Elixir.ExDataSketch.Nif.exs",
        "guides",
        "LICENSE",
        "README.md",
        "CHANGELOG.md",
        "mix.exs",
        ".formatter.exs"
      ]
    ]
  end

  defp docs do
    [
      main: "ExDataSketch",
      extras: [
        "guides/quick_start.md",
        "guides/usage_guide.md",
        "guides/integrations.md",
        "guides/streaming_sketches.md",
        "guides/hash_strategies.md",
        "guides/hll_performance.md",
        "guides/precompiled_nifs.md",
        "guides/serialization_compatibility.md",
        "guides/v0.8.0_migration_notes.md",
        "guides/v0.8.0_architecture.md",
        "guides/roadmap.md",
        "docs/frequent_items_format.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ],
      groups_for_modules: [
        "Sketch Algorithms": [
          ExDataSketch.HLL,
          ExDataSketch.CMS,
          ExDataSketch.Theta,
          ExDataSketch.KLL,
          ExDataSketch.DDSketch,
          ExDataSketch.FrequentItems,
          ExDataSketch.Bloom,
          ExDataSketch.Cuckoo,
          ExDataSketch.Quotient,
          ExDataSketch.CQF,
          ExDataSketch.XorFilter,
          ExDataSketch.IBLT,
          ExDataSketch.FilterChain,
          ExDataSketch.REQ,
          ExDataSketch.MisraGries,
          ExDataSketch.ULL,
          ExDataSketch.Quantiles
        ],
        "Stream Integration": [
          ExDataSketch.Stream
        ],
        Infrastructure: [
          ExDataSketch.Hash,
          ExDataSketch.Codec,
          ExDataSketch.Backend,
          ExDataSketch.Backend.Pure,
          ExDataSketch.Backend.Rust
        ],
        Errors: [
          ExDataSketch.Errors
        ]
      ]
    ]
  end

  defp aliases do
    [
      lint: [
        "format --check-formatted",
        "credo --strict",
        "dialyzer"
      ],
      bench: [
        "run bench/hll_bench.exs",
        "run bench/cms_bench.exs",
        "run bench/theta_bench.exs",
        "run bench/kll_bench.exs",
        "run bench/ddsketch_bench.exs",
        "run bench/frequent_items_bench.exs",
        "run bench/bloom_bench.exs",
        "run bench/cuckoo_bench.exs",
        "run bench/quotient_bench.exs",
        "run bench/cqf_bench.exs",
        "run bench/xor_filter_bench.exs",
        "run bench/iblt_bench.exs",
        "run bench/filter_chain_bench.exs",
        "run bench/req_bench.exs",
        "run bench/misra_gries_bench.exs",
        "run bench/ull_bench.exs",
        "run bench/xxhash3_bench.exs",
        "run bench/stream_ingestion_bench.exs"
      ],
      # Switching between NIF-on and NIF-off modes locally requires cleaning
      # rustler_precompiled's per-env compiled config (which captures the
      # value of EX_DATA_SKETCH_BUILD at compile time). The two aliases
      # below do that automatically so maintainers can flip modes without
      # remembering the underlying incantation. CI sets the env once per
      # job and does not flip modes mid-job, so it does not need them.
      "test.nif_on": [&clean_precompiled_marker/1, "test"],
      "test.nif_off": [&clean_precompiled_marker/1, "test"],
      verify: &verify/1
    ]
  end

  # Invalidate the per-env rustler_precompiled build artifact so the
  # compile-time vs runtime force_build check does not trip when toggling
  # EX_DATA_SKETCH_BUILD between runs. Safe to call unconditionally.
  defp clean_precompiled_marker(_args) do
    Mix.shell().info([
      :bright,
      "==> cleaning rustler_precompiled to allow NIF mode switch",
      :reset
    ])

    Mix.Task.run("deps.clean", ["rustler_precompiled", "--build"])
  end

  defp verify(_) do
    steps = [
      {"compile --warnings-as-errors", :dev},
      {"format --check-formatted", :dev},
      {"credo --strict", :dev},
      {"dialyzer", :dev},
      {"test --cover", :test},
      {"docs --warnings-as-errors", :dev}
    ]

    Enum.each(steps, fn {task, env} ->
      Mix.shell().info([:bright, "==> mix #{task}", :reset])

      {_, exit_code} =
        System.cmd("mix", String.split(task),
          env: [{"MIX_ENV", to_string(env)}],
          into: IO.stream()
        )

      if exit_code != 0 do
        Mix.raise("mix #{task} failed (exit code #{exit_code})")
      end
    end)

    Mix.shell().info([:green, :bright, "\nAll verification checks passed!", :reset])
  end
end
