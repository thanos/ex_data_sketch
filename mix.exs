defmodule ExDataSketch.MixProject do
  use Mix.Project

  @version "0.10.1"
  @source_url "https://github.com/thanos/ex_data_sketch"

  def project do
    [
      app: :ex_data_sketch,
      version: @version,
      elixir: "~> 1.18",
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
        plt_add_apps: [
          :mix,
          :ex_unit,
          :mnesia,
          :cubdb,
          :ecto_sql,
          :telemetry_metrics,
          :phoenix_live_dashboard
        ]
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
    extra_apps = [:logger]

    extra_apps =
      if Mix.env() in [:test, :dev] do
        [:mnesia | extra_apps]
      else
        extra_apps
      end

    [extra_applications: extra_apps]
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
      {:ex_slop, "~> 0.1", only: [:dev, :test], runtime: false},
      {:telemetry, "~> 1.0"},
      {:telemetry_metrics, "~> 1.0"},
      {:opentelemetry_api, "~> 1.0", optional: true},
      {:phoenix_live_dashboard, "~> 0.8", optional: true},
      {:broadway, "~> 1.0", optional: true},
      {:flow, "~> 1.2", optional: true},
      {:gen_stage, "~> 1.0", optional: true},
      {:cubdb, "~> 2.0", optional: true},
      {:ecto_sql, "~> 3.0", optional: true}
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
        # -- Getting Started --
        "guides/quick_start.md",
        "guides/usage_guide.md",
        "guides/integrations.md",
        "guides/livebooks.md",

        # -- Sketch Family Tutorials (grouped by task below) --
        {"livebooks/sketches/hll.livemd", [filename: "livebook_hll"]},
        {"livebooks/sketches/ull.livemd", [filename: "livebook_ull"]},
        {"livebooks/sketches/cms.livemd", [filename: "livebook_cms"]},
        {"livebooks/sketches/frequent_items.livemd", [filename: "livebook_frequent_items"]},
        {"livebooks/sketches/misra_gries.livemd", [filename: "livebook_misra_gries"]},
        {"livebooks/sketches/kll.livemd", [filename: "livebook_kll"]},
        {"livebooks/sketches/ddsketch.livemd", [filename: "livebook_ddsketch"]},
        {"livebooks/sketches/req.livemd", [filename: "livebook_req"]},
        {"livebooks/sketches/theta.livemd", [filename: "livebook_theta"]},
        {"livebooks/sketches/bloom.livemd", [filename: "livebook_bloom"]},
        {"livebooks/sketches/cuckoo.livemd", [filename: "livebook_cuckoo"]},
        {"livebooks/sketches/quotient.livemd", [filename: "livebook_quotient"]},
        {"livebooks/sketches/cqf.livemd", [filename: "livebook_cqf"]},
        {"livebooks/sketches/xor_filter.livemd", [filename: "livebook_xor_filter"]},
        {"livebooks/sketches/filter_chain.livemd", [filename: "livebook_filter_chain"]},
        {"livebooks/sketches/iblt.livemd", [filename: "livebook_iblt"]},

        # -- Core Concepts --
        "guides/aggregation_wall.md",
        "guides/distributed_merge_semantics.md",
        "guides/hash_strategies.md",

        # -- Production & Operations --
        "guides/persistence.md",
        {"livebooks/persistence_snapshots.livemd", [filename: "livebook_persistence_snapshots"]},
        "guides/windowing.md",
        "guides/supervised_sketches.md",
        "guides/telemetry.md",
        "guides/observability.md",
        "guides/precompiled_nifs.md",

        # -- Framework Integrations (guide + matching livebook, paired) --
        "guides/streaming_sketches.md",
        {"livebooks/streaming_cardinality.livemd", [filename: "livebook_streaming_cardinality"]},
        "guides/broadway_integration.md",
        {"livebooks/broadway_integration.livemd",
         [filename: "livebook_broadway_integration", title: "Broadway Integration (Livebook)"]},
        "guides/genstage_integration.md",
        {"livebooks/genstage_aggregation.livemd", [filename: "livebook_genstage_aggregation"]},
        "guides/flow_integration.md",
        {"livebooks/distributed_merges.livemd", [filename: "livebook_distributed_merges"]},

        # -- Case Studies --
        {"livebooks/ai_token_analytics.livemd", [filename: "livebook_ai_token_analytics"]},

        # -- Interop & Compatibility --
        "guides/apache_interop.md",
        "guides/serialization_compatibility.md",
        "docs/frequent_items_format.md",

        # -- Project History & Internals --
        "guides/hll_performance.md",
        "guides/filter_performance.md",
        "guides/v0.8.0_architecture.md",
        "guides/v0.8.0_migration_notes.md",
        "guides/roadmap.md",

        # -- Changelog (kept ungrouped, prominent) --
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        "Getting Started": [
          "guides/quick_start.md",
          "guides/usage_guide.md",
          "guides/integrations.md",
          "guides/livebooks.md"
        ],
        "Tutorials: Cardinality": [
          "livebooks/sketches/hll.livemd",
          "livebooks/sketches/ull.livemd"
        ],
        "Tutorials: Frequency & Heavy Hitters": [
          "livebooks/sketches/cms.livemd",
          "livebooks/sketches/frequent_items.livemd",
          "livebooks/sketches/misra_gries.livemd"
        ],
        "Tutorials: Quantiles": [
          "livebooks/sketches/kll.livemd",
          "livebooks/sketches/ddsketch.livemd",
          "livebooks/sketches/req.livemd"
        ],
        "Tutorials: Set Operations": [
          "livebooks/sketches/theta.livemd"
        ],
        "Tutorials: Membership Filters": [
          "livebooks/sketches/bloom.livemd",
          "livebooks/sketches/cuckoo.livemd",
          "livebooks/sketches/quotient.livemd",
          "livebooks/sketches/cqf.livemd",
          "livebooks/sketches/xor_filter.livemd",
          "livebooks/sketches/filter_chain.livemd"
        ],
        "Tutorials: Set Reconciliation": [
          "livebooks/sketches/iblt.livemd"
        ],
        "Core Concepts": [
          "guides/aggregation_wall.md",
          "guides/distributed_merge_semantics.md",
          "guides/hash_strategies.md"
        ],
        "Production & Operations": [
          "guides/persistence.md",
          "livebooks/persistence_snapshots.livemd",
          "guides/windowing.md",
          "guides/supervised_sketches.md",
          "guides/telemetry.md",
          "guides/observability.md",
          "guides/precompiled_nifs.md"
        ],
        "Framework Integrations": [
          "guides/streaming_sketches.md",
          "livebooks/streaming_cardinality.livemd",
          "guides/broadway_integration.md",
          "livebooks/broadway_integration.livemd",
          "guides/genstage_integration.md",
          "livebooks/genstage_aggregation.livemd",
          "guides/flow_integration.md",
          "livebooks/distributed_merges.livemd"
        ],
        "Case Studies": [
          "livebooks/ai_token_analytics.livemd"
        ],
        "Interop & Compatibility": [
          "guides/apache_interop.md",
          "guides/serialization_compatibility.md",
          "docs/frequent_items_format.md"
        ],
        "Project History & Internals": [
          "guides/hll_performance.md",
          "guides/filter_performance.md",
          "guides/v0.8.0_architecture.md",
          "guides/v0.8.0_migration_notes.md",
          "guides/roadmap.md"
        ]
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
        "Dataflow Integration": [
          ExDataSketch.Broadway,
          ExDataSketch.Broadway.PeriodicAggregator,
          ExDataSketch.GenStage,
          ExDataSketch.GenStage.SketchConsumer,
          ExDataSketch.GenStage.SketchProducer,
          ExDataSketch.GenStage.SketchStage,
          ExDataSketch.Flow
        ],
        Persistence: [
          ExDataSketch.Storage,
          ExDataSketch.Storage.ETS,
          ExDataSketch.Storage.DETS,
          ExDataSketch.Storage.CubDB,
          ExDataSketch.Storage.Mnesia,
          ExDataSketch.Storage.Ecto,
          ExDataSketch.Storage.Ecto.Schema,
          ExDataSketch.Storage.Ecto.Migration
        ],
        Windowing: [
          ExDataSketch.Window
        ],
        Supervision: [
          ExDataSketch.Server,
          ExDataSketch.Sketches
        ],
        Infrastructure: [
          ExDataSketch.Sketch,
          ExDataSketch.Hash,
          ExDataSketch.Codec,
          ExDataSketch.Backend,
          ExDataSketch.Backend.Pure,
          ExDataSketch.Backend.Rust,
          ExDataSketch.Telemetry,
          ExDataSketch.Telemetry.Metrics,
          ExDataSketch.Telemetry.OpenTelemetry
        ],
        Dashboard: [
          ExDataSketch.LiveDashboard.Page
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
