defmodule ExDataSketch.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/yourorg/ex_data_sketch"

  def project do
    [
      app: :ex_data_sketch,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),

      # Docs
      name: "ExDataSketch",
      source_url: @source_url,
      docs: docs(),

      # Test coverage
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test,
        "coveralls.json": :test
      ],

      # Dialyzer
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:mix, :ex_unit]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:stream_data, "~> 1.0", only: [:test], runtime: false},
      {:benchee, "~> 1.0", only: :dev, runtime: false},
      {:benchee_json, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test, runtime: false}
    ]
  end

  defp docs do
    [
      main: "ExDataSketch",
      extras: [
        "guides/quick_start.md",
        "guides/usage_guide.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ],
      groups_for_modules: [
        "Sketch Algorithms": [
          ExDataSketch.HLL,
          ExDataSketch.CMS,
          ExDataSketch.Theta
        ],
        Infrastructure: [
          ExDataSketch.Hash,
          ExDataSketch.Codec,
          ExDataSketch.Backend,
          ExDataSketch.Backend.Pure
        ],
        Errors: [
          ExDataSketch.Errors
        ]
      ]
    ]
  end

  defp aliases do
    [
      lint: ["format --check-formatted", "credo --strict", "dialyzer"],
      bench: ["run bench/hll_bench.exs", "run bench/cms_bench.exs"]
    ]
  end
end
