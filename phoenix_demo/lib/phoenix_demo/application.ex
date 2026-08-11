defmodule PhoenixDemo.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      PhoenixDemoWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:phoenix_demo, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: PhoenixDemo.PubSub},
      # Two ExDataSketch.Server instances backing the /sketches demo page.
      # See PhoenixDemoWeb.SketchLive for what drives their updates, and
      # guides/supervised_sketches.md for what these options mean.
      # Must start before :visitors_server, which loads from it on init.
      PhoenixDemo.SnapshotTable,
      Supervisor.child_spec(
        {ExDataSketch.Server,
         name: :visitors,
         sketch: :hll,
         sketch_opts: [p: 14],
         window: [every: :timer.seconds(5), keep: 6, track_all_time: true],
         snapshot: [
           to: {ExDataSketch.Storage.ETS, :phoenix_demo_snapshots, "visitors"},
           every: :timer.seconds(8)
         ]},
        id: :visitors_server
      ),
      Supervisor.child_spec(
        {ExDataSketch.Server,
         name: :search_terms,
         sketch: :misra_gries,
         sketch_opts: [k: 20],
         flush: [interval: :timer.seconds(20)]},
        id: :search_terms_server
      ),
      PhoenixDemo.TrafficSimulator,
      # Start to serve requests, typically the last entry
      PhoenixDemoWeb.Endpoint
    ]

    # See https://elixir.hexdocs.pm/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: PhoenixDemo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    PhoenixDemoWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
