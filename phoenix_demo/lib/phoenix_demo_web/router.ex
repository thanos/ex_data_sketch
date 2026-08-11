defmodule PhoenixDemoWeb.Router do
  use PhoenixDemoWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {PhoenixDemoWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", PhoenixDemoWeb do
    pipe_through :browser

    live "/", SketchLive
  end

  # Other scopes may use custom stacks.
  # scope "/api", PhoenixDemoWeb do
  #   pipe_through :api
  # end

  # Enable LiveDashboard in development. Mounts both the standard
  # LiveDashboard metrics page (fed by PhoenixDemoWeb.Telemetry.metrics/0,
  # which includes ExDataSketch.Telemetry.Metrics.all/1) and
  # ExDataSketch.LiveDashboard.Page as an additional page listing every
  # ExDataSketch telemetry event and the metric names derived from it.
  if Application.compile_env(:phoenix_demo, :dev_routes) do
    import Phoenix.LiveDashboard.Router

    scope "/" do
      pipe_through :browser

      live_dashboard "/dashboard",
        metrics: PhoenixDemoWeb.Telemetry,
        additional_pages: [
          sketches: ExDataSketch.LiveDashboard.Page
        ]
    end
  end
end
