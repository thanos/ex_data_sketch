if Code.ensure_loaded?(Phoenix.LiveDashboard.PageBuilder) do
  defmodule ExDataSketch.LiveDashboard.Page do
    @moduledoc """
    A `Phoenix.LiveDashboard.PageBuilder` page listing every ExDataSketch
    telemetry event, its measurements, and the `Telemetry.Metrics` names
    `ExDataSketch.Telemetry.Metrics.all/1` derives from them.

    This page is a static reference, not a live view of any particular
    running sketch -- it has no way to know which `ExDataSketch.Server` or
    `ExDataSketch.Sketches` instances a host application has started. For
    live per-instance estimates, build an application-specific
    LiveDashboard page (or a plain LiveView) that calls
    `ExDataSketch.Server.estimate/2` on the instances your application
    actually runs.

    ## Dependency

    This module only exists when the optional `phoenix_live_dashboard`
    dependency is loaded (`Code.ensure_loaded?/1` gates the whole module,
    matching `ExDataSketch.Storage.Ecto`'s pattern). Add it to your
    application:

        {:phoenix_live_dashboard, "~> 0.8"}

    ## Usage

        live_dashboard "/dashboard",
          additional_pages: [
            sketches: ExDataSketch.LiveDashboard.Page
          ]

    """

    use Phoenix.LiveDashboard.PageBuilder

    alias ExDataSketch.Telemetry.Metrics

    @impl true
    def menu_link(_session, _capabilities) do
      {:ok, "ExDataSketch"}
    end

    @impl true
    def render(assigns) do
      assigns = Map.put(assigns, :rows, metric_rows())

      ~H"""
      <div>
        <p>
          Every ExDataSketch telemetry event and the
          <code>Telemetry.Metrics</code>
          names <code>ExDataSketch.Telemetry.Metrics.all/1</code>
          derives from them. Attach a reporter to these names to chart them
          elsewhere on this dashboard, or in Prometheus/StatsD/etc.
        </p>
        <table>
          <thead>
            <tr>
              <th>Event</th>
              <th>Metric name</th>
              <th>Type</th>
              <th>Tags</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- @rows}>
              <td><code>{row.event}</code></td>
              <td><code>{row.name}</code></td>
              <td>{row.type}</td>
              <td>{row.tags}</td>
            </tr>
          </tbody>
        </table>
      </div>
      """
    end

    defp metric_rows do
      for metric <- Metrics.all() do
        %{
          event: Enum.join(metric.event_name, "."),
          name: Enum.join(metric.name, "."),
          type: metric_type(metric),
          tags: Enum.join(metric.tags, ", ")
        }
      end
    end

    defp metric_type(%Elixir.Telemetry.Metrics.Summary{}), do: "summary"
    defp metric_type(%Elixir.Telemetry.Metrics.Counter{}), do: "counter"
  end
end
