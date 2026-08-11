defmodule PhoenixDemoWeb.DashboardMetricsTest do
  use PhoenixDemoWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  @moduledoc """
  Verifies that ExDataSketch telemetry actually renders into
  LiveDashboard's chart HTML, not just that the raw `:telemetry` event
  fires. `Phoenix.LiveDashboard.ChartComponent` uses `temporary_assigns`
  and renders each new data point as `<span data-x=... data-y=...>` inside
  a `phx-hook`-driven container (see its source in the `phoenix_live_dashboard`
  dependency) rather than via `push_event`, so this is directly observable
  through `render/1` -- no browser/JS runtime required.

  Only covers the events this test can trigger deterministically
  (`from_enumerable/2`, `merge_many/1`, `Server.flush/1`) rather than the
  ones that only fire off a real timer (`:visitors`' periodic snapshot,
  its window rolling), which would make this test slow and flaky. See
  `phoenix_demo/README.md`'s "What you'll see, and how soon" for the
  full, manually-verified picture.
  """

  test "chart HTML receives a real data point for ingest, merge, and flush", %{conn: conn} do
    {:ok, view, _html} = live(conn, "/dashboard/metrics?nav=ex_data_sketch")

    nav_name = fn metric ->
      to_string(metric.reporter_options[:nav] || hd(metric.name))
    end

    ex_metrics =
      PhoenixDemoWeb.Telemetry.metrics()
      |> Enum.filter(&(nav_name.(&1) == "ex_data_sketch"))
      |> Enum.with_index()
      |> Map.new(fn {metric, idx} -> {Enum.join(metric.name, "."), idx} end)

    partials = for _ <- 1..3, do: ExDataSketch.HLL.from_enumerable(["a", "b", "c"], p: 10)
    ExDataSketch.HLL.merge_many(partials)
    ExDataSketch.Server.flush(:search_terms)

    # ChartComponent updates asynchronously via Phoenix.LiveView.send_update;
    # give the LiveView process a moment to process them before asserting.
    Process.sleep(200)
    html = render(view)

    for metric_name <- [
          "ex_data_sketch.sketch.ingest.duration",
          "ex_data_sketch.sketch.ingest.size_bytes",
          "ex_data_sketch.sketch.ingest.count",
          "ex_data_sketch.sketch.merge.duration",
          "ex_data_sketch.sketch.merge.merge_count",
          "ex_data_sketch.server.flush.duration"
        ] do
      idx = Map.fetch!(ex_metrics, metric_name)
      assert_chart_has_data(html, idx, metric_name)
    end
  end

  defp assert_chart_has_data(html, idx, metric_name) do
    chart_id = "chart-ex_data_sketch-#{idx}-datasets"

    section =
      case :binary.match(html, "id=\"#{chart_id}\"") do
        {start, _} -> binary_part(html, start, min(150, byte_size(html) - start))
        :nomatch -> ""
      end

    assert section =~ "<span data-x",
           "expected #{metric_name} (chart #{chart_id}) to have received a data point, " <>
             "but its chart div was empty or missing"
  end
end
