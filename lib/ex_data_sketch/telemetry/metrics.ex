defmodule ExDataSketch.Telemetry.Metrics do
  @moduledoc """
  Ready-made `Telemetry.Metrics` definitions for every ExDataSketch
  telemetry event.

  Pass `all/1`'s result straight to a `Telemetry.Metrics` reporter (the
  `Telemetry.Metrics.ConsoleReporter` built into the `:telemetry_metrics`
  package, `TelemetryMetricsPrometheus`, `TelemetryMetricsStatsd`, or
  Phoenix LiveDashboard's own metrics page) instead of hand-writing a
  metric per event.

  ## Usage

      # In a Phoenix application's telemetry.ex:
      def metrics do
        ExDataSketch.Telemetry.Metrics.all() ++ [
          # ... your application's own metrics
        ]
      end

  See `ExDataSketch.Telemetry`'s moduledoc for the full event/measurement/
  metadata table this module's metric list is derived from.

  ## Coverage

  Every event `ExDataSketch.Telemetry.all_event_names/0` returns has at
  least one metric here: a `summary` for each numeric measurement it
  carries, or a `counter` (an event-occurrence count) when it carries
  none. `[:ex_data_sketch, :stream, :reduce]` is the only event with no
  measurements at all, so it is the only event that gets a counter purely
  for coverage; `[:ex_data_sketch, :sketch, :ingest]` additionally gets a
  counter alongside its duration/size_bytes summaries as a call-volume
  metric for the library's most common operation.

  ## What is deliberately not a metric here

  - `pipeline.accumulate`'s `batch_size` metadata is not a tag on any
    metric -- it is a per-call variable integer, and tagging by it would
    explode cardinality in a real reporter (StatsD, Prometheus). Build a
    custom metric with `Telemetry.Metrics.summary/2` directly if you need
    it.
  - `window.roll`'s `oldest_age_ms` is metadata describing the dropped
    slot's age, not a measurement in the `:telemetry.execute/3`
    measurements map, so no `Telemetry.Metrics` definition can read it.
  - `persistence.delete` has no `sketch_type` tag: the sketch struct is
    already discarded by the time that event fires (see
    `ExDataSketch.Telemetry`'s moduledoc).
  """

  import Telemetry.Metrics

  @typedoc "Options accepted by `all/1`."
  @type opts :: [prefix: String.t()]

  @doc """
  Returns a `Telemetry.Metrics` definition for every event
  `ExDataSketch.Telemetry.all_event_names/0` returns.

  ## Options

  - `:prefix` -- the dot-separated namespace prepended to every metric
    name (default: `"ex_data_sketch"`). Useful when running more than one
    ExDataSketch-backed component that needs distinct metric namespaces in
    a shared reporter. Only the metric's display name changes -- the
    underlying `:telemetry` event listened to is always the real,
    unprefixed `[:ex_data_sketch, ...]` event name, so metrics still fire
    regardless of this option.

  ## Examples

      iex> metrics = ExDataSketch.Telemetry.Metrics.all()
      iex> length(metrics) > 0
      true
      iex> Enum.all?(metrics, fn m -> match?(%Telemetry.Metrics.Summary{}, m) or match?(%Telemetry.Metrics.Counter{}, m) end)
      true

      iex> metrics = ExDataSketch.Telemetry.Metrics.all(prefix: "my_app")
      iex> [metric | _] = metrics
      iex> String.starts_with?(metric.name |> Enum.join("."), "my_app.")
      true

  """
  @spec all(opts()) :: [Telemetry.Metrics.t()]
  def all(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "ex_data_sketch")

    sketch_metrics(prefix) ++
      persistence_metrics(prefix) ++
      stream_metrics(prefix) ++
      pipeline_metrics(prefix) ++
      window_metrics(prefix) ++
      server_metrics(prefix)
  end

  defp sketch_metrics(prefix) do
    [
      duration_summary(prefix, :sketch, :ingest, [:sketch_type]),
      size_bytes_summary(prefix, :sketch, :ingest, [:sketch_type]),
      event_counter(prefix, :sketch, :ingest, [:sketch_type]),
      duration_summary(prefix, :sketch, :merge, [:sketch_type]),
      value_summary(prefix, :sketch, :merge, :merge_count, [:sketch_type]),
      duration_summary(prefix, :sketch, :serialize, [:sketch_type]),
      size_bytes_summary(prefix, :sketch, :serialize, [:sketch_type]),
      duration_summary(prefix, :sketch, :deserialize, [:sketch_type]),
      size_bytes_summary(prefix, :sketch, :deserialize, [:sketch_type])
    ]
  end

  defp persistence_metrics(prefix) do
    [
      duration_summary(prefix, :persistence, :save, [:sketch_type, :backend]),
      size_bytes_summary(prefix, :persistence, :save, [:sketch_type, :backend]),
      duration_summary(prefix, :persistence, :load, [:sketch_type, :backend]),
      duration_summary(prefix, :persistence, :merge, [:sketch_type, :backend]),
      duration_summary(prefix, :persistence, :delete, [:backend])
    ]
  end

  defp stream_metrics(prefix) do
    [
      event_counter(prefix, :stream, :reduce, [:sketch_type]),
      duration_summary(prefix, :stream, :partition_merge, [:sketch_type]),
      value_summary(prefix, :stream, :partition_merge, :partition_count, [:sketch_type])
    ]
  end

  defp pipeline_metrics(prefix) do
    [
      duration_summary(prefix, :pipeline, :accumulate, [:sketch_type]),
      value_summary(prefix, :pipeline, :accumulate, :count, [:sketch_type]),
      duration_summary(prefix, :pipeline, :periodic_flush, [:sketch_type])
    ]
  end

  defp window_metrics(prefix) do
    [
      value_summary(prefix, :window, :roll, :slot_count, [:sketch_type]),
      value_summary(prefix, :window, :roll, :dropped_count, [:sketch_type])
    ]
  end

  defp server_metrics(prefix) do
    [
      duration_summary(prefix, :server, :snapshot, [:sketch_type, :backend]),
      size_bytes_summary(prefix, :server, :snapshot, [:sketch_type, :backend]),
      duration_summary(prefix, :server, :restore, [:sketch_type, :backend, :found]),
      duration_summary(prefix, :server, :flush, [:sketch_type]),
      value_summary(prefix, :server, :drop, :queue_len, [:sketch_type])
    ]
  end

  defp duration_summary(prefix, category, action, tags) do
    summary(metric_name(prefix, category, action, :duration),
      event_name: event_name(category, action),
      measurement: :duration,
      unit: {:native, :millisecond},
      tags: tags
    )
  end

  defp size_bytes_summary(prefix, category, action, tags) do
    summary(metric_name(prefix, category, action, :size_bytes),
      event_name: event_name(category, action),
      measurement: :size_bytes,
      unit: :byte,
      tags: tags
    )
  end

  defp value_summary(prefix, category, action, measurement, tags) do
    summary(metric_name(prefix, category, action, measurement),
      event_name: event_name(category, action),
      measurement: measurement,
      tags: tags
    )
  end

  defp event_counter(prefix, category, action, tags) do
    counter(metric_name(prefix, category, action, :count),
      event_name: event_name(category, action),
      tags: tags
    )
  end

  defp event_name(category, action), do: [:ex_data_sketch, category, action]

  defp metric_name(prefix, category, action, measurement) do
    Enum.join([prefix, category, action, measurement], ".")
  end
end
