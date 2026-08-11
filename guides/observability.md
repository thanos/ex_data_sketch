# Observability Guide

ExDataSketch provides multiple observability surfaces for production systems:
structured telemetry events, OpenTelemetry integration, and storage-layer
metrics.

## Observability Architecture

ExDataSketch follows the BEAM telemetry standard. Events are emitted at
compound operation boundaries (not per-item), minimizing overhead while
providing actionable production metrics.

Three layers of observability:

1. **Telemetry events** -- structured events with measurements and metadata
2. **OpenTelemetry spans** -- optional bridge to distributed tracing
3. **Storage metrics** -- persistence-layer instrumentation (ETS, DETS, etc.)

## Telemetry.Metrics

`ExDataSketch.Telemetry.Metrics.all/1` returns a ready-made
`Telemetry.Metrics` definition -- a `summary` for every numeric
measurement and a `counter` for occurrence-only events -- for every event
`ExDataSketch.Telemetry.all_event_names/0` lists, so wiring the library
into any `:telemetry_metrics`-based reporter (Prometheus, StatsD,
`Telemetry.Metrics.ConsoleReporter`, or Phoenix LiveDashboard's own
metrics page) is one call instead of hand-writing a metric per event:

    # In your application's telemetry.ex:
    def metrics do
      ExDataSketch.Telemetry.Metrics.all() ++ [
        # ... your application's own metrics
      ]
    end

Pass `prefix:` if you're running more than one ExDataSketch-backed
component and need distinct metric namespaces in a shared reporter:

    ExDataSketch.Telemetry.Metrics.all(prefix: "my_app")

See `ExDataSketch.Telemetry.Metrics`'s moduledoc for what is deliberately
left out (high-cardinality metadata like `batch_size`, measurements that
aren't in the `:telemetry.execute/3` measurements map) and why.

## LiveDashboard

`ExDataSketch.LiveDashboard.Page` (requires the optional
`phoenix_live_dashboard` dependency) adds a page listing every
ExDataSketch telemetry event alongside the `Telemetry.Metrics` name(s)
`ExDataSketch.Telemetry.Metrics.all/1` derives from it -- a static reference for
wiring up a reporter, not a live view of any particular running sketch
(it has no way to know which `ExDataSketch.Server`/`ExDataSketch.Sketches`
instances your application started):

    live_dashboard "/dashboard",
      additional_pages: [
        sketches: ExDataSketch.LiveDashboard.Page
      ]

For live per-instance estimates (not just event/metric reference), build
an application-specific LiveDashboard page or LiveView that calls
`ExDataSketch.Server.estimate/2` on the instances your application
actually runs -- see `guides/supervised_sketches.md`.

## Production Checklist

- [ ] Attach telemetry handlers for ingest, merge, and persistence events
- [ ] Configure `telemetry_enabled: true` (default)
- [ ] Set up alerting on high-latency merge or persistence operations
- [ ] Monitor `size_bytes` trends to detect memory pressure
- [ ] Enable OpenTelemetry spans for distributed tracing correlation
- [ ] Use LiveDashboard for real-time ad-hoc investigation

## Event Aggregation Patterns

ExDataSketch events are designed for aggregation:

    # Ingest rate by sketch type (events/second)
    :telemetry.attach_many("ingest-rate",
      [[:ex_data_sketch, :sketch, :ingest]],
      fn _name, _measurements, metadata, _config ->
        :counters.update(:ingest_counters, metadata.sketch_type, 1)
      end,
      nil
    )

    # P99 merge latency
    :telemetry.attach("merge-p99",
      [:ex_data_sketch, :sketch, :merge],
      fn _name, measurements, metadata, _config ->
        latency_us = System.convert_time_unit(measurements.duration, :native, :microsecond)
        :histogram_record(:merge_latencies, metadata.sketch_type, latency_us)
      end,
      nil
    )