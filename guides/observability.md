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

## LiveDashboard

ExDataSketch telemetry events integrate directly with Phoenix LiveDashboard.
When LiveDashboard is configured, you can see real-time sketch ingest rates,
merge latencies, and persistence operation durations.

### Attaching a LiveDashboard Handler

    # In your application supervisor
    :telemetry.attach(
      "exds-live_dashboard-handler",
      [:ex_data_sketch, :sketch, :ingest],
      fn _name, measurements, metadata, _config ->
        Phoenix.LiveDashboard.push_event("ex_data_sketch_ingest", %{
          sketch_type: metadata.sketch_type,
          duration_ms: System.convert_time_unit(measurements.duration, :native, :millisecond)
        })
      end,
      nil
    )

## Grafana Dashboards

ExDataSketch events can be forwarded to Grafana via the
`telemetry_metrics` ecosystem:

    # In your application's `start/2`:
    Telemetry.Metrics.summary("ex_data_sketch.sketch.ingest.duration",
      description: "Sketch ingestion duration",
      unit: {:native, :millisecond},
      tags: [:sketch_type]
    )

    Telemetry.Metrics.last_value("ex_data_sketch.persistence.save.size_bytes",
      description: "Serialized sketch size",
      tags: [:sketch_type, :backend]
    )

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