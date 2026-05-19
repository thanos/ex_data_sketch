# Telemetry Integration Guide

ExDataSketch emits structured telemetry events at meaningful operation
boundaries, enabling production observability without impacting hot-path
performance.

## Why Telemetry Matters

Individual `update/2` calls can run at billions per second -- emitting events
for each would cripple throughput. Instead, ExDataSketch emits events at
**compound operation boundaries**:

- `from_enumerable/2` -- batch ingestion
- `merge_many/1` -- bulk merge
- `serialize/1` / `deserialize/1` -- serialization (HLL only in this release)
- Storage operations -- save, load, merge, delete
- Stream operations -- partition merge

## Configuration

Telemetry is enabled by default. Disable globally:

    config :ex_data_sketch, telemetry_enabled: false

Disable specific categories:

    config :ex_data_sketch, telemetry: [
      sketch: true,
      persistence: true,
      stream: true,
      pipeline: false
    ]

## Event Reference

### Sketch Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :sketch, :ingest]` | `duration`, `size_bytes` (HLL only) | `sketch_type` |
| `[:ex_data_sketch, :sketch, :merge]` | `duration`, `merge_count` | `sketch_type` |
| `[:ex_data_sketch, :sketch, :serialize]` | `duration`, `size_bytes` | `sketch_type` |
| `[:ex_data_sketch, :sketch, :deserialize]` | `duration`, `size_bytes` | `sketch_type` |

> **Note:** The `:ingest` event's `size_bytes` measurement is only available
> for HLL. All other sketch types emit `%{duration}` only. This is because
> `from_enumerable/2` consumes a lazy stream and the item count is not
> available without forcing evaluation.

### Persistence Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :persistence, :save]` | `duration`, `size_bytes` | `sketch_type`, `backend`, `key` |
| `[:ex_data_sketch, :persistence, :load]` | `duration` | `sketch_type`, `backend`, `key` |
| `[:ex_data_sketch, :persistence, :merge]` | `duration` | `sketch_type`, `backend`, `key` |
| `[:ex_data_sketch, :persistence, :delete]` | `duration` | `backend`, `key` |

> **Note:** The `:delete` event does not include `sketch_type` because the
> sketch struct is no longer available at deletion time. The `:load` event
> does not include `size_bytes` because the binary size is only known after
> deserialization completes.

### Stream Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :stream, :reduce]` | (none) | `sketch_type` |
| `[:ex_data_sketch, :stream, :partition_merge]` | `duration`, `partition_count` | `sketch_type` |

> **Note:** The `:reduce` event is a completion signal emitted from
> `Flow.on_trigger/2`. Because the reduce runs inside the Flow runtime, no
> timing measurement is available.

### Pipeline Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :pipeline, :accumulate]` | `duration`, `count` | `sketch_type`, `batch_size` |
| `[:ex_data_sketch, :pipeline, :periodic_flush]` | `duration` | `sketch_type` |

> **Note:** The `:periodic_flush` `duration` measures time since the
> previous flush (or process start), not the time taken to perform the flush
> itself.

### All Event Names

To get a list of all event names programmatically:

    ExDataSketch.Telemetry.all_event_names()
    # => [[:ex_data_sketch, :sketch, :ingest],
    #     [:ex_data_sketch, :sketch, :merge],
    #     [:ex_data_sketch, :sketch, :serialize],
    #     [:ex_data_sketch, :sketch, :deserialize],
    #     [:ex_data_sketch, :persistence, :save],
    #     [:ex_data_sketch, :persistence, :load],
    #     [:ex_data_sketch, :persistence, :merge],
    #     [:ex_data_sketch, :persistence, :delete],
    #     [:ex_data_sketch, :stream, :reduce],
    #     [:ex_data_sketch, :stream, :partition_merge],
    #     [:ex_data_sketch, :pipeline, :accumulate],
    #     [:ex_data_sketch, :pipeline, :periodic_flush]]

## Attaching Handlers

Use `:telemetry.attach/4` to listen for events:

    :telemetry.attach("my-handler", [:ex_data_sketch, :sketch, :ingest], fn _name, measurements, metadata, _config ->
      Logger.info("Ingested \#{metadata.sketch_type}: \#{measurements.size_bytes} bytes in \#{measurements.duration} ns")
    end, nil)

## Measurement Details

All `duration` measurements use native time units (as returned by
`System.monotonic_time/0`). Convert to milliseconds with:

    System.convert_time_unit(duration, :native, :millisecond)

The `sketch_type` metadata field uses atoms: `:hll`, `:cms`, `:theta`, `:ull`,
`:kll`, `:ddsketch`, `:req`, `:frequent_items`, `:misra_gries`, `:bloom`,
`:quotient`, `:cqf`, `:iblt`, `:cuckoo`, `:xor_filter`, `:filter_chain`.

The `backend` metadata field uses atoms: `:ets`, `:dets`, `:cubdb`, `:mnesia`,
`:ecto`.

## OpenTelemetry Integration

When the `:opentelemetry_api` dependency is available, bridge telemetry events
to OTEL spans:

    ExDataSketch.Telemetry.OpenTelemetry.setup()

This attaches handlers that create OpenTelemetry spans for each ExDataSketch
telemetry event. Call this in your application's `start/2` callback.

To disable:

    config :ex_data_sketch, :integrations, opentelemetry: false