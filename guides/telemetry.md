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
- `serialize/1` / `deserialize/1` -- serialization
- Storage operations -- save, load, merge, delete
- Stream operations -- partition merge
- `ExDataSketch.Window` rolling a slot out of its `keep` window
- `ExDataSketch.Server` snapshotting, restoring, flushing, and dropping
  updates under backpressure -- see `guides/supervised_sketches.md`

## Configuration

Telemetry is enabled by default. Disable globally:

    config :ex_data_sketch, telemetry_enabled: false

Disable specific categories:

    config :ex_data_sketch, telemetry: [
      sketch: true,
      persistence: true,
      stream: true,
      pipeline: true,
      window: true,
      server: false
    ]

## Event Reference

### Sketch Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :sketch, :ingest]` | `duration`, `size_bytes` | `sketch_type` |
| `[:ex_data_sketch, :sketch, :merge]` | `duration`, `merge_count` | `sketch_type` |
| `[:ex_data_sketch, :sketch, :serialize]` | `duration`, `size_bytes` | `sketch_type` |
| `[:ex_data_sketch, :sketch, :deserialize]` | `duration`, `size_bytes` | `sketch_type` |

> **Note:** `:ingest` fires from `from_enumerable/2`, which
> `ExDataSketch.XorFilter` (built via `build/2`, a one-shot immutable
> construction with no telemetry wrapper) and `ExDataSketch.FilterChain`
> (no `from_enumerable/2` of its own -- it wraps already-built
> sub-sketches) don't have, so neither ever emits this event. Of the 14
> families that do, all report `size_bytes` alongside `duration` except
> `ExDataSketch.Cuckoo` (its `put_many/2` returns `{:ok, sketch} |
> {:error, :full, sketch}`, not a bare sketch, so its `:ingest` wrapper
> only reports `duration`).

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

### Window Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :window, :roll]` | `slot_count`, `dropped_count` | `sketch_type`, `oldest_age_ms` |

> **Note:** Emitted by `ExDataSketch.Window.update/2,3`, `update_many/2`,
> and `tick/2` whenever at least one slot ages out of the `keep` window.
> Not emitted by `estimate/1`, `merged/1`, or `slots/1`, which filter
> expired slots transiently for the read without mutating or persisting
> the window's stored state. See `guides/windowing.md`.

### Server Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:ex_data_sketch, :server, :snapshot]` | `duration`, `size_bytes` | `sketch_type`, `backend`, `key` |
| `[:ex_data_sketch, :server, :snapshot_failed]` | `duration` | `sketch_type`, `backend`, `key`, `reason` |
| `[:ex_data_sketch, :server, :restore]` | `duration` | `sketch_type`, `backend`, `key`, `found` |
| `[:ex_data_sketch, :server, :flush]` | `duration` | `sketch_type` |
| `[:ex_data_sketch, :server, :drop]` | `queue_len` | `sketch_type` |

> **Note:** `:restore` fires once, when `ExDataSketch.Server` starts with
> `:snapshot` configured -- `found` is `false` both when no snapshot
> exists yet and when loading one failed for any other reason. `:drop`
> fires when `:max_queue` is configured and exceeded, once per dropped
> `update/2`/`update_many/2` call (never for `update_sync/2`, which is
> never subject to `:max_queue`). See `guides/supervised_sketches.md`.

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
    #     [:ex_data_sketch, :pipeline, :periodic_flush],
    #     [:ex_data_sketch, :window, :roll],
    #     [:ex_data_sketch, :server, :snapshot],
    #     [:ex_data_sketch, :server, :snapshot_failed],
    #     [:ex_data_sketch, :server, :restore],
    #     [:ex_data_sketch, :server, :flush],
    #     [:ex_data_sketch, :server, :drop]]

## Attaching Handlers

Use `:telemetry.attach/4` to listen for events:

    :telemetry.attach("my-handler", [:ex_data_sketch, :sketch, :ingest], fn _name, measurements, metadata, _config ->
      Logger.info("Ingested \#{metadata.sketch_type}: \#{measurements.size_bytes} bytes in \#{measurements.duration} ns")
    end, nil)

`[:ex_data_sketch, :window, :roll]` is a good one to watch if you're
tuning a `Window`'s `every`/`keep` sizing -- `dropped_count` tells you how
many slots aged out in that one call (normally 1; more if nothing wrote
to the window for a while and several slot boundaries were crossed at
once), and `slot_count` tells you how many are still live afterward:

    :telemetry.attach(
      "window-roll-handler",
      [:ex_data_sketch, :window, :roll],
      fn _name, measurements, metadata, _config ->
        Logger.info(
          "Rolled \#{metadata.sketch_type} window: \#{measurements.dropped_count} slot(s) dropped, " <>
            "\#{measurements.slot_count} still live"
        )
      end,
      nil
    )

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

## See also

- `guides/observability.md` -- wiring these events into
  `ExDataSketch.Telemetry.Metrics.all/1` and Phoenix LiveDashboard.
- `guides/windowing.md` -- what `:window, :roll` means and when it fires.
- `guides/supervised_sketches.md` -- what the `:server` events mean.
- `phoenix_demo/` -- a real, runnable Phoenix app with all of the above
  wired into a live dashboard (`ExDataSketch.Server`, `Window`, and every
  event category this guide covers except `:stream`/`:pipeline`, which it
  doesn't use).