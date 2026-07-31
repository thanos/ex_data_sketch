# GenStage Integration

ExDataSketch integrates with [GenStage](https://hex.pm/packages/gen_stage) for
event-driven sketch aggregation. GenStage is always available as part of OTP,
so no additional dependency is required.

## Module Overview

- `ExDataSketch.GenStage.SketchConsumer` -- A consumer that accumulates events
  into a sketch with periodic flush support.
- `ExDataSketch.GenStage.SketchProducer` -- A producer that emits accumulated
  sketches on demand.
- `ExDataSketch.GenStage.SketchStage` -- A combined producer-consumer that
  accumulates events and emits merged sketches downstream.

## SketchConsumer

`SketchConsumer` subscribes to a producer, ingests events using the configured
sketch module, and provides read and flush access to the accumulated sketch.

```elixir
{:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
  sketch_module: ExDataSketch.HLL,
  sketch_opts: [p: 14],
  subscribe_to: [{my_producer, max_demand: 1000}]
)
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `:sketch_module` | Required. The sketch module | -- |
| `:sketch_opts` | Options for `sketch_module.new/1` | `[]` |
| `:key_fn` | Function to extract values from events | `fn e -> e end` |
| `:flush_interval` | Auto-flush interval in ms | `:infinity` |
| `:flush_callback` | Called on each automatic flush | `nil` |
| `:subscribe_to` | Producer(s) to subscribe to | `[]` |

### Operations

| Function | Description |
|----------|-------------|
| `merge/2` | Merge a partial sketch into the consumer |
| `flush/1` | Return current sketch and reset |
| `get/1` | Return current sketch without resetting |
| `estimate/1` | Return current estimate |

## SketchProducer

`SketchProducer` holds a sketch that can be updated and emits it to
downstream consumers on demand.

```elixir
{:ok, producer} = ExDataSketch.GenStage.SketchProducer.start_link(
  sketch_module: ExDataSketch.HLL,
  sketch_opts: [p: 14]
)

ExDataSketch.GenStage.SketchProducer.update(producer, "user_1")
ExDataSketch.GenStage.SketchProducer.update(producer, "user_2")
```

## SketchStage

`SketchStage` is a combined producer-consumer that accumulates events and
emits the current sketch downstream:

```elixir
{:ok, stage} = ExDataSketch.GenStage.SketchStage.start_link(
  sketch_module: ExDataSketch.HLL,
  sketch_opts: [p: 14],
  subscribe_to: [{my_producer, max_demand: 1000}]
)
```

## Periodic Flush

Both `SketchConsumer` and `PeriodicAggregator` support periodic flush:

```elixir
# Consumer with periodic flush
{:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
  sketch_module: ExDataSketch.HLL,
  sketch_opts: [p: 14],
  flush_interval: 5_000,
  flush_callback: fn sketch ->
    :telemetry.execute([:app, :cardinality], %{estimate: HLL.estimate(sketch)})
  end,
  subscribe_to: [{producer, max_demand: 1000}]
)
```

## Merge Semantics

All three modules use `sketch_module.merge/2` for accumulation. Because merge
is associative and commutative for most sketch types, partial results from
different workers or time windows can be combined in any order without
affecting the final result.

## See Also

- [Broadway Integration](broadway_integration.md)
- [Streaming Sketches](streaming_sketches.md)