# Broadway Integration

ExDataSketch integrates with [Broadway](https://hex.pm/packages/broadway) for
message queue-driven sketch aggregation. This guide explains how to use
`ExDataSketch.Broadway` and `ExDataSketch.Broadway.PeriodicAggregator` in
production pipelines.

## Dependency

Add `{:broadway, "~> 1.0"}` to your `mix.exs` dependencies. Broadway is an
optional dependency -- if it is not present, calling Broadway integration
functions will raise a clear error directing you to add it.

## Per-Batch Aggregation

Use `ExDataSketch.Broadway.accumulate/3` inside `handle_batch/4` to build a
sketch from a batch of messages:

```elixir
defmodule MyPipeline do
  use Broadway

  @impl true
  def handle_batch(:default, messages, _batch_info, _context) do
    sketch =
      ExDataSketch.Broadway.accumulate(messages, ExDataSketch.HLL,
        p: 14,
        key_fn: fn msg -> msg.data.user_id end
      )

    :telemetry.execute([:my_app, :cardinality], %{
      estimate: ExDataSketch.HLL.estimate(sketch)
    })

    messages
  end
end
```

### `accumulate_into/4`

To merge a batch into an existing sketch:

```elixir
existing = ExDataSketch.HLL.new(p: 14)
sketch = ExDataSketch.Broadway.accumulate_into(messages, existing, ExDataSketch.HLL)
```

## Periodic Aggregation

For rolling windows or periodic flush semantics, use
`ExDataSketch.Broadway.PeriodicAggregator`:

```elixir
{:ok, agg} = ExDataSketch.Broadway.PeriodicAggregator.start_link(
  sketch_module: ExDataSketch.HLL,
  sketch_opts: [p: 14],
  flush_interval: 5_000,
  flush_callback: fn sketch ->
    :telemetry.execute([:my_app, :cardinality], %{
      estimate: ExDataSketch.HLL.estimate(sketch)
    })
  end
)
```

### PeriodicAggregator Operations

| Function | Description |
|----------|-------------|
| `merge/2` | Merge a partial sketch into the aggregator |
| `flush/1` | Return the current sketch and reset |
| `get/1` | Return the current sketch without resetting |
| `estimate/1` | Return the current estimate |

The aggregator automatically flushes at the configured interval. Set
`flush_interval: :infinity` to disable automatic flush.

## Configuration

Broadway integration can be enabled or disabled via application config:

```elixir
config :ex_data_sketch, :integrations, broadway: true
```

## Why Sketches Fit Broadway Pipelines

Broadway processes messages in batches from message queues (SQS, Kafka, etc.).
Sketches are ideal for Broadway because:

1. **Bounded memory**: Sketch size is independent of input cardinality
2. **Associative merge**: Partial sketches from different batches can be merged
   in any order
3. **No random access**: Each message is processed exactly once
4. **Streaming-friendly**: No need to buffer the entire dataset

## See Also

- [GenStage Integration](genstage_integration.md)
- [Streaming Sketches](streaming_sketches.md)
- [Integration Guide](integrations.md)