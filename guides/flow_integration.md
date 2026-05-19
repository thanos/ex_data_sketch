# Flow Integration

ExDataSketch integrates with [Flow](https://hex.pm/packages/flow) for parallel
partition-local sketch reduction. This guide explains how to use
`ExDataSketch.Flow` for distributed cardinality counting, frequency estimation,
and other sketch workloads across multiple partitions.

## Dependency

Add `{:flow, "~> 1.2"}` to your `mix.exs` dependencies. Flow is an optional
dependency -- if it is not present, calling Flow integration functions will
raise a clear error directing you to add it.

## Why Flow?

Flow provides parallel data processing with partition-local reduction. Because
sketch merge is **associative** and **commutative**, partial results from each
partition can be combined in any order to produce the same final result. This
makes sketches ideal Flow accumulators.

## Partition-Local Reduction

The primary pattern is `reduce/3` followed by `merge/2`:

```elixir
alias ExDataSketch.Flow

# Parallel cardinality counting
final =
  File.stream!("events.csv")
  |> Stream.map(&parse_user_id/1)
  |> Flow.from_enumerable()
  |> Flow.partition()
  |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 14)
  |> ExDataSketch.Flow.merge(ExDataSketch.HLL)

ExDataSketch.HLL.estimate(final)
```

### How It Works

1. `Flow.from_enumerable/1` creates a Flow from the stream.
2. `Flow.partition/1` splits the Flow across schedulers (default:
   `System.schedulers_online()` partitions).
3. `ExDataSketch.Flow.reduce/3` creates one sketch per partition and reduces
   each element into it using the sketch's `reducer/0`.
4. `ExDataSketch.Flow.merge/2` collects all partition sketches and merges them
   using `merge_many/1`.

The result is a single merged sketch equivalent to a single-pass
`from_enumerable/2`, but computed in parallel.

## Using Merge Results

After `merge/2`, you have a single sketch struct. You can query it directly:

```elixir
# HLL - cardinality estimation
final = items
  |> Flow.from_enumerable()
  |> Flow.partition()
  |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 14)
  |> ExDataSketch.Flow.merge(ExDataSketch.HLL)

ExDataSketch.HLL.estimate(final)

# CMS - frequency estimation
final = items
  |> Flow.from_enumerable()
  |> Flow.partition()
  |> ExDataSketch.Flow.reduce(ExDataSketch.CMS, width: 2048, depth: 5)
  |> ExDataSketch.Flow.merge(ExDataSketch.CMS)

ExDataSketch.CMS.estimate(final, "popular_item")
```

## Single-Partition Collection

For simpler use cases where parallel reduction is not needed, use `into/3`:

```elixir
sketch =
  1..1000
  |> Flow.from_enumerable()
  |> ExDataSketch.Flow.into(ExDataSketch.HLL, p: 14)
```

This materializes the entire Flow into a single partition. It does not benefit
from parallel reduction. For workloads requiring parallelism, prefer
`reduce/3` followed by `merge/2`.

## Supported Sketches

All mergeable sketch types work with Flow:

| Sketch | Module | Merge? |
|--------|--------|--------|
| HLL | `ExDataSketch.HLL` | Yes |
| CMS | `ExDataSketch.CMS` | Yes |
| Theta | `ExDataSketch.Theta` | Yes |
| KLL | `ExDataSketch.KLL` | Yes |
| DDSketch | `ExDataSketch.DDSketch` | Yes |
| REQ | `ExDataSketch.REQ` | Yes |
| ULL | `ExDataSketch.ULL` | Yes |
| FrequentItems | `ExDataSketch.FrequentItems` | Yes |
| MisraGries | `ExDataSketch.MisraGries` | Yes |
| Bloom | `ExDataSketch.Bloom` | Yes |
| Quotient | `ExDataSketch.Quotient` | Yes |
| CQF | `ExDataSketch.CQF` | Yes |
| IBLT | `ExDataSketch.IBLT` | Yes |

## Comparing Flow with Other Approaches

| Approach | Parallelism | Best for |
|---------|-------------|----------|
| `from_enumerable/2` | None (single-pass) | Simple collections |
| `Enum.into/2` (Collectable) | None (single-pass) | Pipeline integration |
| `ExDataSketch.Stream` | None (lazy stream) | Lazy stream consumption |
| `ExDataSketch.Flow.reduce/3` | Partition-local | Large datasets, multi-core |
| `ExDataSketch.Flow.into/3` | None (single partition) | Simpler use cases |

## Configuration

Flow integration can be explicitly enabled or disabled via application config:

```elixir
config :ex_data_sketch, :integrations, flow: true
```

## See Also

- [Streaming Sketches](streaming_sketches.md)
- [Integration Guide](integrations.md)
- [Broadway Integration](broadway_integration.md)
- [GenStage Integration](genstage_integration.md)