# The Aggregation Wall

## What Is the Aggregation Wall?

When processing high-volume event streams, traditional exact aggregation
hits a performance barrier: memory grows linearly with cardinality, CPU
cost grows with distinct counting, and latency increases as data volume
scales. This barrier is the **aggregation wall** -- the point where
exact computation becomes economically or technically infeasible.

Probabilistic sketches break through this wall by trading a small,
controlled amount of error for dramatically lower resource consumption.

## Exact vs. Approximate: The Scaling Problem

Consider counting distinct users across a stream of 100 million events:

| Method             | Memory   | Time (per event) | Latency (p99) |
|--------------------|----------|-------------------|----------------|
| `MapSet` (exact)   | ~2 GB    | O(1) amortized    | 50-200 ms      |
| `HLL p=14`         | ~16 KB   | O(1)              | < 0.01 ms      |
| `HLL p=10`         | ~1 KB    | O(1)              | < 0.01 ms      |

The exact `MapSet` approach uses **125,000x** more memory than HLL at
p=14. At 1 billion events, the `MapSet` approach requires ~20 GB of
memory and GC pauses that can stall the BEAM for seconds.

## Why the BEAM Makes Sketches Natural

The BEAM's actor model and message-passing semantics make sketch-based
aggregation particularly natural:

1. **Per-process sketch state**: Each BEAM process can hold its own
   sketch instance. No shared mutable state, no locks, no contention.

2. **Merge as message**: Sketch merging is associative and commutative.
   A process can accumulate locally and periodically send its sketch
   to an aggregator process -- the merge is a single message.

3. **Partition-local aggregation**: Each partition of a Broadway or
   GenStage pipeline accumulates into its own sketch. Partitions never
   share state. Merging happens at the consumer stage, exactly where
   you want it.

4. **Hot code upgrades**: Because sketches are BEAM-owned binaries,
   they survive hot code upgrades. You can deploy new aggregator logic
   without losing in-flight sketch state.

## The Aggregation Wall in Practice

### Scenario 1: Real-Time Analytics Dashboard

Your Phoenix LiveView shows "active users in the last 5 minutes."
Exact counting requires maintaining a time-windowed set of user IDs.
At 100K concurrent users:

- **Exact**: 100K entries in an ETS set = ~8 MB, O(n) per query
- **HLL p=14**: 16 KB, O(1) per query, <0.5% error

The dashboard refreshes every second. The exact approach spends most of
its CPU on set maintenance. The HLL approach is effectively free.

### Scenario 2: Distributed Cardinality Across Nodes

You need to count distinct events across a 5-node cluster. Each node
processes 1M events/second:

- **Exact**: Each node must broadcast its full set of IDs to all other
  nodes. Network traffic grows as O(n^2 * cardinality).
- **Sketch merge**: Each node maintains a local HLL (16 KB). Periodic
  broadcast of 16 KB sketches to an aggregator. Network traffic: O(n)
  sketches, each 16 KB.

At 1M events/second with 10M distinct IDs, the exact approach requires
transferring hundreds of MB per second. The sketch approach transfers
80 KB per merge round.

### Scenario 3: Ad Impression Counting

An ad platform counts impressions per campaign. A single campaign
receives 50M impressions per day, with 20M unique viewers. Using CMS
(width=1024, depth=5):

- **Exact count per viewer**: 20M entries = ~160 MB per campaign
- **CMS**: ~5 KB per campaign, O(1) update, O(1) query, <1% error

With 1000 active campaigns, exact counting needs 160 GB. CMS needs 5 MB.

## Breaking Through the Wall

### Pattern 1: Stream Accumulation

```elixir
# Instead of collecting all items into a Set:
sketch = ExDataSketch.HLL.new(p: 14)
sketch = Enum.reduce(events, sketch, fn event, acc ->
  ExDataSketch.HLL.update(acc, event.user_id)
end)

# Or more ergonomically:
sketch = ExDataSketch.Stream.hll(events, p: 14)
```

### Pattern 2: Collectable

```elixir
sketch = Enum.into(events, ExDataSketch.HLL.new(p: 14))
```

### Pattern 3: Broadway Pipeline

```elixir
defmodule MyPipeline do
  use Broadway

  def handle_message(_, message, state) do
    %{sketch: sketch} = state
    updated = ExDataSketch.HLL.update(sketch, message.data.user_id)
    {:ok, message, %{state | sketch: updated}}
  end
end
```

### Pattern 4: Periodic Aggregation

```elixir
# Each of N worker processes holds a local sketch.
# Every 5 seconds, each sends its sketch to the aggregator.
defmodule Aggregator do
  def handle_info(:flush, state) do
    merged = ExDataSketch.HLL.merge_many(state.pending)
    # Store or publish the merged estimate
    {:noreply, %{state | pending: []}}
  end
end
```

## Operational Considerations

### Choosing Precision

Higher `p` means more memory but lower error. The sweet spot depends on
your application:

| p   | Memory  | Error    | Best For                          |
|-----|---------|----------|-----------------------------------|
| 10  | 1 KB    | ~3.25%   | High-volume, low-precision dashboards |
| 12  | 4 KB    | ~1.63%   | General analytics                 |
| 14  | 16 KB   | ~0.81%   | Production monitoring (recommended) |
| 16  | 64 KB   | ~0.41%   | Financial compliance              |

### Memory Budgets

When choosing `p`, consider your total memory budget across all
sketch instances:

- 1000 concurrent sketches at p=14 = 16 MB
- 1000 concurrent sketches at p=10 = 1 MB
- 1000 concurrent sketches at p=16 = 64 MB

### When Sketches Are NOT Appropriate

Sketches are inappropriate when:

1. **Exact answers are required**: Financial reconciliation, audit
   logging, compliance reporting.
2. **Cardinality is very small**: If you expect < 100 distinct values,
   a `MapSet` is faster and uses less memory than any sketch.
3. **You need to enumerate the distinct values**: Sketches estimate
   cardinality; they cannot list the values. Use a `MapSet` if you
   need the actual items.

### Sketch Type Selection Guide

| Question                                      | Use              |
|-----------------------------------------------|------------------|
| How many unique items?                        | HLL or ULL      |
| How many times did item X appear?             | CMS              |
| Is item X a member of the set?                | Bloom or Cuckoo  |
| What's the median/value at percentile P?      | KLL or DDSketch  |
| How many unique items, 20% better accuracy?    | ULL (vs HLL)    |
| Approximate set membership with deletions?    | Quotient or CQF  |

## Further Reading

- `guides/streaming_sketches.md` -- Stream and Collectable integration
- `guides/broadway_integration.md` -- Broadway pipeline patterns
- `guides/distributed_merge_semantics.md` -- Distributed aggregation
- `guides/telemetry.md` -- Monitoring sketch performance in production