# Distributed Merge Semantics

## Overview

One of the most powerful properties of probabilistic sketches is their
**mergeability**: two sketches of the same type and parameters can be
combined into a single sketch that estimates the union of both input
sets. This property makes sketches ideal for distributed aggregation.

This guide explains how merge semantics work, why they matter for
distributed systems, and how to use them effectively with
`ex_data_sketch`.

## Merge Fundamentals

### Associativity and Commutativity

All mergeable sketches in `ex_data_sketch` satisfy two algebraic laws:

1. **Associativity**: `merge(merge(a, b), c) == merge(a, merge(b, c))`
2. **Commutativity**: `merge(a, b) == merge(b, a)`

These laws mean that the order and grouping of merges does not affect
the result. You can merge in any order, in any grouping, at any time,
and always get the same answer.

```elixir
# All three produce the same estimate:
a = HLL.from_enumerable(items_a, p: 14)
b = HLL.from_enumerable(items_b, p: 14)
c = HLL.from_enumerable(items_c, p: 14)

left  = HLL.merge(HLL.merge(a, b), c)
right = HLL.merge(a, HLL.merge(b, c))
flat  = HLL.merge_many([a, b, c])

# All within floating-point tolerance of each other
```

### Idempotency (Approximate)

Merging a sketch with itself is approximately idempotent:

```elixir
sketch = HLL.from_enumerable(items, p: 14)
merged = HLL.merge(sketch, sketch)

# estimate(merged) == estimate(sketch) -- always true
# because HLL merge takes register-wise maximum
```

This is exact for HLL and ULL (register-wise max). For CMS (Count-Min
Sketch), self-merge is exact because counters are summed and the same
items produce the same counters. For Bloom filters, self-merge is a
no-op.

## Distributed Aggregation Patterns

### Pattern 1: Fan-In Aggregation

The simplest pattern: multiple producers each maintain a local sketch,
periodically sending it to a central aggregator.

```
Producer 1 ──[sketch]──┐
Producer 2 ──[sketch]──┤──► Aggregator ──► Merged Result
Producer 3 ──[sketch]──┘
```

```elixir
defmodule CentralAggregator do
  use GenServer

  def init(_), do: {:ok, %{sketch: HLL.new(p: 14)}}

  def handle_cast({:merge, incoming}, state) do
    merged = HLL.merge(state.sketch, incoming)
    {:noreply, %{state | sketch: merged}}
  end

  def handle_call(:estimate, _, state) do
    {:reply, HLL.estimate(state.sketch), state}
  end
end
```

### Pattern 2: Tree Aggregation

For large clusters, use hierarchical aggregation to avoid overwhelming
a single node:

```
Node 1 ─┐                    ┌──► Root
Node 2 ─┤──► Aggregator A ──┤
Node 3 ─┘                    │
Node 4 ─┐                    └──► Root
Node 5 ─┤──► Aggregator B ───┘
Node 6 ─┘
```

```elixir
# Level 1: Each node accumulates locally
local_sketch = HLL.from_enumerable(local_events, p: 14)

# Level 2: Fan into intermediate aggregators
send(aggregator_a, {:merge, local_sketch})

# Level 3: Root merges all aggregator results
final = HLL.merge_many(aggregator_sketches)
```

Tree aggregation reduces the merge burden on any single node from O(N)
to O(log N) rounds, each merging O(sqrt(N)) sketches.

### Pattern 3: Partition-Local with Periodic Merge

Each Broadway or GenStage partition accumulates independently. A
separate process periodically merges all partition sketches:

```elixir
defmodule MyBroadway do
  use Broadway

  # Each processor partition holds its own sketch
  def handle_message(processor, message, %{sketch: sketch} = context) do
    updated = HLL.update(sketch, message.data.user_id)
    {:ok, message, %{context | sketch: updated}}
  end
end

# Periodic aggregation
defmodule PeriodicAggregator do
  use GenServer

  def handle_info(:flush, state) do
    sketches = for pid <- Broadway.list_processors(MyPipeline) do
      GenServer.call(pid, :get_sketch)
    end
    merged = HLL.merge_many(sketches)
    # Publish merged estimate
    {:noreply, %{state | pending: []}}
  end
end
```

### Pattern 4: ETS-Sharded Aggregation

Use ETS tables as a shared sketch store, enabling any process to
contribute to a shared sketch without message passing:

```elixir
# Create a named ETS table
table = :ets.new(:my_sketches, [:set, :public, :named_table])

# Any process can save
ExDataSketch.Storage.ETS.save(local_sketch, table, "daily:active_users")

# Any process can merge
ExDataSketch.Storage.ETS.merge(another_sketch, table, "daily:active_users")

# Any process can load and estimate
{:ok, merged} = ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, table, "daily:active_users")
ExDataSketch.HLL.estimate(merged)
```

## Merge Correctness Guarantees

### Same Parameters Required

All sketches being merged must have the same parameters:

```elixir
# This raises IncompatibleSketchesError:
a = HLL.new(p: 10)
b = HLL.new(p: 14)
HLL.merge(a, b)  # ** (IncompatibleSketchesError) HLL precision mismatch: 10 vs 14
```

### Sketch Type Must Match

You cannot merge an HLL with a ULL or a CMS:

```elixir
# Different sketch types cannot be merged
hll = HLL.new(p: 14)
cms = CMS.new(width: 128, depth: 5)
HLL.merge(hll, cms)  # FunctionClauseError -- wrong type
```

### Hash Strategy Compatibility

When merging sketches that used different hash strategies (e.g., one
with `:phash2` and another with `:xxhash3`), the merge will raise an
error. All merged sketches must use the same hash strategy:

```elixir
a = HLL.new(p: 14, hash_strategy: :phash2)
b = HLL.new(p: 14, hash_strategy: :xxhash3)
HLL.merge(a, b)  # ** (IncompatibleSketchesError) hash strategy mismatch
```

## Distributed Consistency

### Eventual Consistency

Sketch merging provides **eventual consistency** for cardinality
estimates. If partitions accumulate data independently and merge
periodically, the merged estimate converges to the true cardinality.
The convergence rate depends on:

1. **Merge frequency**: More frequent merges = faster convergence
2. **Partition count**: More partitions = more merge rounds needed
3. **Sketch precision**: Higher `p` = faster convergence per merge

### Network Partition Tolerance

During network partitions, each partition continues to accumulate data
independently. When connectivity is restored, merging is straightforward:
just merge all partition sketches. No coordination protocol is needed
because merge is associative and commutative.

```elixir
# During partition: nodes A and B accumulate independently
sketch_a = HLL.from_enumerable(events_a, p: 14)
sketch_b = HLL.from_enumerable(events_b, p: 14)

# After partition heals: simple merge
merged = HLL.merge(sketch_a, sketch_b)
# merged estimate reflects the union of both event sets
```

### Time-Windowed Merging

For sliding-window analytics (e.g., "unique users in the last hour"),
use separate sketch instances per time window:

```elixir
defmodule WindowedSketch do
  # One sketch per 5-minute window
  def merge_windows(sketches, window_ms) do
    now = System.monotonic_time(:millisecond)
    cutoff = now - window_ms

    sketches
    |> Enum.filter(fn {timestamp, _sketch} -> timestamp > cutoff end)
    |> Enum.map(fn {_ts, sketch} -> sketch end)
    |> HLL.merge_many()
  end
end
```

## Performance Characteristics

### Merge Cost

| Sketch Type     | Merge Cost     | Memory per Merge |
|-----------------|----------------|------------------|
| HLL p=14       | O(2^p) = 16 KB | 16 KB            |
| ULL p=14       | O(2^p) + 8    | ~16 KB           |
| CMS 128x5      | O(w*d) = 640B | 640 bytes         |
| Bloom 10K cap  | O(n/8) = 1.2KB| 1.2 KB           |

Merge is a single pass over the register/counter array. For HLL and
ULL, this is a register-wise max operation. For CMS, it's a
counter-wise max operation. For Bloom, it's a bitwise OR.

### Parallel Merge Scalability

`merge_many/1` uses `Enum.reduce/3`, which merges sequentially. For
very large sketch counts, consider parallel reduction:

```elixir
# Sequential (default)
HLL.merge_many(sketches)

# Parallel (for > 100 sketches)
sketches
|> Task.async_stream(fn chunk -> HLL.merge_many(chunk) end, chunk_size: 10)
|> Enum.map(fn {:ok, result} -> result end)
|> HLL.merge_many()
```

## Anti-Patterns

### Don't Merge Too Frequently

Merging on every update defeats the purpose of local accumulation:

```elixir
# BAD: Merge on every update (wasteful)
def handle_info({:event, user_id}, %{sketch: sketch} = state) do
  new_sketch = HLL.update(sketch, user_id)
  send(aggregator, {:merge, new_sketch})  # 16KB message per event!
  {:noreply, %{state | sketch: new_sketch}}
end

# GOOD: Accumulate locally, merge periodically
def handle_info({:event, user_id}, %{sketch: sketch} = state) do
  {:noreply, %{state | sketch: HLL.update(sketch, user_id)}}
end

def handle_info(:flush, %{sketch: sketch} = state) do
  send(aggregator, {:merge, sketch})  # 16KB message per flush interval
  {:noreply, %{state | sketch: HLL.new(p: 14)}}
end
```

### Don't Use Different Precision Values

```elixir
# BAD: Different p values across producers
a = HLL.new(p: 10)  # Producer 1 uses p=10
b = HLL.new(p: 14)  # Producer 2 uses p=14
HLL.merge(a, b)      # IncompatibleSketchesError!
```

### Don't Forget to Reset After Flushing

If you flush a sketch to an aggregator, create a new local sketch for
the next accumulation period. Otherwise you'll double-count events
from the previous period.

## Further Reading

- Flajolet et al., "HyperLogLog: The Analysis of a Near-Optimal Cardinality
  Estimation Algorithm" (2007) -- HLL register-wise max merge semilattice
- Cormode and Muthukrishnan, "An Improved Data Stream Summary: The
  Count-Min Sketch and Its Applications" (2005) -- CMS point-wise max merge
- Heule, Nunkesser, and Hall, "HyperLogLog in Practice: Algorithmic
  Engineering of a State of The Art Cardinality Estimation Algorithm" (2013)
  -- ULL sparse/dense representation and linear counting
- `guides/streaming_sketches.md` -- Stream API for local accumulation
- `guides/broadway_integration.md` -- Broadway pipeline integration
- `guides/genstage_integration.md` -- GenStage consumer integration
- `guides/persistence.md` -- Persistence for sketch state
- `guides/aggregation_wall.md` -- Why sketches break through scaling limits