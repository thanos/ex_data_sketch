# Streaming Sketches

This guide explains how to use ExDataSketch with Elixir streams and the
`Collectable` protocol for idiomatic, memory-efficient sketch construction.

## Why Sketches Fit Streaming Architectures

Probabilistic sketches are natural stream consumers:

- **Bounded memory**: Sketches use a fixed amount of memory regardless of input
  size. An HLL with `p: 14` uses 16 KiB whether it processes 1,000 or
  1,000,000,000 items.
- **Single-pass**: Sketches ingest each item exactly once. No buffering or
  multi-pass scans are needed.
- **Mergeable**: Most sketches support `merge/2`, which is associative and
  commutative. This means partial results from parallel or partitioned
  workers can be combined in any order.
- **No random access**: Sketches never need to revisit earlier items. This
  makes them ideal for lazy streams and pipelines.

## ExDataSketch.Stream

The `ExDataSketch.Stream` module provides terminal stream consumers that
build sketches from lazy enumerables. Each function consumes the stream and
returns a completed sketch struct.

### Building Sketches from Streams

```elixir
# HLL cardinality estimation from a lazy stream
cardinality =
  1..100_000
  |> Stream.map(&to_string/1)
  |> ExDataSketch.Stream.hll(p: 14)
  |> ExDataSketch.HLL.estimate()

# CMS frequency estimation
freq =
  File.stream!("access.log")
  |> Stream.map(&parse_request_path/1)
  |> ExDataSketch.Stream.cms(width: 2048, depth: 5)
  |> ExDataSketch.CMS.estimate("/api/users")
```

### Available Stream Functions

| Function | Sketch |
|----------|--------|
| `hll/2` | HyperLogLog |
| `cms/2` | Count-Min Sketch |
| `theta/2` | Theta Sketch |
| `kll/2` | KLL Quantile Sketch |
| `ddsketch/2` | DDSketch |
| `req/2` | REQ Sketch |
| `ull/2` | UltraLogLog |
| `frequent_items/2` | FrequentItems (SpaceSaving) |
| `misra_gries/2` | Misra-Gries |
| `bloom/2` | Bloom Filter |
| `quotient/2` | Quotient Filter |
| `cqf/2` | Counting Quotient Filter |
| `iblt/2` | Invertible Bloom Lookup Table |

All stream functions delegate to the corresponding `from_enumerable/2`
function. No ingestion logic is duplicated.

### reduce_into/3

`reduce_into/3` accepts either a sketch module atom or an existing sketch
struct:

```elixir
# Create a new sketch from a module
sketch = ExDataSketch.Stream.reduce_into(items, ExDataSketch.HLL, p: 14)

# Accumulate into an existing sketch
existing = ExDataSketch.HLL.new(p: 14)
sketch = ExDataSketch.Stream.reduce_into(more_items, existing)
```

### reduce_partitioned/3

For large streams, `reduce_partitioned/3` splits work into chunks, builds a
sketch per chunk, and merges all partial results:

```elixir
sketch =
  large_stream
  |> ExDataSketch.Stream.reduce_partitioned(ExDataSketch.HLL, partitions: 8, p: 14)
```

The default partition count is `System.schedulers_online()`. Partition count
does not affect result accuracy for mergeable sketches because `merge/2` is
associative and commutative. It only affects throughput and memory usage
during intermediate stages.

## Collectable Protocol

All sketch types that support `merge/2` implement the `Collectable` protocol,
enabling `Enum.into/2` and `Enum.into/3`:

```elixir
# Build an HLL from a range
sketch = Enum.into(1..1000, ExDataSketch.HLL.new(p: 14))

# Build a CMS from a stream
sketch =
  some_stream
  |> Enum.into(ExDataSketch.CMS.new(width: 2048, depth: 5))
```

### Collectable Semantics

`Collectable.into/1` returns `{sketch, collector_fn}` where `collector_fn`
handles:

- `{:cont, item}` -- inserts the item via the sketch's `update/2` or `put/2`
- `:done` -- returns the completed sketch
- `:halt` -- discards the sketch and returns `:ok`

### Collectable vs from_enumerable

For performance-sensitive code, prefer `from_enumerable/2` or `update_many/2`
because they batch items internally. `Collectable` processes items one at a
time, which is correct but may be slower for very large collections.

| Pattern | Performance | Use case |
|---------|------------|----------|
| `from_enumerable/2` | Best (batched) | Building from a known collection |
| `update_many/2` | Best (batched) | Adding a batch to an existing sketch |
| `Enum.into/2` (Collectable) | Good (one at a time) | Pipeline integration, `for` comprehensions |
| `ExDataSketch.Stream.hll/2` | Same as `from_enumerable` | Lazy stream consumption |
| `reducer/1` + `Enum.reduce` | Good (one at a time) | Custom reduce chains |

### Supported Collectable Sketches

Every sketch that supports `merge/2` implements `Collectable`:

- HLL, CMS, Theta, KLL, DDSketch, REQ, ULL
- FrequentItems, MisraGries
- Bloom, Quotient, CQF, IBLT

Skipped sketches:

- **XorFilter** -- static construction requires all items up-front; not
  mergeable.
- **Cuckoo** -- bounded capacity means `put/2` can return `{:error, :full}`;
  `Collectable` has no error signalling mechanism.

## Merge and Partition Awareness

Sketch merge operations are **associative** and **commutative**. This means:

```elixir
# These produce equivalent results
HLL.merge(HLL.merge(a, b), c) == HLL.merge(a, HLL.merge(b, c))
HLL.merge(a, b) == HLL.merge(b, a)            # same cardinality
```

This property is what makes partition-local reduction safe. You can build
partial sketches on different workers, partitions, or time windows and merge
them later without worrying about order.

### Partition-Aware Reduction

```elixir
# Build partial HLLs per partition, then merge
partial_sketches =
  0..3
  |> Enum.map(fn partition ->
    partition_data
    |> ExDataSketch.HLL.from_enumerable(p: 14)
  end)

final = ExDataSketch.HLL.merge_many(partial_sketches)
```

This pattern is exactly what `reduce_partitioned/3` automates:

```elixir
final = ExDataSketch.Stream.reduce_partitioned(data, ExDataSketch.HLL, partitions: 4, p: 14)
```

## Elixir Stream Reduction

Elixir's `Stream` module produces lazy enumerables. When you pipe a stream
into `ExDataSketch.Stream.hll/2`, the stream is consumed once and the sketch
accumulates each element. No intermediate list is created.

```elixir
# Lazy: never holds all items in memory
sketch =
  File.stream!("large_file.csv")
  |> Stream.map(&parse_line/1)
  |> Stream.filter(&valid?/1)
  |> ExDataSketch.Stream.hll(p: 14)
```

This works because `from_enumerable/2` uses `update_many/2` internally, which
chunks input and processes each chunk without materializing the entire stream.