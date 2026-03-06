# ExDataSketch v0.3.0 -- FrequentItems / Heavy-Hitters

ExDataSketch v0.3.0 adds **FrequentItems**, a streaming heavy-hitter sketch based on the SpaceSaving algorithm. This is the sixth sketch family in the library and the first non-numeric sketch type.

## What is FrequentItems?

FrequentItems tracks the approximate top-k most frequent items in a data stream using bounded memory. It maintains at most `k` counters, each storing an item, its estimated count, and a maximum overcount error. The SpaceSaving algorithm guarantees that any item whose true frequency exceeds `N/k` will always be tracked.

```elixir
sketch =
  ExDataSketch.FrequentItems.new(k: 10)
  |> ExDataSketch.FrequentItems.update_many(stream_of_page_views)

# Get the top 5 most frequent items
ExDataSketch.FrequentItems.top_k(sketch, 5)
# [%{item: "/home", estimate: 4821, error: 12, lower: 4809, upper: 4821}, ...]

# Check a specific item
ExDataSketch.FrequentItems.estimate(sketch, "/checkout")
# {:ok, %{estimate: 312, error: 5, lower: 307, upper: 312}}
```

## Highlights

### Full Pure Elixir + Rust NIF dual-backend support

Like all ExDataSketch algorithms, FrequentItems ships with a pure Elixir implementation and optional Rust NIF acceleration. Both backends produce byte-identical serialized output for the same inputs.

### Canonical FI1 binary state format

FrequentItems uses a 32-byte header followed by variable-length entries sorted by item bytes. The format is deterministic and portable across backends.

### Flexible key encoding

Three key encoding modes are supported:

| Encoding | Use case |
|----------|----------|
| `:binary` (default) | String keys, raw binary data |
| `:int` | Integer keys (signed 64-bit little-endian) |
| `{:term, :external}` | Arbitrary Erlang terms via `:erlang.term_to_binary/1` |

### Deterministic merge

FrequentItems merge is commutative. It combines counts additively across the union of keys, then replays weighted updates in sorted key order into an empty sketch. Count (`n`) is always exactly additive regardless of whether entries are dropped during capacity enforcement.

### Smart NIF routing

Query operations (`top_k`, `estimate`) route to Rust only when `k >= 256`, where NIF acceleration outweighs the call boundary overhead. Header reads (`count`, `entry_count`) always use Pure Elixir since they are O(1) binary pattern matches.

## Other changes

- Rust NIF for `theta_compact` with dirty scheduler support.
- Mox added as a test dependency for backend contract testing.
- EXSK codec sketch ID 6 for FrequentItems.

## Upgrading

```elixir
def deps do
  [
    {:ex_data_sketch, "~> 0.3.0"}
  ]
end
```

No breaking changes from v0.2.1. All existing sketches (HLL, CMS, Theta, KLL, DDSketch) are unchanged.

## Algorithm coverage

| Algorithm | Purpose | Status |
|-----------|---------|--------|
| HyperLogLog (HLL) | Cardinality estimation | Pure + Rust |
| Count-Min Sketch (CMS) | Frequency estimation | Pure + Rust |
| Theta Sketch | Set operations on cardinalities | Pure + Rust |
| KLL Quantiles | Rank and quantile estimation | Pure + Rust |
| DDSketch | Relative-error quantile estimation | Pure + Rust |
| FrequentItems (SpaceSaving) | Heavy-hitter detection | Pure + Rust |
