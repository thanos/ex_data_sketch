# Integration Guide

ExDataSketch sketches integrate naturally with the Elixir ecosystem through
pure functions — no adapters or special dependencies required.

Every sketch module (`HLL`, `CMS`, `Theta`) exposes four convenience functions:

| Function             | Purpose                                      |
|----------------------|----------------------------------------------|
| `from_enumerable/2`  | Build a sketch from any `Enumerable`         |
| `merge_many/1`       | Merge a collection of sketches               |
| `reducer/1`          | Returns a 2-arity function for reduce chains |
| `merger/1`           | Returns a 2-arity function for merging       |

## Enum and Stream

### Building a sketch from a collection

```elixir
# One-liner with from_enumerable
sketch = HLL.from_enumerable(user_ids, p: 14)
HLL.estimate(sketch)

# Equivalent long form
sketch = HLL.new(p: 14) |> HLL.update_many(user_ids)
```

### Chunked streaming updates

For large datasets that don't fit in memory, use `Stream.chunk_every/2`
with `update_many/2`:

```elixir
File.stream!("events.csv")
|> Stream.map(&parse_user_id/1)
|> Stream.chunk_every(10_000)
|> Enum.reduce(HLL.new(p: 14), fn chunk, sketch ->
  HLL.update_many(sketch, chunk)
end)
|> HLL.estimate()
```

### Using reducer/1

The `reducer/1` function returns a function compatible with `Enum.reduce/3`:

```elixir
reducer_fn = HLL.reducer()
sketch = Enum.reduce(user_ids, HLL.new(), reducer_fn)
```

## Flow

[Flow](https://hex.pm/packages/flow) provides parallel data processing.
Sketches are ideal Flow accumulators because merge is associative and
commutative.

### Partitioned cardinality counting

```elixir
alias ExDataSketch.HLL

File.stream!("events.csv")
|> Flow.from_enumerable()
|> Flow.partition()
|> Flow.reduce(fn -> HLL.new(p: 14) end, HLL.reducer())
|> Flow.departition(
  fn -> HLL.new(p: 14) end,
  HLL.merger(),
  & &1
)
|> Enum.to_list()
|> hd()
|> HLL.estimate()
```

### Parallel frequency counting

```elixir
alias ExDataSketch.CMS

File.stream!("queries.log")
|> Flow.from_enumerable()
|> Flow.partition()
|> Flow.reduce(fn -> CMS.new() end, CMS.reducer())
|> Flow.departition(
  fn -> CMS.new() end,
  CMS.merger(),
  & &1
)
|> Enum.to_list()
|> hd()
|> CMS.estimate("popular_query")
```

## Broadway

[Broadway](https://hex.pm/packages/broadway) processes data from message
queues. Sketches fit naturally in the batch processing pipeline.

### Per-batch sketch with GenServer aggregator

```elixir
defmodule MyPipeline do
  use Broadway

  alias ExDataSketch.HLL

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    items = Enum.map(messages, fn msg -> msg.data.user_id end)
    sketch = HLL.from_enumerable(items, p: 14)

    # Send partial sketch to an aggregator GenServer
    SketchAggregator.merge(sketch)

    messages
  end
end

defmodule SketchAggregator do
  use GenServer

  alias ExDataSketch.HLL

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  def merge(sketch), do: GenServer.cast(__MODULE__, {:merge, sketch})
  def estimate, do: GenServer.call(__MODULE__, :estimate)

  @impl true
  def init(_opts), do: {:ok, HLL.new(p: 14)}

  @impl true
  def handle_cast({:merge, sketch}, state) do
    {:noreply, HLL.merge(state, sketch)}
  end

  @impl true
  def handle_call(:estimate, _from, state) do
    {:reply, HLL.estimate(state), state}
  end
end
```

## Explorer

[Explorer](https://hex.pm/packages/explorer) provides DataFrames for Elixir.
Convert a Series to a list to feed into a sketch:

```elixir
alias ExDataSketch.HLL

df = Explorer.DataFrame.from_csv!("users.csv")

df["user_id"]
|> Explorer.Series.to_list()
|> HLL.from_enumerable(p: 14)
|> HLL.estimate()
```

For frequency estimation:

```elixir
alias ExDataSketch.CMS

df["search_query"]
|> Explorer.Series.to_list()
|> CMS.from_enumerable()
|> CMS.estimate("popular_query")
```

## Nx

[Nx](https://hex.pm/packages/nx) provides numerical computing. Sketch
operations work on individual values, not tensors, so convert to a flat list
first:

```elixir
alias ExDataSketch.HLL

tensor = Nx.tensor([1, 2, 3, 2, 1])

tensor
|> Nx.to_flat_list()
|> HLL.from_enumerable()
|> HLL.estimate()
```

> **Note:** Sketches operate on discrete items, not continuous numerical data.
> Use Nx for numerical operations and sketches for approximate counting.

## ex_arrow and ExZarr

For columnar / chunked data formats like Arrow and Zarr, use the chunk
iterator pattern: update each chunk separately, then merge:

```elixir
alias ExDataSketch.HLL

chunks
|> Enum.map(fn chunk ->
  chunk
  |> to_list()
  |> HLL.from_enumerable(p: 14)
end)
|> HLL.merge_many()
|> HLL.estimate()
```

This pattern works with any library that provides chunked iteration over
columnar data, including `ex_arrow`, `ExZarr`, and custom Parquet readers.
