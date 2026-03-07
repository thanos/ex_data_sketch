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

## ExArrow (Apache Arrow IPC / Flight / ADBC)

[ExArrow](https://hex.pm/packages/ex_arrow) provides Apache Arrow support
for the BEAM: IPC stream and file reading, Arrow Flight clients, and ADBC
database connections.

### Arrow IPC stream -- chunked sketch aggregation

Arrow IPC streams deliver data as a sequence of RecordBatches. Build a
sketch per batch, then merge:

```elixir
alias ExDataSketch.HLL

{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrows")
{:ok, _schema} = ExArrow.Stream.schema(stream)

stream
|> Stream.unfold(fn s ->
  case ExArrow.Stream.next(s) do
    nil -> nil
    {:error, _} -> nil
    batch -> {batch, s}
  end
end)
|> Stream.map(fn batch ->
  batch
  |> ExArrow.RecordBatch.column("user_id")
  |> ExArrow.Array.to_list()
  |> HLL.from_enumerable(p: 14)
end)
|> Enum.to_list()
|> HLL.merge_many()
|> HLL.estimate()
```

### Arrow IPC file -- random-access batch processing

IPC files support random access to individual batches, useful for parallel
sketch construction:

```elixir
alias ExDataSketch.{Bloom, FrequentItems}

{:ok, file} = ExArrow.IPC.File.from_file("/data/users.arrow")
n = ExArrow.IPC.File.batch_count(file)

# Build a Bloom filter of known user IDs across all batches
bloom =
  0..(n - 1)
  |> Task.async_stream(fn i ->
    {:ok, batch} = ExArrow.IPC.File.get_batch(file, i)
    ids = batch |> ExArrow.RecordBatch.column("user_id") |> ExArrow.Array.to_list()
    Bloom.from_enumerable(ids, capacity: 1_000_000)
  end)
  |> Enum.map(fn {:ok, sketch} -> sketch end)
  |> Bloom.merge_many()

# Build a FrequentItems sketch of search queries
top_queries =
  0..(n - 1)
  |> Enum.reduce(FrequentItems.new(k: 64), fn i, sketch ->
    {:ok, batch} = ExArrow.IPC.File.get_batch(file, i)
    queries = batch |> ExArrow.RecordBatch.column("query") |> ExArrow.Array.to_list()
    FrequentItems.update_many(sketch, queries)
  end)
  |> FrequentItems.top_k(limit: 20)
```

### ADBC -- query databases with sketch aggregation

Use ADBC with DuckDB to query Parquet files or databases and feed results
into sketches:

```elixir
alias ExDataSketch.CMS

{:ok, result} =
  Adbc.Connection.query(MyApp.Conn,
    "SELECT search_query FROM read_parquet('/data/queries/*.parquet')")

result
|> Adbc.Result.to_map()
|> Map.fetch!("search_query")
|> CMS.from_enumerable()
|> CMS.estimate("popular_query")
```

## ExZarr (Zarr v2/v3 N-dimensional arrays)

[ExZarr](https://hex.pm/packages/ex_zarr) provides chunked, compressed
N-dimensional arrays with multiple storage backends (filesystem, S3, GCS,
memory). Its `chunk_stream/1` returns a lazy enumerable -- ideal for
building sketches without loading the full array into memory.

### Chunk streaming -- memory-efficient sketch construction

```elixir
alias ExDataSketch.{HLL, KLL}

{:ok, array} = ExZarr.open(path: "/data/sensor_readings", storage: :filesystem)

# Count distinct sensor values using chunk streaming
# ExZarr returns raw binaries -- decode based on dtype
hll =
  ExZarr.Array.chunk_stream(array)
  |> Enum.reduce(HLL.new(p: 14), fn {_idx, bin}, sketch ->
    values = for <<v::float-64-little <- bin>>, do: v
    HLL.update_many(sketch, values)
  end)

IO.puts("Distinct values: #{HLL.estimate(hll)}")

# Compute quantiles over the same chunked data
kll =
  ExZarr.Array.chunk_stream(array)
  |> Enum.reduce(KLL.new(k: 200), fn {_idx, bin}, sketch ->
    values = for <<v::float-64-little <- bin>>, do: v
    KLL.update_many(sketch, values)
  end)

IO.puts("Median: #{KLL.quantile(kll, 0.5)}")
IO.puts("P99: #{KLL.quantile(kll, 0.99)}")
```

### Parallel chunk processing

For large arrays, process chunks in parallel and merge:

```elixir
alias ExDataSketch.DDSketch

{:ok, array} = ExZarr.open(path: "/data/latencies", storage: :s3,
  bucket: "metrics", prefix: "2026/03")

DDSketch =
  ExZarr.Array.chunk_stream(array, parallel: true)
  |> Task.async_stream(fn {_idx, bin} ->
    values = for <<v::float-64-little <- bin>>, do: v
    DDSketch.from_enumerable(values, alpha: 0.01)
  end, max_concurrency: System.schedulers_online())
  |> Enum.map(fn {:ok, sketch} -> sketch end)
  |> DDSketch.merge_many()

IO.puts("P50: #{DDSketch.quantile(sketch, 0.5)}")
IO.puts("P99: #{DDSketch.quantile(sketch, 0.99)}")
```

### Zarr + Nx bridge

When working with ExZarr's Nx integration, convert tensors to flat lists
for sketch consumption:

```elixir
alias ExDataSketch.HLL

{:ok, array} = ExZarr.open(path: "/data/experiment", storage: :filesystem)
{:ok, tensor} = ExZarr.Nx.to_tensor(array)

tensor
|> Nx.to_flat_list()
|> HLL.from_enumerable(p: 14)
|> HLL.estimate()
```

### Multi-array group analysis

Zarr groups organize related arrays hierarchically. Sketch each array
and compare:

```elixir
alias ExDataSketch.{HLL, CMS}

{:ok, group} = ExZarr.Group.open("/", storage: :filesystem, path: "/data/experiments")
array_names = ExZarr.Group.list_arrays(group)

# Build an HLL per array to compare cardinalities
sketches =
  for name <- array_names, into: %{} do
    {:ok, arr} = ExZarr.Group.get_array(group, name)
    {:ok, bin} = ExZarr.Array.to_binary(arr)
    values = for <<v::float-64-little <- bin>>, do: v
    {name, HLL.from_enumerable(values, p: 14)}
  end

for {name, sketch} <- sketches do
  IO.puts("#{name}: ~#{round(HLL.estimate(sketch))} distinct values")
end
```

## General chunked data pattern

The chunk-iterate-merge pattern works with any data source that provides
batched or chunked iteration:

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

This applies to `ex_arrow`, `ex_zarr`, custom Parquet readers, Kafka
consumer batches, or any other chunked data source.
