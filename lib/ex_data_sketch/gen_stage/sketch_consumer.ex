defmodule ExDataSketch.GenStage.SketchConsumer do
  @moduledoc """
  A GenStage consumer that accumulates events into a sketch.

  `SketchConsumer` subscribes to a producer, ingests events using the
  configured sketch module, and provides read and flush access to the
  accumulated sketch.

  ## Event Contract

  Each incoming event is interpreted in one of two ways:

  1. **Sketch snapshot**: when the event is a struct of the configured
     `:sketch_module`, the consumer merges it into its accumulated sketch
     via `sketch_module.merge/2`. This is the contract used by
     `ExDataSketch.GenStage.SketchProducer` and
     `ExDataSketch.GenStage.SketchStage`, which emit snapshots of their
     internal sketch on demand.

  2. **Raw item**: any other event is passed through `:key_fn` and inserted
     into the accumulated sketch via `sketch_module.update/2` (or
     `sketch_module.from_enumerable/2` for a batch). This is the contract
     used by raw event sources (Kafka offsets, Phoenix events, etc.).

  A single batch may mix both shapes; sketches are merged and raw items are
  updated independently, then folded into a single accumulator.

  ## Options

  - `:sketch_module` -- required, the sketch module (e.g., `ExDataSketch.HLL`).
  - `:sketch_opts` -- options forwarded to `sketch_module.new/1` (default: `[]`).
  - `:key_fn` -- function `(event -> term)` that extracts the value from
    each *raw* event (default: `fn event -> event end`). Not applied to
    sketch snapshots.
  - `:flush_interval` -- milliseconds between automatic flushes (default:
    `:infinity`, no automatic flush). When set, the consumer calls
    `:flush_callback` and resets.
  - `:flush_callback` -- function `(sketch -> term())` called on each automatic
    flush (default: `nil`).
  - `:subscribe_to` -- a producer or `{producer, opts}` tuple to subscribe to.

  ## Examples

      {:ok, consumer} = SketchConsumer.start_link(
        sketch_module: ExDataSketch.HLL,
        sketch_opts: [p: 14],
        subscribe_to: [{my_producer, max_demand: 1000}]
      )

      # After events are consumed
      SketchConsumer.estimate(consumer)

      # Flush to get the accumulated sketch and reset
      sketch = SketchConsumer.flush(consumer)
  """

  use GenStage

  alias ExDataSketch.Telemetry

  @type state :: %{
          sketch_module: module(),
          sketch_opts: keyword(),
          key_fn: (term() -> term()),
          current: struct(),
          flush_callback: (struct() -> term()) | nil,
          flush_interval: non_neg_integer() | :infinity
        }

  @doc """
  Starts a SketchConsumer process.

  ## Examples

      iex> {:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
      ...>   sketch_module: ExDataSketch.HLL,
      ...>   sketch_opts: [p: 10],
      ...> subscribe_to: []
      ...> )
      iex> is_pid(consumer)
      true

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenStage.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Merges a partial sketch into the consumer's accumulated sketch.

  ## Examples

      iex> {:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      iex> :ok = ExDataSketch.GenStage.SketchConsumer.merge(consumer, partial)
      iex> ExDataSketch.GenStage.SketchConsumer.estimate(consumer) > 0.0
      true

  """
  @spec merge(GenServer.server(), struct()) :: :ok
  def merge(server, partial_sketch) do
    GenStage.call(server, {:merge, partial_sketch})
  end

  @doc """
  Returns the current estimate from the accumulated sketch.

  ## Examples

      iex> {:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> ExDataSketch.GenStage.SketchConsumer.estimate(consumer)
      0.0

  """
  @spec estimate(GenServer.server()) :: float()
  def estimate(server) do
    GenStage.call(server, :estimate)
  end

  @doc """
  Returns the current accumulated sketch without resetting.

  ## Examples

      iex> {:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> current = ExDataSketch.GenStage.SketchConsumer.get(consumer)
      iex> ExDataSketch.HLL.estimate(current)
      0.0

  """
  @spec get(GenServer.server()) :: struct()
  def get(server) do
    GenStage.call(server, :get)
  end

  @doc """
  Flushes the accumulated sketch and resets to a new one.

  Returns the flushed sketch.

  ## Examples

      iex> {:ok, consumer} = ExDataSketch.GenStage.SketchConsumer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      iex> :ok = ExDataSketch.GenStage.SketchConsumer.merge(consumer, partial)
      iex> flushed = ExDataSketch.GenStage.SketchConsumer.flush(consumer)
      iex> ExDataSketch.HLL.estimate(flushed) > 0.0
      true

  """
  @spec flush(GenServer.server()) :: struct()
  def flush(server) do
    GenStage.call(server, :flush)
  end

  @impl true
  def init(opts) do
    sketch_module = Keyword.fetch!(opts, :sketch_module)
    sketch_opts = Keyword.get(opts, :sketch_opts, [])
    key_fn = Keyword.get(opts, :key_fn, fn event -> event end)
    flush_interval = Keyword.get(opts, :flush_interval, :infinity)
    flush_callback = Keyword.get(opts, :flush_callback)
    subscribe_to = Keyword.get(opts, :subscribe_to, [])

    current = sketch_module.new(sketch_opts)

    if flush_interval != :infinity do
      Process.send_after(self(), :flush_tick, flush_interval)
    end

    {:consumer,
     %{
       sketch_module: sketch_module,
       sketch_opts: sketch_opts,
       key_fn: key_fn,
       current: current,
       flush_callback: flush_callback,
       flush_interval: flush_interval
     }, subscribe_to: subscribe_to}
  end

  @impl true
  def handle_events(events, _from, state) do
    {sketches, raw_events} =
      Enum.split_with(events, fn event -> is_struct(event, state.sketch_module) end)

    new_current =
      state.current
      |> merge_sketches(sketches, state.sketch_module)
      |> ingest_raw_events(raw_events, state)

    {:noreply, [], %{state | current: new_current}}
  end

  # Merge any upstream sketch snapshots into the accumulator.
  defp merge_sketches(acc, [], _sketch_module), do: acc

  defp merge_sketches(acc, sketches, sketch_module) do
    Enum.reduce(sketches, acc, fn sketch, current ->
      sketch_module.merge(current, sketch)
    end)
  end

  # Ingest raw items via key_fn + from_enumerable (or update fallback).
  defp ingest_raw_events(acc, [], _state), do: acc

  defp ingest_raw_events(acc, raw_events, state) do
    values = Enum.map(raw_events, state.key_fn)

    if function_exported?(state.sketch_module, :from_enumerable, 2) do
      partial = state.sketch_module.from_enumerable(values, state.sketch_opts)
      state.sketch_module.merge(acc, partial)
    else
      Enum.reduce(values, acc, fn value, current ->
        state.sketch_module.update(current, value)
      end)
    end
  end

  @impl true
  def handle_call({:merge, partial_sketch}, _from, state) do
    merged = state.sketch_module.merge(state.current, partial_sketch)
    {:reply, :ok, [], %{state | current: merged}}
  end

  def handle_call(:estimate, _from, state) do
    estimate = state.sketch_module.estimate(state.current)
    {:reply, estimate, [], state}
  end

  def handle_call(:get, _from, state) do
    {:reply, state.current, [], state}
  end

  def handle_call(:flush, _from, state) do
    flushed = state.current
    new_sketch = state.sketch_module.new(state.sketch_opts)

    :ok =
      Telemetry.execute(
        Telemetry.event_name(:pipeline, :periodic_flush),
        %{duration: 0},
        %{sketch_type: Telemetry.sketch_type(flushed)},
        :pipeline
      )

    {:reply, flushed, [], %{state | current: new_sketch}}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    flushed = state.current
    new_sketch = state.sketch_module.new(state.sketch_opts)

    if state.flush_callback do
      state.flush_callback.(flushed)
    end

    if state.flush_interval != :infinity do
      Process.send_after(self(), :flush_tick, state.flush_interval)
    end

    {:noreply, [], %{state | current: new_sketch}}
  end
end
