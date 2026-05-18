defmodule ExDataSketch.GenStage.SketchConsumer do
  @moduledoc """
  A GenStage consumer that accumulates events into a sketch.

  `SketchConsumer` subscribes to a producer, ingests events using the
  configured sketch module, and provides read and flush access to the
  accumulated sketch.

  ## Options

  - `:sketch_module` -- required, the sketch module (e.g., `ExDataSketch.HLL`).
  - `:sketch_opts` -- options forwarded to `sketch_module.new/1` (default: `[]`).
  - `:key_fn` -- function `(event -> term)` that extracts the value from
    each event (default: `fn event -> event end`).
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
    values = Enum.map(events, state.key_fn)

    new_current =
      if function_exported?(state.sketch_module, :from_enumerable, 2) do
        partial = state.sketch_module.from_enumerable(values, state.sketch_opts)
        state.sketch_module.merge(state.current, partial)
      else
        Enum.reduce(values, state.current, fn value, acc ->
          state.sketch_module.update(acc, value)
        end)
      end

    {:noreply, [], %{state | current: new_current}}
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
