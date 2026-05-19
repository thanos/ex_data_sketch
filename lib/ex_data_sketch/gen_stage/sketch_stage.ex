defmodule ExDataSketch.GenStage.SketchStage do
  @moduledoc """
  A combined GenStage producer-consumer for sketch aggregation pipelines.

  `SketchStage` subscribes to an upstream producer, accumulates events into
  a sketch, and emits the current sketch snapshot to downstream consumers
  on demand. This enables pipeline compositions where one stage aggregates
  and the next stage persists or reports.

  ## Event Contract

  Incoming events follow the same convention as
  `ExDataSketch.GenStage.SketchConsumer`:

  1. **Sketch snapshot**: when the event is a struct of the configured
     `:sketch_module`, it is merged into the stage's accumulated sketch
     via `sketch_module.merge/2`.
  2. **Raw item**: any other event is passed through `:key_fn` and
     inserted via `sketch_module.from_enumerable/2`.

  After processing each incoming batch, the stage emits a single snapshot
  of its updated accumulated sketch downstream.

  ## Options

  - `:sketch_module` -- required, the sketch module.
  - `:sketch_opts` -- options forwarded to `sketch_module.new/1` (default: `[]`).
  - `:key_fn` -- function `(event -> term())` to extract values from raw
    events (default: `fn event -> event end`). Not applied to sketch
    snapshots.
  - `:subscribe_to` -- a producer or `{producer, opts}` tuple to subscribe to.

  ## Examples

      {:ok, stage} = SketchStage.start_link(
        sketch_module: ExDataSketch.HLL,
        sketch_opts: [p: 14],
        subscribe_to: [{some_producer, max_demand: 100}]
      )

      # Downstream consumers will receive the current sketch on demand
      SketchStage.estimate(stage)
  """

  use GenStage

  @type state :: %{
          sketch_module: module(),
          sketch_opts: keyword(),
          key_fn: (term() -> term()),
          current: struct()
        }

  @doc """
  Starts a SketchStage process.

  ## Examples

      iex> {:ok, stage} = ExDataSketch.GenStage.SketchStage.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> is_pid(stage)
      true

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenStage.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Returns the current estimate from the stage's accumulated sketch.

  ## Examples

      iex> {:ok, stage} = ExDataSketch.GenStage.SketchStage.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> ExDataSketch.GenStage.SketchStage.estimate(stage)
      0.0

  """
  @spec estimate(GenServer.server()) :: float()
  def estimate(server) do
    GenStage.call(server, :estimate)
  end

  @doc """
  Returns the current accumulated sketch.

  ## Examples

      iex> {:ok, stage} = ExDataSketch.GenStage.SketchStage.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> ExDataSketch.GenStage.SketchStage.get(stage) |> ExDataSketch.HLL.estimate()
      0.0

  """
  @spec get(GenServer.server()) :: struct()
  def get(server) do
    GenStage.call(server, :get)
  end

  @doc """
  Merges a partial sketch into the stage's accumulated sketch.

  ## Examples

      iex> {:ok, stage} = ExDataSketch.GenStage.SketchStage.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], subscribe_to: []
      ...> )
      iex> partial = ExDataSketch.HLL.from_enumerable(["a"], p: 10)
      iex> :ok = ExDataSketch.GenStage.SketchStage.merge(stage, partial)
      iex> ExDataSketch.GenStage.SketchStage.estimate(stage) > 0.0
      true

  """
  @spec merge(GenServer.server(), struct()) :: :ok
  def merge(server, partial_sketch) do
    GenStage.call(server, {:merge, partial_sketch})
  end

  @impl true
  def init(opts) do
    sketch_module = Keyword.fetch!(opts, :sketch_module)
    sketch_opts = Keyword.get(opts, :sketch_opts, [])
    key_fn = Keyword.get(opts, :key_fn, fn event -> event end)
    subscribe_to = Keyword.get(opts, :subscribe_to, [])

    current = sketch_module.new(sketch_opts)

    {:producer_consumer,
     %{
       sketch_module: sketch_module,
       sketch_opts: sketch_opts,
       key_fn: key_fn,
       current: current
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

    {:noreply, [new_current], %{state | current: new_current}}
  end

  defp merge_sketches(acc, [], _sketch_module), do: acc

  defp merge_sketches(acc, sketches, sketch_module) do
    Enum.reduce(sketches, acc, fn sketch, current ->
      sketch_module.merge(current, sketch)
    end)
  end

  defp ingest_raw_events(acc, [], _state), do: acc

  defp ingest_raw_events(acc, raw_events, state) do
    values = Enum.map(raw_events, state.key_fn)
    partial = state.sketch_module.from_enumerable(values, state.sketch_opts)
    state.sketch_module.merge(acc, partial)
  end

  @impl true
  def handle_call({:merge, partial_sketch}, _from, state) do
    new_current = state.sketch_module.merge(state.current, partial_sketch)
    {:reply, :ok, [], %{state | current: new_current}}
  end

  def handle_call(:estimate, _from, state) do
    {:reply, state.sketch_module.estimate(state.current), [], state}
  end

  def handle_call(:get, _from, state) do
    {:reply, state.current, [], state}
  end
end
