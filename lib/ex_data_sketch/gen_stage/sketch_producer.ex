defmodule ExDataSketch.GenStage.SketchProducer do
  @moduledoc """
  A GenStage producer that emits accumulated sketch snapshots on demand.

  `SketchProducer` maintains an internal sketch that can be updated via
  `update/2` and `merge/2`. Each downstream demand request schedules the
  emission of one snapshot of the current sketch; subsequent snapshots
  are emitted as the producer's state changes (via `update/2` or
  `merge/2`).

  This is useful for downstream consumers that need periodic snapshots
  of an evolving sketch (e.g., for persistence, cross-node distribution,
  or metrics reporting).

  ## Emission Semantics

  A snapshot is the current sketch *struct*. Downstream consumers
  (typically `ExDataSketch.GenStage.SketchConsumer` or
  `ExDataSketch.GenStage.SketchStage`) merge each received snapshot into
  their own accumulated sketch via `sketch_module.merge/2`.

  Demand is tracked cumulatively. The producer emits at most one snapshot
  per unit of demand and only when the underlying sketch has been updated
  since the previous emission (or on the first emission after demand
  arrives). This avoids flooding downstream consumers with duplicate
  snapshots while still respecting GenStage back-pressure.

  ## Options

  - `:sketch_module` -- required, the sketch module.
  - `:sketch_opts` -- options forwarded to `sketch_module.new/1` (default: `[]`).

  ## Examples

      {:ok, producer} = SketchProducer.start_link(
        sketch_module: ExDataSketch.HLL,
        sketch_opts: [p: 14]
      )

      # Update with items
      SketchProducer.update(producer, "user_1")
      SketchProducer.update(producer, "user_2")

      # Consumers that subscribe receive snapshots of the current sketch.
  """

  use GenStage

  @type state :: %{
          sketch_module: module(),
          sketch_opts: keyword(),
          current: struct(),
          pending_demand: non_neg_integer(),
          dirty: boolean()
        }

  @doc """
  Starts a SketchProducer process.

  ## Examples

      iex> {:ok, producer} = ExDataSketch.GenStage.SketchProducer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10]
      ...> )
      iex> is_pid(producer)
      true

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenStage.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Updates the producer's sketch with a single item.

  ## Examples

      iex> {:ok, producer} = ExDataSketch.GenStage.SketchProducer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10]
      ...> )
      iex> :ok = ExDataSketch.GenStage.SketchProducer.update(producer, "item")
      iex> ExDataSketch.GenStage.SketchProducer.estimate(producer) > 0.0
      true

  """
  @spec update(GenServer.server(), term()) :: :ok
  def update(server, item) do
    GenStage.call(server, {:update, item})
  end

  @doc """
  Merges a partial sketch into the producer's accumulated sketch.

  ## Examples

      iex> {:ok, producer} = ExDataSketch.GenStage.SketchProducer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10]
      ...> )
      iex> partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      iex> :ok = ExDataSketch.GenStage.SketchProducer.merge(producer, partial)
      iex> ExDataSketch.GenStage.SketchProducer.estimate(producer) > 0.0
      true

  """
  @spec merge(GenServer.server(), struct()) :: :ok
  def merge(server, partial_sketch) do
    GenStage.call(server, {:merge, partial_sketch})
  end

  @doc """
  Returns the current estimate from the producer's sketch.

  ## Examples

      iex> {:ok, producer} = ExDataSketch.GenStage.SketchProducer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10]
      ...> )
      iex> ExDataSketch.GenStage.SketchProducer.estimate(producer)
      0.0

  """
  @spec estimate(GenServer.server()) :: float()
  def estimate(server) do
    GenStage.call(server, :estimate)
  end

  @doc """
  Returns the current accumulated sketch.

  ## Examples

      iex> {:ok, producer} = ExDataSketch.GenStage.SketchProducer.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10]
      ...> )
      iex> sketch = ExDataSketch.GenStage.SketchProducer.get(producer)
      iex> ExDataSketch.HLL.estimate(sketch)
      0.0

  """
  @spec get(GenServer.server()) :: struct()
  def get(server) do
    GenStage.call(server, :get)
  end

  @impl true
  def init(opts) do
    sketch_module = Keyword.fetch!(opts, :sketch_module)
    sketch_opts = Keyword.get(opts, :sketch_opts, [])
    current = sketch_module.new(sketch_opts)

    {:producer,
     %{
       sketch_module: sketch_module,
       sketch_opts: sketch_opts,
       current: current,
       pending_demand: 0,
       # The producer is considered "dirty" (has a fresh snapshot worth
       # emitting) until the first emission. After that, dirty is set to
       # true again whenever update/merge mutates the sketch.
       dirty: true
     }}
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    new_state = %{state | pending_demand: state.pending_demand + demand}
    emit_snapshot(new_state)
  end

  @impl true
  def handle_call({:update, item}, _from, state) do
    new_current = state.sketch_module.update(state.current, item)
    new_state = %{state | current: new_current, dirty: true}
    {events, new_state} = take_snapshot(new_state)
    {:reply, :ok, events, new_state}
  end

  def handle_call({:merge, partial_sketch}, _from, state) do
    new_current = state.sketch_module.merge(state.current, partial_sketch)
    new_state = %{state | current: new_current, dirty: true}
    {events, new_state} = take_snapshot(new_state)
    {:reply, :ok, events, new_state}
  end

  def handle_call(:estimate, _from, state) do
    {:reply, state.sketch_module.estimate(state.current), [], state}
  end

  def handle_call(:get, _from, state) do
    {:reply, state.current, [], state}
  end

  # Emit one snapshot if demand is pending and the sketch is dirty;
  # otherwise no events are emitted and the demand stays queued.
  defp emit_snapshot(state) do
    {events, new_state} = take_snapshot(state)
    {:noreply, events, new_state}
  end

  defp take_snapshot(%{pending_demand: 0} = state), do: {[], state}

  defp take_snapshot(%{dirty: false} = state), do: {[], state}

  defp take_snapshot(state) do
    {[state.current], %{state | pending_demand: state.pending_demand - 1, dirty: false}}
  end
end
