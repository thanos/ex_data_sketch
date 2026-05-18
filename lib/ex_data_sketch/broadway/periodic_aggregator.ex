defmodule ExDataSketch.Broadway.PeriodicAggregator do
  @moduledoc """
  A GenServer that periodically accumulates and flushes sketch data.

  This module provides a process-based aggregator for use in Broadway
  pipelines and other streaming contexts where sketches should be
  accumulated over time and periodically flushed for downstream consumption
  (e.g., telemetry, metrics, persistence).

  The aggregator holds a single sketch and supports two operations:
  - `merge/2` -- merge a partial sketch into the aggregator
  - `flush/1` -- return the current aggregate sketch and reset to a new one

  ## Usage

      # Start an aggregator for HLL cardinality tracking
      {:ok, agg} = PeriodicAggregator.start_link(
        sketch_module: ExDataSketch.HLL,
        sketch_opts: [p: 14],
        flush_interval: 5_000,
        flush_callback: fn sketch ->
          :telemetry.execute([:my_app, :cardinality], %{estimate: HLL.estimate(sketch)})
        end
      )

      # Merge partial sketches
      PeriodicAggregator.merge(agg, partial_sketch)

      # Manually flush
      sketch = PeriodicAggregator.flush(agg)

  ## Flush Semantics

  When `:flush_interval` is set, the aggregator automatically calls the
  `:flush_callback` and resets the sketch at the given interval. If no
  `:flush_callback` is provided, the aggregator simply resets the sketch
  without side effects.

  Calling `flush/1` manually returns the current aggregate sketch and
  resets it to a new empty sketch.

  ## Dependency

  This module depends on `:broadway` being available. Call
  `ExDataSketch.Integration.require_broadway!/0` before use if Broadway
  might not be present.
  """

  use GenServer

  alias ExDataSketch.{Integration, Telemetry}

  @default_flush_interval 5_000

  @type state :: %{
          sketch_module: module(),
          sketch_opts: keyword(),
          current: struct(),
          flush_callback: (struct() -> term()) | nil,
          flush_interval: non_neg_integer()
        }

  @doc """
  Starts a periodic aggregator process.

  ## Options

  - `:sketch_module` -- required, the sketch module (e.g., `ExDataSketch.HLL`).
  - `:sketch_opts` -- options forwarded to `sketch_module.new/1` (default: `[]`).
  - `:flush_interval` -- milliseconds between automatic flushes (default: 5000).
    Set to `:infinity` to disable automatic flush.
  - `:flush_callback` -- function `(sketch -> term)` called on each flush
    (default: `nil`, no side effect).
  - `:name` -- GenServer name registration (default: `nil`).

  ## Examples

      iex> {:ok, agg} = ExDataSketch.Broadway.PeriodicAggregator.start_link(
      ...>   sketch_module: ExDataSketch.HLL,
      ...>   sketch_opts: [p: 10],
      ...>   flush_interval: :infinity
      ...> )
      iex> is_pid(agg)
      true

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    Integration.require_broadway!()

    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Merges a partial sketch into the aggregator.

  The partial sketch is merged with the current aggregate using
  `sketch_module.merge/2`.

  ## Examples

      iex> {:ok, agg} = ExDataSketch.Broadway.PeriodicAggregator.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], flush_interval: :infinity
      ...> )
      iex> partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      iex> :ok = ExDataSketch.Broadway.PeriodicAggregator.merge(agg, partial)
      iex> ExDataSketch.HLL.estimate(ExDataSketch.Broadway.PeriodicAggregator.get(agg)) > 0.0
      true

  """
  @spec merge(GenServer.server(), struct()) :: :ok
  def merge(server, partial_sketch) do
    GenServer.call(server, {:merge, partial_sketch})
  end

  @doc """
  Flushes the aggregator, returning the current sketch and resetting to a new one.

  ## Examples

      iex> {:ok, agg} = ExDataSketch.Broadway.PeriodicAggregator.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], flush_interval: :infinity
      ...> )
      iex> partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      iex> :ok = ExDataSketch.Broadway.PeriodicAggregator.merge(agg, partial)
      iex> flushed = ExDataSketch.Broadway.PeriodicAggregator.flush(agg)
      iex> ExDataSketch.HLL.estimate(flushed) > 0.0
      true

  """
  @spec flush(GenServer.server()) :: struct()
  def flush(server) do
    GenServer.call(server, :flush)
  end

  @doc """
  Returns the current aggregate sketch without resetting it.

  ## Examples

      iex> {:ok, agg} = ExDataSketch.Broadway.PeriodicAggregator.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], flush_interval: :infinity
      ...> )
      iex> current = ExDataSketch.Broadway.PeriodicAggregator.get(agg)
      iex> ExDataSketch.HLL.estimate(current)
      0.0

  """
  @spec get(GenServer.server()) :: struct()
  def get(server) do
    GenServer.call(server, :get)
  end

  @doc """
  Returns the current estimate from the aggregate sketch.

  Convenience function that calls `sketch_module.estimate/1` on the
  current aggregate.

  ## Examples

      iex> {:ok, agg} = ExDataSketch.Broadway.PeriodicAggregator.start_link(
      ...>   sketch_module: ExDataSketch.HLL, sketch_opts: [p: 10], flush_interval: :infinity
      ...> )
      iex> ExDataSketch.Broadway.PeriodicAggregator.estimate(agg)
      0.0

  """
  @spec estimate(GenServer.server()) :: float()
  def estimate(server) do
    GenServer.call(server, :estimate)
  end

  @impl true
  def init(opts) do
    sketch_module = Keyword.fetch!(opts, :sketch_module)
    sketch_opts = Keyword.get(opts, :sketch_opts, [])
    flush_interval = Keyword.get(opts, :flush_interval, @default_flush_interval)
    flush_callback = Keyword.get(opts, :flush_callback)

    current = sketch_module.new(sketch_opts)

    if flush_interval != :infinity do
      Process.send_after(self(), :flush_tick, flush_interval)
    end

    {:ok,
     %{
       sketch_module: sketch_module,
       sketch_opts: sketch_opts,
       current: current,
       flush_callback: flush_callback,
       flush_interval: flush_interval
     }}
  end

  @impl true
  def handle_call({:merge, partial_sketch}, _from, state) do
    merged = state.sketch_module.merge(state.current, partial_sketch)
    {:reply, :ok, %{state | current: merged}}
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

    {:reply, flushed, %{state | current: new_sketch}}
  end

  def handle_call(:get, _from, state) do
    {:reply, state.current, state}
  end

  def handle_call(:estimate, _from, state) do
    estimate = state.sketch_module.estimate(state.current)
    {:reply, estimate, state}
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

    {:noreply, %{state | current: new_sketch}}
  end
end
