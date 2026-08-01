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
  resets it to a new empty sketch, without invoking `:flush_callback` --
  only the automatic, timer-driven flush does that.

  ## Implementation

  This module is a thin wrapper around `ExDataSketch.Server`, translating
  its own options into the equivalent `Server` options and delegating every
  call. It exists to keep this module's pipeline-oriented API stable for
  existing callers; new code that does not need that exact API should use
  `ExDataSketch.Server` directly.

  `[:ex_data_sketch, :pipeline, :periodic_flush]` is emitted on the
  automatic, timer-driven flush only (mirroring `:flush_callback`, which is
  also automatic-only). Both the automatic and the manual `flush/1` path
  emit `[:ex_data_sketch, :server, :flush]`, since that event comes from
  the underlying `ExDataSketch.Server` and always fires on every flush.

  ## Dependency

  This module depends on `:broadway` being available. Call
  `ExDataSketch.Integration.require_broadway!/0` before use if Broadway
  might not be present.
  """

  alias ExDataSketch.{Integration, Server, Telemetry}

  @default_flush_interval 5_000

  @doc """
  Starts a periodic aggregator process.

  ## Options

  - `:sketch_module` -- required, the sketch module (e.g., `ExDataSketch.HLL`).
  - `:sketch_opts` -- options forwarded to `sketch_module.new/1` (default: `[]`).
  - `:flush_interval` -- milliseconds between automatic flushes (default: 5000).
    Set to `:infinity` to disable automatic flush.
  - `:flush_callback` -- function `(sketch -> term)` called on each automatic
    flush (default: `nil`, no side effect).
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

    sketch_module = Keyword.fetch!(opts, :sketch_module)
    sketch_opts = Keyword.get(opts, :sketch_opts, [])
    flush_interval = Keyword.get(opts, :flush_interval, @default_flush_interval)
    flush_callback = Keyword.get(opts, :flush_callback)

    server_opts =
      [
        sketch: sketch_module,
        sketch_opts: sketch_opts,
        flush: [interval: flush_interval, callback: wrap_callback(flush_callback)]
      ]
      |> maybe_put_name(Keyword.get(opts, :name))

    Server.start_link(server_opts)
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
  def merge(server, partial_sketch), do: Server.merge(server, partial_sketch)

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
  def flush(server), do: Server.flush(server)

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
  def get(server), do: Server.sketch(server)

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
  def estimate(server), do: Server.estimate(server)

  defp maybe_put_name(opts, nil), do: opts
  defp maybe_put_name(opts, name), do: Keyword.put(opts, :name, name)

  # `Server`'s `:flush` callback runs inside the `Server` GenServer process,
  # only on the automatic, timer-driven path -- the same path the legacy
  # `:periodic_flush` event fired on. The process dictionary here belongs to
  # that same long-running `Server` process (not the caller), so it is safe
  # to use for tracking time since the last automatic flush.
  defp wrap_callback(flush_callback) do
    fn sketch ->
      now = System.monotonic_time()
      duration = now - Process.get(:periodic_aggregator_last_flush, now)
      Process.put(:periodic_aggregator_last_flush, now)

      Telemetry.execute(
        Telemetry.event_name(:pipeline, :periodic_flush),
        %{duration: duration},
        %{sketch_type: Telemetry.sketch_type(sketch)},
        :pipeline
      )

      if flush_callback, do: flush_callback.(sketch)
    end
  end
end
