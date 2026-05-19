defmodule ExDataSketch.Flow do
  @moduledoc """
  Flow integration for parallel sketch reduction.

  This module provides helpers for using `Flow` (from the `:flow` package)
  with ExDataSketch. It composes existing `reducer/1`, `merger/1`, and
  `from_enumerable/2` APIs to provide partition-local reduction and
  distributed merge operations.

  ## Dependency

  This module requires the `:flow` package. If Flow is not available, calls
  will raise a clear error directing the user to add the dependency.

  ## Quick Start

  The recommended pattern is `reduce/3` followed by `merge/2`:

      # Parallel cardinality counting
      final =
        File.stream!("events.csv")
        |> Stream.map(&parse_user_id/1)
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 14)
        |> ExDataSketch.Flow.merge(ExDataSketch.HLL)

      # final is a single merged HLL struct

  For single-partition collection (no parallel reduction), use `into/3`:

      sketch =
        1..1000
        |> Flow.from_enumerable()
        |> ExDataSketch.Flow.into(ExDataSketch.HLL, p: 14)

  ## Configuration

  Flow integration can be explicitly enabled or disabled via application config:

      config :ex_data_sketch, :integrations, flow: true

  When not configured, availability defaults to whether `:flow` is loaded
  at runtime.
  """

  alias ExDataSketch.{Integration, Telemetry}

  @doc """
  Reduces a Flow into partition-local sketches using the sketch module's
  `reducer/1` (or `reducer/0`) function.

  This function must be called after `Flow.partition/1` to ensure each
  partition accumulates into its own sketch instance. The returned Flow
  produces one sketch per partition when enumerated.

  To merge partition results into a single sketch, follow this call with
  `merge/2`.

  ## Arguments

  - `flow` -- a partitioned `Flow` struct.
  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `opts` -- options forwarded to `sketch_module.new/1`.

  ## Returns

  A `Flow` struct that, when enumerated, produces a list of partition-local
  sketches (one per partition).

  ## Examples

      iex> sketches =
      ...>   1..100
      ...>   |> Flow.from_enumerable()
      ...>   |> Flow.partition()
      ...>   |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 10)
      ...>   |> Enum.to_list()
      iex> is_list(sketches) and length(sketches) >= 1
      true

  """
  @spec reduce(Flow.t(), module(), keyword()) :: Flow.t()
  def reduce(flow, sketch_module, opts \\ []) do
    Integration.require_flow!()

    initial = fn -> sketch_module.new(opts) end
    reducer_fn = sketch_module.reducer()

    flow
    |> Flow.reduce(initial, reducer_fn)
    |> Flow.on_trigger(fn acc, _partition, _reduce_count ->
      :ok =
        Telemetry.execute(
          Telemetry.event_name(:stream, :reduce),
          %{},
          %{sketch_type: Telemetry.sketch_type(acc)},
          :stream
        )

      {[acc], acc}
    end)
  end

  @doc """
  Merges partition-local sketches from `reduce/3` into a single final sketch.

  This function collects all partition-local sketches produced by `reduce/3`
  and merges them using `sketch_module.merge_many/1`. It materializes the
  Flow and performs a single merge pass.

  Must be called after `reduce/3`.

  ## Arguments

  - `flow` -- a `Flow` struct that has had `reduce/3` applied.
  - `sketch_module` -- the sketch module atom.

  ## Returns

  A single merged sketch struct.

  ## Examples

      iex> final =
      ...>   1..100
      ...>   |> Stream.map(&to_string/1)
      ...>   |> Flow.from_enumerable()
      ...>   |> Flow.partition()
      ...>   |> ExDataSketch.Flow.reduce(ExDataSketch.HLL, p: 10)
      ...>   |> ExDataSketch.Flow.merge(ExDataSketch.HLL)
      iex> ExDataSketch.HLL.estimate(final) > 0.0
      true

  """
  @spec merge(Flow.t(), module()) :: struct()
  def merge(flow, sketch_module) do
    Integration.require_flow!()

    partitions = Enum.to_list(flow)

    Telemetry.span(
      Telemetry.event_name(:stream, :partition_merge),
      %{partition_count: length(partitions)},
      %{sketch_type: Telemetry.sketch_type(sketch_module.new())},
      :stream,
      fn ->
        sketch_module.merge_many(partitions)
      end
    )
  end

  @doc """
  Collects a Flow of items into a single sketch using `Enum.into/2`.

  This is a convenience function that collects all items from a Flow into
  a sketch using the `Collectable` protocol. It is equivalent to:

      flow |> Enum.into(sketch_module.new(opts))

  Note: this materializes the entire Flow into a single partition, so it
  does not benefit from parallel reduction. For parallel processing, use
  `reduce/3` followed by `merge/2`.

  ## Arguments

  - `flow` -- a `Flow` struct.
  - `sketch_module` -- the sketch module atom.
  - `opts` -- options forwarded to `sketch_module.new/1`.

  ## Returns

  A single sketch struct.

  ## Examples

      iex> sketch =
      ...>   1..100
      ...>   |> Flow.from_enumerable()
      ...>   |> ExDataSketch.Flow.into(ExDataSketch.HLL, p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

  """
  @spec into(Flow.t(), module(), keyword()) :: struct()
  def into(flow, sketch_module, opts \\ []) do
    Integration.require_flow!()

    flow
    |> Enum.into(sketch_module.new(opts))
  end
end
