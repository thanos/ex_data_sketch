defmodule ExDataSketch.GenStage do
  @moduledoc """
  GenStage integration for sketch aggregation.

  This module provides consumer and producer stages that accumulate sketch
  data from event streams. GenStage is an optional dependency; the
  sub-modules are only compiled when `:gen_stage` is available.

  ## Dependency

  This module requires the `:gen_stage` package. If GenStage is not
  available, the sub-modules will not be compiled and calls to
  `require_gen_stage!/0` will raise an error.

  ## Module Overview

  - `ExDataSketch.GenStage.SketchConsumer` -- A GenStage consumer that
    accumulates events into a sketch and supports periodic flushing.
  - `ExDataSketch.GenStage.SketchProducer` -- A GenStage producer that
    emits merged sketches on demand.
  - `ExDataSketch.GenStage.SketchStage` -- A combined producer-consumer
    that accumulates events and periodically emits merged sketches.

  ## Quick Start

      # Consumer that builds an HLL from events
      {:ok, consumer} = GenStage.SketchConsumer.start_link(
        sketch_module: ExDataSketch.HLL,
        sketch_opts: [p: 14],
        subscribe_to: [{some_producer, max_demand: 100}]
      )

      # Read current estimate
      GenStage.SketchConsumer.estimate(consumer)

      # Flush and reset
      flushed = GenStage.SketchConsumer.flush(consumer)

  See individual module documentation for details.
  """

  @moduledoc since: "0.9.0"

  if Code.ensure_loaded?(GenStage) do
    @doc """
    Returns whether GenStage is available at runtime.

    Checks compile-time availability and runtime configuration. Setting
    `gen_stage: true` in config does not override compile-time unavailability.

    ## Examples

        iex> is_boolean(ExDataSketch.GenStage.available?())
        true
    """
    @spec available?() :: boolean()
    def available?, do: ExDataSketch.Integration.gen_stage_available?()
  else
    @doc false
    def available?, do: false
  end
end
