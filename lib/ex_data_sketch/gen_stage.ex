defmodule ExDataSketch.GenStage do
  @moduledoc """
  GenStage integration for sketch aggregation.

  This module provides consumer and producer stages that accumulate sketch
  data from event streams. GenStage is always available as part of OTP, so
  no optional dependency is required.

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
end
