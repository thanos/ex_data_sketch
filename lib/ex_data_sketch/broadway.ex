defmodule ExDataSketch.Broadway do
  @moduledoc """
  Broadway integration for sketch aggregation.

  This module provides helpers for accumulating sketch data from Broadway
  message batches. It composes the existing `ExDataSketch.Stream` and
  per-sketch `from_enumerable/2` APIs to build sketches from message
  payloads without reimplementing any sketch logic.

  ## Dependency

  This module requires the `:broadway` dependency. If Broadway is not
  available, calls to `accumulate/3` will raise a clear error directing
  the user to add it.

  ## Quick Start

      defmodule MyPipeline do
        use Broadway

        def handle_batch(:default, messages, _batch_info, _context) do
          key_fn = fn msg -> msg.data.user_id end
          sketch = ExDataSketch.Broadway.accumulate(messages, ExDataSketch.HLL, p: 14, key_fn: key_fn)
          :telemetry.execute([:my_app, :cardinality], %{estimate: ExDataSketch.HLL.estimate(sketch)})
          messages
        end
      end

  ## Periodic Aggregation

  For use cases that require periodic flush semantics (e.g., rolling
  cardinality windows), see `ExDataSketch.Broadway.PeriodicAggregator`.

  ## Configuration

  Broadway integration can be explicitly enabled or disabled via
  application config:

      config :ex_data_sketch, :integrations, broadway: true

  When not configured, availability defaults to whether `:broadway` is
  loaded at runtime.
  """

  alias ExDataSketch.{Integration, Telemetry}

  @doc """
  Accumulates sketch data from a list of Broadway messages.

  Extracts values from messages using `key_fn`, then builds a sketch from
  those values using the specified sketch module's `from_enumerable/2`.

  ## Arguments

  - `messages` -- a list of Broadway messages (any struct with a `data` field,
    or any value if `key_fn` extracts the relevant data).
  - `sketch_module` -- the sketch module atom (e.g., `ExDataSketch.HLL`).
  - `opts` -- keyword list:
    - `:key_fn` -- function `(message -> term)` that extracts the value
      from each message. Defaults to `fn msg -> msg.data end`.
    - All other options are forwarded to `sketch_module.from_enumerable/2`.

  ## Examples

      iex> messages = [%{data: "a"}, %{data: "b"}, %{data: "a"}]
      iex> sketch = ExDataSketch.Broadway.accumulate(messages, ExDataSketch.HLL, p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) > 0.0
      true

      iex> messages = [%Broadway.Message{data: "x", acknowledger: nil}, %{data: "y", acknowledger: nil}]
      iex> sketch = ExDataSketch.Broadway.accumulate(messages, ExDataSketch.CMS, width: 64, depth: 3, key_fn: fn msg -> msg.data end)
      iex> ExDataSketch.CMS.estimate(sketch, "x") >= 1
      true

  """
  @spec accumulate([term()], module(), keyword()) :: struct()
  def accumulate(messages, sketch_module, opts \\ []) do
    Integration.require_broadway!()

    {key_fn, sketch_opts} = Keyword.pop(opts, :key_fn, fn msg -> msg.data end)

    Telemetry.span(
      Telemetry.event_name(:pipeline, :accumulate),
      %{count: length(messages)},
      %{sketch_type: Telemetry.sketch_type(sketch_module.new()), batch_size: length(messages)},
      :pipeline,
      fn ->
        values = Enum.map(messages, key_fn)
        sketch_module.from_enumerable(values, sketch_opts)
      end
    )
  end

  @doc """
  Accumulates sketch data from a list of Broadway messages into an existing
  sketch.

  Builds a batch sketch from the messages using `sketch_module.from_enumerable/2`,
  then merges it into the provided `sketch` using `sketch_module.merge/2`.
  This works with all mergeable sketch types, including those that use `put/2`
  instead of `update/2` (e.g., Bloom, Quotient).

  ## Arguments

  - `messages` -- a list of Broadway messages.
  - `sketch` -- an existing sketch struct to merge into.
  - `opts` -- keyword list:
    - `:key_fn` -- function `(message -> term)` that extracts the value
      from each message. Defaults to `fn msg -> msg.data end`.
    - All other options are forwarded to `sketch_module.from_enumerable/2`
      when building the batch sketch.

  ## Examples

      iex> existing = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("existing")
      iex> messages = [%{data: "a"}, %{data: "b"}]
      iex> sketch = ExDataSketch.Broadway.accumulate_into(messages, existing, p: 10)
      iex> ExDataSketch.HLL.estimate(sketch) >= 3
      true

  """
  @spec accumulate_into([term()], struct(), keyword()) :: struct()
  def accumulate_into(messages, sketch, opts \\ []) do
    Integration.require_broadway!()

    {key_fn, sketch_opts} = Keyword.pop(opts, :key_fn, fn msg -> msg.data end)

    Telemetry.span(
      Telemetry.event_name(:pipeline, :accumulate),
      %{count: length(messages)},
      %{sketch_type: Telemetry.sketch_type(sketch), batch_size: length(messages)},
      :pipeline,
      fn ->
        sketch_module = sketch.__struct__
        values = Enum.map(messages, key_fn)

        batch = sketch_module.from_enumerable(values, sketch_opts)
        sketch_module.merge(sketch, batch)
      end
    )
  end
end
