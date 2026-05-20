defmodule ExDataSketch.Telemetry.OpenTelemetry do
  @moduledoc """
  OpenTelemetry span integration for ExDataSketch telemetry events.

  This module bridges ExDataSketch's `:telemetry` events to OpenTelemetry
  spans when the `:opentelemetry_api` dependency is available. It attaches
  handlers that create spans for sketch, persistence, stream, and pipeline
  events.

  ## Dependency

  This module requires the `:opentelemetry_api` package. If it is not
  available, calling `setup/0` will raise an error directing the user to
  add it as a dependency.

  ## Usage

      # In your application's `start/2` callback:
      ExDataSketch.Telemetry.OpenTelemetry.setup()

  This attaches handlers for all ExDataSketch telemetry events and creates
  corresponding OpenTelemetry spans with appropriate attributes.

  ## Span Attributes

  Each span includes the following attributes:

  - `ex_data_sketch.sketch_type` -- the sketch type (e.g., `:hll`, `:cms`)
  - `ex_data_sketch.backend` -- the persistence backend (for persistence events)
  - `ex_data_sketch.key` -- the storage key (for persistence events)
  - `ex_data_sketch.merge_count` -- number of sketches merged
  - `ex_data_sketch.item_count` -- number of items ingested
  - `ex_data_sketch.size_bytes` -- serialized size in bytes
  - `ex_data_sketch.category` -- event category (`:sketch`, `:persistence`, etc.)

  ## Configuration

  OpenTelemetry integration can be disabled via application config:

      config :ex_data_sketch, :integrations, opentelemetry: false

  When not explicitly configured, availability defaults to whether
  `:opentelemetry_api` is loaded at runtime.
  """

  @compile {:no_warn_undefined, OpenTelemetry.Tracer}
  @compile {:no_warn_undefined, :opentelemetry}
  @compile {:no_warn_undefined, :otel_tracer}
  @compile {:no_warn_undefined, :otel_span}

  alias ExDataSketch.{Integration, Telemetry}

  @handler_id "ex_data_sketch_opentelemetry"

  @doc """
  Attaches OpenTelemetry span handlers for all ExDataSketch telemetry events.

  Creates one handler per event that starts an OTEL span with the event's
  measurements and metadata as span attributes. When a `duration` measurement
  is present, the span's start and end times are set from it, producing a span
  with the correct duration. Events without a `duration` measurement produce
  signalling spans (start time equals end time).

  Calling `setup/0` when the `:opentelemetry_api` package is not available
  raises an error with installation instructions.

  ## Examples

      ExDataSketch.Telemetry.OpenTelemetry.setup()
      :ok

  """
  @spec setup() :: :ok | {:error, term()}
  def setup do
    Integration.require_opentelemetry!()
    detach_if_exists()

    handler = &handle_event/4

    :ok =
      Enum.each(Telemetry.all_event_names(), fn event_name ->
        :telemetry.attach({@handler_id, event_name}, event_name, handler, nil)
      end)

    :ok
  end

  @doc """
  Detaches all OpenTelemetry handlers previously attached by `setup/0`.

  ## Examples

      ExDataSketch.Telemetry.OpenTelemetry.teardown()
      :ok

  """
  @spec teardown() :: :ok
  def teardown do
    Enum.each(Telemetry.all_event_names(), fn event_name ->
      :telemetry.detach({@handler_id, event_name})
    end)

    :ok
  end

  defp detach_if_exists do
    Enum.each(Telemetry.all_event_names(), fn event_name ->
      :telemetry.detach({@handler_id, event_name})
    end)
  end

  if Code.ensure_loaded?(OpenTelemetry.Tracer) do
    defp handle_event(event_name, measurements, metadata, _config) do
      if Integration.opentelemetry_available?() do
        span_name = event_name |> Enum.join(".")
        attributes = build_attributes(event_name, measurements, metadata)

        end_time_ns = System.os_time(:nanosecond)
        duration_native = Map.get(measurements, :duration, 0)
        duration_ns = System.convert_time_unit(duration_native, :native, :nanosecond)
        start_time_ns = end_time_ns - duration_ns

        tracer = :opentelemetry.get_tracer()

        span_ctx =
          :otel_tracer.start_span(tracer, span_name, %{
            start_time: start_time_ns,
            attributes: attributes
          })

        :otel_span.end_span(span_ctx, end_time_ns)
      end
    end
  else
    defp handle_event(_event_name, _measurements, _metadata, _config) do
      :ok
    end
  end

  defp build_attributes(event_name, measurements, metadata) do
    base = %{
      "ex_data_sketch.category" => event_name |> Enum.at(1) |> to_string(),
      "ex_data_sketch.action" => event_name |> Enum.at(2) |> to_string()
    }

    measurements_attrs =
      measurements
      |> Map.drop([:duration])
      |> Map.new(fn {k, v} -> {"ex_data_sketch.#{k}", v} end)

    metadata_attrs =
      metadata
      |> Map.new(fn {k, v} -> {"ex_data_sketch.#{k}", stringify(v)} end)

    Map.merge(base, Map.merge(measurements_attrs, metadata_attrs))
  end

  defp stringify(v) when is_atom(v), do: Atom.to_string(v)
  defp stringify(v) when is_binary(v), do: v
  defp stringify(v) when is_integer(v), do: Integer.to_string(v)
  defp stringify(v) when is_float(v), do: Float.to_string(v)
  defp stringify(v), do: inspect(v)
end
