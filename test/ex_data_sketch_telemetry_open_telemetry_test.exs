defmodule ExDataSketch.Telemetry.OpenTelemetryTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.Telemetry.OpenTelemetry

  @handler_id "ex_data_sketch_opentelemetry"

  describe "setup/0" do
    @tag :opentelemetry
    test "attaches handlers for all event names when opentelemetry is available" do
      unless ExDataSketch.Integration.opentelemetry_available?() do
        :telemetry.attach({@handler_id, [:ex_data_sketch, :sketch, :ingest]}, &IO.puts/4, nil)
        :telemetry.detach({@handler_id, [:ex_data_sketch, :sketch, :ingest]})
      end

      if ExDataSketch.Integration.opentelemetry_available?() do
        :ok = OpenTelemetry.setup()

        event_names = ExDataSketch.Telemetry.all_event_names()

        Enum.each(event_names, fn event_name ->
          handlers = :telemetry.list_handlers(event_name)

          otel_handlers =
            Enum.filter(handlers, fn handler ->
              handler.id == {@handler_id, event_name}
            end)

          assert length(otel_handlers) == 1
        end)

        OpenTelemetry.teardown()
      else
        assert_raise RuntimeError, ~r/opentelemetry_api/, fn ->
          OpenTelemetry.setup()
        end
      end
    end

    @tag :opentelemetry
    test "teardown removes all attached handlers" do
      if ExDataSketch.Integration.opentelemetry_available?() do
        :ok = OpenTelemetry.setup()
        :ok = OpenTelemetry.teardown()

        event_names = ExDataSketch.Telemetry.all_event_names()

        Enum.each(event_names, fn event_name ->
          handlers = :telemetry.list_handlers(event_name)

          otel_handlers =
            Enum.filter(handlers, fn handler ->
              handler.id == {@handler_id, event_name}
            end)

          assert Enum.empty?(otel_handlers)
        end)
      end
    end

    @tag :opentelemetry
    test "setup is idempotent" do
      if ExDataSketch.Integration.opentelemetry_available?() do
        :ok = OpenTelemetry.setup()
        :ok = OpenTelemetry.setup()

        event_name = [:ex_data_sketch, :sketch, :ingest]
        handlers = :telemetry.list_handlers(event_name)

        otel_handlers =
          Enum.filter(handlers, fn handler ->
            handler.id == {@handler_id, event_name}
          end)

        assert length(otel_handlers) == 1

        OpenTelemetry.teardown()
      end
    end
  end

  describe "handle_event/4" do
    @tag :opentelemetry
    test "does not crash when receiving telemetry events" do
      if ExDataSketch.Integration.opentelemetry_available?() do
        :ok = OpenTelemetry.setup()

        :telemetry.execute(
          [:ex_data_sketch, :sketch, :ingest],
          %{duration: 100, count: 50},
          %{sketch_type: :hll}
        )

        OpenTelemetry.teardown()
      end
    end
  end
end
