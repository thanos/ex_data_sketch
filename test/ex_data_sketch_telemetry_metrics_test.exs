defmodule ExDataSketch.Telemetry.MetricsTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Telemetry.Metrics

  alias ExDataSketch.HLL
  alias ExDataSketch.Telemetry, as: ExTelemetry
  alias ExDataSketch.Telemetry.Metrics

  describe "all/1" do
    test "covers every event in Telemetry.all_event_names/0" do
      metrics = Metrics.all()
      covered_events = metrics |> Enum.map(& &1.event_name) |> MapSet.new()

      for event_name <- ExTelemetry.all_event_names() do
        assert event_name in covered_events,
               "no metric covers #{inspect(event_name)}"
      end
    end

    test "every metric's event_name is a real ExDataSketch event" do
      known_events = MapSet.new(ExTelemetry.all_event_names())

      for metric <- Metrics.all() do
        assert metric.event_name in known_events
      end
    end

    test "defaults to the ex_data_sketch prefix" do
      [metric | _] = Metrics.all()
      assert List.first(metric.name) == :ex_data_sketch
    end

    test ":prefix changes every metric's name prefix" do
      metrics = Metrics.all(prefix: "my_app")
      assert Enum.all?(metrics, fn m -> List.first(m.name) == :my_app end)
    end

    test ":prefix does not change what event a metric listens to" do
      default_events = Metrics.all() |> Enum.map(& &1.event_name) |> MapSet.new()
      prefixed_events = Metrics.all(prefix: "my_app") |> Enum.map(& &1.event_name) |> MapSet.new()

      assert default_events == prefixed_events
    end

    test "every metric is a Summary or a Counter" do
      assert Enum.all?(Metrics.all(), fn
               %Elixir.Telemetry.Metrics.Summary{} -> true
               %Elixir.Telemetry.Metrics.Counter{} -> true
               _ -> false
             end)
    end

    test "end-to-end: a metric's event_name and measurement match a real emitted event" do
      duration_metric =
        Enum.find(Metrics.all(), fn m ->
          m.name == [:ex_data_sketch, :sketch, :ingest, :duration]
        end)

      refute is_nil(duration_metric)

      test_pid = self()

      :telemetry.attach(
        "metrics-test-e2e",
        duration_metric.event_name,
        fn _event, measurements, _metadata, _config ->
          value = extract_measurement(duration_metric.measurement, measurements)
          send(test_pid, {:measurement, value})
        end,
        nil
      )

      HLL.from_enumerable(["a", "b"], p: 10)

      assert_receive {:measurement, value}
      assert is_number(value)

      :telemetry.detach("metrics-test-e2e")
    end
  end

  defp extract_measurement(fun, measurements) when is_function(fun, 1), do: fun.(measurements)
  defp extract_measurement(key, measurements) when is_atom(key), do: Map.get(measurements, key)
end
