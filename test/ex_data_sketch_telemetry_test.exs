defmodule ExDataSketch.TelemetryTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.{CMS, HLL, Storage, Stream, Telemetry, Theta}

  describe "event_name/2" do
    test "returns canonical event names" do
      assert Telemetry.event_name(:sketch, :ingest) == [:ex_data_sketch, :sketch, :ingest]
      assert Telemetry.event_name(:persistence, :save) == [:ex_data_sketch, :persistence, :save]
      assert Telemetry.event_name(:stream, :reduce) == [:ex_data_sketch, :stream, :reduce]

      assert Telemetry.event_name(:pipeline, :accumulate) ==
               [:ex_data_sketch, :pipeline, :accumulate]
    end
  end

  describe "categories/0" do
    test "returns all supported categories" do
      assert Telemetry.categories() == [:sketch, :persistence, :stream, :pipeline]
    end
  end

  describe "all_event_names/0" do
    test "returns all canonical event names" do
      events = Telemetry.all_event_names()
      assert length(events) == 12
      assert [:ex_data_sketch, :sketch, :ingest] in events
      assert [:ex_data_sketch, :persistence, :save] in events
      assert [:ex_data_sketch, :stream, :reduce] in events
      assert [:ex_data_sketch, :pipeline, :accumulate] in events
    end
  end

  describe "sketch_type/1" do
    test "returns correct sketch type for each sketch struct" do
      assert Telemetry.sketch_type(%HLL{}) == :hll
      assert Telemetry.sketch_type(%CMS{}) == :cms
      assert Telemetry.sketch_type(%Theta{}) == :theta
      assert Telemetry.sketch_type(%ExDataSketch.ULL{}) == :ull
      assert Telemetry.sketch_type(%ExDataSketch.KLL{}) == :kll
      assert Telemetry.sketch_type(%ExDataSketch.DDSketch{}) == :ddsketch
      assert Telemetry.sketch_type(%ExDataSketch.REQ{}) == :req
      assert Telemetry.sketch_type(%ExDataSketch.Bloom{}) == :bloom
      assert Telemetry.sketch_type(%ExDataSketch.FrequentItems{}) == :frequent_items
      assert Telemetry.sketch_type(%ExDataSketch.MisraGries{}) == :misra_gries
      assert Telemetry.sketch_type(%ExDataSketch.Quotient{}) == :quotient
      assert Telemetry.sketch_type(%ExDataSketch.CQF{}) == :cqf
      assert Telemetry.sketch_type(%ExDataSketch.IBLT{}) == :iblt
    end
  end

  describe "enabled?/1" do
    test "returns true by default" do
      assert Telemetry.enabled?(:sketch) == true
      assert Telemetry.enabled?(:persistence) == true
      assert Telemetry.enabled?(:stream) == true
      assert Telemetry.enabled?(:pipeline) == true
    end

    test "returns false when globally disabled" do
      original = Application.get_env(:ex_data_sketch, :telemetry_enabled)
      Application.put_env(:ex_data_sketch, :telemetry_enabled, false)

      assert Telemetry.enabled?(:sketch) == false

      if original do
        Application.put_env(:ex_data_sketch, :telemetry_enabled, original)
      else
        Application.delete_env(:ex_data_sketch, :telemetry_enabled)
      end
    end

    test "returns false when category disabled" do
      original = Application.get_env(:ex_data_sketch, :telemetry)
      Application.put_env(:ex_data_sketch, :telemetry, sketch: false)

      assert Telemetry.enabled?(:sketch) == false
      assert Telemetry.enabled?(:persistence) == true

      if original do
        Application.put_env(:ex_data_sketch, :telemetry, original)
      else
        Application.delete_env(:ex_data_sketch, :telemetry)
      end
    end
  end

  describe "execute/4" do
    test "emits telemetry event when enabled" do
      ref = :telemetry.attach(self(), [:ex_data_sketch, :sketch, :test], &__handler__/4, nil)

      Telemetry.execute(
        [:ex_data_sketch, :sketch, :test],
        %{count: 1},
        %{sketch_type: :hll},
        :sketch
      )

      assert_received {:event, [:ex_data_sketch, :sketch, :test], %{count: 1},
                       %{sketch_type: :hll}}

      :telemetry.detach(ref)
    end

    test "does not emit when globally disabled" do
      original = Application.get_env(:ex_data_sketch, :telemetry_enabled)
      Application.put_env(:ex_data_sketch, :telemetry_enabled, false)

      ref = :telemetry.attach(self(), [:ex_data_sketch, :sketch, :test2], &__handler__/4, nil)

      Telemetry.execute(
        [:ex_data_sketch, :sketch, :test2],
        %{count: 1},
        %{sketch_type: :hll},
        :sketch
      )

      refute_received {:event, [:ex_data_sketch, :sketch, :test2], _, _}

      if original do
        Application.put_env(:ex_data_sketch, :telemetry_enabled, original)
      else
        Application.delete_env(:ex_data_sketch, :telemetry_enabled)
      end

      :telemetry.detach(ref)
    end
  end

  describe "span/5" do
    test "emits duration event and returns result" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :merge], &__handler__/4, nil)

      result =
        Telemetry.span(
          [:ex_data_sketch, :sketch, :merge],
          %{merge_count: 3},
          %{sketch_type: :hll},
          :sketch,
          fn -> :hello end
        )

      assert result == :hello

      assert_received {:event, [:ex_data_sketch, :sketch, :merge], measurements, metadata}
      assert measurements.merge_count == 3
      assert Map.has_key?(measurements, :duration)
      assert metadata.sketch_type == :hll

      :telemetry.detach(ref)
    end

    test "does not emit when disabled" do
      original = Application.get_env(:ex_data_sketch, :telemetry_enabled)
      Application.put_env(:ex_data_sketch, :telemetry_enabled, false)

      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :merge2], &__handler__/4, nil)

      result =
        Telemetry.span(
          [:ex_data_sketch, :sketch, :merge2],
          %{},
          %{},
          :sketch,
          fn -> :world end
        )

      assert result == :world
      refute_received {:event, [:ex_data_sketch, :sketch, :merge2], _, _}

      if original do
        Application.put_env(:ex_data_sketch, :telemetry_enabled, original)
      else
        Application.delete_env(:ex_data_sketch, :telemetry_enabled)
      end

      :telemetry.detach(ref)
    end
  end

  describe "span_with_result/6" do
    test "emits event with result-derived measurements" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :ingest], &__handler__/4, nil)

      result =
        Telemetry.span_with_result(
          [:ex_data_sketch, :sketch, :ingest],
          %{},
          %{sketch_type: :hll},
          :sketch,
          fn -> HLL.new(p: 10) end,
          fn sketch -> %{size_bytes: HLL.size_bytes(sketch)} end
        )

      assert result.__struct__ == HLL

      assert_received {:event, [:ex_data_sketch, :sketch, :ingest], measurements, metadata}
      assert Map.has_key?(measurements, :duration)
      assert measurements.size_bytes > 0
      assert metadata.sketch_type == :hll

      :telemetry.detach(ref)
    end
  end

  describe "sketch ingest event" do
    test "emits :ingest event on from_enumerable" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :ingest], &__handler__/4, nil)

      HLL.from_enumerable(["a", "b", "c"], p: 10)

      assert_received {:event, [:ex_data_sketch, :sketch, :ingest], measurements, metadata}
      assert metadata.sketch_type == :hll
      assert Map.has_key?(measurements, :duration)

      :telemetry.detach(ref)
    end

    test "emits :ingest event for CMS" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :ingest], &__handler__/4, nil)

      CMS.from_enumerable(["a", "b", "c"], width: 64, depth: 3)

      assert_received {:event, [:ex_data_sketch, :sketch, :ingest], measurements, metadata}
      assert metadata.sketch_type == :cms
      assert Map.has_key?(measurements, :duration)

      :telemetry.detach(ref)
    end
  end

  describe "sketch merge event" do
    test "emits :merge event on merge_many" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :merge], &__handler__/4, nil)

      a = HLL.new(p: 10) |> HLL.update("x")
      b = HLL.new(p: 10) |> HLL.update("y")
      HLL.merge_many([a, b])

      assert_received {:event, [:ex_data_sketch, :sketch, :merge], measurements, metadata}
      assert metadata.sketch_type == :hll
      assert measurements.merge_count == 2
      assert Map.has_key?(measurements, :duration)

      :telemetry.detach(ref)
    end
  end

  describe "sketch serialize event" do
    test "emits :serialize event on HLL serialize" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :serialize], &__handler__/4, nil)

      sketch = HLL.new(p: 10) |> HLL.update("a")
      HLL.serialize(sketch)

      assert_received {:event, [:ex_data_sketch, :sketch, :serialize], measurements, metadata}
      assert metadata.sketch_type == :hll
      assert Map.has_key?(measurements, :duration)
      assert Map.has_key?(measurements, :size_bytes)
      assert measurements.size_bytes > 0

      :telemetry.detach(ref)
    end
  end

  describe "sketch deserialize event" do
    test "emits :deserialize event on HLL deserialize" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :sketch, :deserialize], &__handler__/4, nil)

      sketch = HLL.new(p: 10) |> HLL.update("a")
      binary = HLL.serialize(sketch)
      {:ok, _loaded} = HLL.deserialize(binary)

      assert_received {:event, [:ex_data_sketch, :sketch, :deserialize], measurements, metadata}
      assert metadata.sketch_type == :hll
      assert Map.has_key?(measurements, :duration)
      assert Map.has_key?(measurements, :size_bytes)

      :telemetry.detach(ref)
    end
  end

  describe "persistence events" do
    test "emits :save event on ETS save" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :persistence, :save], &__handler__/4, nil)

      :ets.new(:telemetry_test_ets_save, [:set, :public, :named_table])
      sketch = HLL.new(p: 10) |> HLL.update("a")
      Storage.ETS.save(sketch, :telemetry_test_ets_save, "test:key")
      :ets.delete(:telemetry_test_ets_save)

      assert_received {:event, [:ex_data_sketch, :persistence, :save], measurements, metadata}
      assert metadata.backend == :ets
      assert metadata.sketch_type == :hll
      assert metadata.key == "test:key"
      assert Map.has_key?(measurements, :duration)
      assert Map.has_key?(measurements, :size_bytes)

      :telemetry.detach(ref)
    end

    test "emits :load event on ETS load" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :persistence, :load], &__handler__/4, nil)

      :ets.new(:telemetry_test_ets_load, [:set, :public, :named_table])
      sketch = HLL.new(p: 10) |> HLL.update("a")
      Storage.ETS.save(sketch, :telemetry_test_ets_load, "test:key")
      Storage.ETS.load(HLL, :telemetry_test_ets_load, "test:key")
      :ets.delete(:telemetry_test_ets_load)

      assert_received {:event, [:ex_data_sketch, :persistence, :load], measurements, metadata}
      assert metadata.backend == :ets
      assert Map.has_key?(measurements, :duration)

      :telemetry.detach(ref)
    end

    test "emits :merge event on ETS merge" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :persistence, :merge], &__handler__/4, nil)

      :ets.new(:telemetry_test_ets_merge, [:set, :public, :named_table])
      sketch_a = HLL.new(p: 10) |> HLL.update("a")
      Storage.ETS.save(sketch_a, :telemetry_test_ets_merge, "test:key")
      sketch_b = HLL.new(p: 10) |> HLL.update("b")
      Storage.ETS.merge(sketch_b, :telemetry_test_ets_merge, "test:key")
      :ets.delete(:telemetry_test_ets_merge)

      assert_received {:event, [:ex_data_sketch, :persistence, :merge], measurements, metadata}
      assert metadata.backend == :ets
      assert Map.has_key?(measurements, :duration)

      :telemetry.detach(ref)
    end

    test "emits :delete event on ETS delete" do
      ref =
        :telemetry.attach(self(), [:ex_data_sketch, :persistence, :delete], &__handler__/4, nil)

      :ets.new(:telemetry_test_ets_del, [:set, :public, :named_table])
      Storage.ETS.delete(:telemetry_test_ets_del, "any:key")
      :ets.delete(:telemetry_test_ets_del)

      assert_received {:event, [:ex_data_sketch, :persistence, :delete], measurements, metadata}
      assert metadata.backend == :ets
      assert metadata.key == "any:key"
      assert Map.has_key?(measurements, :duration)

      :telemetry.detach(ref)
    end
  end

  describe "stream events" do
    test "emits :partition_merge event on reduce_partitioned" do
      ref =
        :telemetry.attach(
          self(),
          [:ex_data_sketch, :stream, :partition_merge],
          &__handler__/4,
          nil
        )

      Stream.reduce_partitioned(1..100, HLL, p: 10)

      assert_received {:event, [:ex_data_sketch, :stream, :partition_merge], measurements,
                       metadata}

      assert metadata.sketch_type == :hll
      assert Map.has_key?(measurements, :duration)
      assert Map.has_key?(measurements, :partition_count)

      :telemetry.detach(ref)
    end
  end

  defp __handler__(event_name, measurements, metadata, _config) do
    send(self(), {:event, event_name, measurements, metadata})
  end
end
