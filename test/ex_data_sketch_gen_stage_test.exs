defmodule ExDataSketch.GenStageTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.GenStage.{SketchConsumer, SketchProducer, SketchStage}

  # Test helper: a minimal GenStage producer that emits a pre-supplied list
  # of events once any downstream consumer requests demand. Used to drive
  # SketchConsumer / SketchStage through real GenStage delivery.
  defmodule ListProducer do
    use GenStage

    def start_link(events) do
      GenStage.start_link(__MODULE__, events)
    end

    @impl true
    def init(events), do: {:producer, events}

    @impl true
    def handle_demand(_demand, []), do: {:noreply, [], []}

    def handle_demand(demand, events) when demand > 0 do
      {to_emit, rest} = Enum.split(events, demand)
      {:noreply, to_emit, rest}
    end
  end

  # Test helper: a GenStage consumer that forwards every batch it receives
  # to a target pid via `{:events, events}` messages. Used to assert on the
  # exact stream of events a producer emits.
  defmodule ForwardingConsumer do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      target = Keyword.fetch!(opts, :target)
      subscribe_to = Keyword.fetch!(opts, :subscribe_to)
      {:consumer, target, subscribe_to: subscribe_to}
    end

    @impl true
    def handle_events(events, _from, target) do
      send(target, {:events, events})
      {:noreply, [], target}
    end
  end

  describe "SketchConsumer" do
    setup do
      {:ok, consumer} =
        SketchConsumer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          subscribe_to: []
        )

      %{consumer: consumer}
    end

    test "starts and accepts manual merge", %{consumer: consumer} do
      partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      :ok = SketchConsumer.merge(consumer, partial)
      assert SketchConsumer.estimate(consumer) > 0.0
    end

    test "flush returns sketch and resets", %{consumer: consumer} do
      partial = ExDataSketch.HLL.from_enumerable(["a", "b", "c"], p: 10)
      :ok = SketchConsumer.merge(consumer, partial)

      flushed = SketchConsumer.flush(consumer)
      assert ExDataSketch.HLL.estimate(flushed) > 0.0
      assert SketchConsumer.estimate(consumer) == 0.0
    end

    test "get returns current sketch without resetting", %{consumer: consumer} do
      partial = ExDataSketch.HLL.from_enumerable(["a"], p: 10)
      :ok = SketchConsumer.merge(consumer, partial)

      current = SketchConsumer.get(consumer)
      assert ExDataSketch.HLL.estimate(current) > 0.0
      assert SketchConsumer.estimate(consumer) > 0.0
    end

    test "CMS consumer accumulates frequencies" do
      {:ok, cms_consumer} =
        SketchConsumer.start_link(
          sketch_module: ExDataSketch.CMS,
          sketch_opts: [width: 128, depth: 3],
          subscribe_to: []
        )

      partial = ExDataSketch.CMS.from_enumerable(["a", "a", "b"], width: 128, depth: 3)
      :ok = SketchConsumer.merge(cms_consumer, partial)
      assert SketchConsumer.get(cms_consumer) |> ExDataSketch.CMS.estimate("a") >= 2

      GenStage.stop(cms_consumer)
    end
  end

  describe "SketchConsumer with producer" do
    test "processes events from a producer" do
      {:ok, producer} =
        SketchProducer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10]
        )

      {:ok, consumer} =
        SketchConsumer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          subscribe_to: [{producer, max_demand: 10}]
        )

      :ok = SketchProducer.update(producer, "item_1")
      :ok = SketchProducer.update(producer, "item_2")
      :ok = SketchProducer.update(producer, "item_3")

      Process.sleep(100)

      estimate = SketchConsumer.estimate(consumer)
      assert estimate > 0.0

      GenStage.stop(consumer)
      GenStage.stop(producer)
    end

    test "producer/consumer round-trip preserves cardinality" do
      # Verifies that items inserted on the producer are reflected in the
      # consumer's estimate with the same accuracy as if they had been
      # inserted directly. This catches C3-style regressions where the
      # consumer treated sketch snapshots as opaque items to be re-hashed.
      {:ok, producer} =
        SketchProducer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 14]
        )

      {:ok, consumer} =
        SketchConsumer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 14],
          subscribe_to: [{producer, max_demand: 4}]
        )

      items = Enum.map(1..500, &"user_#{&1}")

      Enum.each(items, fn item -> :ok = SketchProducer.update(producer, item) end)

      # Allow the demand/emit loop to settle.
      Process.sleep(200)

      estimate = SketchConsumer.estimate(consumer)

      # Cardinality is 500; at p=14 the relative error is well under 5%.
      assert_in_delta estimate, 500.0, 500 * 0.05

      GenStage.stop(consumer)
      GenStage.stop(producer)
    end

    test "consumer merges upstream sketch snapshots without re-hashing them" do
      # Direct test of the event-detection branch: feed the consumer
      # sketch structs of the configured module via a real upstream
      # producer and verify they are merged (not treated as raw items).
      a = ExDataSketch.HLL.from_enumerable(1..1000, p: 14)
      b = ExDataSketch.HLL.from_enumerable(501..1500, p: 14)
      {:ok, upstream} = ListProducer.start_link([a, b])

      {:ok, consumer} =
        SketchConsumer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 14],
          subscribe_to: [{upstream, max_demand: 10}]
        )

      # Wait for the upstream events to flow through.
      Process.sleep(100)

      estimate = SketchConsumer.estimate(consumer)
      # True cardinality of {1..1000} ∪ {501..1500} is 1500.
      assert_in_delta estimate, 1500.0, 1500 * 0.05

      GenStage.stop(consumer)
      GenStage.stop(upstream)
    end

    test "consumer handles mixed batches of sketches and raw items" do
      # When a batch contains both upstream sketch snapshots and raw items
      # (e.g., a sketch and per-user maps), the consumer must merge the
      # sketch and update with the raw items, applying key_fn only to the
      # raw items.
      sketch = ExDataSketch.HLL.from_enumerable(1..500, p: 14)
      raw = Enum.map(501..1000, fn i -> %{user_id: i} end)
      events = [sketch | raw]
      {:ok, upstream} = ListProducer.start_link(events)

      {:ok, consumer} =
        SketchConsumer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 14],
          key_fn: fn %{user_id: id} -> id end,
          subscribe_to: [{upstream, max_demand: 1000}]
        )

      Process.sleep(100)

      estimate = SketchConsumer.estimate(consumer)
      # True cardinality of {1..500} ∪ {501..1000} is 1000.
      assert_in_delta estimate, 1000.0, 1000 * 0.05

      GenStage.stop(consumer)
      GenStage.stop(upstream)
    end
  end

  describe "SketchProducer" do
    setup do
      {:ok, producer} =
        SketchProducer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10]
        )

      %{producer: producer}
    end

    test "starts and accepts updates", %{producer: producer} do
      :ok = SketchProducer.update(producer, "item_1")
      assert SketchProducer.estimate(producer) > 0.0
    end

    test "accepts merge", %{producer: producer} do
      partial = ExDataSketch.HLL.from_enumerable(["a", "b", "c"], p: 10)
      :ok = SketchProducer.merge(producer, partial)
      assert SketchProducer.estimate(producer) > 0.0
    end

    test "get returns current sketch", %{producer: producer} do
      sketch = SketchProducer.get(producer)
      assert ExDataSketch.HLL.estimate(sketch) == 0.0
    end

    test "emits at most one snapshot per update under standing demand" do
      # Verifies the snapshot-producer semantics: a downstream demand of N
      # does not cause the producer to emit N duplicates of the same
      # snapshot. Instead, one snapshot is emitted per state change.
      {:ok, producer} =
        SketchProducer.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10]
        )

      {:ok, forwarder} =
        ForwardingConsumer.start_link(
          target: self(),
          subscribe_to: [{producer, max_demand: 100, min_demand: 0}]
        )

      # Initial demand triggers exactly one snapshot of the (empty) sketch.
      assert_receive {:events, [first_snapshot]}, 500
      assert is_struct(first_snapshot, ExDataSketch.HLL)
      assert ExDataSketch.HLL.estimate(first_snapshot) == 0.0

      # Each update produces exactly one additional snapshot.
      :ok = SketchProducer.update(producer, "a")
      assert_receive {:events, [snap1]}, 500
      assert is_struct(snap1, ExDataSketch.HLL)
      assert ExDataSketch.HLL.estimate(snap1) > 0.0

      :ok = SketchProducer.update(producer, "b")
      assert_receive {:events, [snap2]}, 500
      assert is_struct(snap2, ExDataSketch.HLL)
      assert ExDataSketch.HLL.estimate(snap2) >= ExDataSketch.HLL.estimate(snap1)

      # No further snapshots arrive without further updates.
      refute_receive {:events, _}, 50

      GenStage.stop(forwarder)
      GenStage.stop(producer)
    end
  end

  describe "SketchStage" do
    setup do
      {:ok, stage} =
        SketchStage.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 10],
          subscribe_to: []
        )

      %{stage: stage}
    end

    test "starts and provides estimate", %{stage: stage} do
      assert SketchStage.estimate(stage) == 0.0
    end

    test "merge accumulates into stage", %{stage: stage} do
      partial = ExDataSketch.HLL.from_enumerable(["a", "b"], p: 10)
      :ok = SketchStage.merge(stage, partial)
      assert SketchStage.estimate(stage) > 0.0
    end

    test "get returns current sketch", %{stage: stage} do
      sketch = SketchStage.get(stage)
      assert ExDataSketch.HLL.estimate(sketch) == 0.0
    end

    test "merges upstream sketch snapshots and emits its own snapshot" do
      # SketchStage is a producer-consumer that subscribes upstream, merges
      # whatever it receives (raw items or sketches), and republishes a
      # snapshot of its own accumulated sketch downstream.
      sketch_a = ExDataSketch.HLL.from_enumerable(1..1000, p: 14)
      sketch_b = ExDataSketch.HLL.from_enumerable(501..1500, p: 14)
      {:ok, upstream} = ListProducer.start_link([sketch_a, sketch_b])

      {:ok, stage} =
        SketchStage.start_link(
          sketch_module: ExDataSketch.HLL,
          sketch_opts: [p: 14],
          subscribe_to: [{upstream, max_demand: 10}]
        )

      # A producer_consumer only demands from upstream when it has
      # downstream demand of its own. Attach a forwarder to drive demand
      # and capture the re-emitted snapshots.
      {:ok, forwarder} =
        ForwardingConsumer.start_link(
          target: self(),
          subscribe_to: [{stage, max_demand: 10}]
        )

      # Wait for at least one re-emitted snapshot.
      assert_receive {:events, [_ | _]}, 500

      estimate = SketchStage.estimate(stage)
      # Union cardinality is 1500.
      assert_in_delta estimate, 1500.0, 1500 * 0.05

      GenStage.stop(forwarder)
      GenStage.stop(stage)
      GenStage.stop(upstream)
    end
  end
end
