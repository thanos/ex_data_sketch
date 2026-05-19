defmodule ExDataSketch.GenStageTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.GenStage.{SketchConsumer, SketchProducer, SketchStage}

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
  end
end
