defmodule ExDataSketch.FrequentItemsMockTest do
  @moduledoc """
  Mox contract tests verifying FrequentItems delegates correctly to the backend.
  """
  use ExUnit.Case, async: true

  import Mox

  alias ExDataSketch.FrequentItems

  setup :verify_on_exit!

  # A minimal valid FI1 empty state for k=5, flags=0
  @empty_state <<"FI1\0", 1::8, 0::8, 0::unsigned-little-16, 5::unsigned-little-32,
                 0::unsigned-little-64, 0::unsigned-little-32, 0::unsigned-little-32,
                 0::unsigned-little-32>>

  @updated_state <<"FI1\0", 1::8, 0::8, 0::unsigned-little-16, 5::unsigned-little-32,
                   1::unsigned-little-64, 1::unsigned-little-32, 0::unsigned-little-32,
                   0::unsigned-little-32, 5::unsigned-little-32, "hello"::binary,
                   1::unsigned-little-64, 0::unsigned-little-64>>

  describe "update/2 delegates to fi_update_many" do
    test "calls fi_update_many with single-element list, not fi_update" do
      ExDataSketch.MockBackend
      |> expect(:fi_new, fn _opts -> @empty_state end)
      |> expect(:fi_update_many, fn state, items, _opts ->
        assert state == @empty_state
        assert items == ["hello"]
        @updated_state
      end)

      sketch = FrequentItems.new(k: 5, backend: ExDataSketch.MockBackend)
      updated = FrequentItems.update(sketch, "hello")
      assert updated.state == @updated_state
    end
  end

  describe "merge/2 delegates to fi_merge" do
    test "calls fi_merge with correct state binaries" do
      state_a = @empty_state
      state_b = @updated_state

      ExDataSketch.MockBackend
      |> expect(:fi_new, 2, fn _opts -> @empty_state end)
      |> expect(:fi_update_many, fn _state, _items, _opts -> @updated_state end)
      |> expect(:fi_merge, fn a, b, _opts ->
        assert a == state_a
        assert b == state_b
        @updated_state
      end)

      sketch_a = FrequentItems.new(k: 5, backend: ExDataSketch.MockBackend)
      sketch_b = FrequentItems.new(k: 5, backend: ExDataSketch.MockBackend)
      sketch_b = FrequentItems.update(sketch_b, "hello")

      merged = FrequentItems.merge(sketch_a, sketch_b)
      assert merged.state == @updated_state
    end
  end

  describe "error propagation" do
    test "estimate propagates {:error, :not_tracked} from backend" do
      ExDataSketch.MockBackend
      |> expect(:fi_new, fn _opts -> @empty_state end)
      |> expect(:fi_estimate, fn _state, _item_bytes, _opts -> {:error, :not_tracked} end)

      sketch = FrequentItems.new(k: 5, backend: ExDataSketch.MockBackend)
      assert {:error, :not_tracked} = FrequentItems.estimate(sketch, "missing")
    end

    test "estimate propagates {:ok, map} from backend" do
      result = %{estimate: 5, error: 1, lower: 4, upper: 5}

      ExDataSketch.MockBackend
      |> expect(:fi_new, fn _opts -> @empty_state end)
      |> expect(:fi_estimate, fn _state, _item_bytes, _opts -> {:ok, result} end)

      sketch = FrequentItems.new(k: 5, backend: ExDataSketch.MockBackend)
      assert {:ok, ^result} = FrequentItems.estimate(sketch, "item")
    end
  end
end
