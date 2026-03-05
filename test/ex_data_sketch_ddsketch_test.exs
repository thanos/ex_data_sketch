defmodule ExDataSketch.DDSketchTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.DDSketch

  describe "struct" do
    test "has correct fields" do
      sketch = %DDSketch{state: <<>>, opts: [alpha: 0.01], backend: ExDataSketch.Backend.Pure}
      assert sketch.state == <<>>
      assert sketch.opts == [alpha: 0.01]
      assert sketch.backend == ExDataSketch.Backend.Pure
    end
  end

  describe "option validation" do
    test "alpha defaults to 0.01" do
      # new/1 calls the backend which raises for now, so we test validation only
      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        DDSketch.new()
      end
    end

    test "alpha must be a float in (0.0, 1.0)" do
      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: 0.0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: 1.0)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: -0.5)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: 1.5)
      end

      assert_raise ExDataSketch.Errors.InvalidOptionError, ~r/alpha must be a float/, fn ->
        DDSketch.new(alpha: "not a float")
      end
    end

    test "valid alpha passes validation (backend raises since not implemented)" do
      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        DDSketch.new(alpha: 0.01)
      end

      assert_raise RuntimeError, ~r/not yet implemented/, fn ->
        DDSketch.new(alpha: 0.5)
      end
    end
  end

  describe "serialize/deserialize" do
    test "deserialize rejects invalid binary" do
      assert {:error, %ExDataSketch.Errors.DeserializationError{}} =
               DDSketch.deserialize(<<"invalid">>)
    end

    test "deserialize rejects wrong sketch ID" do
      # Build a valid EXSK binary with KLL sketch_id (4) instead of DDSketch (5)
      params = <<0.01::float-little-64>>
      state = <<0>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_kll(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               DDSketch.deserialize(binary)

      assert msg =~ "expected DDSketch sketch ID (5)"
    end

    test "deserialize rejects invalid alpha in params" do
      params = <<0.0::float-little-64>>
      state = <<0>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_ddsketch(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:error, %ExDataSketch.Errors.DeserializationError{message: msg}} =
               DDSketch.deserialize(binary)

      assert msg =~ "invalid DDSketch alpha"
    end

    test "deserialize accepts valid EXSK binary" do
      params = <<0.01::float-little-64>>
      state = <<1, 2, 3>>

      binary =
        ExDataSketch.Codec.encode(
          ExDataSketch.Codec.sketch_id_ddsketch(),
          ExDataSketch.Codec.version(),
          params,
          state
        )

      assert {:ok, sketch} = DDSketch.deserialize(binary)
      assert sketch.opts == [alpha: 0.01]
      assert sketch.state == <<1, 2, 3>>
    end
  end

  describe "codec" do
    test "sketch_id_ddsketch is 5" do
      assert ExDataSketch.Codec.sketch_id_ddsketch() == 5
    end
  end

  describe "convenience functions" do
    test "reducer returns a 2-arity function" do
      assert is_function(DDSketch.reducer(), 2)
    end

    test "merger returns a 2-arity function" do
      assert is_function(DDSketch.merger(), 2)
    end
  end
end
