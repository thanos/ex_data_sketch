defmodule Mix.Tasks.ExDataSketch.Gen.MigrationTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.ExDataSketch.Gen.Migration

  describe "run/1" do
    test "raises when --repo is not provided" do
      assert_raise Mix.Error, ~r/Expected --repo to be given/, fn ->
        Migration.run([])
      end
    end
  end

  describe "timestamp/0" do
    test "generates a 14-digit timestamp string" do
      ts = Migration.timestamp()
      assert String.match?(ts, ~r/^\d{14}$/)
    end

    test "timestamp is monotonically increasing" do
      ts1 = Migration.timestamp()
      Process.sleep(1)
      ts2 = Migration.timestamp()
      assert ts2 >= ts1
    end
  end
end
