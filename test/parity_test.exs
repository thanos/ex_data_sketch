defmodule ExDataSketch.ParityTest do
  @moduledoc """
  Asserts byte-identical serialization and identical estimates between
  the Pure Elixir and Rust NIF backends for all sketch algorithms.

  Tagged :rust_nif — skipped when Rust backend is unavailable.
  """
  use ExUnit.Case, async: true

  alias ExDataSketch.Backend.{Pure, Rust}
  alias ExDataSketch.{CMS, HLL, Theta}

  # Deterministic input strings
  @items_1000 Enum.map(0..999, &"parity_item_#{&1}")
  @items_a Enum.map(0..499, &"set_a_#{&1}")
  @items_b Enum.map(500..999, &"set_b_#{&1}")

  describe "HLL parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and estimate" do
      pure = HLL.new(backend: Pure) |> HLL.update_many(@items_1000)
      rust = HLL.new(backend: Rust) |> HLL.update_many(@items_1000)

      assert HLL.serialize(pure) == HLL.serialize(rust)
      assert HLL.estimate(pure) == HLL.estimate(rust)
    end

    test "merge produces identical serialization and estimate" do
      pure_a = HLL.new(backend: Pure) |> HLL.update_many(@items_a)
      pure_b = HLL.new(backend: Pure) |> HLL.update_many(@items_b)
      pure_merged = HLL.merge(pure_a, pure_b)

      rust_a = HLL.new(backend: Rust) |> HLL.update_many(@items_a)
      rust_b = HLL.new(backend: Rust) |> HLL.update_many(@items_b)
      rust_merged = HLL.merge(rust_a, rust_b)

      assert HLL.serialize(pure_merged) == HLL.serialize(rust_merged)
      assert HLL.estimate(pure_merged) == HLL.estimate(rust_merged)
    end
  end

  describe "CMS parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and point estimates" do
      pure = CMS.new(backend: Pure) |> CMS.update_many(@items_1000)
      rust = CMS.new(backend: Rust) |> CMS.update_many(@items_1000)

      assert CMS.serialize(pure) == CMS.serialize(rust)

      # Verify point estimates match for a sample of items
      for item <- Enum.take(@items_1000, 20) do
        assert CMS.estimate(pure, item) == CMS.estimate(rust, item),
               "CMS estimate mismatch for #{item}"
      end
    end

    test "merge produces identical serialization and point estimates" do
      pure_a = CMS.new(backend: Pure) |> CMS.update_many(@items_a)
      pure_b = CMS.new(backend: Pure) |> CMS.update_many(@items_b)
      pure_merged = CMS.merge(pure_a, pure_b)

      rust_a = CMS.new(backend: Rust) |> CMS.update_many(@items_a)
      rust_b = CMS.new(backend: Rust) |> CMS.update_many(@items_b)
      rust_merged = CMS.merge(rust_a, rust_b)

      assert CMS.serialize(pure_merged) == CMS.serialize(rust_merged)

      for item <- Enum.take(@items_a ++ @items_b, 20) do
        assert CMS.estimate(pure_merged, item) == CMS.estimate(rust_merged, item),
               "CMS merge estimate mismatch for #{item}"
      end
    end
  end

  describe "Theta parity" do
    @describetag :rust_nif

    test "update_many produces identical serialization and estimate" do
      pure = Theta.new(backend: Pure) |> Theta.update_many(@items_1000)
      rust = Theta.new(backend: Rust) |> Theta.update_many(@items_1000)

      assert Theta.serialize(pure) == Theta.serialize(rust)
      assert Theta.estimate(pure) == Theta.estimate(rust)
    end

    test "merge produces identical serialization and estimate" do
      pure_a = Theta.new(backend: Pure) |> Theta.update_many(@items_a)
      pure_b = Theta.new(backend: Pure) |> Theta.update_many(@items_b)
      pure_merged = Theta.merge(pure_a, pure_b)

      rust_a = Theta.new(backend: Rust) |> Theta.update_many(@items_a)
      rust_b = Theta.new(backend: Rust) |> Theta.update_many(@items_b)
      rust_merged = Theta.merge(rust_a, rust_b)

      assert Theta.serialize(pure_merged) == Theta.serialize(rust_merged)
      assert Theta.estimate(pure_merged) == Theta.estimate(rust_merged)
    end

    test "compact + merge produces identical serialization and estimate" do
      pure_a = Theta.new(backend: Pure) |> Theta.update_many(@items_a) |> Theta.compact()
      pure_b = Theta.new(backend: Pure) |> Theta.update_many(@items_b) |> Theta.compact()
      pure_merged = Theta.merge(pure_a, pure_b)

      rust_a = Theta.new(backend: Rust) |> Theta.update_many(@items_a) |> Theta.compact()
      rust_b = Theta.new(backend: Rust) |> Theta.update_many(@items_b) |> Theta.compact()
      rust_merged = Theta.merge(rust_a, rust_b)

      assert Theta.serialize(pure_merged) == Theta.serialize(rust_merged)
      assert Theta.estimate(pure_merged) == Theta.estimate(rust_merged)
    end
  end
end
