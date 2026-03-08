defmodule ExDataSketch.FilterChainTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ExDataSketch.{Bloom, CQF, Cuckoo, FilterChain, IBLT, Quotient, XorFilter}

  # -- new/0 --

  describe "new/0" do
    test "creates empty chain" do
      chain = FilterChain.new()
      assert FilterChain.stages(chain) == []
      assert FilterChain.adjuncts(chain) == []
    end
  end

  # -- add_stage/2 --

  describe "add_stage/2" do
    test "adds Bloom as query stage" do
      chain = FilterChain.new() |> FilterChain.add_stage(Bloom.new(capacity: 100))
      assert length(FilterChain.stages(chain)) == 1
    end

    test "adds Cuckoo as query stage" do
      chain = FilterChain.new() |> FilterChain.add_stage(Cuckoo.new())
      assert length(FilterChain.stages(chain)) == 1
    end

    test "adds Quotient as query stage" do
      chain = FilterChain.new() |> FilterChain.add_stage(Quotient.new(q: 10, r: 8))
      assert length(FilterChain.stages(chain)) == 1
    end

    test "adds CQF as query stage" do
      chain = FilterChain.new() |> FilterChain.add_stage(CQF.new(q: 10, r: 8))
      assert length(FilterChain.stages(chain)) == 1
    end

    test "adds XorFilter as terminal query stage" do
      {:ok, xor} = XorFilter.build(["a", "b", "c"])
      chain = FilterChain.new() |> FilterChain.add_stage(xor)
      assert length(FilterChain.stages(chain)) == 1
    end

    test "adds IBLT to adjuncts" do
      chain = FilterChain.new() |> FilterChain.add_stage(IBLT.new())
      assert FilterChain.stages(chain) == []
      assert length(FilterChain.adjuncts(chain)) == 1
    end

    test "supports multiple query stages" do
      chain =
        FilterChain.new()
        |> FilterChain.add_stage(Bloom.new(capacity: 100))
        |> FilterChain.add_stage(Cuckoo.new())

      assert length(FilterChain.stages(chain)) == 2
    end

    test "rejects query stage after XorFilter terminal" do
      {:ok, xor} = XorFilter.build(["a", "b"])

      assert_raise ExDataSketch.Errors.InvalidChainCompositionError, fn ->
        FilterChain.new()
        |> FilterChain.add_stage(xor)
        |> FilterChain.add_stage(Bloom.new(capacity: 100))
      end
    end

    test "rejects second XorFilter after XorFilter terminal" do
      {:ok, xor1} = XorFilter.build(["a"])
      {:ok, xor2} = XorFilter.build(["b"])

      assert_raise ExDataSketch.Errors.InvalidChainCompositionError, fn ->
        FilterChain.new()
        |> FilterChain.add_stage(xor1)
        |> FilterChain.add_stage(xor2)
      end
    end

    test "allows IBLT after XorFilter terminal (adjunct)" do
      {:ok, xor} = XorFilter.build(["a"])

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(xor)
        |> FilterChain.add_stage(IBLT.new())

      assert length(FilterChain.stages(chain)) == 1
      assert length(FilterChain.adjuncts(chain)) == 1
    end
  end

  # -- member?/2 --

  describe "member?/2" do
    test "returns false for empty chain" do
      refute FilterChain.member?(FilterChain.new(), "hello")
    end

    test "single Bloom stage works" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(bloom)
      assert FilterChain.member?(chain, "hello")
      refute FilterChain.member?(chain, "world")
    end

    test "single Cuckoo stage works" do
      {:ok, cuckoo} = Cuckoo.new() |> Cuckoo.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(cuckoo)
      assert FilterChain.member?(chain, "hello")
      refute FilterChain.member?(chain, "world")
    end

    test "multi-stage: all stages must agree" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("hello") |> Bloom.put("shared")
      {:ok, cuckoo} = Cuckoo.new() |> Cuckoo.put("shared")

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(bloom)
        |> FilterChain.add_stage(cuckoo)

      # "shared" is in both stages
      assert FilterChain.member?(chain, "shared")
      # "hello" is only in Bloom, not Cuckoo -> false from Cuckoo short-circuits
      refute FilterChain.member?(chain, "hello")
    end

    test "XorFilter as terminal stage works" do
      {:ok, xor} = XorFilter.build(["a", "b", "c"])
      chain = FilterChain.new() |> FilterChain.add_stage(xor)
      assert FilterChain.member?(chain, "a")
      assert FilterChain.member?(chain, "b")
    end

    test "does not query adjuncts" do
      iblt = IBLT.new() |> IBLT.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(iblt)
      # IBLT is adjunct, not in query path -> empty query stages -> false
      refute FilterChain.member?(chain, "hello")
    end
  end

  # -- put/2 --

  describe "put/2" do
    test "inserts into Bloom stage" do
      chain = FilterChain.new() |> FilterChain.add_stage(Bloom.new(capacity: 100))
      {:ok, chain} = FilterChain.put(chain, "hello")
      assert FilterChain.member?(chain, "hello")
    end

    test "inserts into Cuckoo stage" do
      chain = FilterChain.new() |> FilterChain.add_stage(Cuckoo.new())
      {:ok, chain} = FilterChain.put(chain, "hello")
      assert FilterChain.member?(chain, "hello")
    end

    test "inserts into multiple dynamic stages" do
      chain =
        FilterChain.new()
        |> FilterChain.add_stage(Bloom.new(capacity: 100))
        |> FilterChain.add_stage(CQF.new(q: 10, r: 8))

      {:ok, chain} = FilterChain.put(chain, "hello")
      assert FilterChain.member?(chain, "hello")
    end

    test "skips XorFilter stage" do
      {:ok, xor} = XorFilter.build(["existing"])

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(Bloom.new(capacity: 100))
        |> FilterChain.add_stage(xor)

      {:ok, chain} = FilterChain.put(chain, "new_item")
      # Only Bloom gets the insert, XorFilter is skipped
      [bloom, _xor] = FilterChain.stages(chain)
      assert Bloom.member?(bloom, "new_item")
    end

    test "returns {:error, :full} when Cuckoo is full" do
      # Create a tiny Cuckoo that will fill quickly
      cuckoo = Cuckoo.new(capacity: 4)
      chain = FilterChain.new() |> FilterChain.add_stage(cuckoo)

      result =
        Enum.reduce_while(1..10_000, {:ok, chain}, fn i, {:ok, c} ->
          case FilterChain.put(c, "item_#{i}") do
            {:ok, updated} -> {:cont, {:ok, updated}}
            {:error, :full} -> {:halt, {:error, :full}}
          end
        end)

      assert result == {:error, :full}
    end

    test "put on empty chain succeeds" do
      {:ok, chain} = FilterChain.put(FilterChain.new(), "hello")
      assert FilterChain.stages(chain) == []
    end
  end

  # -- delete/2 --

  describe "delete/2" do
    test "deletes from Cuckoo stage" do
      {:ok, cuckoo} = Cuckoo.new() |> Cuckoo.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(cuckoo)
      {:ok, chain} = FilterChain.delete(chain, "hello")
      refute FilterChain.member?(chain, "hello")
    end

    test "deletes from CQF stage" do
      cqf = CQF.new(q: 10, r: 8) |> CQF.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(cqf)
      {:ok, chain} = FilterChain.delete(chain, "hello")
      refute FilterChain.member?(chain, "hello")
    end

    test "deletes from multiple stages" do
      {:ok, cuckoo} = Cuckoo.new() |> Cuckoo.put("hello")
      cqf = CQF.new(q: 10, r: 8) |> CQF.put("hello")

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(cuckoo)
        |> FilterChain.add_stage(cqf)

      {:ok, chain} = FilterChain.delete(chain, "hello")
      refute FilterChain.member?(chain, "hello")
    end

    test "raises UnsupportedOperationError if Bloom in chain" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(bloom)

      assert_raise ExDataSketch.Errors.UnsupportedOperationError, fn ->
        FilterChain.delete(chain, "hello")
      end
    end

    test "raises UnsupportedOperationError if XorFilter in chain" do
      {:ok, xor} = XorFilter.build(["hello"])
      chain = FilterChain.new() |> FilterChain.add_stage(xor)

      assert_raise ExDataSketch.Errors.UnsupportedOperationError, fn ->
        FilterChain.delete(chain, "hello")
      end
    end

    test "delete on empty chain succeeds" do
      {:ok, chain} = FilterChain.delete(FilterChain.new(), "hello")
      assert FilterChain.stages(chain) == []
    end
  end

  # -- stages/1 and adjuncts/1 --

  describe "stages/1 and adjuncts/1" do
    test "returns correct lists" do
      bloom = Bloom.new(capacity: 100)
      iblt = IBLT.new()

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(bloom)
        |> FilterChain.add_stage(iblt)

      assert length(FilterChain.stages(chain)) == 1
      assert length(FilterChain.adjuncts(chain)) == 1
      assert [%Bloom{}] = FilterChain.stages(chain)
      assert [%IBLT{}] = FilterChain.adjuncts(chain)
    end
  end

  # -- count/1 --

  describe "count/1" do
    test "empty chain has count 0" do
      assert FilterChain.count(FilterChain.new()) == 0
    end

    test "sum across stages" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("a") |> Bloom.put("b")

      chain = FilterChain.new() |> FilterChain.add_stage(bloom)
      # Bloom.count returns popcount of bitset, not item count
      assert FilterChain.count(chain) > 0
    end
  end

  # -- serialize/deserialize --

  describe "serialize/1 and deserialize/1" do
    test "round-trip with single Bloom" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("hello")
      chain = FilterChain.new() |> FilterChain.add_stage(bloom)
      binary = FilterChain.serialize(chain)
      {:ok, recovered} = FilterChain.deserialize(binary)

      assert length(FilterChain.stages(recovered)) == 1
      assert FilterChain.member?(recovered, "hello")
    end

    test "round-trip with multiple stages" do
      bloom = Bloom.new(capacity: 100) |> Bloom.put("shared")
      {:ok, cuckoo} = Cuckoo.new() |> Cuckoo.put("shared")

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(bloom)
        |> FilterChain.add_stage(cuckoo)

      binary = FilterChain.serialize(chain)
      {:ok, recovered} = FilterChain.deserialize(binary)

      assert length(FilterChain.stages(recovered)) == 2
      assert FilterChain.member?(recovered, "shared")
    end

    test "round-trip with adjuncts" do
      bloom = Bloom.new(capacity: 100)
      iblt = IBLT.new() |> IBLT.put("reconcile")

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(bloom)
        |> FilterChain.add_stage(iblt)

      binary = FilterChain.serialize(chain)
      {:ok, recovered} = FilterChain.deserialize(binary)

      assert length(FilterChain.stages(recovered)) == 1
      assert length(FilterChain.adjuncts(recovered)) == 1
    end

    test "round-trip with XorFilter terminal" do
      items = ["a", "b", "c"]
      bloom = Bloom.from_enumerable(items, capacity: 100)
      {:ok, xor} = XorFilter.build(items)

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(bloom)
        |> FilterChain.add_stage(xor)

      binary = FilterChain.serialize(chain)
      {:ok, recovered} = FilterChain.deserialize(binary)
      assert length(FilterChain.stages(recovered)) == 2
      assert FilterChain.member?(recovered, "a")
    end

    test "FCN1 magic" do
      binary = FilterChain.serialize(FilterChain.new())
      assert <<"FCN1", _rest::binary>> = binary
    end

    test "rejects invalid binary" do
      assert {:error, _} = FilterChain.deserialize(<<"BAAD", 1, 0, 0, 0>>)
    end

    test "rejects unsupported version" do
      assert {:error, _} = FilterChain.deserialize(<<"FCN1", 99, 0, 0, 0>>)
    end
  end

  # -- capabilities/0 --

  describe "capabilities/0" do
    test "returns expected capabilities" do
      caps = FilterChain.capabilities()
      assert MapSet.member?(caps, :new)
      assert MapSet.member?(caps, :put)
      assert MapSet.member?(caps, :member?)
      assert MapSet.member?(caps, :delete)
      assert MapSet.member?(caps, :serialize)
      assert MapSet.member?(caps, :deserialize)
    end
  end

  # -- size_bytes/1 --

  describe "size_bytes/1" do
    test "empty chain has 0 bytes" do
      assert FilterChain.size_bytes(FilterChain.new()) == 0
    end

    test "positive for chain with stages" do
      chain = FilterChain.new() |> FilterChain.add_stage(Bloom.new(capacity: 100))
      assert FilterChain.size_bytes(chain) > 0
    end
  end

  # -- Bloom capabilities/0 --

  describe "Bloom.capabilities/0" do
    test "Bloom has capabilities" do
      caps = Bloom.capabilities()
      assert MapSet.member?(caps, :put)
      assert MapSet.member?(caps, :member?)
      assert MapSet.member?(caps, :merge)
      refute MapSet.member?(caps, :delete)
    end
  end

  # -- lifecycle tier pattern --

  describe "lifecycle tier: Cuckoo -> XorFilter" do
    test "hot Cuckoo + cold XorFilter" do
      shared = ["shared_a", "shared_b"]
      {:ok, xor} = XorFilter.build(shared ++ ["old_only"])
      {:ok, cuckoo} = Cuckoo.new() |> Cuckoo.put("shared_a")
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "shared_b")
      {:ok, cuckoo} = Cuckoo.put(cuckoo, "new_only")

      chain =
        FilterChain.new()
        |> FilterChain.add_stage(cuckoo)
        |> FilterChain.add_stage(xor)

      # Items in both stages are members
      assert FilterChain.member?(chain, "shared_a")
      assert FilterChain.member?(chain, "shared_b")
      # "new_only" is in Cuckoo but not XorFilter -> false from XorFilter
      refute FilterChain.member?(chain, "new_only")
      # "old_only" is in XorFilter but not Cuckoo -> false from Cuckoo
      refute FilterChain.member?(chain, "old_only")
    end
  end

  # -- Property tests --

  describe "property: no false negatives through chain" do
    property "items inserted via put are found via member?" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20)
            ) do
        chain = FilterChain.new() |> FilterChain.add_stage(Bloom.new(capacity: 1000))

        chain =
          Enum.reduce(items, chain, fn item, acc ->
            {:ok, updated} = FilterChain.put(acc, item)
            updated
          end)

        Enum.each(items, fn item ->
          assert FilterChain.member?(chain, item),
                 "false negative for #{inspect(item)}"
        end)
      end
    end
  end

  describe "property: serialize/deserialize round-trip" do
    property "round-trip preserves membership" do
      check all(
              items <-
                list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 10)
            ) do
        bloom = Bloom.from_enumerable(items, capacity: 1000)
        chain = FilterChain.new() |> FilterChain.add_stage(bloom)
        binary = FilterChain.serialize(chain)
        {:ok, recovered} = FilterChain.deserialize(binary)

        Enum.each(items, fn item ->
          assert FilterChain.member?(recovered, item)
        end)
      end
    end
  end
end
