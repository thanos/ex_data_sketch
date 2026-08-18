defmodule ExDataSketch.SampleDataTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.SampleData

  # Every generator function below is called with small :count/:pool_size/
  # :half_count overrides, which (per SampleData's own documented contract)
  # skips the real livebook cache entirely -- these tests never read or
  # write livebooks/sketches' `ex_data_sketch_livebook_cache` files except
  # in the "default call uses the cache" test at the bottom, which uses a
  # function cheap enough at real scale to run uncached in tens of ms.

  defp prefix_index(item, prefix) do
    prefix_size = byte_size(prefix)
    <<^prefix::binary-size(^prefix_size), rest::binary>> = item
    String.to_integer(rest)
  end

  describe "string_events-backed generators" do
    test "hll_events/1 returns :count strings within the pool, prefixed \"visitor_\"" do
      events = SampleData.hll_events(count: 500, pool_size: 50)
      assert length(events) == 500
      indices = Enum.map(events, &prefix_index(&1, "visitor_"))
      assert Enum.all?(indices, &(&1 >= 1 and &1 <= 50))
    end

    test "hll_events/1 (exponent 1.0, uniform) covers most of the pool at 20x oversampling" do
      events =
        SampleData.hll_events(count: 1000, pool_size: 50, backend: ExDataSketch.Backend.Pure)

      distinct = events |> MapSet.new() |> MapSet.size()
      assert distinct >= 45
    end

    test "ull_events/1 returns :count strings within the pool, prefixed \"session_\"" do
      events = SampleData.ull_events(count: 300, pool_size: 30)
      assert length(events) == 300
      indices = Enum.map(events, &prefix_index(&1, "session_"))
      assert Enum.all?(indices, &(&1 >= 1 and &1 <= 30))
    end

    test "cms_events/1 (power-law) skews heavily toward low indices" do
      events =
        SampleData.cms_events(
          count: 5000,
          pool_size: 100,
          backend: ExDataSketch.Backend.Pure
        )

      assert length(events) == 5000
      indices = Enum.map(events, &prefix_index(&1, "page_"))
      # Under a fair (exponent 1.0) draw the top decile would get ~10% of
      # the mass; exponent 2.0 power-law concentrates far more than that.
      top_decile_share =
        indices
        |> Enum.frequencies()
        |> Map.values()
        |> Enum.sort(:desc)
        |> Enum.take(10)
        |> Enum.sum()
        |> Kernel./(5000)

      assert top_decile_share > 0.2
    end

    test "frequent_items_queries/1 and misra_gries_queries/1 share the same generation shape" do
      fi = SampleData.frequent_items_queries(count: 400, pool_size: 40)
      mg = SampleData.misra_gries_queries(count: 400, pool_size: 40)
      assert length(fi) == 400
      assert length(mg) == 400
      assert Enum.all?(fi, &String.starts_with?(&1, "query_"))
      assert Enum.all?(mg, &String.starts_with?(&1, "query_"))
    end

    test "cqf_events/1 returns :count strings within the pool, prefixed \"api_key_\"" do
      events = SampleData.cqf_events(count: 400, pool_size: 40)
      assert length(events) == 400
      indices = Enum.map(events, &prefix_index(&1, "api_key_"))
      assert Enum.all?(indices, &(&1 >= 1 and &1 <= 40))
    end
  end

  describe "theta_sets/1" do
    test "set A, set B, and their intersection match the requested sizes exactly" do
      {users_a, users_b} = SampleData.theta_sets(count_a: 600, count_b: 600, overlap: 200)

      assert length(users_a) == 600
      assert length(users_b) == 600

      intersection =
        MapSet.intersection(MapSet.new(users_a), MapSet.new(users_b)) |> MapSet.size()

      assert intersection == 200
    end

    test "set B starts exactly :overlap items before set A ends" do
      {users_a, users_b} = SampleData.theta_sets(count_a: 60, count_b: 60, overlap: 20)
      assert Enum.at(users_a, 0) == "user_1"
      assert Enum.at(users_b, 0) == "user_41"
      assert Enum.at(users_b, -1) == "user_100"
    end
  end

  describe "numeric-value generators" do
    test "kll_latencies/1 returns :count non-negative floats" do
      values = SampleData.kll_latencies(count: 500, backend: ExDataSketch.Backend.Pure)
      assert length(values) == 500
      assert Enum.all?(values, &(is_float(&1) and &1 >= 0))
    end

    test "ddsketch_durations/1 returns :count non-negative floats spanning tiers" do
      values = SampleData.ddsketch_durations(count: 2000, backend: ExDataSketch.Backend.Pure)
      assert length(values) == 2000
      assert Enum.all?(values, &(is_float(&1) and &1 >= 0))
      # the rare slow-job tier (~1% of draws) should occasionally appear
      # and dwarf the fast-tier bulk at this sample size.
      assert Enum.max(values) > 1000
    end

    test "req_latencies/1 returns :count non-negative floats, mostly in the tight bulk range" do
      values = SampleData.req_latencies(count: 2000, backend: ExDataSketch.Backend.Pure)
      assert length(values) == 2000
      assert Enum.all?(values, &(is_float(&1) and &1 >= 0))
      bulk = Enum.count(values, &(&1 < 30))
      assert bulk / 2000 > 0.9
    end
  end

  describe "inserted/novel generators" do
    test "bloom_urls/1 returns disjoint inserted and novel sets of the requested size" do
      {inserted, novel} = SampleData.bloom_urls(half_count: 50)
      assert length(inserted) == 50
      assert length(novel) == 50
      assert MapSet.disjoint?(MapSet.new(inserted), MapSet.new(novel))
      assert hd(inserted) == "https://example.com/page/1"
    end

    test "cuckoo_sessions/1 returns disjoint inserted and novel sessions" do
      {inserted, novel} = SampleData.cuckoo_sessions(half_count: 50)
      assert length(inserted) == 50
      assert length(novel) == 50
      assert MapSet.disjoint?(MapSet.new(inserted), MapSet.new(novel))
    end

    test "quotient_api_keys/1 returns disjoint inserted and novel keys" do
      {inserted, novel} = SampleData.quotient_api_keys(half_count: 50)
      assert length(inserted) == 50
      assert length(novel) == 50
      assert MapSet.disjoint?(MapSet.new(inserted), MapSet.new(novel))
    end

    test "xor_filter_domains/1 returns disjoint blocklist and novel domains" do
      {blocklist, novel} = SampleData.xor_filter_domains(half_count: 50)
      assert length(blocklist) == 50
      assert length(novel) == 50
      assert MapSet.disjoint?(MapSet.new(blocklist), MapSet.new(novel))
      assert hd(blocklist) == "malicious-domain-1.example"
    end

    test "filter_chain_users/1 returns disjoint inserted and novel users" do
      {inserted, novel} = SampleData.filter_chain_users(half_count: 50)
      assert length(inserted) == 50
      assert length(novel) == 50
      assert MapSet.disjoint?(MapSet.new(inserted), MapSet.new(novel))
    end
  end

  describe "iblt_keys/1" do
    test "server key sets are exactly shared plus each side's own-only keys" do
      {server_a_keys, server_b_keys, only_in_a, only_in_b} =
        SampleData.iblt_keys(shared_count: 20, only_a_count: 3, only_b_count: 2)

      assert length(only_in_a) == 3
      assert length(only_in_b) == 2
      assert length(server_a_keys) == 23
      assert length(server_b_keys) == 22

      diff_a = MapSet.difference(MapSet.new(server_a_keys), MapSet.new(server_b_keys))
      diff_b = MapSet.difference(MapSet.new(server_b_keys), MapSet.new(server_a_keys))
      assert diff_a == MapSet.new(only_in_a)
      assert diff_b == MapSet.new(only_in_b)
    end
  end

  describe "backend override" do
    test "backend: Pure always works, regardless of NIF availability" do
      events = SampleData.hll_events(count: 10, pool_size: 5, backend: ExDataSketch.Backend.Pure)
      assert length(events) == 10
    end

    @tag :rust_nif
    test "backend: Rust works when the NIF is available" do
      events = SampleData.hll_events(count: 10, pool_size: 5, backend: ExDataSketch.Backend.Rust)
      assert length(events) == 10
    end
  end

  describe "caching" do
    test "any option besides :backend skips the cache file entirely" do
      cache_path =
        Path.join(System.tmp_dir!(), "ex_data_sketch_livebook_cache/iblt_sample.bin")

      before = if File.exists?(cache_path), do: File.read!(cache_path)

      SampleData.iblt_keys(shared_count: 3, only_a_count: 1, only_b_count: 1)

      after_call = if File.exists?(cache_path), do: File.read!(cache_path)
      assert before == after_call
    end

    test "the default (no-override) call writes the cache file" do
      cache_path =
        Path.join(System.tmp_dir!(), "ex_data_sketch_livebook_cache/iblt_sample.bin")

      File.rm(cache_path)
      refute File.exists?(cache_path)

      SampleData.iblt_keys()

      assert File.exists?(cache_path)
    end
  end
end
