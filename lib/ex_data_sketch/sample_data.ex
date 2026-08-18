defmodule ExDataSketch.SampleData do
  @moduledoc """
  Sample-data generators for the tutorial livebooks under `livebooks/sketches/`.

  Not part of the sketch API -- this module exists purely so each livebook's
  "Sample data" section is a single function call instead of a duplicated
  cache-path/generate/cache-write block. Every function here generates a
  fixed *shape* of data (documented per function below) at a default size
  matching what its livebook actually uses, and transparently caches that
  default result under `System.tmp_dir!()`, so re-running a livebook (or a
  whole verification sweep across all of them) after the first pass is
  instant.

  Every generator also accepts real size overrides (`:count`, `:pool_size`,
  `:half_count`, or a couple of Theta/IBLT-specific keys -- see each
  function's `@doc`), primarily so this module's own regression tests don't
  need to generate millions of items to exercise it. **Caching only applies
  to the default (no-override) call** -- passing any option other than
  `:backend` skips the cache file entirely, so a small test run can never
  read stale-shaped data from, or overwrite, the real livebook cache.

  Randomized generators use the `ExDataSketch.Backend.Rust` NIF when it's
  available, since the defaults are 1-2 million items; pass `backend:
  ExDataSketch.Backend.Pure` to force the Elixir fallback (used
  automatically when the NIF isn't loaded). Deterministic range-based
  generators (no randomness involved -- `bloom_urls/1`, `cuckoo_sessions/1`,
  `quotient_api_keys/1`, `xor_filter_domains/1`, `filter_chain_users/1`,
  `theta_sets/1`, `iblt_keys/1`) are plain Elixir only: a single linear pass
  building formatted strings has nothing for a NIF to meaningfully
  accelerate.
  """

  alias ExDataSketch.{Backend, Nif}

  @doc """
  HLL tutorial: events drawn uniformly from a visitor pool.

  Options: `:count` (default 2,000,000), `:pool_size` (default 500,000).
  """
  @spec hll_events(keyword()) :: [String.t()]
  def hll_events(opts \\ []) do
    count = Keyword.get(opts, :count, 2_000_000)
    pool_size = Keyword.get(opts, :pool_size, 500_000)

    cached("hll_sample.bin", opts, fn ->
      string_events("visitor_", count, pool_size, 1.0, opts)
    end)
  end

  @doc """
  ULL tutorial: events drawn uniformly from a session pool.

  Options: `:count` (default 2,000,000), `:pool_size` (default 300,000).
  """
  @spec ull_events(keyword()) :: [String.t()]
  def ull_events(opts \\ []) do
    count = Keyword.get(opts, :count, 2_000_000)
    pool_size = Keyword.get(opts, :pool_size, 300_000)

    cached("ull_sample.bin", opts, fn ->
      string_events("session_", count, pool_size, 1.0, opts)
    end)
  end

  @doc """
  CMS tutorial: page-view events, power-law distributed (a few pages dominate).

  Options: `:count` (default 2,000,000), `:pool_size` (default 10,000).
  """
  @spec cms_events(keyword()) :: [String.t()]
  def cms_events(opts \\ []) do
    count = Keyword.get(opts, :count, 2_000_000)
    pool_size = Keyword.get(opts, :pool_size, 10_000)

    cached("cms_sample.bin", opts, fn -> string_events("page_", count, pool_size, 2.0, opts) end)
  end

  @doc """
  Theta tutorial: two overlapping user-ID sets with a known true union/intersection.

  Options: `:count_a` (default 600,000), `:count_b` (default 600,000),
  `:overlap` (default 200,000, must be `<= min(count_a, count_b)`). Set A is
  `1..count_a`; set B starts `overlap` items before set A ends, giving an
  intersection of exactly `overlap` items.
  """
  @spec theta_sets(keyword()) :: {[String.t()], [String.t()]}
  def theta_sets(opts \\ []) do
    count_a = Keyword.get(opts, :count_a, 600_000)
    count_b = Keyword.get(opts, :count_b, 600_000)
    overlap = Keyword.get(opts, :overlap, 200_000)

    cached("theta_sample.bin", opts, fn ->
      b_start = count_a - overlap + 1
      users_a = for i <- 1..count_a, do: "user_#{i}"
      users_b = for i <- b_start..(b_start + count_b - 1), do: "user_#{i}"
      {users_a, users_b}
    end)
  end

  @doc """
  KLL tutorial: simulated latencies (ms), mostly fast with a long tail.

  Options: `:count` (default 1,000,000).
  """
  @spec kll_latencies(keyword()) :: [float()]
  def kll_latencies(opts \\ []) do
    count = Keyword.get(opts, :count, 1_000_000)
    cached("kll_sample.bin", opts, fn -> kll_latency_values(count, opts) end)
  end

  @doc """
  DDSketch tutorial: operation durations (ms) spanning several orders of
  magnitude (fast API calls, medium DB queries, rare slow jobs).

  Options: `:count` (default 1,000,000).
  """
  @spec ddsketch_durations(keyword()) :: [float()]
  def ddsketch_durations(opts \\ []) do
    count = Keyword.get(opts, :count, 1_000_000)
    cached("ddsketch_sample.bin", opts, fn -> ddsketch_duration_values(count, opts) end)
  end

  @doc """
  REQ tutorial: latencies (ms), tight and boring in the bulk with a rare,
  important tail.

  Options: `:count` (default 1,000,000).
  """
  @spec req_latencies(keyword()) :: [float()]
  def req_latencies(opts \\ []) do
    count = Keyword.get(opts, :count, 1_000_000)
    cached("req_sample.bin", opts, fn -> req_latency_values(count, opts) end)
  end

  @doc """
  FrequentItems tutorial: search queries, power-law distributed.

  Options: `:count` (default 1,000,000), `:pool_size` (default 5,000).
  """
  @spec frequent_items_queries(keyword()) :: [String.t()]
  def frequent_items_queries(opts \\ []) do
    count = Keyword.get(opts, :count, 1_000_000)
    pool_size = Keyword.get(opts, :pool_size, 5_000)

    cached("frequent_items_sample.bin", opts, fn ->
      string_events("query_", count, pool_size, 3.0, opts)
    end)
  end

  @doc """
  MisraGries tutorial: same shape as `frequent_items_queries/1` (so the two
  tutorials are directly comparable), cached separately.

  Options: `:count` (default 1,000,000), `:pool_size` (default 5,000).
  """
  @spec misra_gries_queries(keyword()) :: [String.t()]
  def misra_gries_queries(opts \\ []) do
    count = Keyword.get(opts, :count, 1_000_000)
    pool_size = Keyword.get(opts, :pool_size, 5_000)

    cached("misra_gries_sample.bin", opts, fn ->
      string_events("query_", count, pool_size, 3.0, opts)
    end)
  end

  @doc """
  Bloom tutorial: inserted URLs plus an equal number of novel URLs to test
  the false-positive rate against.

  Options: `:half_count` (default 500,000).
  """
  @spec bloom_urls(keyword()) :: {[String.t()], [String.t()]}
  def bloom_urls(opts \\ []) do
    half_count = Keyword.get(opts, :half_count, 500_000)

    cached("bloom_sample.bin", opts, fn ->
      inserted_novel(half_count, &"https://example.com/page/#{&1}")
    end)
  end

  @doc """
  Cuckoo tutorial: inserted sessions plus an equal number of novel sessions.

  Options: `:half_count` (default 500,000).
  """
  @spec cuckoo_sessions(keyword()) :: {[String.t()], [String.t()]}
  def cuckoo_sessions(opts \\ []) do
    half_count = Keyword.get(opts, :half_count, 500_000)
    cached("cuckoo_sample.bin", opts, fn -> inserted_novel(half_count, &"session_#{&1}") end)
  end

  @doc """
  Quotient tutorial: inserted API keys plus an equal number of novel API keys.

  Options: `:half_count` (default 300,000).
  """
  @spec quotient_api_keys(keyword()) :: {[String.t()], [String.t()]}
  def quotient_api_keys(opts \\ []) do
    half_count = Keyword.get(opts, :half_count, 300_000)
    cached("quotient_sample.bin", opts, fn -> inserted_novel(half_count, &"api_key_#{&1}") end)
  end

  @doc """
  CQF tutorial: rate-limit-check events, power-law distributed.

  Options: `:count` (default 1,000,000), `:pool_size` (default 50,000).
  """
  @spec cqf_events(keyword()) :: [String.t()]
  def cqf_events(opts \\ []) do
    count = Keyword.get(opts, :count, 1_000_000)
    pool_size = Keyword.get(opts, :pool_size, 50_000)

    cached("cqf_sample.bin", opts, fn ->
      string_events("api_key_", count, pool_size, 2.0, opts)
    end)
  end

  @doc """
  XorFilter tutorial: blocklisted domains plus an equal number of novel
  (safe) domains.

  Options: `:half_count` (default 500,000).
  """
  @spec xor_filter_domains(keyword()) :: {[String.t()], [String.t()]}
  def xor_filter_domains(opts \\ []) do
    half_count = Keyword.get(opts, :half_count, 500_000)

    cached("xor_filter_sample.bin", opts, fn ->
      inserted_novel(half_count, &"malicious-domain-#{&1}.example")
    end)
  end

  @doc """
  IBLT tutorial: two mostly-agreeing key sets simulating drifted replicas.

  Options: `:shared_count` (default 200,000), `:only_a_count` (default 7),
  `:only_b_count` (default 5).
  """
  @spec iblt_keys(keyword()) :: {[String.t()], [String.t()], [String.t()], [String.t()]}
  def iblt_keys(opts \\ []) do
    shared_count = Keyword.get(opts, :shared_count, 200_000)
    only_a_count = Keyword.get(opts, :only_a_count, 7)
    only_b_count = Keyword.get(opts, :only_b_count, 5)

    cached("iblt_sample.bin", opts, fn ->
      shared = for i <- 1..shared_count, do: "key_#{i}"
      only_in_a = for i <- 1..only_a_count, do: "server_a_only_#{i}"
      only_in_b = for i <- 1..only_b_count, do: "server_b_only_#{i}"

      server_a_keys = shared ++ only_in_a
      server_b_keys = shared ++ only_in_b

      {server_a_keys, server_b_keys, only_in_a, only_in_b}
    end)
  end

  @doc """
  FilterChain tutorial: inserted users plus an equal number of novel users.

  Options: `:half_count` (default 500,000).
  """
  @spec filter_chain_users(keyword()) :: {[String.t()], [String.t()]}
  def filter_chain_users(opts \\ []) do
    half_count = Keyword.get(opts, :half_count, 500_000)
    cached("filter_chain_sample.bin", opts, fn -> inserted_novel(half_count, &"user_#{&1}") end)
  end

  # -- Shared helpers --

  # Caching only applies to the default (no-override) call -- any option
  # besides :backend changes the data's shape, so caching it under the same
  # filename as the canonical dataset would either serve a test run stale
  # production-scale data or, worse, overwrite the real livebook cache with
  # a tiny test-sized one.
  defp cached(filename, opts, generator) do
    if cacheable?(opts) do
      cache_path = Path.join(System.tmp_dir!(), "ex_data_sketch_livebook_cache/#{filename}")

      if File.exists?(cache_path) do
        IO.puts("Loading cached sample data from #{cache_path}")
        cache_path |> File.read!() |> :erlang.binary_to_term()
      else
        IO.puts("Generating sample data (this takes a few seconds)...")
        data = generator.()
        File.mkdir_p!(Path.dirname(cache_path))
        File.write!(cache_path, :erlang.term_to_binary(data))
        data
      end
    else
      generator.()
    end
  end

  defp cacheable?(opts), do: Keyword.keys(opts) -- [:backend] == []

  defp use_rust?(opts) do
    case Keyword.get(opts, :backend) do
      nil -> Backend.Rust.available?()
      Backend.Rust -> true
      Backend.Pure -> false
    end
  end

  # `count` strings "{prefix}{idx}", idx in 1..pool_size. exponent 1.0 gives
  # a uniform pool; exponent > 1.0 skews low indices to dominate (power-law).
  defp string_events(prefix, count, pool_size, exponent, opts) do
    if use_rust?(opts) do
      Nif.sample_data_string_events_nif(prefix, count, pool_size, exponent)
    else
      span = pool_size - 1

      for _ <- 1..count do
        idx = trunc(:math.pow(:rand.uniform(), exponent) * span) + 1
        "#{prefix}#{idx}"
      end
    end
  end

  defp kll_latency_values(count, opts) do
    if use_rust?(opts) do
      Nif.sample_data_kll_latencies_nif(count)
    else
      for _ <- 1..count, do: kll_latency_value()
    end
  end

  defp kll_latency_value do
    base = :rand.uniform() * :rand.uniform() * 200
    if :rand.uniform(100) == 1, do: base + :rand.uniform() * 2000, else: base
  end

  defp ddsketch_duration_values(count, opts) do
    if use_rust?(opts) do
      Nif.sample_data_ddsketch_durations_nif(count)
    else
      for _ <- 1..count, do: ddsketch_duration_value()
    end
  end

  defp ddsketch_duration_value do
    case :rand.uniform(1000) do
      n when n <= 900 -> :rand.uniform() * 50
      n when n <= 990 -> 50 + :rand.uniform() * 450
      _ -> 10_000 + :rand.uniform() * 90_000
    end
  end

  defp req_latency_values(count, opts) do
    if use_rust?(opts) do
      Nif.sample_data_req_latencies_nif(count)
    else
      for _ <- 1..count, do: req_latency_value()
    end
  end

  defp req_latency_value do
    if :rand.uniform(1000) == 1 do
      500 + :rand.uniform() * 4500
    else
      10 + :rand.uniform() * 20
    end
  end

  defp inserted_novel(half_count, formatter) do
    inserted = for i <- 1..half_count, do: formatter.(i)
    novel = for i <- (half_count + 1)..(2 * half_count), do: formatter.(i)
    {inserted, novel}
  end
end
