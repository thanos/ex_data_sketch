defmodule ExDataSketch.BackendTest.StubBackend do
  @moduledoc false
  @behaviour ExDataSketch.Backend

  def hll_new(_opts), do: <<>>
  def hll_update(s, _h, _o), do: s
  def hll_update_many(s, _h, _o), do: s
  def hll_merge(s, _b, _o), do: s
  def hll_estimate(_s, _o), do: 0.0
  def cms_new(_opts), do: <<>>
  def cms_update(s, _h, _i, _o), do: s
  def cms_update_many(s, _p, _o), do: s
  def cms_merge(s, _b, _o), do: s
  def cms_estimate(_s, _h, _o), do: 0
  def theta_new(_opts), do: <<>>
  def theta_update(s, _h, _o), do: s
  def theta_update_many(s, _h, _o), do: s
  def theta_compact(s, _o), do: s
  def theta_merge(s, _b, _o), do: s
  def theta_estimate(_s, _o), do: 0.0
  def theta_from_components(_k, _t, _e), do: <<>>
  def kll_new(_opts), do: <<>>
  def kll_update(s, _v, _o), do: s
  def kll_update_many(s, _v, _o), do: s
  def kll_merge(s, _b, _o), do: s
  def kll_quantile(_s, _r, _o), do: nil
  def kll_rank(_s, _v, _o), do: nil
  def kll_count(_s, _o), do: 0
  def kll_min(_s, _o), do: nil
  def kll_max(_s, _o), do: nil
  def kll_cdf(_s, _sp, _o), do: nil
  def kll_pmf(_s, _sp, _o), do: nil
  def ddsketch_new(_opts), do: <<>>
  def ddsketch_update(s, _v, _o), do: s
  def ddsketch_update_many(s, _v, _o), do: s
  def ddsketch_merge(s, _b, _o), do: s
  def ddsketch_quantile(_s, _r, _o), do: nil
  def ddsketch_count(_s, _o), do: 0
  def ddsketch_min(_s, _o), do: nil
  def ddsketch_max(_s, _o), do: nil
  def ddsketch_rank(_s, _v, _o), do: nil
  def req_new(_opts), do: <<>>
  def req_update(s, _v, _o), do: s
  def req_update_many(s, _v, _o), do: s
  def req_merge(s, _b, _o), do: s
  def req_quantile(_s, _r, _o), do: nil
  def req_rank(_s, _v, _o), do: nil
  def req_cdf(_s, _sp, _o), do: nil
  def req_pmf(_s, _sp, _o), do: nil
  def req_count(_s, _o), do: 0
  def req_min(_s, _o), do: nil
  def req_max(_s, _o), do: nil
  def mg_new(_opts), do: <<>>
  def mg_update(s, _ib, _o), do: s
  def mg_update_many(s, _items, _o), do: s
  def mg_merge(s, _b, _o), do: s
  def mg_estimate(_s, _ib, _o), do: 0
  def mg_top_k(_s, _l, _o), do: []
  def mg_count(_s, _o), do: 0
  def mg_entry_count(_s, _o), do: 0
  def ull_new(_opts), do: <<>>
  def ull_update(s, _h, _o), do: s
  def ull_update_many(s, _h, _o), do: s
  def ull_merge(s, _b, _o), do: s
  def ull_estimate(_s, _o), do: 0.0
  def bloom_new(_opts), do: <<>>
  def bloom_put(s, _h, _o), do: s
  def bloom_put_many(s, _h, _o), do: s
  def bloom_member?(_s, _h, _o), do: false
  def bloom_merge(s, _b, _o), do: s
  def bloom_count(_s, _o), do: 0
  def cuckoo_new(_opts), do: <<>>
  def cuckoo_put(s, _h, _o), do: {:ok, s}
  def cuckoo_put_many(s, _h, _o), do: {:ok, s}
  def cuckoo_member?(_s, _h, _o), do: false
  def cuckoo_delete(_s, _h, _o), do: {:error, :not_found}
  def cuckoo_count(_s, _o), do: 0
  def fi_new(_opts), do: <<>>
  def fi_update(s, _ib, _o), do: s
  def fi_update_many(s, _items, _o), do: s
  def fi_merge(s, _b, _o), do: s
  def fi_estimate(_s, _ib, _o), do: {:error, :not_tracked}
  def fi_top_k(_s, _l, _o), do: []
  def fi_count(_s, _o), do: 0
  def fi_entry_count(_s, _o), do: 0
  def quotient_new(_opts), do: <<>>
  def quotient_put(s, _h, _o), do: s
  def quotient_put_many(s, _h, _o), do: s
  def quotient_member?(_s, _h, _o), do: false
  def quotient_delete(s, _h, _o), do: s
  def quotient_merge(s, _b, _o), do: s
  def quotient_count(_s, _o), do: 0
  def cqf_new(_opts), do: <<>>
  def cqf_put(s, _h, _o), do: s
  def cqf_put_many(s, _h, _o), do: s
  def cqf_member?(_s, _h, _o), do: false
  def cqf_estimate_count(_s, _h, _o), do: 0
  def cqf_delete(s, _h, _o), do: s
  def cqf_merge(s, _b, _o), do: s
  def cqf_count(_s, _o), do: 0
  def xor_build(_hashes, _o), do: {:ok, <<>>}
  def xor_member?(_s, _h, _o), do: false
  def xor_count(_s, _o), do: 0
  def iblt_new(_opts), do: <<>>
  def iblt_put(s, _kh, _vh, _o), do: s
  def iblt_put_many(s, _pairs, _o), do: s
  def iblt_member?(_s, _kh, _o), do: false
  def iblt_delete(s, _kh, _vh, _o), do: s
  def iblt_subtract(s, _b, _o), do: s
  def iblt_list_entries(_s, _o), do: {:ok, %{positive: [], negative: []}}
  def iblt_count(_s, _o), do: 0
  def iblt_merge(s, _b, _o), do: s
end

defmodule ExDataSketch.BackendTest do
  use ExUnit.Case, async: false

  alias ExDataSketch.Backend
  alias ExDataSketch.Backend.{Pure, Rust}
  alias ExDataSketch.BackendTest.StubBackend

  describe "Backend.default/0" do
    setup do
      original = Application.get_env(:ex_data_sketch, :backend)
      on_exit(fn -> Application.put_env(:ex_data_sketch, :backend, original) end)
    end

    test "returns Pure when no config" do
      Application.delete_env(:ex_data_sketch, :backend)
      assert Backend.default() == Pure
    end

    @tag :rust_nif
    test "returns Rust when configured and available" do
      Application.put_env(:ex_data_sketch, :backend, Rust)
      assert Backend.default() == Rust
    end

    @tag :no_rust_nif
    test "returns Pure when Rust configured but not available" do
      Application.put_env(:ex_data_sketch, :backend, Rust)
      assert Backend.default() == Pure
    end

    test "returns custom module when configured" do
      Application.put_env(:ex_data_sketch, :backend, StubBackend)
      assert Backend.default() == StubBackend
    end
  end

  describe "Backend.resolve/1" do
    test "returns explicit backend from options" do
      assert Backend.resolve(backend: Rust) == Rust
      assert Backend.resolve(backend: Pure) == Pure
    end

    test "falls back to default when no backend option" do
      assert Backend.resolve([]) == Backend.default()
    end
  end

  describe "Rust.available?/0" do
    test "returns a boolean" do
      assert is_boolean(Rust.available?())
    end
  end

  describe "Nif stubs" do
    @tag :no_rust_nif
    test "nif_loaded raises when NIF is not loaded" do
      assert_raise ErlangError, fn -> ExDataSketch.Nif.nif_loaded() end
    end

    @tag :rust_nif
    test "nif_loaded returns :ok when NIF is loaded" do
      assert ExDataSketch.Nif.nif_loaded() == :ok
    end

    @tag :no_rust_nif
    test "hll stubs raise when NIF is not loaded" do
      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.hll_update_many_nif(<<>>, <<>>, 14)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.hll_update_many_dirty_nif(<<>>, <<>>, 14)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.hll_merge_nif(<<>>, <<>>, 14)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.hll_merge_dirty_nif(<<>>, <<>>, 14)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.hll_estimate_nif(<<>>, 14)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.hll_estimate_dirty_nif(<<>>, 14)
      end
    end

    @tag :no_rust_nif
    test "cms stubs raise when NIF is not loaded" do
      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.cms_update_many_nif(<<>>, <<>>, 1, 1, 32)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.cms_update_many_dirty_nif(<<>>, <<>>, 1, 1, 32)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.cms_merge_nif(<<>>, <<>>, 1, 1, 32)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.cms_merge_dirty_nif(<<>>, <<>>, 1, 1, 32)
      end
    end

    @tag :no_rust_nif
    test "theta stubs raise when NIF is not loaded" do
      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.theta_update_many_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.theta_update_many_dirty_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.theta_compact_nif(<<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.theta_compact_dirty_nif(<<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.theta_merge_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.theta_merge_dirty_nif(<<>>, <<>>)
      end
    end

    @tag :no_rust_nif
    test "kll stubs raise when NIF is not loaded" do
      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.kll_update_many_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.kll_update_many_dirty_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.kll_merge_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.kll_merge_dirty_nif(<<>>, <<>>)
      end
    end

    @tag :no_rust_nif
    test "ddsketch stubs raise when NIF is not loaded" do
      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.ddsketch_update_many_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.ddsketch_update_many_dirty_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.ddsketch_merge_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.ddsketch_merge_dirty_nif(<<>>, <<>>)
      end
    end

    @tag :no_rust_nif
    test "frequent_items stubs raise when NIF is not loaded" do
      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_new_nif(10, 0)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_update_many_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_update_many_dirty_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_merge_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_merge_dirty_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_estimate_nif(<<>>, <<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_top_k_nif(<<>>, 10)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_count_nif(<<>>)
      end

      assert_raise ErlangError, fn ->
        ExDataSketch.Nif.fi_entry_count_nif(<<>>)
      end
    end
  end

  describe "Rust backend dirty scheduler paths" do
    @tag :rust_nif
    test "hll_update_many uses dirty scheduler when exceeding threshold" do
      opts = [p: 14, backend: Rust]
      state = Rust.hll_new(opts)
      hashes = Enum.map(1..5, &ExDataSketch.Hash.hash64/1)
      result = Rust.hll_update_many(state, hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "cms_update_many uses dirty scheduler when exceeding threshold" do
      opts = [width: 64, depth: 3, counter_width: 32, backend: Rust]
      state = Rust.cms_new(opts)
      pairs = Enum.map(1..5, fn i -> {ExDataSketch.Hash.hash64(i), 1} end)
      result = Rust.cms_update_many(state, pairs, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "cms_merge uses dirty scheduler when exceeding threshold" do
      opts = [width: 64, depth: 3, counter_width: 32, backend: Rust]
      a = Rust.cms_new(opts)
      b = Rust.cms_new(opts)
      result = Rust.cms_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "theta_update_many uses dirty scheduler when exceeding threshold" do
      opts = [k: 64, backend: Rust]
      state = Rust.theta_new(opts)
      hashes = Enum.map(1..5, &ExDataSketch.Hash.hash64/1)
      result = Rust.theta_update_many(state, hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "theta_merge uses dirty scheduler when exceeding threshold" do
      opts = [k: 64, backend: Rust]
      a = Rust.theta_new(opts)
      b = Rust.theta_new(opts)
      a = Rust.theta_update(a, ExDataSketch.Hash.hash64("a"), opts)
      b = Rust.theta_update(b, ExDataSketch.Hash.hash64("b"), opts)
      result = Rust.theta_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end
  end

  describe "Rust backend theta_from_components" do
    @tag :rust_nif
    test "delegates to Pure" do
      k = 64
      theta = 0xFFFFFFFFFFFFFFFF
      entries = [100, 200, 300]
      result = Rust.theta_from_components(k, theta, entries)
      assert result == Pure.theta_from_components(k, theta, entries)
    end
  end

  describe "Rust backend empty list early returns" do
    @tag :rust_nif
    test "bloom_put_many with empty list returns state unchanged" do
      opts = [
        capacity: 100,
        hash_count: 7,
        bit_count: 958,
        seed: 0,
        backend: Rust
      ]

      state = Rust.bloom_new(opts)
      result = Rust.bloom_put_many(state, [], opts)
      assert result == state
    end

    @tag :rust_nif
    test "cuckoo_put_many with empty list returns {:ok, state}" do
      opts = [
        fingerprint_size: 8,
        bucket_size: 4,
        bucket_count: 64,
        max_kicks: 500,
        seed: 0,
        backend: Rust
      ]

      state = Rust.cuckoo_new(opts)
      assert {:ok, ^state} = Rust.cuckoo_put_many(state, [], opts)
    end

    @tag :rust_nif
    test "quotient_put_many with empty list returns state unchanged" do
      opts = [q: 8, r: 5, slot_count: 256, backend: Rust]
      state = Rust.quotient_new(opts)
      result = Rust.quotient_put_many(state, [], opts)
      assert result == state
    end

    @tag :rust_nif
    test "cqf_put_many with empty list returns state unchanged" do
      opts = [q: 8, r: 5, slot_count: 256, backend: Rust]
      state = Rust.cqf_new(opts)
      result = Rust.cqf_put_many(state, [], opts)
      assert result == state
    end

    @tag :rust_nif
    test "iblt_put_many with empty list returns state unchanged" do
      opts = [cell_count: 64, hash_count: 3, seed: 0, backend: Rust]
      state = Rust.iblt_new(opts)
      result = Rust.iblt_put_many(state, [], opts)
      assert result == state
    end

    @tag :rust_nif
    test "xor_build with empty list delegates to Pure" do
      opts = [fingerprint_bits: 8, seed: 0, backend: Rust]
      result = Rust.xor_build([], opts)
      expected = Pure.xor_build([], opts)
      assert result == expected
    end
  end

  describe "Rust backend membership filter dirty scheduler paths" do
    @tag :rust_nif
    test "bloom_put_many uses dirty scheduler when exceeding threshold" do
      opts = [
        capacity: 100,
        hash_count: 7,
        bit_count: 958,
        seed: 0,
        backend: Rust
      ]

      state = Rust.bloom_new(opts)
      hashes = Enum.map(1..5, &ExDataSketch.Hash.hash64/1)
      result = Rust.bloom_put_many(state, hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "bloom_merge uses dirty scheduler when exceeding threshold" do
      opts = [
        capacity: 100,
        hash_count: 7,
        bit_count: 958,
        seed: 0,
        backend: Rust
      ]

      a = Rust.bloom_new(opts)
      b = Rust.bloom_new(opts)
      result = Rust.bloom_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "cuckoo_put_many uses dirty scheduler when exceeding threshold" do
      opts = [
        fingerprint_size: 8,
        bucket_size: 4,
        bucket_count: 64,
        max_kicks: 500,
        seed: 0,
        backend: Rust
      ]

      state = Rust.cuckoo_new(opts)
      hashes = Enum.map(1..5, &ExDataSketch.Hash.hash64/1)
      result = Rust.cuckoo_put_many(state, hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert {:ok, bin} = result
      assert is_binary(bin)
    end

    @tag :rust_nif
    test "quotient_put_many uses dirty scheduler when exceeding threshold" do
      opts = [q: 8, r: 5, slot_count: 256, backend: Rust]
      state = Rust.quotient_new(opts)
      hashes = Enum.map(1..5, &ExDataSketch.Hash.hash64/1)
      result = Rust.quotient_put_many(state, hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "quotient_merge uses dirty scheduler when exceeding threshold" do
      opts = [q: 8, r: 5, slot_count: 256, backend: Rust]
      a = Rust.quotient_new(opts)
      b = Rust.quotient_new(opts)
      result = Rust.quotient_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "cqf_put_many uses dirty scheduler when exceeding threshold" do
      opts = [q: 8, r: 5, slot_count: 256, backend: Rust]
      state = Rust.cqf_new(opts)
      hashes = Enum.map(1..5, &ExDataSketch.Hash.hash64/1)
      result = Rust.cqf_put_many(state, hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "cqf_merge uses dirty scheduler when exceeding threshold" do
      opts = [q: 8, r: 5, slot_count: 256, backend: Rust]
      a = Rust.cqf_new(opts)
      b = Rust.cqf_new(opts)
      result = Rust.cqf_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "xor_build uses dirty scheduler when exceeding threshold" do
      hashes = Enum.map(1..20, &ExDataSketch.Hash.hash64/1) |> Enum.uniq()
      opts = [fingerprint_bits: 8, seed: 0, backend: Rust]
      result = Rust.xor_build(hashes, Keyword.put(opts, :dirty_threshold, 0))
      assert {:ok, bin} = result
      assert is_binary(bin)
    end

    @tag :rust_nif
    test "iblt_put_many uses dirty scheduler when exceeding threshold" do
      opts = [cell_count: 64, hash_count: 3, seed: 0, backend: Rust]
      state = Rust.iblt_new(opts)

      pairs =
        Enum.map(1..5, fn i ->
          {ExDataSketch.Hash.hash64("k#{i}"), ExDataSketch.Hash.hash64("v#{i}")}
        end)

      result = Rust.iblt_put_many(state, pairs, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "iblt_merge uses dirty scheduler when exceeding threshold" do
      opts = [cell_count: 64, hash_count: 3, seed: 0, backend: Rust]
      a = Rust.iblt_new(opts)
      b = Rust.iblt_new(opts)
      result = Rust.iblt_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "kll_update_many uses dirty scheduler when exceeding threshold" do
      opts = [k: 200, backend: Rust]
      state = Rust.kll_new(opts)
      values = Enum.map(1..5, &(&1 * 1.0))
      result = Rust.kll_update_many(state, values, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "kll_merge uses dirty scheduler when exceeding threshold" do
      opts = [k: 200, backend: Rust]
      a = Rust.kll_new(opts)
      b = Rust.kll_new(opts)
      a = Rust.kll_update(a, 1.0, opts)
      b = Rust.kll_update(b, 2.0, opts)
      result = Rust.kll_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "ddsketch_update_many uses dirty scheduler when exceeding threshold" do
      opts = [alpha: 0.01, backend: Rust]
      state = Rust.ddsketch_new(opts)
      values = Enum.map(1..5, &(&1 * 1.0))
      result = Rust.ddsketch_update_many(state, values, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "ddsketch_merge uses dirty scheduler when exceeding threshold" do
      opts = [alpha: 0.01, backend: Rust]
      a = Rust.ddsketch_new(opts)
      b = Rust.ddsketch_new(opts)
      a = Rust.ddsketch_update(a, 1.0, opts)
      b = Rust.ddsketch_update(b, 2.0, opts)
      result = Rust.ddsketch_merge(a, b, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "fi_update_many uses dirty scheduler when exceeding threshold" do
      opts = [k: 10, flags: 0, backend: Rust]
      state = Rust.fi_new(opts)
      items = Enum.map(1..5, &"item_#{&1}")
      result = Rust.fi_update_many(state, items, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end

    @tag :rust_nif
    test "fi_merge uses dirty scheduler when exceeding threshold" do
      opts = [k: 10, flags: 0, dirty_threshold: 0, backend: Rust]
      a = Rust.fi_new(opts)
      b = Rust.fi_new(opts)
      a = Rust.fi_update(a, "x", opts)
      b = Rust.fi_update(b, "y", opts)
      result = Rust.fi_merge(a, b, opts)
      assert is_binary(result)
    end

    @tag :rust_nif
    test "theta_compact uses dirty scheduler when exceeding threshold" do
      opts = [k: 64, backend: Rust]
      state = Rust.theta_new(opts)
      state = Rust.theta_update(state, ExDataSketch.Hash.hash64("a"), opts)
      result = Rust.theta_compact(state, Keyword.put(opts, :dirty_threshold, 0))
      assert is_binary(result)
    end
  end

  describe "Rust backend cuckoo error translation" do
    @tag :rust_nif
    test "cuckoo_put_many returns {:error, :full, binary} when filter is full" do
      # Tiny filter that fills quickly
      opts = [
        fingerprint_size: 8,
        bucket_size: 2,
        bucket_count: 4,
        max_kicks: 10,
        seed: 0,
        dirty_threshold: 100_000,
        backend: Rust
      ]

      state = Rust.cuckoo_new(opts)
      # Generate many unique hashes to overflow the tiny filter
      hashes = Enum.map(1..100, &ExDataSketch.Hash.hash64("overflow_#{&1}"))

      result = Rust.cuckoo_put_many(state, hashes, opts)

      case result do
        {:ok, _bin} -> :ok
        {:error, :full, bin} -> assert is_binary(bin)
      end
    end
  end

  describe "Rust backend unwrap_ok! error path" do
    @tag :rust_nif
    test "raises on NIF error" do
      assert_raise RuntimeError, ~r/Rust NIF error/, fn ->
        # Pass an impossibly short binary to bloom merge
        Rust.bloom_merge(<<>>, <<>>, bit_count: 100, dirty_threshold: 100_000, backend: Rust)
      end
    end
  end

  describe "ensure_binaries/1" do
    test "passes binaries through unchanged" do
      assert Rust.ensure_binaries(["a", "b", "c"]) == ["a", "b", "c"]
    end

    test "converts non-binary terms via term_to_binary" do
      result = Rust.ensure_binaries([123, :atom, {1, 2}])
      assert length(result) == 3
      assert Enum.all?(result, &is_binary/1)
      assert Enum.at(result, 0) == :erlang.term_to_binary(123)
      assert Enum.at(result, 1) == :erlang.term_to_binary(:atom)
      assert Enum.at(result, 2) == :erlang.term_to_binary({1, 2})
    end

    test "handles mixed binary and non-binary items" do
      result = Rust.ensure_binaries(["hello", 42, "world"])
      assert result == ["hello", :erlang.term_to_binary(42), "world"]
    end

    test "returns empty list for empty input" do
      assert Rust.ensure_binaries([]) == []
    end
  end

  describe "normalize_cms_items/1" do
    test "wraps bare items with default increment 1" do
      result = Rust.normalize_cms_items(["a", "b"])
      assert result == [{"a", 1}, {"b", 1}]
    end

    test "preserves explicit {item, increment} tuples" do
      result = Rust.normalize_cms_items([{"a", 5}, {"b", 10}])
      assert result == [{"a", 5}, {"b", 10}]
    end

    test "converts non-binary items to binary" do
      result = Rust.normalize_cms_items([42, {99, 3}])
      assert result == [{:erlang.term_to_binary(42), 1}, {:erlang.term_to_binary(99), 3}]
    end

    test "handles mixed bare and tuple items" do
      result = Rust.normalize_cms_items(["x", {"y", 7}, :z])

      assert result == [
               {"x", 1},
               {"y", 7},
               {:erlang.term_to_binary(:z), 1}
             ]
    end

    test "returns empty list for empty input" do
      assert Rust.normalize_cms_items([]) == []
    end
  end

  describe "Rust backend dirty threshold configuration" do
    setup do
      original = Application.get_env(:ex_data_sketch, :dirty_thresholds)
      on_exit(fn -> Application.put_env(:ex_data_sketch, :dirty_thresholds, original) end)
    end

    @tag :rust_nif
    test "reads threshold from app config" do
      Application.put_env(:ex_data_sketch, :dirty_thresholds, %{hll_update_many: 1})
      opts = [p: 14, backend: Rust]
      state = Rust.hll_new(opts)
      hashes = Enum.map(1..3, &ExDataSketch.Hash.hash64/1)
      result = Rust.hll_update_many(state, hashes, opts)
      assert is_binary(result)
    end
  end
end
