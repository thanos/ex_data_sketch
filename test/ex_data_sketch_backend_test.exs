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
