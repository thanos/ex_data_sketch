defmodule ExDataSketch.Nif do
  @moduledoc false

  unless System.get_env("EX_DATA_SKETCH_SKIP_NIF") in ["1", "true"] do
    version = Mix.Project.config()[:version]

    use RustlerPrecompiled,
      otp_app: :ex_data_sketch,
      crate: "ex_data_sketch_nif",
      base_url: "https://github.com/thanos/ex_data_sketch/releases/download/v#{version}",
      version: version,
      nif_versions: ["2.16", "2.17"],
      targets: [
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-gnu",
        "aarch64-unknown-linux-musl"
      ]
  end

  @doc false
  def nif_loaded, do: :erlang.nif_error(:not_loaded)

  # HLL
  def hll_update_many_nif(_state_bin, _hashes_bin, _p), do: :erlang.nif_error(:not_loaded)
  def hll_update_many_dirty_nif(_state_bin, _hashes_bin, _p), do: :erlang.nif_error(:not_loaded)
  def hll_merge_nif(_a_bin, _b_bin, _p), do: :erlang.nif_error(:not_loaded)
  def hll_merge_dirty_nif(_a_bin, _b_bin, _p), do: :erlang.nif_error(:not_loaded)
  def hll_estimate_nif(_state_bin, _p), do: :erlang.nif_error(:not_loaded)
  def hll_estimate_dirty_nif(_state_bin, _p), do: :erlang.nif_error(:not_loaded)

  # CMS
  def cms_update_many_nif(_state_bin, _pairs_bin, _w, _d, _cw), do: :erlang.nif_error(:not_loaded)

  def cms_update_many_dirty_nif(_state_bin, _pairs_bin, _w, _d, _cw),
    do: :erlang.nif_error(:not_loaded)

  def cms_merge_nif(_a_bin, _b_bin, _w, _d, _cw), do: :erlang.nif_error(:not_loaded)
  def cms_merge_dirty_nif(_a_bin, _b_bin, _w, _d, _cw), do: :erlang.nif_error(:not_loaded)

  # Theta
  def theta_update_many_nif(_state_bin, _hashes_bin), do: :erlang.nif_error(:not_loaded)
  def theta_update_many_dirty_nif(_state_bin, _hashes_bin), do: :erlang.nif_error(:not_loaded)
  def theta_compact_nif(_state_bin), do: :erlang.nif_error(:not_loaded)
  def theta_compact_dirty_nif(_state_bin), do: :erlang.nif_error(:not_loaded)
  def theta_merge_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def theta_merge_dirty_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)

  # KLL
  def kll_update_many_nif(_state_bin, _values_bin), do: :erlang.nif_error(:not_loaded)
  def kll_update_many_dirty_nif(_state_bin, _values_bin), do: :erlang.nif_error(:not_loaded)
  def kll_merge_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def kll_merge_dirty_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)

  # DDSketch
  def ddsketch_update_many_nif(_state_bin, _values_bin), do: :erlang.nif_error(:not_loaded)
  def ddsketch_update_many_dirty_nif(_state_bin, _values_bin), do: :erlang.nif_error(:not_loaded)
  def ddsketch_merge_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def ddsketch_merge_dirty_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)

  # Hash (XXHash3)
  def xxhash3_64_nif(_data), do: :erlang.nif_error(:not_loaded)
  def xxhash3_64_seeded_nif(_data, _seed), do: :erlang.nif_error(:not_loaded)

  # Bloom
  def bloom_put_many_nif(_state_bin, _hashes_bin, _hash_count, _bit_count),
    do: :erlang.nif_error(:not_loaded)

  def bloom_put_many_dirty_nif(_state_bin, _hashes_bin, _hash_count, _bit_count),
    do: :erlang.nif_error(:not_loaded)

  def bloom_merge_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def bloom_merge_dirty_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)

  # Cuckoo
  def cuckoo_put_many_nif(
        _state_bin,
        _hashes_bin,
        _fp_bits,
        _bucket_size,
        _bucket_count,
        _max_kicks,
        _seed
      ),
      do: :erlang.nif_error(:not_loaded)

  def cuckoo_put_many_dirty_nif(
        _state_bin,
        _hashes_bin,
        _fp_bits,
        _bucket_size,
        _bucket_count,
        _max_kicks,
        _seed
      ),
      do: :erlang.nif_error(:not_loaded)

  # Quotient
  def quotient_put_many_nif(_state_bin, _hashes_bin, _q, _r),
    do: :erlang.nif_error(:not_loaded)

  def quotient_put_many_dirty_nif(_state_bin, _hashes_bin, _q, _r),
    do: :erlang.nif_error(:not_loaded)

  def quotient_merge_nif(_a_bin, _b_bin, _q, _r), do: :erlang.nif_error(:not_loaded)
  def quotient_merge_dirty_nif(_a_bin, _b_bin, _q, _r), do: :erlang.nif_error(:not_loaded)

  # CQF
  def cqf_put_many_nif(_state_bin, _hashes_bin, _q, _r),
    do: :erlang.nif_error(:not_loaded)

  def cqf_put_many_dirty_nif(_state_bin, _hashes_bin, _q, _r),
    do: :erlang.nif_error(:not_loaded)

  def cqf_merge_nif(_a_bin, _b_bin, _q, _r), do: :erlang.nif_error(:not_loaded)
  def cqf_merge_dirty_nif(_a_bin, _b_bin, _q, _r), do: :erlang.nif_error(:not_loaded)

  # XorFilter
  def xor_build_nif(_hashes_bin, _fp_bits, _seed), do: :erlang.nif_error(:not_loaded)
  def xor_build_dirty_nif(_hashes_bin, _fp_bits, _seed), do: :erlang.nif_error(:not_loaded)

  # IBLT
  def iblt_put_many_nif(_state_bin, _pairs_bin, _hash_count, _cell_count, _seed),
    do: :erlang.nif_error(:not_loaded)

  def iblt_put_many_dirty_nif(_state_bin, _pairs_bin, _hash_count, _cell_count, _seed),
    do: :erlang.nif_error(:not_loaded)

  def iblt_merge_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def iblt_merge_dirty_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)

  # ULL
  def ull_update_many_nif(_state_bin, _hashes_bin, _p), do: :erlang.nif_error(:not_loaded)
  def ull_update_many_dirty_nif(_state_bin, _hashes_bin, _p), do: :erlang.nif_error(:not_loaded)
  def ull_merge_nif(_a_bin, _b_bin, _p), do: :erlang.nif_error(:not_loaded)
  def ull_merge_dirty_nif(_a_bin, _b_bin, _p), do: :erlang.nif_error(:not_loaded)
  def ull_estimate_nif(_state_bin, _p), do: :erlang.nif_error(:not_loaded)
  def ull_estimate_dirty_nif(_state_bin, _p), do: :erlang.nif_error(:not_loaded)

  # FrequentItems
  def fi_new_nif(_k, _flags), do: :erlang.nif_error(:not_loaded)
  def fi_update_many_nif(_state_bin, _packed_items_bin), do: :erlang.nif_error(:not_loaded)
  def fi_update_many_dirty_nif(_state_bin, _packed_items_bin), do: :erlang.nif_error(:not_loaded)
  def fi_merge_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def fi_merge_dirty_nif(_a_bin, _b_bin), do: :erlang.nif_error(:not_loaded)
  def fi_estimate_nif(_state_bin, _item_bytes), do: :erlang.nif_error(:not_loaded)
  def fi_top_k_nif(_state_bin, _limit), do: :erlang.nif_error(:not_loaded)
  def fi_count_nif(_state_bin), do: :erlang.nif_error(:not_loaded)
  def fi_entry_count_nif(_state_bin), do: :erlang.nif_error(:not_loaded)
end
