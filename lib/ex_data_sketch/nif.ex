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
end
