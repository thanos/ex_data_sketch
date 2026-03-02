defmodule ExDataSketch.Nif do
  @moduledoc false

  use Rustler,
    otp_app: :ex_data_sketch,
    crate: "ex_data_sketch_nif"

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
end
