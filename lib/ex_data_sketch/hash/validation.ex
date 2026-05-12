defmodule ExDataSketch.Hash.Validation do
  @moduledoc """
  Compatibility checks for hash configurations across merging sketches.

  Two sketches may only be merged when their hashing configuration is provably
  identical. This module centralizes that check and is used by all sketch
  modules' `merge/2` operations from v0.8.0 onward.

  The existing `ExDataSketch.Hash.validate_merge_hash_compat!/3` remains as a
  thin compatibility shim that delegates here, so external callers do not
  experience a breaking change.

  ## What counts as compatible?

  - Same hash strategy (`:phash2 | :xxhash3 | :murmur3`).
  - Same seed.
  - Neither side uses `:custom` (closure-based hashes cannot be compared
    structurally and so cannot be proven compatible).

  ## What counts as compatible across metadata blocks?

  When both sketches carry an `ExDataSketch.Hash.Metadata` block, the check is:

  - same `algorithm`;
  - same `seed`;
  - same `sketch_family`;
  - `sketch_family_version` compatible (equal, or both sides explicitly support
    forward compatibility — currently equal only).

  `block_version`, `backend`, and `flags` are intentionally NOT part of the
  equivalence relation: identical hash output across a Rust/Pure split or a
  block-version bump is the whole point of the binary contract.
  """

  alias ExDataSketch.Errors.IncompatibleSketchesError
  alias ExDataSketch.Hash.Metadata

  @doc """
  Validates that two keyword-list option sets describe compatible hash
  configurations.

  Raises `ExDataSketch.Errors.IncompatibleSketchesError` on mismatch.
  Returns `:ok` on success.

  ## Examples

      iex> ExDataSketch.Hash.Validation.validate_options!([hash_strategy: :xxhash3, seed: 0], [hash_strategy: :xxhash3, seed: 0], "HLL")
      :ok

      iex> try do
      ...>   ExDataSketch.Hash.Validation.validate_options!([hash_strategy: :xxhash3], [hash_strategy: :phash2], "HLL")
      ...> rescue
      ...>   ExDataSketch.Errors.IncompatibleSketchesError -> :raised
      ...> end
      :raised

  """
  @spec validate_options!(Keyword.t(), Keyword.t(), String.t()) :: :ok
  def validate_options!(opts_a, opts_b, sketch_type) when is_list(opts_a) and is_list(opts_b) do
    strategy_a = Keyword.get(opts_a, :hash_strategy, :phash2)
    strategy_b = Keyword.get(opts_b, :hash_strategy, :phash2)

    if strategy_a == :custom or strategy_b == :custom do
      raise IncompatibleSketchesError,
        reason:
          "#{sketch_type} merge is not supported with custom :hash_fn (cannot verify hash compatibility)"
    end

    if strategy_a != strategy_b do
      raise IncompatibleSketchesError,
        reason: "#{sketch_type} hash strategy mismatch: #{strategy_a} vs #{strategy_b}"
    end

    seed_a = Keyword.get(opts_a, :seed, 0)
    seed_b = Keyword.get(opts_b, :seed, 0)

    if seed_a != seed_b do
      raise IncompatibleSketchesError,
        reason: "#{sketch_type} seed mismatch: #{seed_a} vs #{seed_b}"
    end

    :ok
  end

  @doc """
  Validates that two `ExDataSketch.Hash.Metadata` blocks describe compatible
  sketches.

  Raises `ExDataSketch.Errors.IncompatibleSketchesError` on mismatch.
  Returns `:ok` on success.

  ## Examples

      iex> a = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :pure)
      iex> b = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :rust)
      iex> ExDataSketch.Hash.Validation.validate_metadata!(a, b, "HLL")
      :ok

      iex> a = ExDataSketch.Hash.Metadata.new(:xxhash3, 0, 1, 1, :pure)
      iex> b = ExDataSketch.Hash.Metadata.new(:murmur3, 0, 1, 1, :pure)
      iex> try do
      ...>   ExDataSketch.Hash.Validation.validate_metadata!(a, b, "HLL")
      ...> rescue
      ...>   ExDataSketch.Errors.IncompatibleSketchesError -> :raised
      ...> end
      :raised

  """
  @spec validate_metadata!(Metadata.t(), Metadata.t(), String.t()) :: :ok
  def validate_metadata!(%Metadata{} = a, %Metadata{} = b, sketch_type) do
    cond do
      a.algorithm == :custom or b.algorithm == :custom ->
        raise IncompatibleSketchesError,
          reason: "#{sketch_type} merge is not supported with :custom hash algorithm"

      a.algorithm != b.algorithm ->
        raise IncompatibleSketchesError,
          reason: "#{sketch_type} hash algorithm mismatch: #{a.algorithm} vs #{b.algorithm}"

      a.seed != b.seed ->
        raise IncompatibleSketchesError,
          reason: "#{sketch_type} hash seed mismatch: #{a.seed} vs #{b.seed}"

      a.sketch_family != b.sketch_family ->
        raise IncompatibleSketchesError,
          reason:
            "#{sketch_type} sketch_family mismatch: #{a.sketch_family} vs #{b.sketch_family}"

      a.sketch_family_version != b.sketch_family_version ->
        raise IncompatibleSketchesError,
          reason:
            "#{sketch_type} sketch_family_version mismatch: " <>
              "#{a.sketch_family_version} vs #{b.sketch_family_version}"

      true ->
        :ok
    end
  end

  @doc """
  Returns `true` when the given two option sets describe compatible hashing,
  `false` otherwise. Never raises.

  Useful in code paths that want to make a decision without exception flow.

  ## Examples

      iex> ExDataSketch.Hash.Validation.compatible_options?([hash_strategy: :xxhash3], [hash_strategy: :xxhash3])
      true

      iex> ExDataSketch.Hash.Validation.compatible_options?([hash_strategy: :xxhash3], [hash_strategy: :phash2])
      false

  """
  @spec compatible_options?(Keyword.t(), Keyword.t()) :: boolean()
  def compatible_options?(opts_a, opts_b) when is_list(opts_a) and is_list(opts_b) do
    validate_options!(opts_a, opts_b, "compat-check")
    true
  rescue
    IncompatibleSketchesError -> false
  end
end
