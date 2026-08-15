defmodule ExDataSketch.Config do
  @moduledoc """
  Per-family default option overrides from Application config.

  Every sketch family's `new/1` (or, for `ExDataSketch.XorFilter`,
  `build/2`) resolves its options by merging explicitly-passed `opts`
  over whatever this module returns -- explicit opts always win.
  Configure via a single flat `:defaults` key, keyed by the same atoms
  `ExDataSketch.sketches/0` uses:

      config :ex_data_sketch,
        backend: ExDataSketch.Backend.Rust,
        defaults: [
          hll: [p: 16],
          cqf: [q: 20, r: 8],
          bloom: [capacity: 50_000]
        ]

  This mirrors the existing flat `config :ex_data_sketch, :backend, ...`
  / `:storage` / `:telemetry` keys already used elsewhere in this
  library, rather than introducing a new per-module config convention.

  `ExDataSketch.FilterChain.new/0` takes no options at all, so it has no
  corresponding `:filter_chain` defaults key.
  """

  @doc """
  Merges `family`'s configured default options underneath `opts`.

  `opts` always wins on any key present in both. Unconfigured families
  (no `:defaults` entry, or no `:defaults` key at all) return `opts`
  unchanged.

  ## Examples

      iex> ExDataSketch.Config.merge_defaults(:hll, [])
      []

      iex> ExDataSketch.Config.merge_defaults(:hll, [p: 12])
      [p: 12]

  """
  @spec merge_defaults(atom(), keyword()) :: keyword()
  def merge_defaults(family, opts) when is_atom(family) and is_list(opts) do
    # `Application.get_env/3`'s default only applies when the key is
    # *absent* -- an explicitly-set `nil` (e.g. from a test or config
    # that cleared it) is returned as-is, so `|| []` covers that case too.
    (Application.get_env(:ex_data_sketch, :defaults) || [])
    |> Keyword.get(family, [])
    |> Keyword.merge(opts)
  end
end
