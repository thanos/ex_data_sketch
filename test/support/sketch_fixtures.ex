defmodule ExDataSketch.SketchFixtures do
  @moduledoc false

  alias ExDataSketch.{
    Bloom,
    CMS,
    CQF,
    Cuckoo,
    DDSketch,
    FrequentItems,
    HLL,
    IBLT,
    KLL,
    MisraGries,
    Quotient,
    REQ,
    Theta,
    ULL,
    XorFilter
  }

  @doc """
  Small-scale construction metadata for every sketch family that has a
  `Codec.sketch_id` (i.e. everything except `FilterChain`, which uses its
  own bespoke FCN1 container format).

  - `:module` -- the family's module.
  - `:args` -- small constructor options, matching each family's own test
    conventions.
  - `:sketch_id` -- the family's `Codec.sketch_id_*` byte value.
  - `:hashed?` -- whether the family has a `:hash_strategy` option (and
    therefore whether `format: :v1` guards on `:phash2`).
  - `:retains_hash_strategy?` -- whether `:hash_strategy` survives into
    the constructed sketch's own `.opts` (true for HLL/CMS/Theta/ULL,
    which read it back out for `Binary.metadata_from_opts/3` on every
    `serialize/1` call). The 6 membership filters *use* `:hash_strategy`
    only transiently at construction/insert time and never retain it in
    `clean_opts` -- a pre-existing characteristic of those modules, not
    something this phase changes -- so their `format: :v1` guard, while
    present and correct, can never observe a non-default value in
    practice today.
  - `:numeric?` -- whether the family operates on raw numeric values
    (KLL, DDSketch, REQ) rather than hashed items.
  - `:shape` -- construction return-shape quirk, if any: `:cuckoo`
    (`from_enumerable/2` can return `{:error, :full, t()}`) or
    `:xor_filter` (no `from_enumerable/2`, uses `build/2`).
  """
  @spec families() :: %{atom() => map()}
  def families do
    %{
      hll: %{
        module: HLL,
        args: [p: 10],
        sketch_id: 1,
        hashed?: true,
        retains_hash_strategy?: true
      },
      cms: %{
        module: CMS,
        args: [width: 100, depth: 5],
        sketch_id: 2,
        hashed?: true,
        retains_hash_strategy?: true
      },
      theta: %{
        module: Theta,
        args: [k: 64],
        sketch_id: 3,
        hashed?: true,
        retains_hash_strategy?: true
      },
      kll: %{module: KLL, args: [k: 200], sketch_id: 4, hashed?: false, numeric?: true},
      ddsketch: %{
        module: DDSketch,
        args: [alpha: 0.01],
        sketch_id: 5,
        hashed?: false,
        numeric?: true
      },
      frequent_items: %{module: FrequentItems, args: [k: 10], sketch_id: 6, hashed?: false},
      bloom: %{module: Bloom, args: [capacity: 100], sketch_id: 7, hashed?: true},
      cuckoo: %{
        module: Cuckoo,
        args: [capacity: 100],
        sketch_id: 8,
        hashed?: true,
        shape: :cuckoo
      },
      quotient: %{module: Quotient, args: [q: 10, r: 8], sketch_id: 9, hashed?: true},
      cqf: %{module: CQF, args: [q: 10, r: 8], sketch_id: 10, hashed?: true},
      xor_filter: %{module: XorFilter, args: [], sketch_id: 11, hashed?: true, shape: :xor_filter},
      iblt: %{module: IBLT, args: [], sketch_id: 12, hashed?: true},
      req: %{module: REQ, args: [k: 12], sketch_id: 13, hashed?: false, numeric?: true},
      misra_gries: %{module: MisraGries, args: [k: 10], sketch_id: 14, hashed?: false},
      ull: %{
        module: ULL,
        args: [p: 10],
        sketch_id: 15,
        hashed?: true,
        retains_hash_strategy?: true
      }
    }
  end

  @doc "The 20 default items for a family: numeric floats or hashable strings."
  @spec default_items(atom()) :: [String.t()] | [float()]
  def default_items(family) do
    case Map.fetch!(families(), family) do
      %{numeric?: true} -> for i <- 1..20, do: i * 1.0
      _ -> for i <- 1..20, do: "item_#{i}"
    end
  end

  @doc """
  Builds a small sketch for `family` with `items` (defaults to
  `default_items/1`) and `extra_args` merged into the family's base args,
  unwrapping the `:cuckoo`/`:xor_filter` construction-shape quirks.
  """
  @spec build(atom(), [term()] | nil, keyword()) :: struct()
  def build(:cuckoo, items, extra_args) do
    items = items || default_items(:cuckoo)
    merged_args = Keyword.merge(families()[:cuckoo].args, extra_args)

    case Cuckoo.from_enumerable(items, merged_args) do
      {:ok, sketch} -> sketch
      {:error, :full, sketch} -> sketch
    end
  end

  def build(:xor_filter, items, extra_args) do
    items = items || default_items(:xor_filter)
    merged_args = Keyword.merge(families()[:xor_filter].args, extra_args)
    {:ok, sketch} = XorFilter.build(items, merged_args)
    sketch
  end

  def build(family, items, extra_args) do
    %{module: mod, args: args} = Map.fetch!(families(), family)
    items = items || default_items(family)
    merged_args = Keyword.merge(args, extra_args)
    mod.from_enumerable(items, merged_args)
  end

  def build(family), do: build(family, nil, [])
  def build(family, items), do: build(family, items, [])
end
