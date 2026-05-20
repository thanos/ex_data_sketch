defimpl Collectable, for: ExDataSketch.HLL do
  @moduledoc false

  @spec into(ExDataSketch.HLL.t()) ::
          {ExDataSketch.HLL.t(),
           (ExDataSketch.HLL.t(), :done | :halt | {:cont, term()} -> ExDataSketch.HLL.t())}
  def into(%ExDataSketch.HLL{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.HLL.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.CMS do
  @moduledoc false

  @spec into(ExDataSketch.CMS.t()) ::
          {ExDataSketch.CMS.t(),
           (ExDataSketch.CMS.t(), :done | :halt | {:cont, term()} -> ExDataSketch.CMS.t())}
  def into(%ExDataSketch.CMS{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.CMS.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.Theta do
  @moduledoc false

  @spec into(ExDataSketch.Theta.t()) ::
          {ExDataSketch.Theta.t(),
           (ExDataSketch.Theta.t(), :done | :halt | {:cont, term()} -> ExDataSketch.Theta.t())}
  def into(%ExDataSketch.Theta{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.Theta.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.KLL do
  @moduledoc false

  @spec into(ExDataSketch.KLL.t()) ::
          {ExDataSketch.KLL.t(),
           (ExDataSketch.KLL.t(), :done | :halt | {:cont, term()} -> ExDataSketch.KLL.t())}
  def into(%ExDataSketch.KLL{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.KLL.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.DDSketch do
  @moduledoc false

  @spec into(ExDataSketch.DDSketch.t()) ::
          {ExDataSketch.DDSketch.t(),
           (ExDataSketch.DDSketch.t(), :done | :halt | {:cont, term()} ->
              ExDataSketch.DDSketch.t())}
  def into(%ExDataSketch.DDSketch{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.DDSketch.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.REQ do
  @moduledoc false

  @spec into(ExDataSketch.REQ.t()) ::
          {ExDataSketch.REQ.t(),
           (ExDataSketch.REQ.t(), :done | :halt | {:cont, term()} -> ExDataSketch.REQ.t())}
  def into(%ExDataSketch.REQ{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.REQ.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.ULL do
  @moduledoc false

  @spec into(ExDataSketch.ULL.t()) ::
          {ExDataSketch.ULL.t(),
           (ExDataSketch.ULL.t(), :done | :halt | {:cont, term()} -> ExDataSketch.ULL.t())}
  def into(%ExDataSketch.ULL{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.ULL.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.FrequentItems do
  @moduledoc false

  @spec into(ExDataSketch.FrequentItems.t()) ::
          {ExDataSketch.FrequentItems.t(),
           (ExDataSketch.FrequentItems.t(), :done | :halt | {:cont, term()} ->
              ExDataSketch.FrequentItems.t())}
  def into(%ExDataSketch.FrequentItems{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.FrequentItems.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.MisraGries do
  @moduledoc false

  @spec into(ExDataSketch.MisraGries.t()) ::
          {ExDataSketch.MisraGries.t(),
           (ExDataSketch.MisraGries.t(), :done | :halt | {:cont, term()} ->
              ExDataSketch.MisraGries.t())}
  def into(%ExDataSketch.MisraGries{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.MisraGries.update(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.Bloom do
  @moduledoc false

  @spec into(ExDataSketch.Bloom.t()) ::
          {ExDataSketch.Bloom.t(),
           (ExDataSketch.Bloom.t(), :done | :halt | {:cont, term()} -> ExDataSketch.Bloom.t())}
  def into(%ExDataSketch.Bloom{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.Bloom.put(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.Quotient do
  @moduledoc false

  @spec into(ExDataSketch.Quotient.t()) ::
          {ExDataSketch.Quotient.t(),
           (ExDataSketch.Quotient.t(), :done | :halt | {:cont, term()} ->
              ExDataSketch.Quotient.t())}
  def into(%ExDataSketch.Quotient{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.Quotient.put(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.CQF do
  @moduledoc false

  @spec into(ExDataSketch.CQF.t()) ::
          {ExDataSketch.CQF.t(),
           (ExDataSketch.CQF.t(), :done | :halt | {:cont, term()} -> ExDataSketch.CQF.t())}
  def into(%ExDataSketch.CQF{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.CQF.put(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end

defimpl Collectable, for: ExDataSketch.IBLT do
  @moduledoc false

  @spec into(ExDataSketch.IBLT.t()) ::
          {ExDataSketch.IBLT.t(),
           (ExDataSketch.IBLT.t(), :done | :halt | {:cont, term()} -> ExDataSketch.IBLT.t())}
  def into(%ExDataSketch.IBLT{} = sketch) do
    collector_fn = fn
      acc, {:cont, item} -> ExDataSketch.IBLT.put(acc, item)
      acc, :done -> acc
      acc, :halt -> acc
    end

    {sketch, collector_fn}
  end
end
