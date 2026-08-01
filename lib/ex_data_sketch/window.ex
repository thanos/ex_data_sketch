defmodule ExDataSketch.Window do
  @moduledoc """
  A ring of tumbling sub-sketches for answering "in the last N" questions
  without a user-side timer.

  `ExDataSketch.Window` wraps any mergeable sketch family in a fixed-size
  ring of `keep` tumbling slots, each spanning `every` milliseconds. Writes
  go to the slot for the current time; reads merge every live (non-expired)
  slot. This is the pattern demonstrated by hand in
  `livebooks/rolling_telemetry.livemd` -- a `current` sketch plus a list of
  `keep` retired ones, rotated by a timer -- made functional, deterministic,
  and reusable across every mergeable family.

  ## What this is not

  This is **not** an exact sliding window. A window of `every: 60_000,
  keep: 5` retains between 4 and 5 minutes of full history, not exactly 5,
  because the current slot is still filling: a write at the very start of
  its slot has almost 5 complete prior slots behind it, while a write at the
  very end has almost 4. This slot-boundary granularity is the cost of
  tumbling windows; an exact sliding window (for example, the
  Chabchoub-Hebrail sliding HLL construction) is more accurate but
  algorithm-specific and is explicitly out of scope here -- see the roadmap
  for possible future work. For most operational uses ("distinct users in
  about the last 5 minutes") this bound is the right trade for working with
  any mergeable sketch family unmodified.

  ## Restriction: mergeable families only

  Reading a window merges its live slots, so the wrapped family must support
  `merge/2`. `new/3` rejects `ExDataSketch.Cuckoo`, `ExDataSketch.XorFilter`,
  and `ExDataSketch.FilterChain` (none of which have `merge/2` -- see
  `ExDataSketch.Sketch` and `baoulo/plans/0.10.0_phase1_stub_review.md`)
  with a clear `ExDataSketch.Errors.UnsupportedOperationError` naming the
  reason.

  `estimate/1` additionally requires the wrapped family to support
  `ExDataSketch.estimate/1` (cardinality families via `estimate/1`, quantile
  and heavy-hitter families via `count/1`). `CMS` and the membership filters
  that *are* mergeable (`Bloom`, `Quotient`, `CQF`, `IBLT`) have no
  single-value reading and raise the same `UnsupportedOperationError` from
  `estimate/1` that `ExDataSketch.estimate/1` does; use `merged/1` to get the
  raw merged sketch and call the family's own item-specific query function
  (`Bloom.member?/2`, `CMS.estimate/2`, and so on) directly.

  ## Clock

  Slot keys are `div(now, every)` where `now` defaults to
  `System.monotonic_time(:millisecond)` -- immune to wall-clock adjustments,
  which matters for relative slot bucketing. Tests never need to sleep:
  `update/3` and `tick/2` accept an explicit `now` override, and `new/3`
  accepts a `:time_fn` option (a zero-arity function returning the current
  time in milliseconds) for injecting a fully controlled clock.

  Callers supplying an explicit `now` are expected to supply non-decreasing
  values across calls, mirroring the guarantee `System.monotonic_time/1`
  already gives the default clock. A `now` earlier than a previous call
  computes a smaller live-slot range and can expire slots out of order;
  this is a caller contract, not a case `Window` defends against.

  ## Quick Example

      window =
        ExDataSketch.Window.new(:hll, [p: 14], every: :timer.minutes(1), keep: 5)

      window = ExDataSketch.Window.update(window, user_id)
      ExDataSketch.Window.estimate(window)

  ## Persistence

  `serialize/1` and `deserialize/1` persist the window's configuration and
  slot contents (each slot via its own sketch module's `serialize/1`, so the
  underlying sketch state stays in the same EXSK v2 format used everywhere
  else). `:time_fn` is not persisted -- it is a runtime/testing injection
  point, not sketch data -- and `deserialize/1` always restores the default
  real-clock function.

  A consequence: reading a restored window (`estimate/1`, `merged/1`,
  `slots/1`) filters its slots against the real clock, not whatever clock
  built them. A window serialized under a custom or synthetic `:time_fn`
  (as in a test) can read back as fully expired once restored with the
  default clock, even though `deserialize/1` itself succeeded and the slots
  are present in `restored.slots`.
  """

  alias ExDataSketch.{Errors, Sketch, Telemetry}

  @typedoc "Milliseconds since an arbitrary reference point, per `System.monotonic_time/1`."
  @type time_ms :: integer()

  @typedoc "A single tumbling slot's key: `div(time_ms(), every)`."
  @type slot_key :: integer()

  @type t :: %__MODULE__{
          module: module(),
          sketch_opts: keyword(),
          every: pos_integer(),
          keep: pos_integer(),
          time_fn: (-> time_ms()),
          slots: %{slot_key() => struct()}
        }

  defstruct [:module, :sketch_opts, :every, :keep, :time_fn, slots: %{}]

  @doc """
  Creates a new, empty window wrapping a sketch family.

  `type_or_module` is either a registry atom looked up via
  `ExDataSketch.sketches/0` (for example, `:hll`) or a sketch module
  directly (for example, `ExDataSketch.HLL`). Atoms and modules are the same
  underlying Elixir type, so this is resolved by registry membership, not by
  pattern shape: if `type_or_module` is a key in `ExDataSketch.sketches/0`,
  the registered module is used; otherwise `type_or_module` is used directly
  as the module, which supports windowing a custom `ExDataSketch.Sketch`
  implementation that was never added to the registry.

  ## Options

  - `:every` -- required. Slot duration in milliseconds (for example,
    `:timer.minutes(1)`).
  - `:keep` -- required. Number of tumbling slots to retain, including the
    current, still-filling one.
  - `:time_fn` -- a zero-arity function returning the current time in
    milliseconds (default: `fn -> System.monotonic_time(:millisecond) end`).
    Inject a controlled clock for deterministic tests.

  ## Raises

  - `ExDataSketch.Errors.UnsupportedOperationError` if the resolved module
    has no `merge/2` (see the module documentation's "Restriction" section).
  - `ExDataSketch.Errors.InvalidOptionError` if `:every` or `:keep` is
    missing or not a positive integer.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5)
      iex> {window.module, window.every}
      {ExDataSketch.HLL, 60_000}

      iex> window = ExDataSketch.Window.new(ExDataSketch.HLL, [p: 10], every: 60_000, keep: 5)
      iex> window.module
      ExDataSketch.HLL

      iex> try do
      ...>   ExDataSketch.Window.new(:cuckoo, [], every: 60_000, keep: 5)
      ...> rescue
      ...>   e in ExDataSketch.Errors.UnsupportedOperationError -> e.message
      ...> end
      "ExDataSketch.Cuckoo (no merge/2; windowing requires a mergeable family) does not support windowing"

  """
  @spec new(ExDataSketch.sketch_type() | module(), keyword(), keyword()) :: t()
  def new(type_or_module, sketch_opts \\ [], window_opts \\ [])
      when is_atom(type_or_module) and is_list(sketch_opts) do
    module = Map.get(ExDataSketch.sketches(), type_or_module, type_or_module)
    require_mergeable!(module)
    every = fetch_positive_integer!(window_opts, :every)
    keep = fetch_positive_integer!(window_opts, :keep)
    time_fn = Keyword.get(window_opts, :time_fn, &default_time_fn/0)

    %__MODULE__{
      module: module,
      sketch_opts: sketch_opts,
      every: every,
      keep: keep,
      time_fn: time_fn,
      slots: %{}
    }
  end

  @doc """
  Updates the window with a single item at the current time (from the
  window's configured `:time_fn`).

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update(window, "user_1")
      iex> ExDataSketch.Window.estimate(window) > 0.0
      true

  """
  @spec update(t(), term()) :: t()
  def update(%__MODULE__{} = window, item) do
    update(window, item, window.time_fn.())
  end

  @doc """
  Updates the window with a single item at an explicit time, bypassing the
  configured `:time_fn`.

  Intended for deterministic tests: advance `now` by hand instead of
  sleeping or faking a clock function per call.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update(window, "user_1", 0)
      iex> ExDataSketch.Window.slots(window) |> Enum.map(fn {key, _sketch} -> key end)
      [0]

  """
  @spec update(t(), term(), time_ms()) :: t()
  def update(%__MODULE__{} = window, item, now) when is_integer(now) do
    apply_at(window, now, &window.module.update(&1, item))
  end

  @doc """
  Updates the window with every item in an enumerable, at a single point in
  time (the window's configured `:time_fn`, read once for the whole batch).

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update_many(window, ["a", "b", "c"])
      iex> ExDataSketch.Window.estimate(window) > 0.0
      true

  """
  @spec update_many(t(), Enumerable.t()) :: t()
  def update_many(%__MODULE__{} = window, items) do
    apply_at(window, window.time_fn.(), &window.module.update_many(&1, items))
  end

  @doc """
  Returns the window's headline scalar estimate: the live slots merged, then
  read via `ExDataSketch.estimate/1`.

  Raises `ExDataSketch.Errors.UnsupportedOperationError` if the wrapped
  family has no single-value reading (`CMS`, or a mergeable membership
  filter) -- see the module documentation's "Restriction" section.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> ExDataSketch.Window.estimate(window)
      0.0
      iex> window = ExDataSketch.Window.update(window, "user_1")
      iex> ExDataSketch.Window.estimate(window) > 0.0
      true

  """
  @spec estimate(t()) :: number()
  def estimate(%__MODULE__{} = window) do
    window |> merged() |> ExDataSketch.estimate()
  end

  @doc """
  Returns the raw sketch merged from every live slot, as of now.

  Use this for families with no single-value `estimate/1` (see the module
  documentation), or when a caller needs the sketch itself rather than a
  scalar reading (for example, `KLL.quantile/2` on a windowed KLL). Returns
  a fresh, empty sketch (`module.new(sketch_opts)`) if there are no live
  slots.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update(window, "user_1")
      iex> match?(%ExDataSketch.HLL{}, ExDataSketch.Window.merged(window))
      true

  """
  @spec merged(t()) :: struct()
  def merged(%__MODULE__{} = window) do
    case window |> live_slots_now() |> Map.values() do
      [] -> window.module.new(window.sketch_opts)
      sketches -> ExDataSketch.merge_many(sketches)
    end
  end

  @doc """
  Forces slot-expiry bookkeeping at an explicit time, without inserting any
  item.

  Drops slots older than `keep` as of `now` and emits
  `[:ex_data_sketch, :window, :roll]` if any slot was dropped. Useful in
  tests that want to assert expiry behavior independent of an update.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5)
      iex> window = ExDataSketch.Window.update(window, "user_1", 0)
      iex> window = ExDataSketch.Window.tick(window, 10 * 60_000)
      iex> ExDataSketch.Window.slots(window)
      []

  """
  @spec tick(t(), time_ms()) :: t()
  def tick(%__MODULE__{} = window, now) when is_integer(now) do
    {oldest, current} = live_range(window, now)
    live = expire(window.slots, oldest, current)
    dropped_count = map_size(window.slots) - map_size(live)

    emit_roll_if_dropped(window, now, dropped_count, live)

    %{window | slots: live}
  end

  @doc """
  Returns the window's live (non-expired, as of now) slots as a list of
  `{slot_key, sketch}` pairs, newest first.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update(window, "user_1")
      iex> ExDataSketch.Window.slots(window)
      [{0, ExDataSketch.Window.merged(window)}]

  """
  @spec slots(t()) :: [{slot_key(), struct()}]
  def slots(%__MODULE__{} = window) do
    window |> live_slots_now() |> Enum.sort_by(fn {key, _sketch} -> key end, :desc)
  end

  @doc """
  Returns the total size, in bytes, of the window's live slots serialized.

  Equivalent to `byte_size(serialize(window))` (with a small, constant
  envelope overhead not included), but does not build the binary. Present
  so a `Window` can be measured the same way any other sketch struct can
  (`module.size_bytes(sketch)`), for example by
  `ExDataSketch.Server`'s snapshot telemetry.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> ExDataSketch.Window.size_bytes(window)
      0
      iex> window = ExDataSketch.Window.update(window, "a")
      iex> ExDataSketch.Window.size_bytes(window) > 0
      true

  """
  @spec size_bytes(t()) :: non_neg_integer()
  def size_bytes(%__MODULE__{module: module} = window) do
    window
    |> live_slots_now()
    |> Map.values()
    |> Enum.reduce(0, fn sketch, acc -> acc + module.size_bytes(sketch) end)
  end

  @doc """
  Serializes a window's configuration and live slot contents to a binary.

  Each slot is serialized via the wrapped module's own `serialize/1`, so
  sketch state stays in the same EXSK v2 format used everywhere else; the
  envelope carrying `module`, `sketch_opts`, `every`, `keep`, and the slot
  keys is encoded as an Erlang term. `:time_fn` is not persisted;
  `deserialize/1` always restores the default real-clock function. See the
  module documentation's "Persistence" section.

  Expired slots are dropped (as of now) before serializing, same as any
  other read.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update(window, "a")
      iex> binary = ExDataSketch.Window.serialize(window)
      iex> is_binary(binary)
      true

  """
  @spec serialize(t()) :: binary()
  def serialize(%__MODULE__{} = window) do
    slot_binaries =
      window
      |> live_slots_now()
      |> Map.new(fn {key, sketch} -> {key, window.module.serialize(sketch)} end)

    :erlang.term_to_binary(%{
      module: window.module,
      sketch_opts: window.sketch_opts,
      every: window.every,
      keep: window.keep,
      slots: slot_binaries
    })
  end

  @doc """
  Deserializes a binary produced by `serialize/1` back into a window.

  Returns `{:error, exception}` rather than raising: an
  `ExDataSketch.Errors.DeserializationError` for malformed input or a slot
  binary that fails to deserialize, or an
  `ExDataSketch.Errors.UnsupportedOperationError` if the envelope names a
  module that is not mergeable (the same check `new/3` runs, applied again
  here because `deserialize/1` builds a window without going through
  `new/3`). The restored window uses the default real-clock `:time_fn`.

  ## Examples

      iex> window = ExDataSketch.Window.new(:hll, [p: 10], every: 60_000, keep: 5, time_fn: fn -> 0 end)
      iex> window = ExDataSketch.Window.update(window, "a")
      iex> {:ok, restored} = window |> ExDataSketch.Window.serialize() |> ExDataSketch.Window.deserialize()
      iex> {restored.module, restored.every, restored.keep}
      {ExDataSketch.HLL, 60_000, 5}
      iex> ExDataSketch.HLL.estimate(restored.slots[0]) > 0.0
      true

      iex> ExDataSketch.Window.deserialize(<<0, 1, 2>>)
      {:error, %ExDataSketch.Errors.DeserializationError{message: "deserialization failed: invalid Window binary"}}

  """
  @spec deserialize(binary()) :: {:ok, t()} | {:error, Exception.t()}
  def deserialize(binary) when is_binary(binary) do
    case safe_binary_to_term(binary) do
      {:ok,
       %{
         module: module,
         sketch_opts: sketch_opts,
         every: every,
         keep: keep,
         slots: slot_binaries
       }} ->
        decode_envelope(module, sketch_opts, every, keep, slot_binaries)

      {:ok, _other} ->
        Errors.error(Errors.DeserializationError.exception(reason: "invalid Window envelope"))

      :error ->
        Errors.error(Errors.DeserializationError.exception(reason: "invalid Window binary"))
    end
  end

  defp decode_envelope(module, sketch_opts, every, keep, slot_binaries)
       when is_atom(module) and is_list(sketch_opts) and is_integer(every) and every > 0 and
              is_integer(keep) and keep > 0 and is_map(slot_binaries) do
    with :ok <- validate_mergeable(module),
         {:ok, slots} <- decode_slots(module, slot_binaries) do
      {:ok,
       %__MODULE__{
         module: module,
         sketch_opts: sketch_opts,
         every: every,
         keep: keep,
         time_fn: &default_time_fn/0,
         slots: slots
       }}
    end
  end

  defp decode_envelope(_module, _sketch_opts, _every, _keep, _slot_binaries) do
    Errors.error(Errors.DeserializationError.exception(reason: "invalid Window envelope"))
  end

  defp validate_mergeable(module) do
    require_mergeable!(module)
    :ok
  rescue
    e in Errors.UnsupportedOperationError -> Errors.error(e)
  end

  defp decode_slots(module, slot_binaries) do
    Enum.reduce_while(slot_binaries, {:ok, %{}}, fn {key, binary}, {:ok, acc} ->
      with true <- is_integer(key),
           true <- is_binary(binary),
           {:ok, sketch} <- module.deserialize(binary) do
        {:cont, {:ok, Map.put(acc, key, sketch)}}
      else
        _ ->
          {:halt,
           Errors.error(Errors.DeserializationError.exception(reason: "invalid Window slot"))}
      end
    end)
  end

  defp safe_binary_to_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  defp apply_at(%__MODULE__{} = window, now, update_fn) do
    {oldest, current} = live_range(window, now)
    live = expire(window.slots, oldest, current)
    dropped_count = map_size(window.slots) - map_size(live)

    current_sketch = Map.get(live, current) || window.module.new(window.sketch_opts)
    updated_sketch = update_fn.(current_sketch)
    new_slots = Map.put(live, current, updated_sketch)

    emit_roll_if_dropped(window, now, dropped_count, new_slots)

    %{window | slots: new_slots}
  end

  defp live_slots_now(%__MODULE__{} = window) do
    {oldest, current} = live_range(window, window.time_fn.())
    expire(window.slots, oldest, current)
  end

  defp live_range(%__MODULE__{every: every, keep: keep}, now) do
    current = div(now, every)
    {current - keep + 1, current}
  end

  defp expire(slots, oldest, current) do
    slots
    |> Enum.filter(fn {key, _sketch} -> key >= oldest and key <= current end)
    |> Map.new()
  end

  defp emit_roll_if_dropped(_window, _now, 0, _new_slots), do: :ok

  defp emit_roll_if_dropped(window, now, dropped_count, new_slots) do
    oldest_age_ms =
      case new_slots |> Map.keys() |> Enum.min(fn -> nil end) do
        nil -> 0
        oldest_key -> now - oldest_key * window.every
      end

    Telemetry.execute(
      Telemetry.event_name(:window, :roll),
      %{slot_count: map_size(new_slots), dropped_count: dropped_count},
      %{
        sketch_type: Telemetry.sketch_type_from_module(window.module),
        oldest_age_ms: oldest_age_ms
      },
      :window
    )
  end

  defp default_time_fn, do: System.monotonic_time(:millisecond)

  defp require_mergeable!(module) do
    mergeable? = Sketch.implemented?(module) and MapSet.member?(module.capabilities(), :merge)

    unless mergeable? do
      raise Errors.UnsupportedOperationError,
        operation: "windowing",
        structure: "#{inspect(module)} (no merge/2; windowing requires a mergeable family)"
    end
  end

  defp fetch_positive_integer!(opts, key) do
    case Keyword.get(opts, key) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise Errors.InvalidOptionError,
          option: key,
          value: value,
          message: "#{inspect(key)} must be a positive integer, got: #{inspect(value)}"
    end
  end
end
