# Windowing

`ExDataSketch.Window` answers "in the last N" questions -- distinct users in
the last 5 minutes, top pages in the last hour -- without a hand-rolled timer
or GenServer. It wraps any mergeable sketch family in a fixed-size ring of
tumbling slots and merges the live ones on read.

## Quick Start

```elixir
window = ExDataSketch.Window.new(:hll, [p: 14], every: :timer.minutes(1), keep: 5)

window = ExDataSketch.Window.update(window, user_id)
ExDataSketch.Window.estimate(window)  # distinct users in about the last 5 minutes
```

That is the whole thing: no `Process.send_after/3`, no `GenServer` `init`
callback scheduling a flush, no list of retired sketches to manage by hand.
`Window`
is a plain, immutable struct -- store it in whatever process already holds
your counter's state (a GenServer, an Agent, a script variable) and call
`update/2` and `estimate/1` on it directly.

## What "the last 5 minutes" actually means

`Window` is a ring of tumbling slots, not an exact sliding window. A window
configured with `every: :timer.minutes(1), keep: 5` retains between 4 and 5
minutes of full history, not exactly 5:

- Every write lands in the slot for the current time: `div(now, every)`.
- A read merges every slot within `keep` of the current one, including the
  current, still-filling slot.
- A write at the very start of the current slot has almost 5 complete prior
  slots behind it. A write at the very end has almost 4.

This slot-boundary granularity is the standard cost of tumbling windows, in
exchange for working with *any* mergeable sketch family unmodified -- no
per-algorithm sliding-window construction required. An exact sliding window
(for example, the Chabchoub-Hebrail sliding HLL construction) is more
accurate but algorithm-specific; it is not what `Window` implements.

For most operational dashboards and alerts, "the last 5 minutes, give or
take up to one slot" is the right trade. If you need exact boundaries,
`every: :timer.seconds(1), keep: 300` narrows the slop to one second at the
cost of 300 live sketches instead of 5.

## Which families can be windowed

Reading a window merges its live slots, so the wrapped family must support
`merge/2`. `ExDataSketch.Cuckoo`, `ExDataSketch.XorFilter`, and
`ExDataSketch.FilterChain` have no `merge/2` and are rejected at
construction:

```elixir
iex> ExDataSketch.Window.new(:cuckoo, [], every: 60_000, keep: 5)
** (ExDataSketch.Errors.UnsupportedOperationError) ExDataSketch.Cuckoo (no merge/2; windowing requires a mergeable family) does not support windowing
```

Every other family -- cardinality (`HLL`, `ULL`, `Theta`), quantile (`KLL`,
`DDSketch`, `REQ`), frequency and heavy-hitter (`CMS`, `FrequentItems`,
`MisraGries`), and the mergeable membership filters (`Bloom`, `Quotient`,
`CQF`, `IBLT`) -- can be windowed.

`estimate/1` is narrower than that: it delegates to `ExDataSketch.estimate/1`,
which only has a single-value reading for cardinality, quantile, and
heavy-hitter families. `CMS` and the windowed membership filters have no
scalar "estimate" -- use `merged/1` to get the raw merged sketch and call the
family's own item-specific function directly:

```elixir
window = ExDataSketch.Window.new(:bloom, [capacity: 10_000], every: 60_000, keep: 5)
window = ExDataSketch.Window.update(window, "user_123")

# Window.estimate(window) would raise UnsupportedOperationError here.
merged = ExDataSketch.Window.merged(window)
ExDataSketch.Bloom.member?(merged, "user_123")
```

## Constructing a window: atom or module

`new/3`'s first argument is either a registry atom (`:hll`, looked up via
`ExDataSketch.sketches/0`) or a sketch module directly
(`ExDataSketch.HLL`). Both work identically; the atom form matches the rest
of the top-level `ExDataSketch` facade, and the module form additionally
supports windowing a custom `ExDataSketch.Sketch` implementation that was
never added to the registry.

```elixir
ExDataSketch.Window.new(:hll, [p: 14], every: 60_000, keep: 5)
ExDataSketch.Window.new(ExDataSketch.HLL, [p: 14], every: 60_000, keep: 5)
```

## Deterministic testing: no sleeping required

By default, slot keys are computed from `System.monotonic_time(:millisecond)`
-- immune to wall-clock adjustments, which matters for relative slot
bucketing. Tests never need to sleep, though, because the clock is fully
injectable:

- `update/3` and `tick/2` accept an explicit `now` (in milliseconds),
  bypassing the configured clock for that one call.
- `new/3` accepts `:time_fn`, a zero-arity function returning the current
  time in milliseconds, for a fully controlled clock across every call.

```elixir
window = ExDataSketch.Window.new(:hll, [p: 10], every: 1000, keep: 3)

window = ExDataSketch.Window.update(window, "a", 0)      # slot 0
window = ExDataSketch.Window.update(window, "b", 2999)   # slot 2, slot 0 still live (keep: 3)
window = ExDataSketch.Window.update(window, "c", 3000)   # slot 3, slot 0 now expires

Map.keys(window.slots) |> Enum.sort()
# [2, 3]
```

`slots/1`, `estimate/1`, and `merged/1` always read through the window's
configured `:time_fn` (the real clock, by default), not whatever explicit
`now` a prior `update/3` call used -- so a test asserting on those needs a
matching `:time_fn` too, not just explicit `now` arguments on `update/3`:

```elixir
window = ExDataSketch.Window.new(:hll, [p: 10], every: 1000, keep: 3, time_fn: fn -> 3000 end)
window = window |> ExDataSketch.Window.update("a", 0) |> ExDataSketch.Window.update("c", 3000)

ExDataSketch.Window.slots(window) |> Enum.map(fn {key, _sketch} -> key end)
# [3]
```

Explicit `now` values (whether passed to `update/3`/`tick/2`, or produced by
a custom `:time_fn`) are expected to be non-decreasing across calls, the
same guarantee `System.monotonic_time/1` gives the default clock. This is a
caller contract, not something `Window` defends against at runtime.

`tick/2` is the same expiry bookkeeping `update/3` does, without inserting
an item -- useful for a test that wants to assert expiry behavior on its
own, separate from a write.

## Reading a window

- `estimate/1` -- the headline scalar (distinct count, quantile sketch
  item count, and so on), for families `ExDataSketch.estimate/1` supports.
- `merged/1` -- the raw sketch merged from every live slot. Use this for
  families with no single-value `estimate/1`, or when you need the sketch
  itself (`KLL.quantile/2` on a windowed KLL, `Bloom.member?/2` on a
  windowed Bloom filter).
- `slots/1` -- the live `{slot_key, sketch}` pairs, newest first. Mostly
  useful for introspection and debugging.

None of these mutate the window or emit telemetry -- they filter expired
slots transiently for the read. Only `update/2,3`, `update_many/2`, and
`tick/2` persist expiry and emit `[:ex_data_sketch, :window, :roll]` (see
`guides/telemetry.md`) when a slot actually ages out.

## Persistence

`serialize/1` and `deserialize/1` persist a window's configuration and live
slot contents. Each slot is serialized through its own sketch module's
`serialize/1`, so the underlying sketch state stays in the standard EXSK v2
format used everywhere else in this library; only the envelope around it
(which slots exist, their keys, `every`/`keep`/`module`/`sketch_opts`) is
encoded as an Erlang term, decoded safely (`:erlang.binary_to_term/2` with
`[:safe]`, so untrusted input cannot create new atoms).

```elixir
binary = ExDataSketch.Window.serialize(window)
{:ok, restored} = ExDataSketch.Window.deserialize(binary)
```

`:time_fn` is not persisted -- it is a runtime/testing injection point, not
sketch data -- so `deserialize/1` always restores the default real-clock
function. A consequence: reading a restored window filters its slots
against the real clock, not whatever clock built them. A window serialized
under a synthetic `:time_fn` (as in a test) can read back as fully expired
once restored, even though `deserialize/1` succeeded and the slots are
present in `restored.slots`. This only matters for tests; a window built
with the default clock and restored with the default clock behaves exactly
as expected.

## See also

- `ExDataSketch.Window` module documentation -- full API reference.
- `guides/telemetry.md` -- the `[:ex_data_sketch, :window, :roll]` event.
- `livebooks/rolling_telemetry.livemd` -- a runnable walkthrough.
