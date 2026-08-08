# Supervised Sketches

`ExDataSketch.Server` wraps a single sketch (or `ExDataSketch.Window`) in a
`GenServer`, so an application can hold a running counter -- "distinct users
right now", "distinct users in the last 5 minutes" -- as an ordinary named
process instead of hand-writing concurrency, backpressure, and crash
recovery around a bare sketch struct. `ExDataSketch.Sketches` adds a
supervisor for starting many `Server` processes at runtime, addressed by an
arbitrary term (a tenant ID, a user ID) rather than a compile-time name.

## Quick Start

```elixir
{:ok, _pid} = ExDataSketch.Server.start_link(
  name: :uniques,
  sketch: :hll,
  sketch_opts: [p: 14]
)

ExDataSketch.Server.update(:uniques, user_id)
ExDataSketch.Server.estimate(:uniques)
```

Add this to a supervision tree the ordinary way:

```elixir
children = [
  {ExDataSketch.Server, name: :uniques, sketch: :hll, sketch_opts: [p: 14]}
]
```

## Backpressure

`update/2` and `update_many/2` are casts: they return immediately and do
not wait for the update to be applied. Under sustained load faster than the
server can process, its mailbox grows without bound unless `:max_queue` is
configured. `update_sync/2` is a call instead -- it blocks until the update
has been applied, and as an ordinary call provides natural backpressure (a
caller cannot get more than one `update_sync/2` ahead of the server).

```elixir
{:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 14], max_queue: 10_000)
```

When `:max_queue` is set, `update/2` and `update_many/2` drop the update
(instead of applying it) whenever the server's own mailbox is at or above
that threshold, emitting `[:ex_data_sketch, :server, :drop]` (see
`guides/telemetry.md`). This bounds memory growth from a backed-up mailbox
at the cost of the data loss `update/2` already implies under overload --
`update_sync/2` is never subject to `:max_queue`.

## Windowing

Pass `:window` (the same options `ExDataSketch.Window.new/3` accepts) to
have the server hold a `ExDataSketch.Window` instead of a bare sketch.
`estimate/1` then answers "in the last `keep * every`" instead of "ever".
See `guides/windowing.md` for what that means precisely and which sketch
families can be windowed.

```elixir
{:ok, pid} =
  ExDataSketch.Server.start_link(
    sketch: :hll,
    sketch_opts: [p: 14],
    window: [every: :timer.minutes(1), keep: 5]
  )
```

Add `track_all_time: true` to `:window` to also maintain a second,
un-windowed sketch alongside the window (the same `:sketch_opts`, so the
same fixed size as any other sketch of that family -- not a growing or
unbounded structure). `estimate(server, window: :all)` reads it:

```elixir
{:ok, pid} =
  ExDataSketch.Server.start_link(
    sketch: :hll,
    sketch_opts: [p: 14],
    window: [every: :timer.minutes(1), keep: 5, track_all_time: true]
  )

ExDataSketch.Server.estimate(pid, window: :all)
```

Calling `estimate(server, window: :all)` without `track_all_time: true`
raises `ExDataSketch.Errors.UnsupportedOperationError`. If `:snapshot` is
also configured, it always persists the window, not this second sketch,
which restarts empty after a crash even when `track_all_time: true` is set
(see `baoulo/plans/0.10.0_phase4_design_review.md` section 4.2 for the
rationale).

A windowed server does not support `merge/2` in this release --
`ExDataSketch.Window` has no primitive to merge an already-built sketch
into its current slot (only raw-item `update/2,3` and `update_many/2`).
Calling `merge/2` on a windowed server raises
`ExDataSketch.Errors.UnsupportedOperationError`. This is tracked as
follow-up work in `baoulo/plans/plan-0.10.0.md` section 9, not an
oversight.

## Snapshotting

Pass `:snapshot` (`:to`, an `{backend_module, ref, key}` triple naming any
`ExDataSketch.Storage` backend; `:every`, a millisecond interval or
`:infinity`) to persist the server's current state periodically and on
graceful shutdown:

```elixir
{:ok, pid} =
  ExDataSketch.Server.start_link(
    sketch: :hll,
    sketch_opts: [p: 14],
    snapshot: [to: {ExDataSketch.Storage.ETS, :sketches, "uniques"}, every: :timer.seconds(30)]
  )
```

On start, the server attempts to load from the same location first (crash
recovery); if nothing is found, or loading fails for any other reason, it
starts from a fresh sketch either way -- `[:ex_data_sketch, :server, :restore]`
fires regardless, with `found` in its metadata saying which happened.

Worst-case data loss from an ordinary (non-`:kill`) process termination is
bounded by `:every` -- a graceful stop or supervisor-initiated shutdown
additionally snapshots on termination before exiting, so that case loses
nothing. An untrappable `Process.exit(pid, :kill)` skips graceful
termination entirely (this is a BEAM guarantee, not something any process
can opt out of), so in that specific case the periodic `:every` interval is
the only protection against data loss.

## Flushing

Pass `:flush` (`:interval`, a millisecond interval or `:infinity`;
`:callback`, an optional `(struct() -> term())`) for a return-and-reset-on-
a-timer pattern: on each interval, the callback (if any) receives the
current sketch, `[:ex_data_sketch, :server, :flush]` fires, and the server
resets to a fresh sketch. Calling `flush/1` manually does the same reset
and emits the same event on demand, without invoking the callback -- only
the timer does that.

```elixir
{:ok, pid} =
  ExDataSketch.Server.start_link(
    sketch: :hll,
    sketch_opts: [p: 14],
    flush: [interval: :timer.seconds(5), callback: fn sketch -> report(sketch) end]
  )
```

`ExDataSketch.Broadway.PeriodicAggregator` is a thin wrapper around a
`:flush`-configured `Server`, kept for pipelines that already use its
pre-existing API -- see its module documentation.

## Running many servers at runtime: ExDataSketch.Sketches

`ExDataSketch.Server` names are fixed at compile/supervision-tree time.
`ExDataSketch.Sketches` starts and stops `Server` processes on demand,
addressed by any term -- a tenant ID read from a request, for example --
rather than a name you have to know in advance.

Add one instance to a supervision tree:

```elixir
children = [
  {ExDataSketch.Sketches, name: MyApp.Sketches}
]
```

Then start and address servers by term:

```elixir
{:ok, _pid} =
  ExDataSketch.Sketches.start_child(MyApp.Sketches, tenant_id, sketch: :hll, sketch_opts: [p: 14])

ExDataSketch.Server.update(ExDataSketch.Sketches.via(MyApp.Sketches, tenant_id), user_id)
ExDataSketch.Server.estimate(ExDataSketch.Sketches.via(MyApp.Sketches, tenant_id))
```

`via/2`'s result is an ordinary `:via` tuple, accepted by every
`ExDataSketch.Server` client function -- there is no separate lookup step.
`whereis/2` returns the raw pid (or `nil`) when that is what you need
instead; `stop_child/2` stops a server started under a given key.

Internally, each `Sketches` instance is a plain `Supervisor` with two
children: a `Registry` (`keys: :unique`) and a `DynamicSupervisor`.
`start_child/3` starts an `ExDataSketch.Server` under the
`DynamicSupervisor`, registered in the `Registry` under the given key --
the standard OTP pattern for addressing dynamically-started,
independently-addressable processes by term.

## See also

- `ExDataSketch.Server` module documentation -- full API reference.
- `ExDataSketch.Sketches` module documentation -- full API reference.
- `guides/windowing.md` -- what `:window` means precisely.
- `guides/telemetry.md` -- the `:server` telemetry events.
- `guides/persistence.md` -- the `ExDataSketch.Storage` backends usable with `:snapshot`.
