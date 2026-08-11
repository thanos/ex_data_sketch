# PhoenixDemo

A minimal, real, runnable Phoenix app demonstrating `ExDataSketch`'s
LiveDashboard integration and telemetry -- not a Livebook full of comments
describing what a router *would* look like, an actual `router.ex` with a
mounted dashboard.

It supersedes `livebooks/phoenix_observability.livemd` and
`livebooks/livedashboard_integration.livemd` (which only had commented-out
pseudocode for the router/application wiring shown here as working code),
and the telemetry section of `livebooks/rolling_telemetry.livemd` (whose
`ExDataSketch.Window` content moved to `guides/windowing.md` instead --
see `guides/livebooks.md`). All three are removed.

## What it demonstrates

- **`ExDataSketch.Server`** -- two supervised sketch processes started in
  `PhoenixDemo.Application`: `:visitors` (a windowed `HLL`, snapshotted to
  ETS) and `:search_terms` (a `MisraGries` top-K sketch). See
  `lib/phoenix_demo/application.ex`.
- **`ExDataSketch.Telemetry.Metrics.all/1`** -- wired directly into
  `PhoenixDemoWeb.Telemetry.metrics/0`, feeding LiveDashboard's built-in
  Metrics page.
- **`ExDataSketch.LiveDashboard.Page`** -- mounted as an additional
  LiveDashboard page listing every ExDataSketch telemetry event and the
  metric names derived from it. See `lib/phoenix_demo_web/router.ex`.
- **A live, interactive homepage** (`/`) reading real-time estimates via
  `ExDataSketch.Server.estimate/2` and `ExDataSketch.MisraGries.top_k/2`
  -- the kind of *live per-instance* view `ExDataSketch.LiveDashboard.Page`
  explicitly does not attempt to be (it's a static event/metric reference).
- **`PhoenixDemo.TrafficSimulator`** -- generates a steady trickle of
  synthetic visits and searches against the live Servers, plus a
  separate, independent simulation of parallel-worker batch aggregation
  (`ExDataSketch.HLL.from_enumerable/2` + `merge_many/1` on throwaway
  sketches) needed to actually exercise `:ingest`/`:merge` telemetry --
  see the module's own moduledoc for why that's a second mechanism and
  not just more `update/2` calls.

## What you'll see, and how soon

Telemetry events are emitted at compound-operation boundaries, not per
update (see `ExDataSketch.HLL`'s moduledoc) -- and LiveDashboard only
charts events that fire *while you're watching*, not history from before
you opened the page. So a freshly-loaded dashboard starts empty, and
different charts fill in on different schedules:

| Event(s) | Source | First appears after |
|---|---|---|
| `sketch.ingest`, `sketch.merge` | `TrafficSimulator`'s batch-import simulation | ~6s |
| `server.snapshot`, `sketch.serialize`, `persistence.save` | `:visitors`' periodic snapshot | ~8s |
| `window.roll` | `:visitors`' window aging out its oldest slot | ~30s (`keep: 6, every: 5s`) |
| `server.flush` | `:search_terms`' periodic reset | ~20s |

All four were confirmed firing *and rendering into the actual chart HTML*
during development, via `Phoenix.LiveViewTest` against
`/dashboard/metrics?nav=ex_data_sketch` -- both by triggering
`from_enumerable/2`/`merge_many/1`/`Server.flush/1` directly, and by
waiting out the real snapshot interval. If a chart stays empty well past
these numbers, it's a real problem, not just "give it a second."

If you still see nothing after a minute or so, something's actually
wrong -- check the server log for crashes, and confirm
`PhoenixDemo.TrafficSimulator` and both `ExDataSketch.Server` processes
are alive (`Process.whereis/1` in a remote console).

**Intentionally not exercised** by this demo, and why: `sketch.deserialize`
and `persistence.load` fire once at boot (crash-recovery restore), before
any dashboard viewer is watching; `server.restore` likewise; `server.drop`
needs a configured `:max_queue` under real backpressure, which would make
the live homepage numbers look broken; `server.snapshot_failed` needs the
snapshot's ETS table or backend to actually fail, which nothing here
induces; `persistence.merge`/`.delete` and `stream.*`/`pipeline.*` aren't
exercised by anything this app does (no direct `Storage.merge/3`/`.delete/2`
calls, no Stream/Broadway usage) -- `stream.reduce.count` in particular
will never fire here regardless, since it comes from `ExDataSketch.Flow`,
which this app doesn't depend on.

`sketch.ingest.count` (the counter next to `sketch.ingest`'s duration/
size_bytes summaries) looked like it belonged in this list too, but
didn't: `ExDataSketch.Telemetry.Metrics.event_counter/4` was silently
never firing due to a default-measurement-key bug in `ex_data_sketch`
itself (fixed upstream, not a demo-app issue) -- see its CHANGELOG.

## Running it

From this directory:

```sh
mix setup
EX_DATA_SKETCH_BUILD=1 mix phx.server
```

`EX_DATA_SKETCH_BUILD=1` is required because `ex_data_sketch` (a `path: ".."`
dependency here) has no precompiled NIF release published yet, so it must
compile its Rust NIF locally instead of fetching a prebuilt binary -- see
`config/config.exs`'s `:rustler_precompiled, :force_build` note. This
won't be necessary once `ex_data_sketch` ships precompiled artifacts for
a tagged release.

Then visit:

- [`localhost:4000/`](http://localhost:4000/) -- the live demo homepage
  (buttons + live-updating estimates).
- [`localhost:4000/dashboard/metrics?nav=ex_data_sketch`](http://localhost:4000/dashboard/metrics?nav=ex_data_sketch)
  -- LiveDashboard's metrics page, scoped directly to ExDataSketch's
  metrics group.
- [`localhost:4000/dashboard/sketches`](http://localhost:4000/dashboard/sketches)
  -- `ExDataSketch.LiveDashboard.Page`.

## Tests

```sh
EX_DATA_SKETCH_BUILD=1 mix test
```

`test/phoenix_demo_web/live/sketch_live_test.exs` exercises the actual
`phx-click`/`phx-submit` handlers via `Phoenix.LiveViewTest`, not just a
static render.
