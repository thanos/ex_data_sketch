defmodule ExDataSketch.Server do
  @moduledoc """
  A supervised, named, concurrently-updatable sketch process with optional
  windowing and snapshotting.

  `ExDataSketch.Server` is a `GenServer` wrapping a single sketch (or
  `ExDataSketch.Window`), so an application can hold a running counter --
  "distinct users right now", "distinct users in the last 5 minutes" -- as
  an ordinary named process instead of hand-writing state management,
  concurrency, and crash recovery.

  ## Quick Example

      {:ok, _pid} = ExDataSketch.Server.start_link(
        name: :uniques,
        sketch: :hll,
        sketch_opts: [p: 14],
        window: [every: :timer.minutes(1), keep: 5],
        snapshot: [to: {ExDataSketch.Storage.ETS, :sketches, "uniques"}, every: :timer.seconds(30)]
      )

      ExDataSketch.Server.update(:uniques, user_id)
      ExDataSketch.Server.estimate(:uniques)

  ## Backpressure

  `update/2` and `update_many/2` are `GenServer.cast/2` calls: they return
  immediately and do not wait for the update to be applied. Under
  sustained load faster than the server can process, its mailbox grows
  without bound unless `:max_queue` is configured. This is a deliberate
  trade for throughput -- callers that need a guarantee the update was
  applied before continuing use `update_sync/2` instead, a `GenServer.call/2`
  that blocks until processed (and, as an ordinary call, provides natural
  backpressure: a caller cannot get more than one `update_sync/2` ahead of
  the server).

  When `:max_queue` is set, `update/2` and `update_many/2` drop the update
  (rather than applying it) whenever the server's own mailbox is at or
  above that threshold, emitting `[:ex_data_sketch, :server, :drop]`. This
  bounds memory growth from a backed-up mailbox at the cost of the data
  loss `update/2` already implies under overload -- `update_sync/2` is never
  subject to `:max_queue`.

  ## Windowing

  Pass `:window` (the same options `ExDataSketch.Window.new/3` accepts:
  `:every`, `:keep`, `:time_fn`) to have the server hold a
  `ExDataSketch.Window` instead of a bare sketch. `estimate/1` then answers
  "in the last `keep * every`" instead of "ever". See `guides/windowing.md`
  for what that means precisely (a tumbling ring, not an exact sliding
  window) and which sketch families can be windowed.

  Add `track_all_time: true` to `:window` to also maintain a second,
  un-windowed sketch alongside the window (same `:sketch_opts`, so the same
  fixed size as any other sketch of that family -- not a growing or
  unbounded structure). `estimate(server, window: :all)` reads it;
  `snapshot`, if configured, always persists the window, not this second
  sketch, which restarts empty after a crash even when enabled (see
  `baoulo/plans/0.10.0_phase4_design_review.md` section 4.2 for the
  rationale).

  A windowed server does not support `merge/2` in this release --
  `ExDataSketch.Window` has no primitive to merge an already-built sketch
  into its current slot (only raw-item `update/2,3` and `update_many/2`).
  Calling `merge/2` on a windowed server raises
  `ExDataSketch.Errors.UnsupportedOperationError`. This is tracked as
  follow-up work in `baoulo/plans/plan-0.10.0.md` section 9, not an
  oversight.

  ## Snapshotting

  Pass `:snapshot` (`:to`, an `{backend_module, ref, key}` triple naming
  any `ExDataSketch.Storage` backend; `:every`, a millisecond interval or
  `:infinity`) to persist the server's current state periodically and on
  graceful shutdown. On start, the server attempts to load from the same
  location first (crash recovery); if nothing is found, or loading fails
  for any other reason, it starts from a fresh sketch either way --
  `[:ex_data_sketch, :server, :restore]` fires either way, with `found` in
  its metadata saying which happened.

  Worst-case data loss from an ordinary (non-`:kill`) process termination
  is bounded by `:every` -- a graceful stop or supervisor-initiated shutdown
  additionally snapshots on `terminate/2` before exiting, so that case loses
  nothing. An untrappable `Process.exit(pid, :kill)` never runs
  `terminate/2` at all (this is a BEAM guarantee, not something any
  process can opt out of), so in that specific case the periodic `:every`
  interval is the only protection -- this is the "kill" scenario the
  plan's own test targets.

  ## Flushing

  Pass `:flush` (`:interval`, a millisecond interval or `:infinity`;
  `:callback`, an optional `(struct() -> term())`) for the
  return-and-reset-on-a-timer pattern `ExDataSketch.Broadway.PeriodicAggregator`
  uses: on each interval, the callback (if any) receives the current
  sketch, `[:ex_data_sketch, :server, :flush]` fires, and the server resets
  to a fresh sketch. `PeriodicAggregator` is a thin wrapper around a
  `:flush`-configured `Server` -- see its moduledoc for the relationship.
  `:flush` and `:window` are independent options; combining them is not
  rejected, but resets the whole window (every slot) on each flush and is
  not a use case this module specifically targets.
  """

  use GenServer

  alias ExDataSketch.{Errors, Storage, Telemetry, Window}

  @typedoc "Either a registry atom (`:hll`) or a sketch module (`ExDataSketch.HLL`)."
  @type sketch_type :: ExDataSketch.sketch_type() | module()

  @typedoc "Where and how often to persist state. `:every` may be `:infinity` (snapshot on graceful stop only)."
  @type snapshot_opts :: [to: {module(), term(), Storage.key()}, every: pos_integer() | :infinity]

  @typedoc "Periodic return-and-reset configuration, matching `ExDataSketch.Broadway.PeriodicAggregator`."
  @type flush_opts :: [interval: pos_integer() | :infinity, callback: (struct() -> term()) | nil]

  @typedoc "The same options `ExDataSketch.Window.new/3` accepts, plus `:track_all_time`."
  @type window_opts :: [
          every: pos_integer(),
          keep: pos_integer(),
          time_fn: (-> integer()),
          track_all_time: boolean()
        ]

  @type start_opts :: [
          sketch: sketch_type(),
          sketch_opts: keyword(),
          name: GenServer.name(),
          window: window_opts() | nil,
          snapshot: snapshot_opts() | nil,
          flush: flush_opts() | nil,
          max_queue: non_neg_integer() | :infinity
        ]

  @type estimate_opts :: [window: :all]

  ## Client API

  @doc """
  Starts a server process.

  ## Options

  - `:sketch` -- required. A registry atom (`:hll`, looked up via
    `ExDataSketch.sketches/0`) or a sketch module directly. Must resolve to
    a mergeable family (see `ExDataSketch.Window.new/3`) when `:window` is
    also given.
  - `:sketch_opts` -- options forwarded to the sketch module's `new/1`
    (default: `[]`).
  - `:name` -- optional, forwarded to `GenServer.start_link/3`'s `:name`
    (an atom, `{:global, term}`, or `{:via, module, term}`).
  - `:window` -- optional. See the module documentation's "Windowing"
    section.
  - `:snapshot` -- optional. See the module documentation's "Snapshotting"
    section.
  - `:flush` -- optional. See the module documentation's "Flushing" section.
  - `:max_queue` -- optional non-negative integer or `:infinity` (default:
    `:infinity`). See the module documentation's "Backpressure" section.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> is_pid(pid)
      true

  """
  @spec start_link(start_opts()) :: GenServer.on_start()
  def start_link(opts) do
    gen_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Updates the server with a single item.

  Asynchronous (`GenServer.cast/2`) -- returns immediately, does not wait
  for the update to be applied, and can be dropped under `:max_queue`
  pressure. See the module documentation's "Backpressure" section, and
  `update_sync/2` for a guaranteed alternative.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> ExDataSketch.Server.update(pid, "user_1")
      :ok

  """
  @spec update(GenServer.server(), term()) :: :ok
  def update(server, item), do: GenServer.cast(server, {:update, item})

  @doc """
  Updates the server with a single item, blocking until it has been applied.

  Never dropped by `:max_queue` -- callers that need a guarantee the
  update was applied use this instead of `update/2`.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> :ok = ExDataSketch.Server.update_sync(pid, "user_1")
      iex> ExDataSketch.Server.estimate(pid) > 0.0
      true

  """
  @spec update_sync(GenServer.server(), term()) :: :ok
  def update_sync(server, item), do: GenServer.call(server, {:update, item})

  @doc """
  Updates the server with every item in an enumerable.

  Asynchronous, like `update/2` -- see the module documentation's
  "Backpressure" section.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> ExDataSketch.Server.update_many(pid, ["a", "b", "c"])
      :ok

  """
  @spec update_many(GenServer.server(), Enumerable.t()) :: :ok
  def update_many(server, items), do: GenServer.cast(server, {:update_many, items})

  @doc """
  Returns the server's current headline estimate.

  With no `:window` configured, this is `ExDataSketch.estimate/1` on the
  underlying sketch. With `:window` configured, it defaults to the windowed
  reading (`ExDataSketch.Window.estimate/1`); pass `window: :all` to read
  the un-windowed, all-time accumulator instead, which requires the server
  to have been started with `window: [..., track_all_time: true]`.

  Raises `ExDataSketch.Errors.UnsupportedOperationError` for `window: :all`
  when `track_all_time: true` was not set, or for any `estimate/1,2` call
  against a family with no single-value reading (see
  `ExDataSketch.estimate/1`).

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> :ok = ExDataSketch.Server.update_sync(pid, "user_1")
      iex> ExDataSketch.Server.estimate(pid) > 0.0
      true

      iex> {:ok, pid} = ExDataSketch.Server.start_link(
      ...>   sketch: :hll, sketch_opts: [p: 10],
      ...>   window: [every: 60_000, keep: 5, track_all_time: true]
      ...> )
      iex> :ok = ExDataSketch.Server.update_sync(pid, "user_1")
      iex> ExDataSketch.Server.estimate(pid) > 0.0 and ExDataSketch.Server.estimate(pid, window: :all) > 0.0
      true

  """
  @spec estimate(GenServer.server(), estimate_opts()) :: number()
  def estimate(server, opts \\ []) do
    unwrap(GenServer.call(server, {:estimate, opts}))
  end

  @doc """
  Returns the server's current raw state: a `%ExDataSketch.Window{}` if
  `:window` was configured, otherwise the bare sketch struct.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> match?(%ExDataSketch.HLL{}, ExDataSketch.Server.sketch(pid))
      true

  """
  @spec sketch(GenServer.server()) :: struct()
  def sketch(server), do: GenServer.call(server, :sketch)

  @doc """
  Merges an already-built partial sketch into the server's current state.

  Raises `ExDataSketch.Errors.UnsupportedOperationError` if the server is
  windowed -- see the module documentation's "Windowing" section.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> partial = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> :ok = ExDataSketch.Server.merge(pid, partial)
      iex> ExDataSketch.Server.estimate(pid) > 0.0
      true

  """
  @spec merge(GenServer.server(), struct()) :: :ok
  def merge(server, partial_sketch) do
    unwrap(GenServer.call(server, {:merge, partial_sketch}))
  end

  @doc """
  Resets the server to a fresh, empty state (the window and, if configured,
  the all-time accumulator, when windowed).

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> :ok = ExDataSketch.Server.update_sync(pid, "user_1")
      iex> :ok = ExDataSketch.Server.reset(pid)
      iex> ExDataSketch.Server.estimate(pid)
      0.0

  """
  @spec reset(GenServer.server()) :: :ok
  def reset(server), do: GenServer.call(server, :reset)

  @doc """
  Returns the server's current state and resets it to a fresh, empty one,
  on demand (independent of any configured `:flush` interval).

  Unlike the automatic timer-driven flush (see the module documentation's
  "Flushing" section), a manual call here does not invoke the `:flush`
  callback -- only the timer does. Both emit
  `[:ex_data_sketch, :server, :flush]`.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Server.start_link(sketch: :hll, sketch_opts: [p: 10])
      iex> :ok = ExDataSketch.Server.update_sync(pid, "user_1")
      iex> flushed = ExDataSketch.Server.flush(pid)
      iex> ExDataSketch.HLL.estimate(flushed) > 0.0
      true
      iex> ExDataSketch.Server.estimate(pid)
      0.0

  """
  @spec flush(GenServer.server()) :: struct()
  def flush(server), do: GenServer.call(server, :flush)

  @spec unwrap({:error, Exception.t()} | term()) :: term() | no_return()
  defp unwrap({:error, exception}), do: raise(exception)
  defp unwrap(value), do: value

  ## GenServer callbacks

  @impl true
  def init(opts) do
    module = resolve_module(Keyword.fetch!(opts, :sketch))
    sketch_opts = Keyword.get(opts, :sketch_opts, [])
    max_queue = Keyword.get(opts, :max_queue, :infinity)

    windowed? = Keyword.get(opts, :window) != nil

    {track_all_time?, window_opts} =
      case Keyword.get(opts, :window) do
        nil -> {false, []}
        window_opts -> Keyword.pop(window_opts, :track_all_time, false)
      end

    snapshot = normalize_snapshot(Keyword.get(opts, :snapshot))
    flush = normalize_flush(Keyword.get(opts, :flush))

    {current, all_time} =
      load_initial_state(snapshot, module, sketch_opts, windowed?, window_opts, track_all_time?)

    if snapshot, do: schedule(:snapshot_tick, snapshot.every)
    if flush, do: schedule(:flush_tick, flush.interval)

    state = %{
      module: module,
      sketch_opts: sketch_opts,
      windowed?: windowed?,
      window_opts: window_opts,
      current: current,
      all_time: all_time,
      snapshot: snapshot,
      flush: flush,
      max_queue: max_queue,
      last_flush_time: System.monotonic_time()
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:update, item}, state) do
    if over_capacity?(state) do
      emit_drop(state)
      {:noreply, state}
    else
      {:noreply, apply_update(state, item)}
    end
  end

  def handle_cast({:update_many, items}, state) do
    if over_capacity?(state) do
      emit_drop(state)
      {:noreply, state}
    else
      {:noreply, apply_update_many(state, items)}
    end
  end

  @impl true
  def handle_call({:update, item}, _from, state) do
    {:reply, :ok, apply_update(state, item)}
  end

  def handle_call({:estimate, opts}, _from, state) do
    # do_estimate/2 can raise ExDataSketch.Errors.UnsupportedOperationError
    # for families with no single-value reading (ExDataSketch.estimate/1's
    # own documented behavior). Caught here and turned into an {:error, _}
    # reply so an unsupported call from a client raises in the *client*
    # process via estimate/2's unwrap/1, rather than crashing this server
    # and losing all of its accumulated state.
    reply =
      try do
        do_estimate(state, opts)
      rescue
        e in Errors.UnsupportedOperationError -> Errors.error(e)
      end

    {:reply, reply, state}
  end

  def handle_call(:sketch, _from, state) do
    {:reply, state.current, state}
  end

  def handle_call({:merge, partial_sketch}, _from, state) do
    case do_merge(state, partial_sketch) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call(:reset, _from, state) do
    {:reply, :ok, reset_state(state)}
  end

  def handle_call(:flush, _from, state) do
    flushed = state.current
    {:reply, flushed, flush_and_reset(state)}
  end

  @impl true
  def handle_info(:snapshot_tick, state) do
    new_state = do_snapshot(state)
    schedule(:snapshot_tick, state.snapshot.every)
    {:noreply, new_state}
  end

  def handle_info(:flush_tick, state) do
    flushed = state.current
    if state.flush.callback, do: state.flush.callback.(flushed)
    new_state = flush_and_reset(state)
    schedule(:flush_tick, state.flush.interval)
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, state) do
    do_snapshot(state)
    :ok
  end

  ## Private

  defp resolve_module(type_or_module) when is_atom(type_or_module) do
    Map.get(ExDataSketch.sketches(), type_or_module, type_or_module)
  end

  defp normalize_snapshot(nil), do: nil

  defp normalize_snapshot(opts) do
    to = Keyword.fetch!(opts, :to)
    every = Keyword.get(opts, :every, :infinity)
    %{to: to, every: every}
  end

  defp normalize_flush(nil), do: nil

  defp normalize_flush(opts) do
    %{interval: Keyword.get(opts, :interval, :infinity), callback: Keyword.get(opts, :callback)}
  end

  defp schedule(_message, :infinity), do: :ok

  defp schedule(message, interval) when is_integer(interval) do
    Process.send_after(self(), message, interval)
  end

  defp load_initial_state(nil, module, sketch_opts, windowed?, window_opts, track_all_time?) do
    {build_current(module, sketch_opts, windowed?, window_opts),
     build_all_time(module, sketch_opts, track_all_time?)}
  end

  defp load_initial_state(
         %{to: {backend_module, ref, key}},
         module,
         sketch_opts,
         windowed?,
         window_opts,
         track_all_time?
       ) do
    restore_module = if windowed?, do: Window, else: module
    start_time = System.monotonic_time()

    {current, found} =
      case Storage.load(restore_module, {backend_module, ref}, key) do
        {:ok, restored} -> {restored, true}
        {:error, _reason} -> {build_current(module, sketch_opts, windowed?, window_opts), false}
      end

    Telemetry.execute(
      Telemetry.event_name(:server, :restore),
      %{duration: System.monotonic_time() - start_time},
      %{
        sketch_type: Telemetry.sketch_type_from_module(module),
        backend: backend_module,
        key: key,
        found: found
      },
      :server
    )

    {current, build_all_time(module, sketch_opts, track_all_time?)}
  end

  defp build_current(module, sketch_opts, true, window_opts) do
    Window.new(module, sketch_opts, window_opts)
  end

  defp build_current(module, sketch_opts, false, _window_opts) do
    module.new(sketch_opts)
  end

  defp build_all_time(_module, _sketch_opts, false), do: nil
  defp build_all_time(module, sketch_opts, true), do: module.new(sketch_opts)

  defp over_capacity?(%{max_queue: :infinity}), do: false

  defp over_capacity?(%{max_queue: max_queue}) do
    {:message_queue_len, len} = Process.info(self(), :message_queue_len)
    len >= max_queue
  end

  defp emit_drop(state) do
    {:message_queue_len, len} = Process.info(self(), :message_queue_len)

    Telemetry.execute(
      Telemetry.event_name(:server, :drop),
      %{queue_len: len},
      %{sketch_type: Telemetry.sketch_type_from_module(state.module)},
      :server
    )
  end

  defp apply_update(%{windowed?: true} = state, item) do
    current = Window.update(state.current, item)
    all_time = state.all_time && state.module.update(state.all_time, item)
    %{state | current: current, all_time: all_time}
  end

  defp apply_update(%{windowed?: false} = state, item) do
    %{state | current: state.module.update(state.current, item)}
  end

  defp apply_update_many(%{windowed?: true} = state, items) do
    items = Enum.to_list(items)
    current = Window.update_many(state.current, items)
    all_time = state.all_time && state.module.update_many(state.all_time, items)
    %{state | current: current, all_time: all_time}
  end

  defp apply_update_many(%{windowed?: false} = state, items) do
    %{state | current: state.module.update_many(state.current, items)}
  end

  defp do_estimate(%{windowed?: false} = state, []) do
    ExDataSketch.estimate(state.current)
  end

  defp do_estimate(%{windowed?: true} = state, []) do
    Window.estimate(state.current)
  end

  defp do_estimate(%{all_time: nil}, window: :all) do
    Errors.error(
      Errors.UnsupportedOperationError.exception(
        operation: "estimate/2 with window: :all",
        structure: "this server (not started with window: [track_all_time: true])"
      )
    )
  end

  defp do_estimate(%{all_time: all_time}, window: :all) do
    ExDataSketch.estimate(all_time)
  end

  defp do_merge(%{windowed?: true}, _partial_sketch) do
    Errors.error(
      Errors.UnsupportedOperationError.exception(
        operation: "merge/2",
        structure:
          "a windowed ExDataSketch.Server (ExDataSketch.Window has no primitive yet to merge an already-built sketch into its current slot; tracked for v0.11.0)"
      )
    )
  end

  defp do_merge(%{windowed?: false} = state, partial_sketch) do
    {:ok, %{state | current: state.module.merge(state.current, partial_sketch)}}
  end

  defp reset_state(%{windowed?: true} = state) do
    %{
      state
      | current: Window.new(state.module, state.sketch_opts, state.window_opts),
        all_time: state.all_time && state.module.new(state.sketch_opts)
    }
  end

  defp reset_state(%{windowed?: false} = state) do
    %{state | current: state.module.new(state.sketch_opts)}
  end

  defp do_snapshot(%{snapshot: nil} = state), do: state

  defp do_snapshot(%{snapshot: %{to: {backend_module, ref, key}}} = state) do
    start_time = System.monotonic_time()
    data = state.current

    case Storage.save(data, {backend_module, ref}, key) do
      :ok ->
        Telemetry.execute(
          Telemetry.event_name(:server, :snapshot),
          %{
            duration: System.monotonic_time() - start_time,
            size_bytes: data.__struct__.size_bytes(data)
          },
          %{
            sketch_type: Telemetry.sketch_type_from_module(state.module),
            backend: backend_module,
            key: key
          },
          :server
        )

      {:error, _reason} ->
        :ok
    end

    state
  end

  # Shared by the manual `flush/1` call and the automatic `:flush_tick`
  # path: resets state and emits `[:ex_data_sketch, :server, :flush]`.
  # Invoking `state.flush.callback` is exclusive to the automatic path --
  # see `handle_info(:flush_tick, ...)`.
  defp flush_and_reset(state) do
    now = System.monotonic_time()
    duration = now - state.last_flush_time

    Telemetry.execute(
      Telemetry.event_name(:server, :flush),
      %{duration: duration},
      %{sketch_type: Telemetry.sketch_type_from_module(state.module)},
      :server
    )

    state
    |> reset_state()
    |> Map.put(:last_flush_time, now)
  end
end
