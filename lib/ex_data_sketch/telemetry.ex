defmodule ExDataSketch.Telemetry do
  @moduledoc """
  Structured telemetry event emission for ExDataSketch.

  This module provides a unified interface for emitting telemetry events
  at meaningful operation boundaries within the ExDataSketch library.
  Individual `update/2` calls do **not** emit events (they can run at
  billions per second). Instead, events are emitted at batch/compound
  operations like `from_enumerable/2`, `merge_many/1`, `serialize/1`,
  `deserialize/1`, and all storage and pipeline operations.

  ## Configuration

  Telemetry can be disabled entirely or per-category:

      # Disable all telemetry (default: true)
      config :ex_data_sketch, telemetry_enabled: false

      # Disable specific categories (default: all true)
      config :ex_data_sketch, telemetry: [
        sketch: true,
        persistence: true,
        stream: true,
        pipeline: true
      ]

  When telemetry is disabled, `:telemetry.execute/3` is never called,
  ensuring zero overhead in production.

  ## Event Names

  ### Sketch Events

  | Event | Measurements | Metadata |
  |-------|-------------|----------|
  | `[:ex_data_sketch, :sketch, :ingest]` | `duration`, `size_bytes` (HLL only) | `sketch_type` |
  | `[:ex_data_sketch, :sketch, :merge]` | `duration`, `merge_count` | `sketch_type` |
  | `[:ex_data_sketch, :sketch, :serialize]` | `duration`, `size_bytes` | `sketch_type` |
  | `[:ex_data_sketch, :sketch, :deserialize]` | `duration`, `size_bytes` | `sketch_type` |

  > **Note on `:ingest` measurements:** All sketch types emit `duration`.
  > Only HLL emits the additional `size_bytes` measurement via its result
  > callback. Other sketch types emit `%{duration}` only. This is because
  > `from_enumerable/2` consumes a lazy stream and the item count is not
  > available without forcing evaluation.

  ### Persistence Events

  | Event | Measurements | Metadata |
  |-------|-------------|----------|
  | `[:ex_data_sketch, :persistence, :save]` | `duration`, `size_bytes` | `sketch_type`, `backend`, `key` |
  | `[:ex_data_sketch, :persistence, :load]` | `duration` | `sketch_type`, `backend`, `key` |
  | `[:ex_data_sketch, :persistence, :merge]` | `duration` | `sketch_type`, `backend`, `key` |
  | `[:ex_data_sketch, :persistence, :delete]` | `duration` | `backend`, `key` |

  > **Note on `:delete` metadata:** No `sketch_type` is available at deletion
  > time because the sketch struct has already been discarded.

  ### Stream Events

  | Event | Measurements | Metadata |
  |-------|-------------|----------|
  | `[:ex_data_sketch, :stream, :reduce]` | (none) | `sketch_type` |
  | `[:ex_data_sketch, :stream, :partition_merge]` | `duration`, `partition_count` | `sketch_type` |

  > **Note on `:reduce` measurements:** The `Flow.reduce/3` integration emits
  > this event as a completion signal from `Flow.on_trigger/2`. Because the
  > reduce runs inside the Flow runtime, the timing is not accessible; the
  > event carries no measurements beyond `sketch_type` in metadata.

  ### Pipeline Events

  | Event | Measurements | Metadata |
  |-------|-------------|----------|
  | `[:ex_data_sketch, :pipeline, :accumulate]` | `duration`, `count` | `sketch_type`, `batch_size` |
  | `[:ex_data_sketch, :pipeline, :periodic_flush]` | `duration` | `sketch_type` |

  > **Note on `:periodic_flush` duration:** The `duration` measurement
  > represents the time elapsed since the previous flush (or since process
  > start), not the time taken to perform the flush itself.

  ## Usage

  Users attach handlers via `:telemetry.attach/4`:

       :telemetry.attach("my-handler", [:ex_data_sketch, :sketch, :ingest], fn _name, measurements, metadata, _config ->
         Logger.info("Ingested \#{metadata.sketch_type}: \#{measurements.size_bytes} bytes in \#{measurements.duration} ns")
       end, nil)

  Or use `ExDataSketch.Telemetry.OpenTelemetry.setup/0` to bridge to
  OpenTelemetry spans when the `:opentelemetry_api` dependency is available.
  """

  @type event_name :: [atom(), ...]
  @type measurements :: %{atom() => number()}
  @type metadata :: %{atom() => term()}

  @categories [:sketch, :persistence, :stream, :pipeline]

  @doc """
  Emits a telemetry event if telemetry is enabled for the given category.

  This function checks both the global `telemetry_enabled` config and the
  per-category config before emitting. When disabled, it returns `:ok`
  immediately without calling `:telemetry.execute/3`.

  ## Arguments

  - `event_name` -- the telemetry event name as a list of atoms.
  - `measurements` -- a map of numeric measurements.
  - `metadata` -- a map of event metadata.
  - `category` -- the event category (`:sketch`, `:persistence`,
    `:stream`, or `:pipeline`).

  ## Examples

      ExDataSketch.Telemetry.execute(
        [:ex_data_sketch, :sketch, :ingest],
        %{count: 1000, duration: 500_000},
        %{sketch_type: :hll},
        :sketch
      )

  """
  @spec execute(event_name(), measurements(), metadata(), atom()) :: :ok
  def execute(event_name, measurements, metadata, category) do
    if enabled?(category) do
      :telemetry.execute(event_name, measurements, metadata)
    end

    :ok
  end

  @doc """
  Executes a function and emits a start/stop telemetry event with duration.

  The `event_name` is used as-is for the stop event. Measurements include
  `duration` in native time units. Base measurements are merged with the
  computed duration.

  Returns the result of `fun.`

  ## Arguments

  - `event_name` -- the telemetry event name.
  - `base_measurements` -- a map of pre-computed measurements (can be empty).
  - `metadata` -- a map of event metadata.
  - `category` -- the event category.
  - `fun` -- the zero-arity function to time.

  ## Examples

      result = ExDataSketch.Telemetry.span(
        [:ex_data_sketch, :sketch, :merge],
        %{merge_count: 10},
        %{sketch_type: :hll},
        :sketch,
        fn -> ExDataSketch.HLL.merge_many(sketches) end
      )

  """
  @spec span(event_name(), measurements(), metadata(), atom(), (-> result)) :: result
        when result: var
  def span(event_name, base_measurements, metadata, category, fun) do
    if enabled?(category) do
      start_time = System.monotonic_time()
      result = fun.()
      duration = System.monotonic_time() - start_time

      measurements = Map.put(base_measurements, :duration, duration)
      :telemetry.execute(event_name, measurements, metadata)
      result
    else
      fun.()
    end
  end

  @doc """
  Emits a telemetry event with timing, returning the result alongside
  derived measurements.

  Similar to `span/5` but accepts a callback that receives the result and
  returns additional measurements to merge. This is useful when measurements
  depend on the result (e.g., `size_bytes` from `HLL.from_enumerable/2`).

  ## Arguments

  - `event_name` -- the telemetry event name.
  - `base_measurements` -- a map of pre-computed measurements.
  - `metadata` -- a map of event metadata.
  - `category` -- the event category.
  - `fun` -- the zero-arity function to time.
  - `result_callback` -- a function receiving the result and returning
    a map of additional measurements to merge.

  ## Examples

      {sketch, measurements} = ExDataSketch.Telemetry.span_with_result(
        [:ex_data_sketch, :sketch, :ingest],
        %{},
        %{sketch_type: :hll},
        :sketch,
        fn -> ExDataSketch.HLL.from_enumerable(items, p: 14) end,
        fn sketch -> %{size_bytes: ExDataSketch.HLL.size_bytes(sketch)} end
      )

  """
  @spec span_with_result(event_name(), measurements(), metadata(), atom(), (-> result), (result ->
                                                                                           measurements())) ::
          result
        when result: var
  def span_with_result(event_name, base_measurements, metadata, category, fun, result_callback) do
    if enabled?(category) do
      start_time = System.monotonic_time()
      result = fun.()
      duration = System.monotonic_time() - start_time

      extra = result_callback.(result)
      measurements = base_measurements |> Map.put(:duration, duration) |> Map.merge(extra)
      :telemetry.execute(event_name, measurements, metadata)
      result
    else
      fun.()
    end
  end

  @doc """
  Returns whether telemetry events should be emitted for the given category.

  Checks the global `telemetry_enabled` config first, then the per-category
  config under the `:telemetry` key.

  ## Examples

      iex> is_boolean(ExDataSketch.Telemetry.enabled?(:sketch))
      true

  """
  @spec enabled?(atom()) :: boolean()
  def enabled?(category) when category in @categories do
    globally_enabled?() and category_enabled?(category)
  end

  defp globally_enabled? do
    Application.get_env(:ex_data_sketch, :telemetry_enabled, true)
  end

  defp category_enabled?(category) do
    categories = Application.get_env(:ex_data_sketch, :telemetry, [])
    Keyword.get(categories, category, true)
  end

  @doc """
  Returns the sketch type atom for a sketch struct.

  Used in telemetry metadata to identify which sketch type produced an event.

  ## Examples

      iex> ExDataSketch.Telemetry.sketch_type(%ExDataSketch.HLL{})
      :hll

      iex> ExDataSketch.Telemetry.sketch_type(%ExDataSketch.CMS{})
      :cms

  """
  @spec sketch_type(struct()) :: atom()
  def sketch_type(%ExDataSketch.HLL{}), do: :hll
  def sketch_type(%ExDataSketch.ULL{}), do: :ull
  def sketch_type(%ExDataSketch.CMS{}), do: :cms
  def sketch_type(%ExDataSketch.Theta{}), do: :theta
  def sketch_type(%ExDataSketch.KLL{}), do: :kll
  def sketch_type(%ExDataSketch.DDSketch{}), do: :ddsketch
  def sketch_type(%ExDataSketch.REQ{}), do: :req
  def sketch_type(%ExDataSketch.FrequentItems{}), do: :frequent_items
  def sketch_type(%ExDataSketch.MisraGries{}), do: :misra_gries
  def sketch_type(%ExDataSketch.Bloom{}), do: :bloom
  def sketch_type(%ExDataSketch.Cuckoo{}), do: :cuckoo
  def sketch_type(%ExDataSketch.Quotient{}), do: :quotient
  def sketch_type(%ExDataSketch.CQF{}), do: :cqf
  def sketch_type(%ExDataSketch.IBLT{}), do: :iblt

  def sketch_type(other),
    do:
      other.__struct__ |> Module.split() |> List.last() |> Macro.underscore() |> String.to_atom()

  @doc """
  Returns all supported event categories.

  ## Examples

      iex> ExDataSketch.Telemetry.categories()
      [:sketch, :persistence, :stream, :pipeline]

  """
  @spec categories() :: [atom()]
  def categories, do: @categories

  @doc """
  Returns the canonical event name for a given event type.

  Useful for attaching handlers programmatically.

  ## Examples

      iex> ExDataSketch.Telemetry.event_name(:sketch, :ingest)
      [:ex_data_sketch, :sketch, :ingest]

      iex> ExDataSketch.Telemetry.event_name(:persistence, :save)
      [:ex_data_sketch, :persistence, :save]

  """
  @spec event_name(atom(), atom()) :: event_name()
  def event_name(category, action) when category in @categories do
    [:ex_data_sketch, category, action]
  end

  @doc """
  Returns all canonical event names.

  ## Examples

      iex> length(ExDataSketch.Telemetry.all_event_names()) > 0
      true

  """
  @spec all_event_names() :: [event_name()]
  def all_event_names do
    sketch_events =
      for action <- [:ingest, :merge, :serialize, :deserialize] do
        event_name(:sketch, action)
      end

    persistence_events =
      for action <- [:save, :load, :merge, :delete] do
        event_name(:persistence, action)
      end

    stream_events =
      for action <- [:reduce, :partition_merge] do
        event_name(:stream, action)
      end

    pipeline_events =
      for action <- [:accumulate, :periodic_flush] do
        event_name(:pipeline, action)
      end

    sketch_events ++ persistence_events ++ stream_events ++ pipeline_events
  end
end
