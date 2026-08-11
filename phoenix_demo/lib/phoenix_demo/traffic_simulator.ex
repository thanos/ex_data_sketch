defmodule PhoenixDemo.TrafficSimulator do
  @moduledoc """
  Generates a steady trickle of synthetic activity so the LiveDashboard
  metrics page and the homepage have real, continuously-changing numbers
  to show without requiring manual clicking.

  Two independent things happen here, because they exercise genuinely
  different telemetry:

  1. **Live traffic** against the `:visitors` and `:search_terms`
     `ExDataSketch.Server` instances, via `update/2` (and an occasional
     `update_many/2` burst). This is what drives the numbers on the
     homepage.

  2. **Simulated batch-import fan-in**, via `ExDataSketch.HLL.from_enumerable/2`
     and `merge_many/1` on throwaway sketches, disconnected from the live
     Servers. Neither `update/2` nor `update_many/2` -- nor `Server.merge/2`,
     which calls the underlying sketch's pairwise `merge/2` -- ever emits
     `[:ex_data_sketch, :sketch, :ingest]` or `[:ex_data_sketch, :sketch,
     :merge]`: those are compound-boundary events specific to
     `from_enumerable/2` (building a sketch from a full collection) and
     `merge_many/1` (folding a list of sketches), respectively -- see
     `ExDataSketch.HLL`'s moduledoc. A single always-running Server that
     only ever receives one-item-at-a-time updates would never produce
     those two events at all, so this simulates the other half of a
     realistic deployment: parallel workers (or an offline/nightly job)
     each building a partial sketch and merging them, which is exactly
     what `from_enumerable/2` + `merge_many/1` are for.
  """

  use GenServer

  @tick_interval :timer.seconds(1)
  @burst_every_ticks 8
  @batch_import_every_ticks 6
  @user_pool 5_000
  @search_terms ~w(
    hyperloglog bloom_filter count_min_sketch cardinality_estimation
    streaming_analytics telemetry phoenix_livedashboard cuckoo_filter
    quantile_sketch theta_sketch misra_gries kll_sketch elixir_otp
  )

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    schedule_tick()
    {:ok, %{tick: 0}}
  end

  @impl true
  def handle_info(:tick, state) do
    tick = state.tick + 1
    simulate_live_traffic()
    if rem(tick, @burst_every_ticks) == 0, do: simulate_burst()
    if rem(tick, @batch_import_every_ticks) == 0, do: simulate_batch_import()
    schedule_tick()
    {:noreply, %{state | tick: tick}}
  end

  defp simulate_live_traffic do
    ExDataSketch.Server.update(:visitors, random_user())
    ExDataSketch.Server.update(:search_terms, Enum.random(@search_terms))
  end

  defp simulate_burst do
    burst = for _ <- 1..50, do: random_user()
    ExDataSketch.Server.update_many(:visitors, burst)
  end

  defp simulate_batch_import do
    partial_sketches =
      for _worker <- 1..3 do
        items = for _ <- 1..30, do: random_user()
        ExDataSketch.HLL.from_enumerable(items, p: 14)
      end

    _merged = ExDataSketch.HLL.merge_many(partial_sketches)
    :ok
  end

  defp random_user, do: "user_#{:rand.uniform(@user_pool)}"

  defp schedule_tick, do: Process.send_after(self(), :tick, @tick_interval)
end
