defmodule PhoenixDemoWeb.SketchLive do
  @moduledoc """
  The homepage (`/`). Live view of the two `ExDataSketch.Server` instances
  started in `PhoenixDemo.Application`: `:visitors` (a windowed HLL) and
  `:search_terms` (a MisraGries top-K sketch). `PhoenixDemo.TrafficSimulator`
  keeps both moving in the background; the buttons here let you also
  trigger updates directly and immediately see the estimate change.

  See `/dashboard/metrics?nav=ex_data_sketch` (LiveDashboard's metrics
  page, scoped directly to ExDataSketch's metrics) and
  `/dashboard/sketches` (`ExDataSketch.LiveDashboard.Page`) for the
  telemetry side of this same activity.
  """

  use PhoenixDemoWeb, :live_view

  @refresh_interval :timer.seconds(1)

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: Process.send_after(self(), :refresh, @refresh_interval)
    {:ok, assign_estimates(socket)}
  end

  @impl true
  def handle_info(:refresh, socket) do
    Process.send_after(self(), :refresh, @refresh_interval)
    {:noreply, assign_estimates(socket)}
  end

  @impl true
  def handle_event("simulate_visit", _params, socket) do
    ExDataSketch.Server.update(:visitors, "user_#{:rand.uniform(5_000)}")
    {:noreply, assign_estimates(socket)}
  end

  @impl true
  def handle_event("simulate_burst", _params, socket) do
    users = for _ <- 1..200, do: "user_#{:rand.uniform(5_000)}"
    ExDataSketch.Server.update_many(:visitors, users)
    {:noreply, assign_estimates(socket)}
  end

  @impl true
  def handle_event("simulate_search", %{"term" => term}, socket) when term != "" do
    ExDataSketch.Server.update(:search_terms, term)
    {:noreply, assign_estimates(socket)}
  end

  def handle_event("simulate_search", _params, socket), do: {:noreply, socket}

  defp assign_estimates(socket) do
    top_terms = :search_terms |> ExDataSketch.Server.sketch() |> ExDataSketch.MisraGries.top_k(8)

    assign(socket,
      visitors_recent: ExDataSketch.Server.estimate(:visitors),
      visitors_all_time: ExDataSketch.Server.estimate(:visitors, window: :all),
      top_terms: top_terms
    )
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="mx-auto max-w-2xl space-y-8 py-8">
      <div>
        <h1 class="text-2xl font-bold">ExDataSketch Phoenix Demo</h1>
        <p class="text-sm opacity-70">
          Two supervised <code>ExDataSketch.Server</code>
          instances, fed by a
          background traffic simulator plus the buttons below. See
          <a class="link" href="/dashboard/metrics?nav=ex_data_sketch">LiveDashboard</a>
          and <a class="link" href="/dashboard/sketches">the ExDataSketch dashboard page</a>
          for the telemetry this activity generates.
        </p>
      </div>

      <div class="card bg-base-200 shadow">
        <div class="card-body">
          <h2 class="card-title">Visitors (HLL, windowed)</h2>
          <p>Distinct visitors in the last ~30 seconds: <strong>{round(@visitors_recent)}</strong></p>
          <p>Distinct visitors, all time: <strong>{round(@visitors_all_time)}</strong></p>
          <div class="card-actions">
            <button class="btn btn-primary" phx-click="simulate_visit">Simulate 1 visit</button>
            <button class="btn btn-secondary" phx-click="simulate_burst">
              Simulate 200-visit burst
            </button>
          </div>
        </div>
      </div>

      <div class="card bg-base-200 shadow">
        <div class="card-body">
          <h2 class="card-title">Trending search terms (Misra-Gries top-K)</h2>
          <p class="text-sm opacity-70">
            Resets every 20s (see the server's <code>:flush</code> config).
          </p>
          <form phx-submit="simulate_search" class="flex gap-2">
            <input
              type="text"
              name="term"
              placeholder="Type a search term..."
              class="input input-bordered flex-1"
            />
            <button type="submit" class="btn btn-primary">Search</button>
          </form>
          <ol class="list-decimal list-inside">
            <li :for={{term, count} <- @top_terms}>
              {term} <span class="opacity-60">({count})</span>
            </li>
          </ol>
        </div>
      </div>
    </div>
    """
  end
end
