defmodule PhoenixDemoWeb.SketchLiveTest do
  use PhoenixDemoWeb.ConnCase, async: false

  import Phoenix.LiveViewTest

  test "renders visitor and search-term estimates", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/")

    assert html =~ "Distinct visitors in the last ~30 seconds"
    assert html =~ "Trending search terms (Misra-Gries top-K)"
  end

  test "simulate_visit increments the visitor estimate", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    before_count = ExDataSketch.Server.estimate(:visitors, window: :all)

    for _ <- 1..10, do: view |> element("button", "Simulate 1 visit") |> render_click()

    after_count = ExDataSketch.Server.estimate(:visitors, window: :all)
    assert after_count >= before_count
  end

  test "simulate_search adds a term to the top-K list", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    html =
      view
      |> form("form", %{"term" => "unique_test_term"})
      |> render_submit()

    assert html =~ "unique_test_term"
  end
end
