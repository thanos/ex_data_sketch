defmodule ExDataSketch.LiveDashboard.PageTest do
  use ExUnit.Case, async: true

  alias ExDataSketch.LiveDashboard.Page
  alias ExDataSketch.Telemetry
  alias Phoenix.HTML.Safe
  alias Phoenix.LiveDashboard.PageBuilder

  describe "menu_link/2" do
    test "returns an {:ok, label} tuple" do
      assert {:ok, label} = Page.menu_link(%{}, %{})
      assert is_binary(label)
    end
  end

  describe "behaviour" do
    test "implements Phoenix.LiveDashboard.PageBuilder" do
      behaviours = Page.__info__(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten()
      assert PageBuilder in behaviours
    end

    test "exports render/1" do
      assert function_exported?(Page, :render, 1)
    end
  end

  describe "render/1" do
    test "renders a table listing every telemetry event" do
      html = Page.render(%{}) |> Safe.to_iodata() |> IO.iodata_to_binary()

      for event_name <- Telemetry.all_event_names() do
        assert html =~ Enum.join(event_name, ".")
      end
    end
  end
end
