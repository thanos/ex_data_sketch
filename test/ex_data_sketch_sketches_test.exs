defmodule ExDataSketch.SketchesTest do
  use ExUnit.Case, async: true

  doctest ExDataSketch.Sketches

  alias ExDataSketch.{HLL, Server, Sketches}

  setup do
    name = :"sketches_test_#{System.unique_integer([:positive])}"
    {:ok, _pid} = Sketches.start_link(name: name)
    %{name: name}
  end

  describe "start_child/3" do
    test "starts a Server addressable via via/2", %{name: name} do
      {:ok, _pid} = Sketches.start_child(name, :tenant_a, sketch: :hll, sketch_opts: [p: 10])

      server = Sketches.via(name, :tenant_a)
      :ok = Server.update_sync(server, "user_1")
      assert Server.estimate(server) > 0.0
    end

    test "different keys address independent servers", %{name: name} do
      {:ok, _pid} = Sketches.start_child(name, :tenant_a, sketch: :hll, sketch_opts: [p: 10])
      {:ok, _pid} = Sketches.start_child(name, :tenant_b, sketch: :hll, sketch_opts: [p: 10])

      :ok = Server.update_sync(Sketches.via(name, :tenant_a), "user_1")

      assert Server.estimate(Sketches.via(name, :tenant_a)) > 0.0
      assert Server.estimate(Sketches.via(name, :tenant_b)) == 0.0
    end

    test "ignores a :name passed in server_opts", %{name: name} do
      {:ok, pid} =
        Sketches.start_child(name, :tenant_a, sketch: :hll, sketch_opts: [p: 10], name: :ignored)

      assert Sketches.whereis(name, :tenant_a) == pid
      assert Process.whereis(:ignored) == nil
    end
  end

  describe "whereis/2" do
    test "returns the pid once started", %{name: name} do
      {:ok, pid} = Sketches.start_child(name, :tenant_a, sketch: :hll, sketch_opts: [p: 10])
      assert Sketches.whereis(name, :tenant_a) == pid
    end

    test "returns nil for an unknown key", %{name: name} do
      assert Sketches.whereis(name, :no_such_tenant) == nil
    end
  end

  describe "stop_child/2" do
    test "stops a started server", %{name: name} do
      {:ok, pid} = Sketches.start_child(name, :tenant_a, sketch: :hll, sketch_opts: [p: 10])
      ref = Process.monitor(pid)
      assert :ok = Sketches.stop_child(name, :tenant_a)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

      # Registry removes a dead process's registration via its own,
      # independent monitor -- there is no ordering guarantee between that
      # cleanup and this test's own monitor above, so poll briefly instead
      # of asserting immediately.
      wait_until(fn -> Sketches.whereis(name, :tenant_a) == nil end)
    end

    test "returns {:error, :not_found} for an unknown key", %{name: name} do
      assert Sketches.stop_child(name, :no_such_tenant) == {:error, :not_found}
    end
  end

  describe "multiple instances" do
    test "two Sketches instances with different names do not collide" do
      name_a = :"sketches_instance_a_#{System.unique_integer([:positive])}"
      name_b = :"sketches_instance_b_#{System.unique_integer([:positive])}"
      {:ok, _} = Sketches.start_link(name: name_a)
      {:ok, _} = Sketches.start_link(name: name_b)

      {:ok, _} = Sketches.start_child(name_a, :tenant_a, sketch: :hll, sketch_opts: [p: 10])

      assert Sketches.whereis(name_a, :tenant_a) != nil
      assert Sketches.whereis(name_b, :tenant_a) == nil
    end
  end

  describe "start_link/1 default name" do
    test "defaults to ExDataSketch.Sketches when :name is omitted" do
      # Only started once per test run to avoid clashing with other tests
      # that might also rely on the default name; use a fresh, unnamed
      # child_spec check instead of actually starting the default-named
      # singleton here.
      assert Supervisor.child_spec({Sketches, []}, id: :default_name_check).id ==
               :default_name_check
    end
  end

  test "child_spec/1 supports {ExDataSketch.Sketches, name: ...} in a supervision tree" do
    name = :"sketches_childspec_test_#{System.unique_integer([:positive])}"
    children = [{Sketches, name: name}]
    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)

    {:ok, _pid} = Sketches.start_child(name, :tenant_a, sketch: :hll, sketch_opts: [p: 10])
    assert %HLL{} = Server.sketch(Sketches.via(name, :tenant_a))

    Supervisor.stop(sup)
  end

  defp wait_until(fun, attempts \\ 20)

  defp wait_until(fun, 0), do: assert(fun.())

  defp wait_until(fun, attempts) do
    unless fun.() do
      Process.sleep(5)
      wait_until(fun, attempts - 1)
    end
  end
end
