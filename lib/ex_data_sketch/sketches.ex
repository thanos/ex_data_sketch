defmodule ExDataSketch.Sketches do
  @moduledoc """
  A supervisor for starting `ExDataSketch.Server` processes per tenant or
  key at runtime, addressed by an arbitrary term rather than a
  compile-time atom.

  Add one instance to your application's supervision tree:

      children = [
        {ExDataSketch.Sketches, name: MyApp.Sketches}
      ]

  Then start and address servers by any term (a tenant ID, a user ID, and
  so on):

      {:ok, _pid} = ExDataSketch.Sketches.start_child(MyApp.Sketches, tenant_id, sketch: :hll, sketch_opts: [p: 14])
      ExDataSketch.Server.update(ExDataSketch.Sketches.via(MyApp.Sketches, tenant_id), user_id)

  `:name` is optional and defaults to `ExDataSketch.Sketches` itself, for
  applications that only need one instance and do not want to name it
  explicitly.

  ## Implementation

  Each `Sketches` instance is a plain `Supervisor` with two children: a
  `Registry` (`keys: :unique`) and a `DynamicSupervisor`. `start_child/3`
  starts an `ExDataSketch.Server` under the `DynamicSupervisor`, registered
  in the `Registry` under `key` via a `{:via, Registry, {registry, key}}`
  name -- the standard OTP pattern for addressing dynamically-started,
  independently-addressable processes by term. No bespoke lookup or routing
  code is needed: `ExDataSketch.Server`'s own client functions (`update/2`,
  `estimate/1`, and so on) already accept any `GenServer.server()`,
  including a `:via` tuple, so `via/2`'s result can be passed to them
  directly.
  """

  use Supervisor

  alias ExDataSketch.Server

  @doc """
  Starts a `Sketches` supervisor.

  ## Options

  - `:name` -- optional (default: `ExDataSketch.Sketches`). Every other
    function in this module takes this same name (or its default) as its
    first argument, to address the matching instance.

  ## Examples

      iex> {:ok, pid} = ExDataSketch.Sketches.start_link(name: :sketches_doctest)
      iex> is_pid(pid)
      true

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, name, name: name)
  end

  @impl true
  def init(name) do
    children = [
      {Registry, keys: :unique, name: registry_name(name)},
      {DynamicSupervisor, strategy: :one_for_one, name: dynamic_supervisor_name(name)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Starts an `ExDataSketch.Server` under `name`'s `DynamicSupervisor`,
  addressable afterwards as `via(name, key)`.

  `server_opts` are the same options `ExDataSketch.Server.start_link/1`
  accepts, except `:name` is set automatically and any given value for it
  is ignored.

  ## Examples

      iex> {:ok, _sketches} = ExDataSketch.Sketches.start_link(name: :sketches_doctest_start_child)
      iex> {:ok, pid} = ExDataSketch.Sketches.start_child(:sketches_doctest_start_child, :tenant_a, sketch: :hll, sketch_opts: [p: 10])
      iex> is_pid(pid)
      true

  """
  @spec start_child(term(), term(), ExDataSketch.Server.start_opts()) ::
          DynamicSupervisor.on_start_child()
  def start_child(name \\ __MODULE__, key, server_opts) do
    opts = Keyword.put(server_opts, :name, via(name, key))
    DynamicSupervisor.start_child(dynamic_supervisor_name(name), {Server, opts})
  end

  @doc """
  Stops the `ExDataSketch.Server` started under `key`.

  Returns `{:error, :not_found}` if no server is registered under `key`.

  ## Examples

      iex> {:ok, _sketches} = ExDataSketch.Sketches.start_link(name: :sketches_doctest_stop_child)
      iex> {:ok, _pid} = ExDataSketch.Sketches.start_child(:sketches_doctest_stop_child, :tenant_a, sketch: :hll, sketch_opts: [p: 10])
      iex> ExDataSketch.Sketches.stop_child(:sketches_doctest_stop_child, :tenant_a)
      :ok
      iex> ExDataSketch.Sketches.stop_child(:sketches_doctest_stop_child, :no_such_tenant)
      {:error, :not_found}

  """
  @spec stop_child(term(), term()) :: :ok | {:error, :not_found}
  def stop_child(name \\ __MODULE__, key) do
    case whereis(name, key) do
      nil -> {:error, :not_found}
      pid -> DynamicSupervisor.terminate_child(dynamic_supervisor_name(name), pid)
    end
  end

  @doc """
  Returns the `:via` tuple for addressing the `ExDataSketch.Server` started
  under `key`, suitable as the `server` argument to any
  `ExDataSketch.Server` client function.

  ## Examples

      iex> ExDataSketch.Sketches.via(:my_sketches, :tenant_a)
      {:via, Registry, {:"Elixir.ExDataSketch.Sketches.Registry.my_sketches", :tenant_a}}

  """
  @spec via(term(), term()) :: {:via, Registry, {module(), term()}}
  def via(name \\ __MODULE__, key), do: {:via, Registry, {registry_name(name), key}}

  @doc """
  Looks up the pid of the `ExDataSketch.Server` started under `key`, or
  `nil` if none is registered.

  ## Examples

      iex> {:ok, _sketches} = ExDataSketch.Sketches.start_link(name: :sketches_doctest_whereis)
      iex> ExDataSketch.Sketches.whereis(:sketches_doctest_whereis, :no_such_tenant)
      nil

  """
  @spec whereis(term(), term()) :: pid() | nil
  def whereis(name \\ __MODULE__, key) do
    case Registry.lookup(registry_name(name), key) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  defp registry_name(name), do: Module.concat([__MODULE__, Registry, name])
  defp dynamic_supervisor_name(name), do: Module.concat([__MODULE__, DynamicSupervisor, name])
end
