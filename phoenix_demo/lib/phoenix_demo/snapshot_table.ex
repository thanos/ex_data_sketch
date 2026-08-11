defmodule PhoenixDemo.SnapshotTable do
  @moduledoc """
  Owns the `:phoenix_demo_snapshots` ETS table used by the `:visitors`
  `ExDataSketch.Server` to demonstrate `ExDataSketch.Storage.ETS`-backed
  snapshotting (see `guides/persistence.md`'s "application concern" note --
  the table has to be created and owned by a process that outlives any
  single request, which is what this GenServer is for).

  Started before `:visitors` in `PhoenixDemo.Application`'s supervision
  tree, so the table exists by the time the server's `init/1` attempts a
  crash-recovery load from it.
  """

  use GenServer

  @table :phoenix_demo_snapshots

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    :ets.new(@table, [:set, :public, :named_table])
    {:ok, %{}}
  end
end
