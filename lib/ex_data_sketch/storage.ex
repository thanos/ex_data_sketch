defmodule ExDataSketch.Storage do
  @moduledoc """
  Behaviour, registry, and dispatching facade for ExDataSketch's persistence
  backends.

  Each backend module (`ExDataSketch.Storage.ETS`, `DETS`, `CubDB`, `Mnesia`,
  `Ecto`) implements a consistent storage contract:

    - `save/3`   -- persist a sketch under a key
    - `load/3`   -- retrieve and deserialize a sketch by key
    - `merge/3`  -- atomically (or read-modify-write) merge a sketch into the
      persisted value
    - `delete/2` -- remove a sketch by key

  All backends serialize sketches using `sketch_module.serialize/1` /
  `sketch_module.deserialize/1`. No backend stores raw sketch state; every
  stored value is a complete EXSK v2 frame with CRC32C checksum.

  ## Available Backends

  | Backend  | Module                            | Ref type            | Distribution | Durability       |
  |----------|-----------------------------------|----------------------|--------------|-------------------|
  | ETS      | `ExDataSketch.Storage.ETS`        | `atom()` (table)      | Per-node     | Process lifetime |
  | DETS     | `ExDataSketch.Storage.DETS`       | `atom()` (table)      | Per-node     | Disk             |
  | CubDB    | `ExDataSketch.Storage.CubDB`      | `pid() \\| atom()`    | Per-node     | Disk             |
  | Mnesia   | `ExDataSketch.Storage.Mnesia`     | `atom()` (table)      | Multi-node   | Disk+RAM         |
  | Ecto     | `ExDataSketch.Storage.Ecto`       | `module()` (repo)     | Multi-node   | Database         |

  `ExDataSketch.Storage.Ecto` is compiled only when the optional `:ecto_sql`
  dependency is available; the other four backends are always compiled
  (`ExDataSketch.Storage.CubDB`'s functions raise a clear error if the
  optional `:cubdb` dependency is missing, but the module itself always
  exists).

  ## Configuration

  Backends can be enabled or disabled via application config:

      config :ex_data_sketch, :persistence_backends,
        ets: [enabled: true],
        dets: [enabled: true],
        cubdb: [enabled: true],
        mnesia: [enabled: true],
        ecto: [enabled: true]

  When not explicitly configured, a backend defaults to enabled if its
  runtime dependency is available.

  A default backend module for the dispatching facade (`save/3`, `load/3`,
  `merge/3`, `delete/2` below) can be configured separately:

      config :ex_data_sketch, :storage, backend: ExDataSketch.Storage.ETS

  ## `@behaviour ExDataSketch.Storage`

      defmodule MyApp.Storage.Redis do
        @behaviour ExDataSketch.Storage

        @impl true
        def save(sketch, conn, key), do: ...

        @impl true
        def load(sketch_module, conn, key), do: ...

        @impl true
        def merge(sketch, conn, key), do: ...

        @impl true
        def delete(conn, key), do: ...
      end
  """

  @typedoc "A key under which a sketch is stored. Backend-defined; typically a string or atom."
  @type key :: String.t() | atom() | term()

  @typedoc """
  A backend-specific reference identifying where to store or find data --
  an ETS/DETS/Mnesia table name (`atom()`), a CubDB process (`pid() | atom()`),
  or an Ecto repo module (`module()`).
  """
  @type ref :: term()

  @typedoc """
  Either an explicit `{backend_module, ref}` pair, or a bare `ref` to be
  resolved against the default backend configured under
  `config :ex_data_sketch, :storage, backend: ...`. See `save/3` for the
  dispatching rules.
  """
  @type backend_ref :: {module(), ref()} | ref()

  @doc """
  Persists a sketch under `key` via the backend's storage mechanism.

  The sketch is serialized to an EXSK v2 binary frame before storage.
  """
  @callback save(sketch :: struct(), ref(), key()) :: :ok | {:error, term()}

  @doc """
  Retrieves and deserializes a sketch by `key`.

  Returns `{:error, :not_found}` if no value is stored at `key`, or
  `{:error, reason}` for other lookup or deserialization failures. Takes the
  target `sketch_module` (rather than inferring it) because the backend
  cannot otherwise know which module's `deserialize/1` to call.
  """
  @callback load(sketch_module :: module(), ref(), key()) ::
              {:ok, struct()} | {:error, :not_found | term()}

  @doc """
  Merges a sketch into the value persisted at `key`.

  If no value exists at `key`, this is equivalent to `save/3`. Backends
  that support atomic merge (`ExDataSketch.Storage.Mnesia`,
  `ExDataSketch.Storage.Ecto`) do so via a transaction; others
  (`ExDataSketch.Storage.ETS`, `ExDataSketch.Storage.DETS`,
  `ExDataSketch.Storage.CubDB`) perform a read-modify-write cycle -- see each
  backend's own `merge/3` documentation for its concurrency guarantees.
  """
  @callback merge(sketch :: struct(), ref(), key()) :: :ok | {:error, term()}

  @doc """
  Removes the value stored at `key`, if any.

  Succeeds (`:ok`) even if `key` does not exist.
  """
  @callback delete(ref(), key()) :: :ok | {:error, term()}

  @doc """
  Returns a supervisor child spec for backends whose ref must be started and
  supervised (for example, a `CubDB` process). Backends with no process to
  supervise (`ETS`, `DETS`, `Mnesia`, `Ecto`, whose repo is supervised by the
  host application) do not implement this optional callback.
  """
  @callback child_spec(keyword()) :: Supervisor.child_spec() | {module(), term()} | module()

  @optional_callbacks child_spec: 1

  alias ExDataSketch.Errors

  @registry %{
    ets: ExDataSketch.Storage.ETS,
    dets: ExDataSketch.Storage.DETS,
    cubdb: ExDataSketch.Storage.CubDB,
    mnesia: ExDataSketch.Storage.Mnesia,
    ecto: ExDataSketch.Storage.Ecto
  }

  @typedoc "A registry key identifying one of the 5 backend modules. See `backends/0`."
  @type backend_type :: :ets | :dets | :cubdb | :mnesia | :ecto

  @doc """
  Returns the registry mapping every backend's atom key to its module.

  `ExDataSketch.Storage.Ecto` is only compiled when the optional `:ecto_sql`
  dependency is available; its entry is still present in this map (module
  names are atoms, so referencing one costs nothing), but calling any
  function on it without `:ecto_sql` installed raises `UndefinedFunctionError`.

  ## Examples

      iex> ExDataSketch.Storage.backends()[:ets]
      ExDataSketch.Storage.ETS

      iex> map_size(ExDataSketch.Storage.backends())
      5

  """
  @spec backends() :: %{backend_type() => module()}
  def backends, do: @registry

  @doc """
  Persists a sketch under `key` via `backend_ref`, dispatching to the
  resolved backend module's `save/3`.

  `backend_ref` is either an explicit `{backend_module, ref}` pair (for
  example `{ExDataSketch.Storage.ETS, :my_table}`) or a bare `ref`, in which
  case the backend module is read from
  `config :ex_data_sketch, :storage, backend: SomeModule`. Raises
  `ExDataSketch.Errors.InvalidOptionError` if a bare `ref` is given and no
  default backend is configured.

  This is a Phase 2 facade stub: the dispatch body is intentionally not yet
  implemented pending maintainer review of the API surface (see
  `baoulo/plans/plan-0.10.0.md`, Phase 2). It currently raises
  `ExDataSketch.Errors.NotImplementedError`.

  ## Examples

      iex> try do
      ...>   sketch = ExDataSketch.HLL.new(p: 10)
      ...>   ExDataSketch.Storage.save(sketch, {ExDataSketch.Storage.ETS, :some_table}, "key")
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Storage.save is not yet implemented"

  """
  @spec save(struct(), backend_ref(), key()) :: :ok | {:error, term()}
  @dialyzer {:nowarn_function, save: 3}
  def save(%_{} = _sketch, _backend_ref, _key) do
    Errors.not_implemented!(__MODULE__, "save")
  end

  @doc """
  Retrieves and deserializes a sketch by `key` via `backend_ref`, dispatching
  to the resolved backend module's `load/3`.

  See `save/3` for how `backend_ref` is resolved.

  This is a Phase 2 facade stub: the dispatch body is intentionally not yet
  implemented pending maintainer review of the API surface (see
  `baoulo/plans/plan-0.10.0.md`, Phase 2). It currently raises
  `ExDataSketch.Errors.NotImplementedError`.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Storage.load(ExDataSketch.HLL, {ExDataSketch.Storage.ETS, :some_table}, "key")
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Storage.load is not yet implemented"

  """
  @spec load(module(), backend_ref(), key()) :: {:ok, struct()} | {:error, :not_found | term()}
  @dialyzer {:nowarn_function, load: 3}
  def load(sketch_module, _backend_ref, _key) when is_atom(sketch_module) do
    Errors.not_implemented!(__MODULE__, "load")
  end

  @doc """
  Merges a sketch into the value persisted at `key` via `backend_ref`,
  dispatching to the resolved backend module's `merge/3`.

  See `save/3` for how `backend_ref` is resolved.

  This is a Phase 2 facade stub: the dispatch body is intentionally not yet
  implemented pending maintainer review of the API surface (see
  `baoulo/plans/plan-0.10.0.md`, Phase 2). It currently raises
  `ExDataSketch.Errors.NotImplementedError`.

  ## Examples

      iex> try do
      ...>   sketch = ExDataSketch.HLL.new(p: 10)
      ...>   ExDataSketch.Storage.merge(sketch, {ExDataSketch.Storage.ETS, :some_table}, "key")
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Storage.merge is not yet implemented"

  """
  @spec merge(struct(), backend_ref(), key()) :: :ok | {:error, term()}
  @dialyzer {:nowarn_function, merge: 3}
  def merge(%_{} = _sketch, _backend_ref, _key) do
    Errors.not_implemented!(__MODULE__, "merge")
  end

  @doc """
  Removes the value stored at `key` via `backend_ref`, dispatching to the
  resolved backend module's `delete/2`.

  See `save/3` for how `backend_ref` is resolved.

  This is a Phase 2 facade stub: the dispatch body is intentionally not yet
  implemented pending maintainer review of the API surface (see
  `baoulo/plans/plan-0.10.0.md`, Phase 2). It currently raises
  `ExDataSketch.Errors.NotImplementedError`.

  ## Examples

      iex> try do
      ...>   ExDataSketch.Storage.delete({ExDataSketch.Storage.ETS, :some_table}, "key")
      ...> rescue
      ...>   e in ExDataSketch.Errors.NotImplementedError -> e.message
      ...> end
      "ExDataSketch.Storage.delete is not yet implemented"

  """
  @spec delete(backend_ref(), key()) :: :ok | {:error, term()}
  @dialyzer {:nowarn_function, delete: 2}
  def delete(_backend_ref, _key) do
    Errors.not_implemented!(__MODULE__, "delete")
  end
end
