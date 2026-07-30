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

  ## Backend Ref Resolution

  `save/3`, `load/3`, `merge/3`, and `delete/2` accept a `t:backend_ref/0`,
  which is either:

    - an explicit `{backend_module, ref}` pair, for example
      `{ExDataSketch.Storage.ETS, :my_table}`; or
    - a bare `ref` (for example, a plain table name), resolved against the
      module configured under `config :ex_data_sketch, :storage, backend: ...`
      below. Raises `ExDataSketch.Errors.InvalidOptionError` if no default
      backend is configured.

  A value is treated as an explicit pair only when it is a 2-tuple whose
  first element is an atom; none of the five shipped backends' own `ref`
  types (`atom()`, `pid() | atom()`, `module()`) are themselves 2-tuples, so
  this is unambiguous today.

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

  See the "Backend Ref Resolution" section above for how `backend_ref` is
  resolved.

  ## Examples

      iex> :ets.new(:storage_facade_doctest_save, [:set, :public, :named_table])
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.save(sketch, {ExDataSketch.Storage.ETS, :storage_facade_doctest_save}, "key")
      :ok
      iex> :ets.delete(:storage_facade_doctest_save)
      true

  """
  @spec save(struct(), backend_ref(), key()) :: :ok | {:error, term()}
  def save(%_{} = sketch, backend_ref, key) do
    {module, ref} = resolve_backend(backend_ref)
    apply(module, :save, [sketch, ref, key])
  end

  @doc """
  Retrieves and deserializes a sketch by `key` via `backend_ref`, dispatching
  to the resolved backend module's `load/3`.

  See the "Backend Ref Resolution" section above for how `backend_ref` is
  resolved.

  ## Examples

      iex> :ets.new(:storage_facade_doctest_load, [:set, :public, :named_table])
      iex> sketch = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.save(sketch, {ExDataSketch.Storage.ETS, :storage_facade_doctest_load}, "key")
      iex> {:ok, loaded} = ExDataSketch.Storage.load(ExDataSketch.HLL, {ExDataSketch.Storage.ETS, :storage_facade_doctest_load}, "key")
      iex> ExDataSketch.HLL.estimate(loaded) > 0.0
      true
      iex> ExDataSketch.Storage.load(ExDataSketch.HLL, {ExDataSketch.Storage.ETS, :storage_facade_doctest_load}, "nonexistent")
      {:error, :not_found}
      iex> :ets.delete(:storage_facade_doctest_load)
      true

  """
  @spec load(module(), backend_ref(), key()) :: {:ok, struct()} | {:error, :not_found | term()}
  def load(sketch_module, backend_ref, key) when is_atom(sketch_module) do
    {module, ref} = resolve_backend(backend_ref)
    apply(module, :load, [sketch_module, ref, key])
  end

  @doc """
  Merges a sketch into the value persisted at `key` via `backend_ref`,
  dispatching to the resolved backend module's `merge/3`.

  See the "Backend Ref Resolution" section above for how `backend_ref` is
  resolved.

  ## Examples

      iex> :ets.new(:storage_facade_doctest_merge, [:set, :public, :named_table])
      iex> a = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("a")
      iex> ExDataSketch.Storage.save(a, {ExDataSketch.Storage.ETS, :storage_facade_doctest_merge}, "key")
      iex> b = ExDataSketch.HLL.new(p: 10) |> ExDataSketch.HLL.update("b")
      iex> ExDataSketch.Storage.merge(b, {ExDataSketch.Storage.ETS, :storage_facade_doctest_merge}, "key")
      :ok
      iex> {:ok, merged} = ExDataSketch.Storage.load(ExDataSketch.HLL, {ExDataSketch.Storage.ETS, :storage_facade_doctest_merge}, "key")
      iex> ExDataSketch.HLL.estimate(merged) >= 1.9
      true
      iex> :ets.delete(:storage_facade_doctest_merge)
      true

  """
  @spec merge(struct(), backend_ref(), key()) :: :ok | {:error, term()}
  def merge(%_{} = sketch, backend_ref, key) do
    {module, ref} = resolve_backend(backend_ref)
    apply(module, :merge, [sketch, ref, key])
  end

  @doc """
  Removes the value stored at `key` via `backend_ref`, dispatching to the
  resolved backend module's `delete/2`.

  See the "Backend Ref Resolution" section above for how `backend_ref` is
  resolved.

  ## Examples

      iex> :ets.new(:storage_facade_doctest_delete, [:set, :public, :named_table])
      iex> sketch = ExDataSketch.HLL.new(p: 10)
      iex> ExDataSketch.Storage.save(sketch, {ExDataSketch.Storage.ETS, :storage_facade_doctest_delete}, "key")
      iex> ExDataSketch.Storage.delete({ExDataSketch.Storage.ETS, :storage_facade_doctest_delete}, "key")
      :ok
      iex> ExDataSketch.Storage.load(ExDataSketch.HLL, {ExDataSketch.Storage.ETS, :storage_facade_doctest_delete}, "key")
      {:error, :not_found}
      iex> :ets.delete(:storage_facade_doctest_delete)
      true

  """
  @spec delete(backend_ref(), key()) :: :ok | {:error, term()}
  def delete(backend_ref, key) do
    {module, ref} = resolve_backend(backend_ref)
    apply(module, :delete, [ref, key])
  end

  @spec resolve_backend(backend_ref()) :: {module(), ref()}
  defp resolve_backend({module, ref}) when is_atom(module) do
    {module, ref}
  end

  defp resolve_backend(ref) do
    case Application.get_env(:ex_data_sketch, :storage, [])[:backend] do
      module when is_atom(module) and not is_nil(module) ->
        {module, ref}

      _none ->
        raise Errors.InvalidOptionError,
          option: :backend,
          value: nil,
          message:
            "no backend module given and no default configured; " <>
              "pass {backend_module, ref} or set config :ex_data_sketch, :storage, backend: SomeModule"
    end
  end
end
