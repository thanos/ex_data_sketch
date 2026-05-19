# Persistence

ExDataSketch provides five persistence backends for storing and recovering
sketch state. All backends serialize sketches using the EXSK v2 binary format
with CRC32C checksum integrity.

## Supported Backends

| Backend  | Module                            | Distribution | Durability      | Transactional |
|----------|-----------------------------------|--------------|----------------|---------------|
| ETS      | `ExDataSketch.Storage.ETS`        | Per-node     | Process lifetime| No            |
| DETS     | `ExDataSketch.Storage.DETS`       | Per-node     | Disk            | No (file lock)|
| CubDB    | `ExDataSketch.Storage.CubDB`      | Per-node     | Disk            | Yes (MVCC)    |
| Mnesia   | `ExDataSketch.Storage.Mnesia`     | Multi-node   | Disk+RAM        | Yes (ACID)    |
| Ecto     | `ExDataSketch.Storage.Ecto`      | Multi-node   | Database        | Yes (DB)      |

## Unified API

Every backend implements the same operations:

```elixir
# Save a sketch under a key
:ok = Backend.save(sketch, storage, key)

# Load a sketch by key and module
{:ok, sketch} = Backend.load(SketchModule, storage, key)

# Atomic merge into persisted sketch
:ok = Backend.merge(sketch, storage, key)

# Delete a sketch by key
:ok = Backend.delete(storage, key)
```

The `storage` argument varies by backend:

- ETS/DETS: table name (atom)
- CubDB: CubDB pid or name
- Mnesia: table name (atom)
- Ecto: Ecto repo module

## ETS

ETS provides fast in-memory storage. It is always available (no extra
dependencies required).

```elixir
# Create the table (application concern)
:ets.new(:sketches, [:set, :public, :named_table])

# Save
:ok = ExDataSketch.Storage.ETS.save(sketch, :sketches, "cardinality:2024-01")

# Load
{:ok, loaded} = ExDataSketch.Storage.ETS.load(ExDataSketch.HLL, :sketches, "cardinality:2024-01")

# Merge (read-modify-write, not truly atomic under concurrency)
:ok = ExDataSketch.Storage.ETS.merge(partial, :sketches, "cardinality:2024-01")

# Delete
:ok = ExDataSketch.Storage.ETS.delete(:sketches, "cardinality:2024-01")
```

ETS tables must be `:set` or `:ordered_set` type.

## DETS

DETS provides disk-backed storage that survives process and node restarts.

```elixir
# Open the table (application concern)
{:ok, _} = :dets.open_file(:sketches, [type: :set])

# Save, load, merge, delete -- same API as ETS
:ok = ExDataSketch.Storage.DETS.save(sketch, :sketches, "cardinality:2024-01")
{:ok, loaded} = ExDataSketch.Storage.DETS.load(ExDataSketch.HLL, :sketches, "cardinality:2024-01")
:ok = ExDataSketch.Storage.DETS.merge(partial, :sketches, "cardinality:2024-01")
:ok = ExDataSketch.Storage.DETS.delete(:sketches, "cardinality:2024-01")

# Close when done
:ok = :dets.close(:sketches)
```

DETS tables must be `:set` type. `:ordered_set` and `:bag` are not supported.
DETS has a practical 2GB file size limit.

## CubDB

CubDB provides disk-backed key-value storage with MVCC transactions. It
requires the `:cubdb` dependency.

Dependencies:

```elixir
{:cubdb, "~> 2.0"}
```

```elixir
# Start CubDB (application concern)
{:ok, db} = CubDB.start_link(data_dir: "/path/to/data")

# Save
:ok = ExDataSketch.Storage.CubDB.save(sketch, db, "cardinality:2024-01")

# Load
{:ok, loaded} = ExDataSketch.Storage.CubDB.load(ExDataSketch.HLL, db, "cardinality:2024-01")

# Atomic merge (uses CubDB transaction)
:ok = ExDataSketch.Storage.CubDB.merge(partial, db, "cardinality:2024-01")

# Delete
:ok = ExDataSketch.Storage.CubDB.delete(db, "cardinality:2024-01")
```

## Mnesia

Mnesia provides distributed, transactional storage across BEAM cluster nodes.
It is always available (no extra dependencies required).

```elixir
# Setup the table (once per node)
:ok = ExDataSketch.Storage.Mnesia.setup(:sketches)
# Or with disc copies:
:ok = ExDataSketch.Storage.Mnesia.setup(:sketches, disc_copies: [node()])

# Save
:ok = ExDataSketch.Storage.Mnesia.save(sketch, :sketches, "cardinality:2024-01")

# Load
{:ok, loaded} = ExDataSketch.Storage.Mnesia.load(ExDataSketch.HLL, :sketches, "cardinality:2024-01")

# Atomic merge (uses Mnesia transaction)
:ok = ExDataSketch.Storage.Mnesia.merge(partial, :sketches, "cardinality:2024-01")

# Delete
:ok = ExDataSketch.Storage.Mnesia.delete(:sketches, "cardinality:2024-01")
```

### Distributed Mnesia

For multi-node setups, create the table on all nodes before use:

```elixir
:ok = ExDataSketch.Storage.Mnesia.setup(:sketches, disc_copies: [node(), :other@host])
```

Mnesia transactions ensure atomic merge across all replicas. For operational
concerns including network partition recovery, refer to the Mnesia
documentation.

## Ecto

The Ecto backend stores sketches in a SQL database. It requires `:ecto_sql`.

Dependencies:

```elixir
{:ecto_sql, "~> 3.0"}
```

### Setup

Generate and run the migration:

```bash
mix ex_data_sketch.gen.migration --repo MyApp.Repo
mix ecto.migrate
```

Or add the migration manually:

```elixir
defmodule MyApp.Repo.Migrations.AddExDataSketchSketches do
  use Ecto.Migration

  def up do
    ExDataSketch.Storage.Ecto.Migration.up()
  end

  def down do
    ExDataSketch.Storage.Ecto.Migration.down()
  end
end
```

### Usage

```elixir
# Save
:ok = ExDataSketch.Storage.Ecto.save(sketch, MyApp.Repo, "cardinality:2024-01")

# Load
{:ok, loaded} = ExDataSketch.Storage.Ecto.load(ExDataSketch.HLL, MyApp.Repo, "cardinality:2024-01")

# Atomic merge (uses Ecto transaction with SELECT FOR UPDATE)
:ok = ExDataSketch.Storage.Ecto.merge(partial, MyApp.Repo, "cardinality:2024-01")

# Delete
:ok = ExDataSketch.Storage.Ecto.delete(MyApp.Repo, "cardinality:2024-01")
```

The Ecto backend uses `SELECT ... FOR UPDATE` to ensure atomic merge in
concurrent environments.

## Choosing a Backend

| Use Case | Recommended Backend |
|----------|---------------------|
| Fast in-memory cache | ETS |
| Survive process restarts | DETS or CubDB |
| Simple disk persistence | CubDB |
| Distributed cluster | Mnesia |
| Existing Ecto app | Ecto |
| SQL database required | Ecto |
| Need ACID across nodes | Mnesia or Ecto |

## Configuration

Backends can be enabled or disabled via application config:

```elixir
config :ex_data_sketch,
  persistence_backends: [
    ets:   [enabled: true],
    dets:  [enabled: true],
    cubdb: [enabled: true],
    mnesia: [enabled: true],
    ecto:  [enabled: true]
  ]
```

When not explicitly configured, a backend defaults to enabled if its runtime
dependency is available. Set `enabled: false` to disable a backend regardless
of dependency availability.

## See Also

- [Streaming Sketches](streaming_sketches.md)
- [Integration Guide](integrations.md)