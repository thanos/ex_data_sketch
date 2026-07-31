defmodule ExDataSketch.Storage do
  @moduledoc """
  Persistence backends for ExDataSketch sketches.

  This module defines the shared behaviour and types for all persistence
  backends. Each backend implements a consistent storage contract:

    - `save/3`   -- persist a sketch under a key
    - `load/3`   -- retrieve and deserialize a sketch by key
    - `merge/3`  -- atomically merge a sketch into the persisted value
    - `delete/2` -- remove a sketch by key

  All backends serialize sketches using `ExDataSketch.Binary.encode/3` and
  `sketch_module.serialize/1` / `sketch_module.deserialize/1`. No backend
  stores raw sketch state; every stored value is a complete EXSK v2 frame
  with CRC32C checksum.

  ## Available Backends

  | Backend  | Module                            | Distribution | Durability     |
  |----------|-----------------------------------|--------------|----------------|
  | ETS      | `ExDataSketch.Storage.ETS`        | Per-node     | Process lifetime|
  | DETS     | `ExDataSketch.Storage.DETS`       | Per-node     | Disk            |
  | CubDB    | `ExDataSketch.Storage.CubDB`      | Per-node     | Disk            |
  | Mnesia   | `ExDataSketch.Storage.Mnesia`     | Multi-node   | Disk+RAM       |
  | Ecto     | `ExDataSketch.Storage.Ecto`      | Multi-node   | Database        |

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
  """

  @type key :: String.t() | atom() | term()
  @type save_opts :: [table: atom(), encode_opts: keyword()]
  @type load_opts :: [table: atom()]
  @type merge_opts :: [table: atom()]
  @type delete_opts :: [table: atom()]
end
