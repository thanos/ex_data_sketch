import Config

config :rustler_precompiled, :force_build,
  ex_data_sketch: System.get_env("EX_DATA_SKETCH_BUILD") in ["1", "true"]
