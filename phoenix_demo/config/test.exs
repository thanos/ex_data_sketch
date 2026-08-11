import Config

# Enables /dashboard in test too, so dashboard_metrics_test.exs can verify
# ExDataSketch telemetry actually renders into LiveDashboard's chart HTML.
config :phoenix_demo, dev_routes: true

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :phoenix_demo, PhoenixDemoWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "QHSc9b6qGZdWB7fo4JNOo80+57AlZkV2qi5XQWrbh8vwFe8xcereiT8FRGPaLRVr",
  server: false

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true

# Sort query params output of verified routes for robust url comparisons
config :phoenix,
  sort_verified_routes_query_params: true
