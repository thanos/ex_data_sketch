# CI Configuration

This directory contains CI helper scripts and baseline files for
coverage and benchmark regression tracking.

## Files

- `compare_coverage.exs` -- Compares test coverage against baseline threshold.
  Fails CI if coverage drops below 70% or drops more than 0.5 percentage points
  below the stored baseline.

- `compare_bench.exs` -- Compares benchmark results against baseline.
  Fails CI if any benchmark regresses more than 15% from baseline.

- `coverage_baseline.json` -- Stores the coverage threshold. Update this file
  when you intentionally accept a new coverage level.

- `bench_baseline.json` -- Stores baseline benchmark IPS values (Pure backend).
  Used by the bench CI job to detect performance regressions.

## Updating Baselines

### Coverage

After confirming a new acceptable coverage level:

```bash
# Run tests with coverage
MIX_ENV=test mix coveralls.json

# Update the threshold in ci/coverage_baseline.json
# Set "threshold" to the current coverage percentage
```

### Benchmarks

After running benchmarks on main:

```bash
# Run all benchmarks
MIX_ENV=dev mix run bench/hll_bench.exs
MIX_ENV=dev mix run bench/cms_bench.exs
MIX_ENV=dev mix run bench/theta_bench.exs

# The bench scripts produce JSON in bench/output/
# Extract IPS values and update ci/bench_baseline.json as:
# { "scenario_name": ips_value, ... }
#
# The baseline should contain Pure-backend scenarios only,
# since the standard bench CI job runs without Rust.
```

Note: benchmark IPS values are machine-dependent. The baseline is a
ballpark for catching severe regressions (>15%), not exact comparisons.
Update the baseline when running on the same CI runner class or after
intentional performance changes.
