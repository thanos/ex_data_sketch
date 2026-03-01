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

- `bench_baseline.json` -- Stores baseline benchmark IPS values. Created after
  first benchmark run on main.

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
# Run benchmarks
MIX_ENV=dev mix run bench/hll_bench.exs
MIX_ENV=dev mix run bench/cms_bench.exs

# The bench scripts produce JSON in bench/output/
# Merge relevant IPS values into ci/bench_baseline.json as:
# { "scenario_name": ips_value, ... }
```
