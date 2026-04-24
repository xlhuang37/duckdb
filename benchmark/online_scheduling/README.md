# Online TPCH Scheduling Benchmark

This folder contains a small online benchmark harness to evaluate scheduler behavior under concurrent TPCH traffic.

## What it does

- Generates TPCH data (`CALL dbgen(sf=...)`) in a DuckDB database file.
- Loads the canonical TPCH SQL text from `extension/tpch/dbgen/queries/q01.sql` ... `q22.sql`.
- Submits query requests according to a Poisson process (`lambda` arrivals/sec).
- Runs requests concurrently with a configurable worker pool.
- Records per-query latency and aggregates throughput and percentile latency metrics.
- Sets `scheduler_policy` on each worker connection so policies can be swapped from the benchmark CLI.

## Usage

```bash
python3 benchmark/online_scheduling/run_online_tpch.py \
  --db-path /tmp/online_tpch.duckdb \
  --scale-factor 1 \
  --workers 4 \
  --scheduler-policy sfq \
  --arrival-rate 0.5 \
  --duration-seconds 60
```

Compare multiple policies in one run:

```bash
python3 benchmark/online_scheduling/run_online_tpch.py \
  --db-path /tmp/online_tpch.duckdb \
  --scale-factor 1 \
  --workers 4 \
  --arrival-rate 0.5 \
  --duration-seconds 60 \
  --compare-policies no_priority srpt las sfq
```

## Notes

- This script uses the Python DuckDB package and runs independently of the C++ test harness.
- Available policies: `no_priority`, `srpt`, `las`, `sfq`.
