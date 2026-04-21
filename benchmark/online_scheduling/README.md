# Online TPCH Scheduling Benchmark

This folder contains a small online benchmark harness to evaluate scheduler behavior under concurrent TPCH traffic.

## What it does

- Generates TPCH data (`CALL dbgen(sf=...)`) in a DuckDB database file.
- Submits query requests according to a Poisson process (`lambda` arrivals/sec).
- Runs requests concurrently with a configurable worker pool.
- Records per-query latency and aggregates throughput and percentile latency metrics.

## Usage

```bash
python3 benchmark/online_scheduling/run_online_tpch.py \
  --db-path /tmp/online_tpch.duckdb \
  --scale-factor 1 \
  --workers 4 \
  --arrival-rate 0.5 \
  --duration-seconds 60
```

## Notes

- This script uses the Python DuckDB package and runs independently of the C++ test harness.
- You can compare scheduling policies by running identical workloads while changing your scheduler configuration in DuckDB.
