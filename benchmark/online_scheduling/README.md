# Online TPCH Scheduling Benchmark

This folder contains a small online benchmark harness to evaluate scheduler behavior under concurrent TPCH traffic.

## What it does

- Generates TPCH data (`CALL dbgen(sf=...)`) in a DuckDB database file.
- Loads the canonical TPCH SQL text from `extension/tpch/dbgen/queries/q01.sql` ... `q22.sql`.
- Submits query requests according to a Poisson process (`lambda` arrivals/sec).
- Runs requests concurrently with a configurable worker pool.
- Records per-query latency and aggregates throughput and percentile latency metrics.
- Sets `scheduler_policy` on each worker connection so policies can be swapped from the benchmark CLI.
- Executes workload queries with `-readonly` to avoid cross-process write locks when multiple worker subprocesses hit the same DB file.

## Usage

```bash
python3 benchmark/online_scheduling/run_online_tpch.py \
  --duckdb-binary ./build/release/duckdb \
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
  --duckdb-binary ./build/release/duckdb \
  --db-path /tmp/online_tpch.duckdb \
  --scale-factor 1 \
  --workers 4 \
  --arrival-rate 0.5 \
  --duration-seconds 60 \
  --compare-policies no_priority srpt las sfq
```

## Notes

- This script runs every query through the provided DuckDB executable (`--duckdb-binary`), so it uses your compiled branch directly.
- Available policies: `no_priority`, `srpt`, `las`, `sfq`.
- `srpt`/`las`/`sfq` require a DuckDB binary that includes the `scheduler_policy` setting.
- TPCH setup prefers a locally built extension at `<build_dir>/extension/tpch/tpch.duckdb_extension` and only falls back to
  `INSTALL tpch` if it cannot find that file.
- If your binary reports version `v0.0.1`, fetch git tags (or set `OVERRIDE_GIT_DESCRIBE`) before building; otherwise
  extension auto-download URLs may not exist.
