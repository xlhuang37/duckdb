#!/usr/bin/env python3
import argparse
import queue
import random
import statistics
import threading
import time
from dataclasses import dataclass

import duckdb

TPCH_QUERY_TEMPLATES = [
    "SELECT l_returnflag, l_linestatus, sum(l_quantity) FROM lineitem GROUP BY 1, 2",
    "SELECT o_orderpriority, count(*) FROM orders GROUP BY 1",
    "SELECT n_name, sum(l_extendedprice) FROM lineitem JOIN supplier ON l_suppkey = s_suppkey "
    "JOIN nation ON s_nationkey = n_nationkey GROUP BY 1",
    "SELECT c_mktsegment, avg(c_acctbal) FROM customer GROUP BY 1",
    "SELECT l_shipmode, count(*) FROM lineitem GROUP BY 1",
]


@dataclass
class QueryResult:
    query_id: int
    latency_seconds: float
    success: bool
    error: str = ""


def setup_tpch_database(db_path: str, scale_factor: int) -> None:
    con = duckdb.connect(db_path)
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf={scale_factor})")
    con.close()


def worker_loop(
    db_path: str,
    work_queue: "queue.Queue[tuple[int, str] | None]",
    results: list[QueryResult],
    results_lock: threading.Lock,
) -> None:
    con = duckdb.connect(db_path)
    while True:
        item = work_queue.get()
        if item is None:
            work_queue.task_done()
            break
        query_id, query = item
        start = time.perf_counter()
        try:
            con.execute(query).fetchall()
            latency = time.perf_counter() - start
            result = QueryResult(query_id=query_id, latency_seconds=latency, success=True)
        except Exception as ex:
            latency = time.perf_counter() - start
            result = QueryResult(query_id=query_id, latency_seconds=latency, success=False, error=str(ex))
        with results_lock:
            results.append(result)
        work_queue.task_done()
    con.close()


def run_benchmark(db_path: str, workers: int, arrival_rate: float, duration_seconds: int) -> list[QueryResult]:
    work_queue: "queue.Queue[tuple[int, str] | None]" = queue.Queue()
    results: list[QueryResult] = []
    results_lock = threading.Lock()

    worker_threads = []
    for _ in range(workers):
        thread = threading.Thread(target=worker_loop, args=(db_path, work_queue, results, results_lock), daemon=True)
        thread.start()
        worker_threads.append(thread)

    start = time.perf_counter()
    query_id = 0
    while time.perf_counter() - start < duration_seconds:
        next_interval = random.expovariate(arrival_rate)
        time.sleep(next_interval)
        query = random.choice(TPCH_QUERY_TEMPLATES)
        work_queue.put((query_id, query))
        query_id += 1

    for _ in worker_threads:
        work_queue.put(None)
    work_queue.join()
    for thread in worker_threads:
        thread.join()
    return results


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    idx = int((len(values) - 1) * p)
    return sorted(values)[idx]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an online TPCH benchmark with Poisson arrivals.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file")
    parser.add_argument("--scale-factor", type=int, default=1, help="TPCH scale factor for dbgen")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    parser.add_argument("--arrival-rate", type=float, default=0.5, help="Poisson arrival rate (queries/sec)")
    parser.add_argument("--duration-seconds", type=int, default=60, help="Benchmark duration")
    parser.add_argument("--skip-setup", action="store_true", help="Skip tpch INSTALL/LOAD/dbgen")
    args = parser.parse_args()

    if not args.skip_setup:
        setup_tpch_database(args.db_path, args.scale_factor)

    results = run_benchmark(args.db_path, args.workers, args.arrival_rate, args.duration_seconds)
    success = [r for r in results if r.success]
    failures = [r for r in results if not r.success]
    latencies = [r.latency_seconds for r in success]
    throughput = len(success) / max(1, args.duration_seconds)

    print(f"total_queries={len(results)}")
    print(f"successful_queries={len(success)}")
    print(f"failed_queries={len(failures)}")
    print(f"throughput_qps={throughput:.3f}")
    if latencies:
        print(f"latency_mean_s={statistics.mean(latencies):.6f}")
        print(f"latency_p50_s={percentile(latencies, 0.50):.6f}")
        print(f"latency_p95_s={percentile(latencies, 0.95):.6f}")
        print(f"latency_p99_s={percentile(latencies, 0.99):.6f}")
    if failures:
        print("sample_failure=", failures[0].error)


if __name__ == "__main__":
    main()
