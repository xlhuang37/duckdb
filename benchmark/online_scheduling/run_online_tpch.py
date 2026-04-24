#!/usr/bin/env python3
import argparse
import queue
import random
import statistics
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

SUPPORTED_POLICIES = ("no_priority", "srpt", "las", "sfq")


@dataclass
class QueryResult:
    query_id: int
    query_name: str
    latency_seconds: float
    success: bool
    error: str = ""


@dataclass
class BenchmarkSummary:
    policy: str
    total_queries: int
    successful_queries: int
    failed_queries: int
    throughput_qps: float
    latency_mean_s: float
    latency_p50_s: float
    latency_p95_s: float
    latency_p99_s: float
    sample_failure: str


def setup_tpch_database(db_path: str, scale_factor: int) -> None:
    con = duckdb.connect(db_path)
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf={scale_factor})")
    con.close()


def load_tpch_queries_from_repo() -> list[tuple[str, str]]:
    repo_root = Path(__file__).resolve().parents[2]
    query_dir = repo_root / "extension" / "tpch" / "dbgen" / "queries"
    if not query_dir.exists():
        raise FileNotFoundError(f"Could not find TPCH query directory at {query_dir}")

    queries: list[tuple[str, str]] = []
    for qid in range(1, 23):
        file_name = f"q{qid:02d}.sql"
        query_path = query_dir / file_name
        query_sql = query_path.read_text(encoding="utf-8")
        queries.append((f"q{qid:02d}", query_sql))
    return queries


def worker_loop(
    db_path: str,
    scheduler_policy: str,
    work_queue: "queue.Queue[tuple[int, str, str] | None]",
    results: list[QueryResult],
    results_lock: threading.Lock,
) -> None:
    con = duckdb.connect(db_path)
    con.execute(f"SET scheduler_policy='{scheduler_policy}'")
    while True:
        item = work_queue.get()
        if item is None:
            work_queue.task_done()
            break
        query_id, query_name, query = item
        start = time.perf_counter()
        try:
            con.execute(query).fetchall()
            latency = time.perf_counter() - start
            result = QueryResult(query_id=query_id, query_name=query_name, latency_seconds=latency, success=True)
        except Exception as ex:
            latency = time.perf_counter() - start
            result = QueryResult(
                query_id=query_id, query_name=query_name, latency_seconds=latency, success=False, error=str(ex)
            )
        with results_lock:
            results.append(result)
        work_queue.task_done()
    con.close()


def run_benchmark(
    db_path: str,
    workers: int,
    arrival_rate: float,
    duration_seconds: int,
    scheduler_policy: str,
    queries: list[tuple[str, str]],
) -> list[QueryResult]:
    work_queue: "queue.Queue[tuple[int, str, str] | None]" = queue.Queue()
    results: list[QueryResult] = []
    results_lock = threading.Lock()

    worker_threads = []
    for _ in range(workers):
        thread = threading.Thread(
            target=worker_loop, args=(db_path, scheduler_policy, work_queue, results, results_lock), daemon=True
        )
        thread.start()
        worker_threads.append(thread)

    start = time.perf_counter()
    query_id = 0
    while time.perf_counter() - start < duration_seconds:
        next_interval = random.expovariate(arrival_rate)
        time.sleep(next_interval)
        query_name, query = random.choice(queries)
        work_queue.put((query_id, query_name, query))
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


def summarize_results(policy: str, duration_seconds: int, results: list[QueryResult]) -> BenchmarkSummary:
    success = [r for r in results if r.success]
    failures = [r for r in results if not r.success]
    latencies = [r.latency_seconds for r in success]
    throughput = len(success) / max(1, duration_seconds)

    return BenchmarkSummary(
        policy=policy,
        total_queries=len(results),
        successful_queries=len(success),
        failed_queries=len(failures),
        throughput_qps=throughput,
        latency_mean_s=statistics.mean(latencies) if latencies else 0.0,
        latency_p50_s=percentile(latencies, 0.50),
        latency_p95_s=percentile(latencies, 0.95),
        latency_p99_s=percentile(latencies, 0.99),
        sample_failure=failures[0].error if failures else "",
    )


def print_summary(summary: BenchmarkSummary) -> None:
    print(f"policy={summary.policy}")
    print(f"total_queries={summary.total_queries}")
    print(f"successful_queries={summary.successful_queries}")
    print(f"failed_queries={summary.failed_queries}")
    print(f"throughput_qps={summary.throughput_qps:.3f}")
    print(f"latency_mean_s={summary.latency_mean_s:.6f}")
    print(f"latency_p50_s={summary.latency_p50_s:.6f}")
    print(f"latency_p95_s={summary.latency_p95_s:.6f}")
    print(f"latency_p99_s={summary.latency_p99_s:.6f}")
    if summary.sample_failure:
        print("sample_failure=", summary.sample_failure)


def print_comparison(summaries: list[BenchmarkSummary]) -> None:
    print("\nPolicy Comparison")
    print("policy\tqps\tp50_s\tp95_s\tp99_s\tfailures")
    for summary in summaries:
        print(
            f"{summary.policy}\t{summary.throughput_qps:.3f}\t{summary.latency_p50_s:.6f}\t"
            f"{summary.latency_p95_s:.6f}\t{summary.latency_p99_s:.6f}\t{summary.failed_queries}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an online TPCH benchmark with Poisson arrivals.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file")
    parser.add_argument("--scale-factor", type=int, default=1, help="TPCH scale factor for dbgen")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    parser.add_argument("--arrival-rate", type=float, default=0.5, help="Poisson arrival rate (queries/sec)")
    parser.add_argument("--duration-seconds", type=int, default=60, help="Benchmark duration")
    parser.add_argument(
        "--scheduler-policy",
        default="no_priority",
        choices=SUPPORTED_POLICIES,
        help="Scheduler policy to run (ignored when --compare-policies is set)",
    )
    parser.add_argument(
        "--compare-policies",
        nargs="+",
        choices=SUPPORTED_POLICIES,
        help="Run the benchmark once per listed policy and print a summary table",
    )
    parser.add_argument("--skip-setup", action="store_true", help="Skip tpch INSTALL/LOAD/dbgen")
    args = parser.parse_args()

    if not args.skip_setup:
        setup_tpch_database(args.db_path, args.scale_factor)
    queries = load_tpch_queries_from_repo()
    policies = args.compare_policies if args.compare_policies else [args.scheduler_policy]
    summaries: list[BenchmarkSummary] = []
    for policy in policies:
        print(f"\nRunning policy={policy}")
        results = run_benchmark(args.db_path, args.workers, args.arrival_rate, args.duration_seconds, policy, queries)
        summary = summarize_results(policy, args.duration_seconds, results)
        print_summary(summary)
        summaries.append(summary)

    if len(summaries) > 1:
        print_comparison(summaries)


if __name__ == "__main__":
    main()
