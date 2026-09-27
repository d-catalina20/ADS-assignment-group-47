import argparse
import csv
import math
import os
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import matplotlib.pyplot as plt
import rpyc

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18861"))


def percentile(values, p):
    """Linear-interpolated percentile, matching common percentile definitions."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    k = (len(ordered) - 1) * (p / 100.0)
    floor = math.floor(k)
    ceil = math.ceil(k)
    if floor == ceil:
        return ordered[floor]
    return ordered[floor] + (ordered[ceil] - ordered[floor]) * (k - floor)


class RPyCWorker:
    """One persistent RPyC connection per benchmark worker thread."""

    _local = threading.local()

    @classmethod
    def connection(cls):
        conn = getattr(cls._local, "conn", None)
        if conn is None or conn.closed:
            conn = rpyc.connect(SERVER_HOST, SERVER_PORT)
            cls._local.conn = conn
        return conn

    @classmethod
    def close(cls):
        conn = getattr(cls._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            cls._local.conn = None


def rpc_request(keyword, filename):
    """Make one request and measure only the RPC request/response interval."""
    conn = RPyCWorker.connection()
    start_ns = time.perf_counter_ns()
    result = conn.root.count_word(keyword, filename)
    end_ns = time.perf_counter_ns()
    latency_ms = (end_ns - start_ns) / 1_000_000.0
    return latency_ms, result


def run_rate(rate, duration, keyword, filename, workers):
    """
    Open-loop workload generator.

    Requests are scheduled at a fixed inter-arrival time (1/rate), while a
    thread pool provides enough concurrency for requests whose latency is
    longer than the inter-arrival time.
    """
    interval = 1.0 / rate
    total_requests = max(1, int(round(rate * duration)))

    latencies = []
    cache_hits = 0
    errors = []
    start = time.perf_counter()
    futures = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        next_send = start

        for _ in range(total_requests):
            now = time.perf_counter()
            if now < next_send:
                time.sleep(next_send - now)

            futures.append(executor.submit(rpc_request, keyword, filename))
            next_send += interval

        for future in as_completed(futures):
            try:
                latency_ms, result = future.result()
                latencies.append(latency_ms)
                if result.get("cache_hit"):
                    cache_hits += 1
            except Exception as exc:
                errors.append(str(exc))

    elapsed = time.perf_counter() - start
    actual_rate = len(latencies) / elapsed if elapsed > 0 else 0.0

    return {
        "target_rate": rate,
        "requests": total_requests,
        "successful": len(latencies),
        "errors": len(errors),
        "actual_rate": actual_rate,
        "average_ms": statistics.mean(latencies) if latencies else float("nan"),
        "p99_ms": percentile(latencies, 99),
        "min_ms": min(latencies) if latencies else float("nan"),
        "max_ms": max(latencies) if latencies else float("nan"),
        "cache_hits": cache_hits,
        "cache_hit_rate": (cache_hits / len(latencies) * 100.0) if latencies else 0.0,
        "latencies_ms": latencies,
        "errors_detail": errors[:10],
    }


def warm_cache(keyword, filename):
    """Send one request first so that the benchmark measures the steady-state cache path."""
    latency_ms, result = rpc_request(keyword, filename)
    print(
        f"Warm-up: {keyword!r} in {filename!r} -> "
        f"count={result['count']}, cache_hit={result['cache_hit']}, "
        f"latency={latency_ms:.3f} ms"
    )


def save_csv(results, path):
    fieldnames = [
        "target_rate_rps",
        "requests",
        "successful",
        "errors",
        "actual_rate_rps",
        "average_latency_ms",
        "p99_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
        "cache_hits",
        "cache_hit_rate_percent",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({
                "target_rate_rps": row["target_rate"],
                "requests": row["requests"],
                "successful": row["successful"],
                "errors": row["errors"],
                "actual_rate_rps": f"{row['actual_rate']:.3f}",
                "average_latency_ms": f"{row['average_ms']:.6f}",
                "p99_latency_ms": f"{row['p99_ms']:.6f}",
                "min_latency_ms": f"{row['min_ms']:.6f}",
                "max_latency_ms": f"{row['max_ms']:.6f}",
                "cache_hits": row["cache_hits"],
                "cache_hit_rate_percent": f"{row['cache_hit_rate']:.2f}",
            })


def make_plots(results, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    rates = [r["target_rate"] for r in results]
    averages = [r["average_ms"] for r in results]
    p99s = [r["p99_ms"] for r in results]

    plt.figure(figsize=(8, 5))
    plt.plot(rates, averages, marker="o")
    plt.xlabel("Keyword requests per second")
    plt.ylabel("Request execution latency (ms)")
    plt.title("Average Execution Latency")
    plt.xticks(rates)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / "average_latency.png", dpi=200)
    plt.close()

    plt.figure(figsize=(8, 5))
    plt.plot(rates, p99s, marker="o")
    plt.xlabel("Keyword requests per second")
    plt.ylabel("Request execution latency (ms)")
    plt.title("99th-Percentile (Tail) Execution Latency")
    plt.xticks(rates)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / "p99_latency.png", dpi=200)
    plt.close()


def parse_rates(value):
    rates = [int(x.strip()) for x in value.split(",") if x.strip()]
    if len(rates) != 5:
        raise argparse.ArgumentTypeError("Provide exactly five request rates, e.g. 50,70,90,110,130")
    if any(rate < 10 for rate in rates):
        raise argparse.ArgumentTypeError("Every request rate must be at least 10 requests/second")
    if any(b - a < 10 for a, b in zip(rates, rates[1:])):
        raise argparse.ArgumentTypeError("Adjacent request rates must differ by at least 10 requests/second")
    return rates


def main():
    parser = argparse.ArgumentParser(description="Open-loop RPyC word-count latency benchmark")
    parser.add_argument("--keyword", default="the", help="Keyword to request (default: the)")
    parser.add_argument("--file", default="article1.txt", help="TXT file to request (default: article1.txt)")
    parser.add_argument("--rates", type=parse_rates, default=[50, 70, 90, 110, 130],
                        help="Five request rates, e.g. 50,70,90,110,130")
    parser.add_argument("--duration", type=float, default=10.0,
                        help="Seconds to run at each rate (default: 10)")
    parser.add_argument("--workers", type=int, default=64,
                        help="Maximum concurrent RPC requests (default: 64)")
    parser.add_argument("--output-dir", default="benchmark_results",
                        help="Directory for CSV and PNG output")
    parser.add_argument("--cold-cache", action="store_true",
                        help="Do not perform a warm-up request before the benchmark")
    args = parser.parse_args()

    if args.duration <= 0:
        parser.error("--duration must be greater than 0")
    if args.workers <= 0:
        parser.error("--workers must be greater than 0")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("RPyC word-count latency benchmark")
    print(f"Server: {SERVER_HOST}:{SERVER_PORT}")
    print(f"Keyword: {args.keyword!r}")
    print(f"File: {args.file!r}")
    print(f"Rates: {args.rates} requests/second")
    print(f"Duration per rate: {args.duration:.1f} s")
    print(f"Workers: {args.workers}")
    print()

    conn = rpyc.connect(SERVER_HOST, SERVER_PORT)
    try:
        print(f"Server ping: {conn.root.ping()}")
    finally:
        conn.close()

    if not args.cold_cache:
        warm_cache(args.keyword, args.file)

    results = []
    for rate in args.rates:
        print(f"\nRunning {rate} requests/second for {args.duration:.1f} seconds...")
        result = run_rate(rate, args.duration, args.keyword, args.file, args.workers)
        results.append(result)
        print(
            f"  successful={result['successful']}/{result['requests']}, "
            f"errors={result['errors']}, actual_rate={result['actual_rate']:.2f} req/s"
        )
        print(
            f"  average={result['average_ms']:.3f} ms, "
            f"p99={result['p99_ms']:.3f} ms, "
            f"min={result['min_ms']:.3f} ms, max={result['max_ms']:.3f} ms"
        )
        print(f"  cache_hit_rate={result['cache_hit_rate']:.2f}%")
        if result["errors_detail"]:
            print("  sample errors:")
            for error in result["errors_detail"]:
                print(f"    {error}")

    csv_path = output_dir / "latency_results.csv"
    save_csv(results, csv_path)
    make_plots(results, output_dir)

    print("\nSummary")
    print("Rate (req/s) | Average (ms) | P99 (ms) | Actual rate | Errors")
    print("-------------|---------------|----------|-------------|-------")
    for r in results:
        print(
            f"{r['target_rate']:>12} | "
            f"{r['average_ms']:>13.3f} | "
            f"{r['p99_ms']:>8.3f} | "
            f"{r['actual_rate']:>11.2f} | "
            f"{r['errors']:>6}"
        )

    print(f"\nSaved: {csv_path}")
    print(f"Saved: {output_dir / 'average_latency.png'}")
    print(f"Saved: {output_dir / 'p99_latency.png'}")


if __name__ == "__main__":
    main()
