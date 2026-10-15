import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Load data from the three benchmark runs
df_1s = pd.read_csv("client/benchmark_results/latency_results.csv")
df_lc = pd.read_csv("client/results_3s_least_conn/latency_results.csv")
df_lrt = pd.read_csv("client/results_3s_least_rt/latency_results.csv")

rates = df_1s["target_rate_rps"]
out_dir = Path("client/phase3_comparison_plots")
out_dir.mkdir(parents=True, exist_ok=True)

# 1. Comparison of Average Latency
plt.figure(figsize=(8, 5))
plt.plot(rates, df_1s["average_latency_ms"], marker="o", label="1 Server (Phase 2)")
plt.plot(rates, df_lc["average_latency_ms"], marker="s", label="3 Servers (Least Connections)")
plt.plot(rates, df_lrt["average_latency_ms"], marker="^", label="3 Servers (Least Response Time)")
plt.xlabel("Keyword requests per second")
plt.ylabel("Request execution latency (ms)")
plt.title("Comparative Average Execution Latency: 1 vs. 3 Servers")
plt.xticks(rates)
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(out_dir / "comparison_average_latency.png", dpi=200)
plt.close()

# 2. Comparison of P99 (Tail) Latency
plt.figure(figsize=(8, 5))
plt.plot(rates, df_1s["p99_latency_ms"], marker="o", label="1 Server (Phase 2)")
plt.plot(rates, df_lc["p99_latency_ms"], marker="s", label="3 Servers (Least Connections)")
plt.plot(rates, df_lrt["p99_latency_ms"], marker="^", label="3 Servers (Least Response Time)")
plt.xlabel("Keyword requests per second")
plt.ylabel("Request execution latency (ms)")
plt.title("Comparative 99th-Percentile (Tail) Execution Latency: 1 vs. 3 Servers")
plt.xticks(rates)
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(out_dir / "comparison_p99_latency.png", dpi=200)
plt.close()

print(f"Comparison plots saved in {out_dir}")