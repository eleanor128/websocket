"""Run all five existing pilot traces against independent real Star deployments."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    bundle = ROOT / "workloads/generated/pilot-v1"
    manifest = json.loads((bundle / "manifest.json").read_text())
    checks = subprocess.run(["go", "test", "./examples/chat", "./cmd/loadgen", "-count=1", "-v"],
                            cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace")
    (output / "regression-tests.log").write_text(checks.stdout + checks.stderr, encoding="utf-8")
    if checks.returncode:
        raise RuntimeError("Regression tests failed; inspect regression-tests.log")
    sources = list((ROOT / "examples/chat").glob("*.go")) + list((ROOT / "cmd/loadgen").glob("*.go"))
    provenance = {"started_utc": datetime.now(timezone.utc).isoformat(),
                  "environment": "local Windows; four real gateway processes; shared host; no resource shaping",
                  "repeats_per_rate": 1, "clients_sha256": sha(bundle / "clients.json"),
                  "source_sha256": {str(p.relative_to(ROOT)): sha(p) for p in sources}}
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    rows = []
    for rate in [10, 20, 40, 80, 160]:
        trace = bundle / f"rate-{rate}.jsonl"
        entry = next(item for item in manifest["traces"] if item["file"] == trace.name)
        if sha(trace) != entry["sha256"]:
            raise RuntimeError(f"Trace checksum mismatch: {trace}")
        print(f"START rate-{rate}: {entry['messages']} messages", flush=True)
        command = [sys.executable, str(ROOT / "cmd/loadgen/test_star.py"), "--output", str(output / f"rate-{rate}"),
                   "--trace", str(trace), "--clients", str(bundle / "clients.json"), "--duration", "60s"]
        with (output / f"rate-{rate}.log").open("w", encoding="utf-8") as log:
            process = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        summary_path = output / f"rate-{rate}/run/summary.json"
        row = {"target_rate": rate, "exit_code": process.returncode, "passed": False}
        if summary_path.exists():
            summary = json.loads(summary_path.read_text())
            fields = ["scheduled_messages", "successful_writes", "expected_deliveries", "unique_deliveries", "missing",
                      "duplicates", "unexpected_messages", "disconnects", "failed_or_unsent", "on_time_fraction",
                      "scheduled_latency_p50_ms", "scheduled_latency_p95_ms", "scheduled_latency_p99_ms",
                      "send_lag_p95_ms", "send_lag_max_ms", "unique_deliveries_per_second"]
            row.update({field: summary[field] for field in fields})
            row["passed"] = (process.returncode == 0 and summary["scheduled_messages"] == entry["messages"]
                             and summary["trace_sha256"] == entry["sha256"]
                             and summary["clients_sha256"] == provenance["clients_sha256"]
                             and len(summary["gateway_stats"]) == 4
                             and all(g["clients"] == 50 and g["unique_deliveries"] == entry["messages"] * 50
                                     for g in summary["gateway_stats"].values()))
        rows.append(row)
        (output / "matrix.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
        print(f"{'PASS' if row['passed'] else 'FAIL'} rate-{rate}: {json.dumps(row)}", flush=True)
    lines = ["# Star five-trace local verification", "", "One run per rate; 200 clients, four real gateway processes, 60s publication + 2s drain.",
             "Shared local host; no CPU/network limits. Functional results, not formal capacity certification.", "",
             "| Target msg/s | Messages | Unique / expected | Missing | Duplicates | On-time | P95 ms | P99 ms | Pass |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        if "unique_deliveries" in r:
            lines.append(f"| {r['target_rate']} | {r['scheduled_messages']} | {r['unique_deliveries']} / {r['expected_deliveries']} | {r['missing']} | {r['duplicates']} | {r['on_time_fraction']:.2%} | {r['scheduled_latency_p95_ms']:.3f} | {r['scheduled_latency_p99_ms']:.3f} | {r['passed']} |")
        else:
            lines.append(f"| {r['target_rate']} | setup failed | — | — | — | — | — | — | False |")
    (output / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if not all(r["passed"] for r in rows):
        raise SystemExit("One or more runs failed; inspect matrix.json and individual logs.")


if __name__ == "__main__":
    main()
