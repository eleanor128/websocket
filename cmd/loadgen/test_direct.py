"""Verify real Direct gateway processes, client deliveries and per-peer fan-out."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


def health(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2) as response:
        return json.load(response)


def wait_ready(processes, ports):
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        if any(p.poll() is not None for p in processes):
            raise RuntimeError("gateway exited; inspect logs")
        try:
            states = [health(p) for p in ports]
            if all(s["ready"] for s in states):
                return states
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("Direct mesh did not become ready")


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def run_one(args, output, server, loadgen, trace):
    output.mkdir()
    ports = []
    while len(ports) < 4:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
            if port not in ports:
                ports.append(port)
    overlays = {f"G{i}": f"ws://127.0.0.1:{p}/inter-gw" for i, p in enumerate(ports)}
    clients = {f"G{i}": f"ws://127.0.0.1:{p}/ws" for i, p in enumerate(ports)}
    save(output / "overlay.json", overlays)
    save(output / "gateways.json", clients)
    processes, logs = [], []
    hidden = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    try:
        for i, port in enumerate(ports):
            log = (output / f"G{i}.log").open("w", encoding="utf-8")
            logs.append(log)
            processes.append(subprocess.Popen([str(server), "-benchmark", "-topology", "direct", "-id", f"G{i}",
                                               "-addr", f"127.0.0.1:{port}", "-direct-peers", str(output / "overlay.json")],
                                              cwd=ROOT / "examples/chat", stdout=log, stderr=subprocess.STDOUT, creationflags=hidden))
        before = wait_ready(processes, ports)
        save(output / "health-before.json", before)
        subprocess.run([str(loadgen), "-gateways", str(output / "gateways.json"), "-clients", str(args.clients),
                        "-trace", str(trace), "-duration", "60s", "-drain", "2s", "-output", str(output / "run")], cwd=ROOT, check=True)
        after = [health(p) for p in ports]
        save(output / "health-after.json", after)
        summary = json.loads((output / "run/summary.json").read_text())
        total = summary["scheduled_messages"]
        for key in ("missing", "duplicates", "unexpected_messages", "failed_or_unsent", "disconnects"):
            if summary[key] != 0:
                raise RuntimeError(f"{key}={summary[key]}")
        for i, state in enumerate(after):
            gid = f"G{i}"
            expected_peers = sorted(set(clients) - {gid})
            if not state["ready"] or state["peer_ids"] != expected_peers:
                raise RuntimeError(f"{gid}: mesh membership changed")
            m = state["metrics"]
            local = summary["gateway_stats"][gid]["scheduled_messages"]
            if m["local_publications"] != local or m["overlay_sent"] != 3 * local or m["overlay_received"] != total - local:
                raise RuntimeError(f"{gid}: incorrect Direct fan-out: {m}")
            if m["overlay_write_errors"] or m["rejected_peer_messages"]:
                raise RuntimeError(f"{gid}: overlay error")
            for peer, count in m["overlay_sent_by_peer"].items():
                if count != (0 if peer == gid else local):
                    raise RuntimeError(f"{gid}: duplicate/omitted transmission to {peer}")
        save(output / "verification.json", {"passed": True, "overlay_connections": 6, "overlay_sent": 3 * total,
                                            "rule": "each source sends one copy to each other gateway; receivers never forward"})
        return {"trace": trace.name, "messages": total, "unique_deliveries": summary["unique_deliveries"],
                "overlay_sent": 3 * total, "p95_ms": summary["scheduled_latency_p95_ms"],
                "p99_ms": summary["scheduled_latency_p99_ms"], "on_time_fraction": summary["on_time_fraction"], "passed": True}
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--all", action="store_true", help="run five pilot rates, sequential independent deployments")
    p.add_argument("--trace", type=Path, default=ROOT / "workloads/generated/pilot-v1/rate-10.jsonl")
    p.add_argument("--clients", type=Path, default=ROOT / "workloads/generated/pilot-v1/clients.json")
    args = p.parse_args()
    args.clients = args.clients.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    files = list((ROOT / "examples/chat").glob("*.go")) + list((ROOT / "cmd/loadgen").glob("*.go"))
    save(output / "source-hashes.json", {str(f.relative_to(ROOT)): hashlib.sha256(f.read_bytes()).hexdigest() for f in files})
    traces = [ROOT / f"workloads/generated/pilot-v1/rate-{r}.jsonl" for r in (10, 20, 40, 80, 160)] if args.all else [args.trace.resolve()]
    results = []
    with tempfile.TemporaryDirectory(prefix="direct-test-") as temp:
        suffix = ".exe" if os.name == "nt" else ""
        server, loadgen = (Path(temp) / (name + suffix) for name in ("chat", "loadgen"))
        for exe, package in ((server, "./examples/chat"), (loadgen, "./cmd/loadgen")):
            subprocess.run(["go", "build", "-buildvcs=false", "-o", str(exe), package], cwd=ROOT, check=True)
        for trace in traces:
            print(f"START {trace.name}", flush=True)
            try:
                result = run_one(args, output / trace.stem, server, loadgen, trace)
            except Exception as exc:
                result = {"trace": trace.name, "passed": False, "error": str(exc)}
            results.append(result)
            save(output / "matrix.json", results)
            print(json.dumps(result), flush=True)
    if not all(r["passed"] for r in results):
        sys.exit("Direct integration failed; inspect saved logs and summaries")


if __name__ == "__main__":
    main()
