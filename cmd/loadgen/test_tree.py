"""Verify real Tree gateway processes, client deliveries and tree broadcast fan-out."""
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

EXPECTED_PEERS = {
    "G0": ["G1", "G2"],
    "G1": ["G0", "G3"],
    "G2": ["G0"],
    "G3": ["G1"],
}


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
            if all(s.get("ready") for s in states):
                # Verify exact peer topology membership
                all_match = True
                for i, s in enumerate(states):
                    gid = f"G{i}"
                    if s.get("peer_ids") != EXPECTED_PEERS[gid]:
                        all_match = False
                        break
                if all_match:
                    return states
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("Tree network did not become ready with expected peers")


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
            cmd = [
                str(server), "-benchmark", "-topology", "tree", "-id", f"G{i}",
                "-addr", f"127.0.0.1:{port}", "-tree-peers", str(output / "overlay.json")
            ]
            processes.append(subprocess.Popen(cmd, cwd=ROOT / "examples/chat", stdout=log, stderr=subprocess.STDOUT, creationflags=hidden))

        before = wait_ready(processes, ports)
        save(output / "health-before.json", before)
        print(f"Tree cluster ready; replaying {trace.name}...", flush=True)

        subprocess.run([
            str(loadgen), "-gateways", str(output / "gateways.json"), "-clients", str(args.clients),
            "-trace", str(trace), "-duration", "60s", "-drain", "2s", "-output", str(output / "run")
        ], cwd=ROOT, check=True)

        after = [health(p) for p in ports]
        save(output / "health-after.json", after)
        summary = json.loads((output / "run/summary.json").read_text())
        total = summary["scheduled_messages"]

        for key in ("missing", "duplicates", "unexpected_messages", "failed_or_unsent", "disconnects"):
            if summary[key] != 0:
                raise RuntimeError(f"{key}={summary[key]}")

        # Check per-node Tree metrics
        # For M total publications (m_i originating at G_i):
        # G0 sent: m0 + M, recv: M - m0
        # G1 sent: m1 + M, recv: M - m1
        # G2 sent: m2,     recv: M - m2
        # G3 sent: m3,     recv: M - m3
        total_overlay_sent = 0
        for i, state in enumerate(after):
            gid = f"G{i}"
            expected_peers = EXPECTED_PEERS[gid]
            if not state["ready"] or state["peer_ids"] != expected_peers:
                raise RuntimeError(f"{gid}: tree peer membership changed: {state['peer_ids']}")
            m = state["metrics"]
            local = summary["gateway_stats"][gid]["scheduled_messages"]

            if m["local_publications"] != local:
                raise RuntimeError(f"{gid}: local publications mismatch: got {m['local_publications']}, expected {local}")

            expected_sent = (local + total) if gid in ("G0", "G1") else local
            if m["overlay_sent"] != expected_sent:
                raise RuntimeError(f"{gid}: overlay sent mismatch: got {m['overlay_sent']}, expected {expected_sent}")

            expected_recv = total - local
            if m["overlay_received"] != expected_recv:
                raise RuntimeError(f"{gid}: overlay received mismatch: got {m['overlay_received']}, expected {expected_recv}")

            if m["overlay_write_errors"] != 0 or m["rejected_peer_messages"] != 0:
                raise RuntimeError(f"{gid}: overlay errors detected: {m}")

            total_overlay_sent += m["overlay_sent"]

        if total_overlay_sent != 3 * total:
            raise RuntimeError(f"Total cluster overlay sent mismatch: got {total_overlay_sent}, expected {3 * total}")

        save(output / "verification.json", {
            "passed": True,
            "tree_edges": 3,
            "overlay_connections": 3,
            "total_overlay_sent": total_overlay_sent,
            "expected_cluster_sent": 3 * total,
            "rule": "G0-G1, G0-G2, G1-G3 bidirectional tree; forwards along all non-incoming edges"
        })
        return {
            "trace": trace.name, "messages": total,
            "unique_deliveries": summary["unique_deliveries"],
            "overlay_sent": total_overlay_sent,
            "p95_ms": summary["scheduled_latency_p95_ms"],
            "p99_ms": summary["scheduled_latency_p99_ms"],
            "on_time_fraction": summary["on_time_fraction"],
            "passed": True
        }
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
    with tempfile.TemporaryDirectory(prefix="tree-test-") as temp:
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
        sys.exit("Tree integration failed; inspect saved logs and summaries")


if __name__ == "__main__":
    main()
