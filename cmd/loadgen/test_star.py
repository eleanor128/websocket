"""Start four real Star processes and replay the existing workload (Python stdlib)."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request


ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def ready(process, port, peers):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("gateway exited; inspect gateway logs")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
                if json.load(response)["connected_peers"] == peers:
                    return
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"gateway {port} did not establish {peers} peers")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trace", default="workloads/generated/pilot-v1/rate-10.jsonl")
    parser.add_argument("--clients", default="workloads/generated/pilot-v1/clients.json")
    parser.add_argument("--duration", default="60s")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    processes, logs = [], []
    hidden = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with tempfile.TemporaryDirectory(prefix="star-integration-") as temporary:
        suffix = ".exe" if os.name == "nt" else ""
        server, loadgen = (Path(temporary) / (name + suffix) for name in ("chat", "loadgen"))
        # Local integration binaries do not require Git ownership/VCS stamping.
        subprocess.run(["go", "build", "-buildvcs=false", "-o", str(server), "./examples/chat"], cwd=ROOT, check=True)
        subprocess.run(["go", "build", "-buildvcs=false", "-o", str(loadgen), "./cmd/loadgen"], cwd=ROOT, check=True)
        ports = []
        while len(ports) < 4:
            port = free_port()
            if port not in ports:
                ports.append(port)
        try:
            for i, port in enumerate(ports):
                log = (output / f"G{i}.log").open("w", encoding="utf-8")
                logs.append(log)
                command = [str(server), "-benchmark", "-topology", "star", "-id", f"G{i}", "-addr", f"127.0.0.1:{port}"]
                if i:
                    command += ["-peers", f"ws://127.0.0.1:{ports[0]}/inter-gw"]
                process = subprocess.Popen(command, cwd=ROOT / "examples/chat", stdout=log, stderr=subprocess.STDOUT, creationflags=hidden)
                processes.append(process)
                ready(process, port, 0 if i == 0 else 1)
            ready(processes[0], ports[0], 3)
            endpoints = output / "gateways.json"
            endpoints.write_text(json.dumps({f"G{i}": f"ws://127.0.0.1:{port}/ws" for i, port in enumerate(ports)}, indent=2), encoding="utf-8")
            print("Four real Star gateways ready; replaying workload.", flush=True)
            subprocess.run([str(loadgen), "-gateways", str(endpoints), "-clients", args.clients,
                            "-trace", args.trace, "-duration", args.duration, "-drain", "2s",
                            "-output", str(output / "run")], cwd=ROOT, check=True)
            result = json.loads((output / "run" / "summary.json").read_text())
            for field in ("missing", "duplicates", "unexpected_messages", "failed_or_unsent", "disconnects"):
                if result[field] != 0:
                    raise RuntimeError(f"Star integration failed: {field}={result[field]}")
            if result["unique_deliveries"] != result["expected_deliveries"]:
                raise RuntimeError("delivery mismatch")
            print(f"PASS: {result['unique_deliveries']} unique deliveries across four real Star gateways.")
        finally:
            for process in reversed(processes):
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


if __name__ == "__main__":
    main()
