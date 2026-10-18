"""Deterministic TwitchChat schedules; Python 3 standard library only."""
import argparse
import csv
import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")


def positive(value):
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError("duration and rates must be finite and positive")
    return number


def select(source, config):
    start, end = map(Decimal, (config["window_start_seconds"], config["window_end_seconds"]))
    count = config["selected_users"]
    if not start.is_finite() or not end.is_finite() or start < 0 or end <= start:
        raise ValueError("invalid half-open source window")
    if not isinstance(count, int) or count <= 0:
        raise ValueError("selected_users must be a positive integer")
    rows, users, previous = [], {}, None
    with source.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["Time", "User", "Message"]:
            raise ValueError("expected CSV header Time,User,Message")
        for row_id, row in enumerate(reader, 1):
            if None in row or any(row.get(key) is None for key in reader.fieldnames):
                raise ValueError(f"malformed CSV record {row_id}")
            timestamp = Decimal(row["Time"])
            if not timestamp.is_finite() or timestamp < 0 or (previous is not None and timestamp < previous):
                raise ValueError(f"invalid or decreasing timestamp at record {row_id}")
            previous = timestamp
            if not row["User"]:
                raise ValueError(f"empty user at record {row_id}")
            if start <= timestamp < end:
                if row["User"] not in users and len(users) < count:
                    users[row["User"]] = len(users)
                if row["User"] in users:
                    rows.append((row_id, timestamp, row["User"], row["Message"]))
    if len(users) != count:
        raise ValueError(f"window has only {len(users)} users; need {count}")
    return rows, users, end


def generate(source, config, output):
    rows, users, window_end = select(source, config)
    gateways, clients = config["gateways"], config["total_clients"]
    if gateways != 4 or not isinstance(clients, int) or clients < len(users) or clients % gateways:
        raise ValueError("proposal requires four gateways and equally partitioned clients >= selected users")
    duration = positive(config["duration_seconds"])
    rates = [positive(rate) for rate in config["rates_messages_per_second"]]
    if len(rates) != 5 or len(set(rates)) != 5:
        raise ValueError("provide five distinct positive pilot/main rates")
    ids = list(users)
    for index in range(clients - len(users)):
        receiver = f"receiver-only-{index:04d}"
        if receiver in users:
            raise ValueError("synthetic receiver ID collision")
        ids.append(receiver)
    mapping = {user: f"G{index % gateways}" for index, user in enumerate(ids)}
    origin = rows[0][1]
    # Preserve the selected window's tail as an explicit boundary gap.
    period = window_end - origin
    if period <= 0:
        raise ValueError("source period must be positive")
    if output.exists():
        raise ValueError(f"output already exists: {output}; choose a fresh directory")
    output.mkdir(parents=True)
    write_json(output / "config.json", config)
    write_json(output / "clients.json", {"recipient_sets": {"room-all": ids}, "clients": [
        {"client_id": user, "gateway_id": mapping[user], "sender": user in users} for user in ids
    ]})
    results = []
    for rate in rates:
        scale = rate * period / len(rows)
        filename = f"rate-{format(rate.normalize(), 'f')}.jsonl"
        total, by_gateway, max_bytes, last_ns = 0, {f"G{i}": 0 for i in range(gateways)}, 0, -1
        repeat = 0
        with (output / filename).open("w", encoding="utf-8", newline="\n") as stream:
            done = False
            while not done:
                for row_id, timestamp, sender, payload in rows:
                    offset = (repeat * period + timestamp - origin) / scale
                    ns = int((offset * 1_000_000_000).to_integral_value(rounding=ROUND_HALF_UP))
                    if Decimal(ns) >= duration * 1_000_000_000:
                        done = True
                        break
                    payload_bytes = len(payload.encode("utf-8"))
                    event = {"message_id": f"{source.stem}:r{repeat}:row{row_id}",
                             "scheduled_offset_ns": ns, "sender_id": sender,
                             "payload": payload, "payload_bytes": payload_bytes,
                             "source_gateway": mapping[sender], "recipient_set_id": "room-all",
                             "source_row_id": row_id, "repeat_index": repeat}
                    stream.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
                    total += 1
                    by_gateway[mapping[sender]] += 1
                    max_bytes = max(max_bytes, payload_bytes)
                    last_ns = ns
                repeat += 1
        results.append({"file": filename, "sha256": digest(output / filename),
                        "target_cycle_average_messages_per_second": str(rate),
                        "scheduled_window_messages_per_second": str(Decimal(total) / duration),
                        "time_scale_factor": str(scale), "messages": total,
                        "expected_deliveries": total * clients, "publications_by_gateway": by_gateway,
                        "max_payload_bytes": max_bytes, "last_scheduled_offset_ns": last_ns,
                        "scaled_repeat_boundary_gap_seconds": str((window_end - rows[-1][1]) / scale)})
    manifest = {"schema_version": 1, "stage": config["stage"], "source_file": source.name,
                "source_sha256": digest(source), "generator_sha256": digest(Path(__file__)),
                "config_sha256": digest(output / "config.json"), "clients_sha256": digest(output / "clients.json"),
                "user_selection": "first distinct users encountered in the fixed window, original CSV order",
                "source_rows": len(rows), "active_senders": len(users), "total_clients": clients,
                "duration_seconds": str(duration), "first_retained_source_time_seconds": str(origin),
                "unscaled_repeat_period_seconds": str(period),
                "unscaled_repeat_boundary_gap_seconds": str(window_end - rows[-1][1]),
                "recipient_policy": "all fixed clients including sender; resolve room-all in clients.json",
                "timestamp_policy": "relative nanoseconds rounded half-up; retain JSONL order for ties",
                "rate_policy": "uniform source-time scaling; finite-window rate may differ due to preserved bursts",
                "single_server_policy": "same trace and clients; connect all clients to the single endpoint, ignore logical gateway routing",
                "traces": results}
    write_json(output / "manifest.json", manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    manifest = generate(args.data_dir / config["source_file"], config, args.output)
    print(json.dumps({"output": str(args.output), "source_rows": manifest["source_rows"],
                      "trace_messages": [row["messages"] for row in manifest["traces"]]}))


if __name__ == "__main__":
    main()
