# Proposal workload traces

This directory generates offline schedules, not live WebSocket traffic. Python 3.10+ and its standard library are sufficient. The raw TwitchChat dataset remains outside this repository in `../data_set/data`.

From the `websocket` repository root:

```powershell
python workloads/generate.py --config workloads/pilot.json --data-dir ../data_set/data --output workloads/generated/pilot-v1
python -m unittest discover -s workloads -p "test_*.py" -v
```

The generated `pilot-v1` directory is included. To reproduce without overwriting it, choose a new output directory and compare SHA-256 values. Existing output directories are rejected. No current date, random state or absolute filesystem paths enter the artifacts.

## Selection and scope

- One continuous source window: `[0,600)` seconds of `0389b65f31955070df63ae563f08a3a058ab38b6_0.csv`.
- This is the first filename in lexicographic order whose initial 600-second window contains at least 200 distinct users. This reproducible convenience selection is not a claim of representativeness.
- Select the first 200 distinct users encountered within that window, then retain all their messages in the window. CSV record IDs are one-based, excluding the header (not physical line numbers for multiline CSV records).
- Preserve payload text, whitespace, sender IDs and CSV order exactly. Reject decreasing timestamps rather than silently reorder. Equal timestamps retain row order.
- Assign users in selection order round-robin to G0–G3: 50 clients each. Sender activity need not be balanced. Optional extra clients are receiver-only, and total clients must be divisible by four.
- Pilot candidate rates: 10/20/40/80/160 messages/s, 60-second measurement window. These are implementation starting values, NOT pilot-established sustainable rates or the proposal's frozen main settings. Real timestamps retain bursts; these traces are not constant-rate synthetic traffic.

## Timing and repetition

Let M be retained source messages, t0 the first retained timestamp, E the configured source window end, and P=E-t0. For target rate R, use scale factor `a=R*P/M`. Message at source time t in repeat k is scheduled at `(k*P+t-t0)/a` seconds. Thus the unscaled gap from the last retained message to the next repeat is `E-last_timestamp`; this gap is explicitly reported. A repeat is not concatenated at zero gap.

Times are rounded half-up to integer nanoseconds. Only events in `[0,duration)` after rounding are emitted. Preserve JSONL order for ties. The configured rate is the full-cycle average; because a finite window may end partway through a bursty cycle, `messages/duration` can differ. Both are reported in the manifest. Do not smooth or pad messages to force equality.

IDs combine session filename stem, repeat index and original CSV record ID. They are unique within each schedule and stable across architecture runs. A replay system must separately attach `run_id` when combining results from multiple runs.

## Output contract

Each `rate-*.jsonl` record contains:

| Field | Meaning |
| --- | --- |
| `scheduled_offset_ns` | Scheduled publication time relative to measurement start |
| `message_id` | Session, repetition and original row identity |
| `sender_id` | Original dataset user |
| `payload` | Unmodified message text |
| `payload_bytes` | UTF-8 payload bytes, excluding JSON/protocol overhead |
| `source_gateway` | Fixed logical gateway G0–G3 |
| `recipient_set_id` | `room-all`, resolved through `clients.json` |
| `source_row_id`, `repeat_index` | Provenance for validation |

`clients.json` explicitly enumerates every recipient in `room-all`, including the sender, and the fixed client–gateway mapping. Referencing this table avoids copying 200 IDs into every event. Disconnects during replay must not shrink this expected set.

`manifest.json` records source/generator/config/client-table/trace SHA-256 values, source selection, timing rules, event counts, expected deliveries, observed schedule rates and gateway publication counts. `config.json` preserves the generation parameters. Treat these files as one trace bundle.

## Replaying later

Establish all connections, finish and drain warm-up, then replay relative to a common measurement start. Schedule independently of prior delivery completion. Record actual send times and failures at runtime; an offline generator cannot provide these. Use exactly the same bundle for Star, Tree and Direct. Single uses the same clients, payloads and schedule, but connects every client to its single endpoint instead of interpreting logical gateway IDs as physical destinations.

After pilot testing, copy `pilot.json`, set the five agreed rates and duration and label the stage accordingly; generate a new output directory and freeze its checksums. Do not regenerate a different trace for each architecture or independent repetition. Warm-up and drain are runner responsibilities, not part of these measurement schedules. Burst and concentrated-sender extensions are not separately generated here.
