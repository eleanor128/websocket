# Workload replay client (Single and four gateways)

Receipt accounting is owned by each client reader and merged only after replay;
it does not share a statistics lock with senders. Compact receipt slots are
allocated before measurement (approximately 16 bytes per client per trace event
on 64-bit builds). Duplicate and missing deliveries retain their original meaning.

For diagnosis, `-profile-dir <new-directory>` saves CPU, mutex and heap profiles.
The parent directory must exist. Profiles cover replay and result export; use
unprofiled runs for latency comparisons. `go tool pprof` can inspect the files;
when that binary is absent but Go sources are installed, use `go run cmd/pprof`.
Tree runner `passed` checks delivery/topology correctness; `on_time_target_met`
separately reports whether at least 99% of expected deliveries arrived within 1 s.

Run from the repository root with Go 1.20+. No changes to the Gorilla library are required. The example chat server needs the opt-in benchmark flag: it preserves JSON payloads and message boundaries and permits messages up to 1 MiB. Its normal browser mode remains the default.

Server, in a separate terminal (or on EC2):

```powershell
go run ./examples/chat -benchmark -addr :8080
```

Load generator, locally or on a separate EC2:

```powershell
go run ./cmd/loadgen -url ws://localhost:8080/ws -clients workloads/generated/pilot-v1/clients.json -trace workloads/generated/pilot-v1/rate-10.jsonl -duration 60s -drain 10s -output results/pilot-10-01
```

Replace localhost with the existing server's reachable address. Deploy the updated sample before running; the previous sample prefixes usernames and merges messages, so it cannot be used for this JSON protocol. No remote deployment or traffic is performed by building this command.

## Four-gateway mode

### Tree implementation

Tree forms a broadcast tree with three bidirectional connections: G0-G1, G0-G2, and G1-G3. See `examples/chat/TREE.md` for full design and browser demo commands.

```powershell
python cmd/loadgen/test_tree.py --output results/tree-check-01
python cmd/loadgen/test_tree.py --all --output results/tree-matrix-01
```

The second command tests all five pilot rates. It verifies complete delivery with zero missing or duplicate messages, exactly 3 overlay transmissions across the cluster per publication, and each directed edge's message count. Delivery correctness does not imply on-time delivery: `on_time_target_met` separately checks the 99% within 1 second target.

When all four gateways and the generator share one development machine, make the
process parallelism and server queue explicit. The runner records these values in
each trace directory's `execution.json`:

```powershell
python cmd/loadgen/test_tree.py --trace workloads/generated/pilot-v1/rate-160.jsonl --client-queue 1024 --server-gomaxprocs 2 --loadgen-gomaxprocs 2 --output results/tree-rate160-01
```

`GOMAXPROCS` limits simultaneous Go execution in each process; it is not CPU
affinity or a CPU quota. `/healthz` reports `client_queue_capacity` and
`client_queue_overflows`. A correctness run is invalid if the overflow counter is
nonzero. Queue memory must be included later when applying the experiment's
resource budget.

The publication window controls when trace events are scheduled. Writers finish
already-scheduled events with the configured write timeout, and the drain interval
starts after writers finish. `send_completion_lag_ms`, send lag, and scheduled
latency expose a generator that fell behind; late completion is not reported as
low latency merely because delivery was preserved.

### Direct implementation

Direct has a six-connection full mesh, with exact peer readiness and per-peer business-message counters. See `examples/chat/DIRECT.md` for deployment and browser demo commands. A client endpoint file (`/ws`) is distinct from the server overlay configuration (`/inter-gw`). The generator's existing JSON protocol and traces work unchanged.

```powershell
python cmd/loadgen/test_direct.py --output results/direct-check-01
python cmd/loadgen/test_direct.py --all --output results/direct-matrix-01
```

The second command tests all five pilot rates, restarting four processes for each rate. It verifies not only delivery but exactly three overlay writes per original publication, one to each peer, and no remote forwarding. See each run's `health-after.json` and `verification.json` for topology evidence.

### Existing Star implementation

Use the updated `examples/chat` code on all four gateways. Start G0 first, then its three leaves, all with `-benchmark`. From the repository root, in four separate terminals:

```powershell
go run ./examples/chat -benchmark -topology star -id G0 -addr :8081
go run ./examples/chat -benchmark -topology star -id G1 -addr :8082 -peers ws://127.0.0.1:8081/inter-gw
go run ./examples/chat -benchmark -topology star -id G2 -addr :8083 -peers ws://127.0.0.1:8081/inter-gw
go run ./examples/chat -benchmark -topology star -id G3 -addr :8084 -peers ws://127.0.0.1:8081/inter-gw
```

For multiple EC2 hosts, replace each leaf's peer address with G0's reachable private address; configure the generator's four client endpoints separately. Current Star uses the same listening port for `/ws` and `/inter-gw`; classify by actual flows when adding network shaping. Peers dial only once, so start G0 first and verify `/healthz`: G0 must report three connected peers, each leaf one. A connection failure requires restarting that leaf; automatic reconnection is not implemented.

The generator wire format is unchanged. Client ingress stamps `source_gateway` and clears untrusted `from_gateway`; Star forwarding preserves `run_id`. Without these changes, remote deliveries lose run identity and messages can bounce back to the source. Plain-text browser chat is wrapped in the same server-side envelope when benchmark mode is off. Benchmark mode expects structured loadgen messages.

Star now enforces its edges: G0 accepts only G1/G2/G3; leaves reject incoming overlay connections and initiate exactly one connection to G0. Self/unknown peers are rejected before WebSocket upgrade, duplicate live peer IDs return HTTP 409, and leaf routing can send local publications only to G0. Outgoing leaves verify the destination's `X-Gateway-ID: G0` handshake declaration rather than silently labeling any destination G0. This is configuration validation for the controlled experiment, not cryptographic peer authentication. Update all gateways together; old servers without the handshake header are rejected.

To automatically build and launch four **real** local Star processes, wait for overlay readiness, replay the existing 60-second trace and check for missing/duplicate/invalid deliveries:

```powershell
python cmd/loadgen/test_star.py --output results/star-check-01
```

Run all five pilot rates sequentially (10, 20, 40, 80, 160), one independent four-process deployment per rate:

```powershell
python cmd/loadgen/test_star_matrix.py --output results/star-matrix-01
```

This takes approximately six minutes or longer, including building and saving results. It runs regression tests first, checks trace hashes against the manifest, and saves individual runs, logs, source hashes, `matrix.json` and `REPORT.md`. Failed rates are retained and the remaining rates still run. The workload `.gitattributes` rule preserves LF line endings because byte hashes would change under automatic CRLF conversion. Each rate is run once; this is a local functional pilot, not the proposal's three-repeat resource-controlled main experiment.

Requires Go and Python 3. The script uses temporary local ports, saves gateway logs and the generated endpoint configuration, and terminates its own gateway processes on completion/failure. Use a fresh output directory each time. `--trace`, `--clients` and `--duration` can select another workload. This checks functional integration, not controlled EC2 performance. The current Star implementation synchronously writes peers in its hub loop (with a timeout); a slow peer may still block local processing. Separate peer queues/resource instrumentation remain necessary before interpreting saturation experiments.

Copy `cmd/loadgen/gateways.example.json` and replace the four URLs with the client-facing WebSocket endpoints of your gateway deployment. The example uses four local ports; gateways may instead be on separate hosts with the same port. The file must contain exactly G0, G1, G2 and G3, each with a distinct ws/wss endpoint. `-url` and `-gateways` are mutually exclusive; omitting both retains the localhost Single default.

```powershell
go run ./cmd/loadgen -gateways cmd/loadgen/gateways.example.json -clients workloads/generated/pilot-v1/clients.json -trace workloads/generated/pilot-v1/rate-10.jsonl -duration 60s -drain 10s -output results/four-gateway-10-01
```

Each client connects to its `gateway_id` in `clients.json`; the 200-client bundle gives 50 connections to each gateway. Trace `source_gateway` must agree with the sender's assignment. Publications use that sender's connection. Every expected recipient remains counted across all gateways. Endpoint query parameters are retained, except `name`, which is set to the client ID. Invalid/missing gateway entries, unknown client gateways, unequal client allocation and duplicate endpoints (ignoring `name`) fail before networking starts.

The server deployment must already implement Star/Tree/Direct forwarding and preserve the JSON envelope. Starting four independent chat examples does **not** implement a distributed broadcast system: only local recipients will receive messages. The generator neither forwards messages between gateways nor chooses the server topology. Use the same trace/client mapping for all three architectures, changing deployment configuration rather than workload data.

Results include `mode`, the resolved `endpoints`, and `gateway_stats`: connection counts, scheduled/successful/failed publications, expected/unique/missing/duplicate deliveries and disconnect counts. Sending statistics are attributed to the source gateway; delivery statistics to the recipient gateway. Single mode aggregates under `single`. `unexpected_reasons` and `disconnect_errors` help distinguish protocol mismatch from connection failure. Setup failure records identify the failed client and gateway and do not produce a measurement summary.

## Behavior

- Validates client IDs, recipient sets, message IDs, source mapping, UTF-8 byte counts and ordered timestamps before connecting. Duration must match the intended trace measurement window; timestamps outside it are rejected.
- Establishes every client, then allows a configurable settling period (`-settle 1s`). Each reader stays active to receive broadcasts and service WebSocket ping/pong.
- Replays using a common monotonic clock. A central scheduler enqueues independently of delivery; one writer per client serializes socket writes. Per-client queues are bounded (`-queue 256`). Queue overflow, expired measurement windows and socket errors become failed/unsent records, not hidden rate reductions.
- Sends JSON containing `run_id`, `message_id`, `sender_id`, `payload`. The server should broadcast it unchanged. Single connects all clients to `-url`; four-gateway mode resolves connections using `-gateways` and the fixed client mapping.
- Receives through the fixed drain deadline. All expected recipients remain in the denominator after disconnects. Only first valid delivery contributes to latency/throughput; subsequent copies are counted separately. Wrong run/unknown ID, invalid payloads and unexpected recipients are counted as unexpected.
- Writes `sends.json` (scheduled, actual write start/end, error), `receipts.jsonl` (first receipt per pair and copy count), and `summary.json` (counts, rates, latency percentiles, sender lag, checksums and settings). Existing output directories are never overwritten. Connection setup failures leave `failure.json`.

## Metric definitions and limits

On-time means first receipt no later than one second after **scheduled** publication. Missing means not received by measurement plus drain, not proof of permanent loss. Scheduled latency is receipt minus scheduled publication; actual-send latency is receipt minus write start, not server ingress time. Write completion is not acknowledgement of server delivery. Percentiles exclude missing pairs; always inspect delivery fraction alongside latency. Empty latency samples report zero with `latency_sample_count=0` (not a measured zero latency).

Unique delivery throughput counts receipts within measurement only, divided by duration; drain receipts are separately reported. Successful write rate counts writes completed successfully during the publication phase. `transport_clean` only means no failed sends, unexpected frames or early disconnects: it is **not** a sustainability certification. Inspect lag and generator CPU/network independently.

This first version provides connection settling, not low-load message warm-up. It retains first-receipt state in memory, proportional to expected delivered pairs, then writes buffered logs after measurement. The highest pilot trace has about 1.94 million expected pairs: monitor memory/CPU before interpreting capacity, and use a smaller trace for debugging. It does not measure server resources, apply bandwidth limits, reconnect failed clients, or implement multi-host clock synchronization. Those belong to the experiment runner/environment. The generator should have sufficient capacity; shape server traffic according to Experiment A/B.

## Saturation visualization

`plot_saturation.py` reads existing load-generator `summary.json` files without
changing the experiment. It accepts a five-rate matrix directory, a `run`
directory, or an individual loadgen output directory. Repeat `--series` to
combine topologies, bandwidth budgets, and independent repetitions:

```powershell
python cmd/loadgen/plot_saturation.py `
  --series "star|unlimited|results/star-matrix-01" `
  --output results/saturation-star-unlimited
```

For a shaped Docker run, use the configured aggregate overlay egress budget as
the middle field. Supplying `--message-bytes` enables the proposal-model overlay
demand estimate. The value is `S`, the serialized overlay message size in bytes;
do not silently substitute the chat payload size.

```powershell
python cmd/loadgen/plot_saturation.py `
  --series "star|0.5mbit|results/docker-star-500k-matrix-01" `
  --message-bytes 300 `
  --output results/saturation-star-500k-01
```

Multiple single-rate output directories can be supplied separately; runs with
the same topology, budget and rate are treated as repetitions and their plotted
error bars show the min-max range:

```powershell
python cmd/loadgen/plot_saturation.py `
  --series "star|0.5mbit|results/docker-star-500k-rate10-01" `
  --series "star|0.5mbit|results/docker-star-500k-rate20-01" `
  --series "star|0.5mbit|results/docker-star-500k-rate40-01" `
  --series "star|0.5mbit|results/docker-star-500k-rate80-01" `
  --series "star|0.5mbit|results/docker-star-500k-rate160-01" `
  --message-bytes 300 `
  --output results/saturation-star-500k-01
```

The new output directory contains a standalone `index.html`, one observed SVG
figure, an optional analytical overlay SVG, CSV data, and an auditable JSON
summary. The observed state is deliberately conservative:

- `delivery-limited`: delivery or send success is below 99.9%, or the run has a
  disconnect or unexpected message.
- `pressure`: at least 99% delivery is preserved, but on-time delivery is below
  99%, scheduled P99 exceeds one second, or send-lag P95 exceeds 50 ms.
- `clean`: none of those signals occurred. This does not prove unused capacity.

The overlay figure implements the proposal formula using the pilot manifest and
labels the result as estimated. A budget passed on the command line is metadata;
the report does not claim that Docker/`tc` applied it. Check the shaper counters
independently before interpreting a colored utilization cell as a real network
limit. For the final experiment, combine all three topologies and at least three
repetitions per rate in one report.

Inside `index.html`, the Overlay budget model is editable rather than a fixed
image. Change its title, serialized message size `S`, series labels, or per-node
budget in Mbps and the cells update immediately. A blank budget means
unlimited. **Download edited SVG** exports the current edited view; the original
`overlay_model.svg` remains as the reproducible command-line output.

## Serialized overlay size and demand

`overlaycalc` reconstructs the bytes emitted by the current protocol using the
same Go `encoding/json` behavior as the load generator and gateways. It reports
payload, load-generator ingress JSON, gateway-local JSON and forwarded overlay
JSON separately. Its `overlay_envelope_json_bytes.mean_bytes` is the proposal's
`S`. It then walks every event's actual `source_gateway` through Direct, Star and
Tree and calculates each sending gateway's average overlay Mbps.

```powershell
go run ./cmd/overlaycalc `
  -manifest workloads/generated/pilot-v1/manifest.json `
  -output results/overlay-demand-pilot-v1-01
```

The default representative run ID has the same byte length as the load
generator's normal `run-<UnixNano>` value. For byte-exact accounting of a saved
run, pass its real ID:

```powershell
$runId = (Get-Content results/docker-star-rate10-01/summary.json -Raw |
  ConvertFrom-Json).run_id
go run ./cmd/overlaycalc -run-id $runId `
  -output results/overlay-demand-pilot-v1-actual-run-id-01
```

The output contains `overlay-demand.json` for audit and
`overlay-demand.csv` for plotting. `average_json_mbps` is the application-level
value corresponding to the proposal formula. `average_websocket_mbps` also adds
the data-frame header and client mask where applicable. Neither includes
TCP/IP headers, ACKs, retransmissions, connection traffic, ping/pong, or TLS;
use the shaper's `tc -s qdisc` counters for observed interface bytes.

Validation:

```powershell
go test ./cmd/loadgen ./cmd/overlaycalc ./examples/chat
python cmd/loadgen/test_plot_saturation.py
```

Integration tests start four separate real WebSocket listeners with a simulated broadcast fabric. They verify all four sending origins, client allocation, preserved URL query parameters, normal/duplicate/local-only delivery, gateway disconnection, configuration rejection and setup failure diagnostics. They do not certify a production Star/Tree/Direct implementation or EC2 network conditions.
