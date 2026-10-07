# Workload replay client (Single and four gateways)

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

Validation:

```powershell
go test ./cmd/loadgen ./examples/chat
```

Integration tests start four separate real WebSocket listeners with a simulated broadcast fabric. They verify all four sending origins, client allocation, preserved URL query parameters, normal/duplicate/local-only delivery, gateway disconnection, configuration rejection and setup failure diagnostics. They do not certify a production Star/Tree/Direct implementation or EC2 network conditions.
