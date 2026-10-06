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
