# Direct forwarding: implementation and verification

Four gateways form a complete graph: G0-G1, G0-G2, G0-G3, G1-G2, G1-G3, G2-G3. Each pair has one persistent, bidirectional WebSocket connection. The lower ID initiates the connection to the higher ID. This convention prevents duplicate physical connections; messages flow in both directions regardless of which side dialed.

## Routing contract

- Client ingress stamps its actual source gateway and clears `from_gateway`; workload run ID, message ID, sender and payload are preserved.
- A local publication is delivered locally and sent once to each of the three peers.
- A peer publication is delivered locally only; it is never forwarded again.
- Peer messages must declare both source and previous gateway equal to their actual registered peer. Invalid or relayed messages close that peer connection and increment `rejected_peer_messages`.
- The current application is one broadcast room spanning four gateways. It does not implement multiple isolated room IDs.
- No retry/replay of business messages, global multi-sender order, or exactly-once guarantee across failures is claimed. A peer disconnect marks the affected gateway unready; automatic post-disconnect reconnection is not implemented.

## Start a local browser demo

Open four terminals in `examples/chat`. Start all four within 30 seconds (startup dialing retries for that duration):

```powershell
go run . -topology direct -id G0 -addr :8081 -direct-peers direct-peers.example.json
go run . -topology direct -id G1 -addr :8082 -direct-peers direct-peers.example.json
go run . -topology direct -id G2 -addr :8083 -direct-peers direct-peers.example.json
go run . -topology direct -id G3 -addr :8084 -direct-peers direct-peers.example.json
```

Use one command per terminal. Do not supply the Star `-peers` flag. `direct-peers.example.json` contains all four overlay URLs ending in `/inter-gw`. Browser clients instead connect through `/ws`. This initial implementation uses one listening port per gateway for both paths, as Star does; network classification and separate interfaces are later experiment work.

Visit `http://localhost:8081/` through `http://localhost:8084/` in separate windows. Send from any window; all four should receive one copy. Do not use `-benchmark` for this plain-text browser demo. The existing browser page displays sender and source gateway; terminal logs show the three direct destinations.

Before connecting clients, inspect `http://localhost:8081/healthz` and the other three ports. Each must show `topology: direct`, `ready: true`, and `peer_ids` listing exactly the other three gateways. An unready Direct gateway rejects new `/ws` connections with HTTP 503. A startup failure requires correcting configuration/restarting the affected process; readiness is not inferred merely from a running listener.

On multiple EC2 hosts, change overlay addresses to reachable private addresses and client addresses to addresses reachable from the generator/browser. IDs in the handshake detect accidental wrong endpoints/topologies; they are not cryptographic authentication.

## Run existing load generator

Restart all four gateways with the same commands plus `-benchmark`, then run from the repository root:

```powershell
go run ./cmd/loadgen -gateways cmd/loadgen/gateways.example.json -trace workloads/generated/pilot-v1/rate-10.jsonl -output results/direct-manual-01
```

The existing client endpoint example matches ports 8081-8084. Each gateway gets 50 of the 200 clients. No generator protocol change is required. Keep all gateway modes consistent; benchmark mode expects the existing JSON workload envelope.

## Automated verification

```powershell
go test ./examples/chat ./cmd/loadgen -count=3
go vet ./examples/chat ./cmd/loadgen
python cmd/loadgen/test_direct.py --output results/direct-check-01
python cmd/loadgen/test_direct.py --all --output results/direct-matrix-01
```

Python 3 and Go are required. The runner builds binaries, starts four independent processes on free local ports, waits for exact mesh membership, runs the existing generator and cleans up its own processes. Every rate gets a new deployment. Outputs include gateway logs, client summary/raw records, before/after health snapshots, per-run verification, source hashes and a matrix. Never overwrite an existing output directory.

Tests cover local/remote routing, invalid configurations, self/unknown/reverse/duplicate edges, wrong destinations, refusal of unready clients, spoofed/relayed peer messages, four concurrent senders, browser and benchmark modes, Unicode/newlines/long payloads, correct source metadata, no extra deliveries, and disconnect readiness. Star regressions run alongside these tests.

For M total publications, M_i originating at gateway i, N clients and K=4:

- Client expected deliveries: M*N.
- Each gateway sends exactly 3*M_i business overlay messages.
- Each gateway receives M-M_i business overlay messages.
- For each other peer, source i sends exactly M_i copies.
- Total business overlay sends: 3*M; there are six physical connections.

`healthz.metrics` exposes atomic process-lifetime counters for these checks. `overlay_sent` counts successful application writes, not TCP packets/ACKs or proof of receipt. Paired receive counters and client delivery records supply the other evidence. Counters are sampled after drain in the runner; they are not continuously collected time series.

## Scope and limits

The shared chat writer was corrected to keep one JSON envelope per WebSocket message in every mode; concurrent browser traffic previously coalesced multiple envelopes and some JSON clients read only the first. Local-client queues remain bounded and Star/Direct retain the same synchronous peer-writing implementation with deadlines. Slow peers can still block a gateway's broadcast loop. This work verifies topology and functional delivery, not bandwidth/CPU calibration or sustainable capacity. Real process tests use one host without resource shaping. Keep demonstration clients disconnected during measured runs because they change recipient counts and resource use.
