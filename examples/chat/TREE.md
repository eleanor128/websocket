# Tree forwarding: implementation and verification

Four gateways form a broadcast tree with three undirected, bidirectional edges:
- **G0 <--> G1**
- **G0 <--> G2**
- **G1 <--> G3**

```
      G0
     /  \
   G1    G2
   |
   G3
```

Peer connection convention:
- For each edge $(u, v)$ where $u < v$, gateway $u$ initiates the WebSocket connection to gateway $v$.
  - G0 dials G1
  - G0 dials G2
  - G1 dials G3
- Exactly 3 persistent, full-duplex WebSocket connections span the cluster.

## Routing contract

- Client ingress stamps `SourceGateway = currentGatewayID` and initializes `FromGateway = ""`.
- When forwarding along tree edges, each hop updates `FromGateway = currentGatewayID` while preserving original `SourceGateway`, `MessageID`, `RunID`, and payload.
- Forwarding rule:
  - Deliver locally to all connected clients on this gateway.
  - Forward along all adjacent tree edges **except** the edge the message arrived from (`FromGateway`) and the original source (`SourceGateway`).
  - Leaves (G2 and G3) have only 1 neighbor; once a message arrives from that neighbor, it is delivered locally and never forwarded further.
  - Intermediate nodes (G0 and G1) have 2 neighbors; they forward incoming messages across to their other neighbor.
- A single published message traverses the tree without loops or duplicate deliveries.
- Across $M$ total publications ($m_i$ originating at gateway $i$):
  - Expected client deliveries: $M \times N$ (where $N$ is total client count).
  - Total overlay transmissions across cluster: $3 \times M$.
  - Per-gateway overlay sent:
    - G0: $m_0 + M$
    - G1: $m_1 + M$
    - G2: $m_2$
    - G3: $m_3$
  - Per-gateway overlay received:
    - Each gateway receives $M - m_i$ messages.

## Start a local browser demo

Open four terminals in `examples/chat`:

```powershell
go run . -topology tree -id G0 -addr :8081 -tree-peers tree-peers.example.json
go run . -topology tree -id G1 -addr :8082 -tree-peers tree-peers.example.json
go run . -topology tree -id G2 -addr :8083 -tree-peers tree-peers.example.json
go run . -topology tree -id G3 -addr :8084 -tree-peers tree-peers.example.json
```

Visit `http://localhost:8081/` through `http://localhost:8084/`. Send a message from any browser; all four windows will receive exactly one copy. Inspect `http://localhost:8081/healthz` to confirm `topology: tree`, `ready: true`, and correct peer IDs.

## Automated verification

```powershell
go test ./examples/chat ./cmd/loadgen -v
python cmd/loadgen/test_tree.py --output results/tree-check-01
python cmd/loadgen/test_tree.py --all --output results/tree-matrix-01
```
