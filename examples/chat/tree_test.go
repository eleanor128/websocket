package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestTreeRouting(t *testing.T) {
	// 1. Local publication routing (FromGateway == "")
	expectedLocalCounts := map[string]int{
		"G0": 2, // G1, G2
		"G1": 2, // G0, G3
		"G2": 1, // G0
		"G3": 1, // G1
	}
	for _, source := range []string{"G0", "G1", "G2", "G3"} {
		strategy := NewTreeTopology(source)
		count := 0
		for _, target := range []string{"G0", "G1", "G2", "G3", "G4", ""} {
			got := strategy.ShouldForward(target, Message{SourceGateway: source})
			expected := treeEdge(source, target) && target != source && validGatewayID(target)
			if got != expected {
				t.Fatalf("local route %s->%s mismatch: got %v, expected %v", source, target, got, expected)
			}
			if got {
				count++
			}
		}
		if count != expectedLocalCounts[source] {
			t.Fatalf("%s local route count mismatch: got %d, expected %d", source, count, expectedLocalCounts[source])
		}
	}

	// 2. Multi-hop forwarding tests along tree edges
	// Case A: Message from G3 (G3 -> G1 -> G0 -> G2)
	g1Strategy := NewTreeTopology("G1")
	// G1 receives from G3
	if !g1Strategy.ShouldForward("G0", Message{SourceGateway: "G3", FromGateway: "G3"}) {
		t.Fatal("G1 failed to forward G3 message to G0")
	}
	if g1Strategy.ShouldForward("G3", Message{SourceGateway: "G3", FromGateway: "G3"}) {
		t.Fatal("G1 forwarded back to G3")
	}
	if g1Strategy.ShouldForward("G2", Message{SourceGateway: "G3", FromGateway: "G3"}) {
		t.Fatal("G1 forwarded to non-edge G2")
	}

	g0Strategy := NewTreeTopology("G0")
	// G0 receives from G1 (originally from G3)
	if !g0Strategy.ShouldForward("G2", Message{SourceGateway: "G3", FromGateway: "G1"}) {
		t.Fatal("G0 failed to forward G3 message to G2")
	}
	if g0Strategy.ShouldForward("G1", Message{SourceGateway: "G3", FromGateway: "G1"}) {
		t.Fatal("G0 forwarded back to G1")
	}

	g2Strategy := NewTreeTopology("G2")
	// G2 receives from G0 (originally from G3) -> Leaf node, should not forward to anyone
	for _, target := range []string{"G0", "G1", "G2", "G3"} {
		if g2Strategy.ShouldForward(target, Message{SourceGateway: "G3", FromGateway: "G0"}) {
			t.Fatalf("G2 leaf should not forward to %s", target)
		}
	}

	// Case B: Message from G2 (G2 -> G0 -> G1 -> G3)
	// G0 receives from G2
	if !g0Strategy.ShouldForward("G1", Message{SourceGateway: "G2", FromGateway: "G2"}) {
		t.Fatal("G0 failed to forward G2 message to G1")
	}
	if g0Strategy.ShouldForward("G2", Message{SourceGateway: "G2", FromGateway: "G2"}) {
		t.Fatal("G0 forwarded back to G2")
	}
	// G1 receives from G0 (originally from G2)
	if !g1Strategy.ShouldForward("G3", Message{SourceGateway: "G2", FromGateway: "G0"}) {
		t.Fatal("G1 failed to forward G2 message to G3")
	}
	if g1Strategy.ShouldForward("G0", Message{SourceGateway: "G2", FromGateway: "G0"}) {
		t.Fatal("G1 forwarded back to G0")
	}
	g3Strategy := NewTreeTopology("G3")
	// G3 receives from G1 (originally from G2) -> Leaf node
	for _, target := range []string{"G0", "G1", "G2", "G3"} {
		if g3Strategy.ShouldForward(target, Message{SourceGateway: "G2", FromGateway: "G1"}) {
			t.Fatalf("G3 leaf should not forward to %s", target)
		}
	}
}

func TestTreeConfiguration(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "duplicate", "scheme", "wrong-path", "query", "unknown-id"} {
		t.Run(kind, func(t *testing.T) {
			config := map[string]string{}
			for i := 0; i < 4; i++ {
				config[fmt.Sprintf("G%d", i)] = fmt.Sprintf("ws://localhost:%d/inter-gw", 8081+i)
			}
			id := "G0"
			switch kind {
			case "missing":
				delete(config, "G3")
			case "duplicate":
				config["G3"] = config["G2"]
			case "scheme":
				config["G1"] = "http://localhost/inter-gw"
			case "wrong-path":
				config["G1"] = "ws://localhost/ws"
			case "query":
				config["G1"] += "?from=G3"
			case "unknown-id":
				id = "G4"
			}
			data, _ := json.Marshal(config)
			path := filepath.Join(t.TempDir(), "peers.json")
			os.WriteFile(path, data, 0644)
			_, err := loadTreeConfig(path, id)
			if (err == nil) != (kind == "valid") {
				t.Fatal(kind, err)
			}
		})
	}
}

func TestTreeHandshakeAndReadiness(t *testing.T) {
	h := newHub("G1", "tree")
	server := httptest.NewServer(chatHandler(h))
	defer server.Close()
	base := "ws" + strings.TrimPrefix(server.URL, "http")

	// Invalid handshakes
	for _, query := range []string{
		"from=G1&to=G1&topology=tree", // self
		"from=G2&to=G1&topology=tree", // non-edge (G2-G1 is not an edge)
		"from=G3&to=G1&topology=tree", // reverse dial (G3 > G1, must dial from lower to higher)
		"from=G0&to=G2&topology=tree", // wrong target (to=G2 but dialed G1)
		"from=G0&to=G1&topology=star", // wrong topology
	} {
		c, r, e := websocket.DefaultDialer.Dial(base+"/inter-gw?"+query, nil)
		if e == nil {
			c.Close()
			t.Fatal("illegal connection accepted", query)
		}
		if r == nil || r.StatusCode != 403 {
			t.Fatal(r, e)
		}
		r.Body.Close()
	}

	// Unready Tree rejects /ws with 503
	c, r, e := websocket.DefaultDialer.Dial(base+"/ws?name=a", nil)
	if e == nil {
		c.Close()
		t.Fatal("unready Tree accepted client")
	}
	if r == nil || r.StatusCode != 503 {
		t.Fatal(r, e)
	}
	r.Body.Close()

	// Legal dial: G0 dials G1
	source := newHub("G0", "tree")
	if err := connectTree(source, "G1", base+"/inter-gw"); err != nil {
		t.Fatal("failed valid connectTree G0->G1", err)
	}
	source.peerLock.RLock()
	peer := source.peerGateway["G1"]
	source.peerLock.RUnlock()
	defer peer.Close()

	// Duplicate dial rejected
	if err := connectTree(source, "G1", base+"/inter-gw"); err == nil {
		t.Fatal("duplicate dial accepted")
	}
	dup, r, e := websocket.DefaultDialer.Dial(base+"/inter-gw?from=G0&to=G1&topology=tree", nil)
	if e == nil {
		dup.Close()
		t.Fatal("duplicate inbound accepted")
	}
	if r == nil || r.StatusCode != 409 {
		t.Fatal(r, e)
	}
	r.Body.Close()

	// Spoofed message where FromGateway != peerID must be rejected
	if err := peer.WriteJSON(Message{MessageID: "bad", SenderID: "a", SourceGateway: "G0", FromGateway: "G2"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return h.rejectedPeerMessages.Load() == 1 })
}

func TestTreeFourGatewayBroadcast(t *testing.T) {
	for _, structured := range []bool{false, true} {
		t.Run(fmt.Sprintf("benchmark=%t", structured), func(t *testing.T) {
			old := *benchmark
			*benchmark = structured
			defer func() { *benchmark = old }()

			hubs := make([]*Hub, 4)
			servers := make([]*httptest.Server, 4)
			for i := range hubs {
				hubs[i] = newHub(fmt.Sprintf("G%d", i), "tree")
				go hubs[i].run()
				servers[i] = httptest.NewServer(chatHandler(hubs[i]))
				defer servers[i].Close()
			}

			// Tree edges: (G0, G1), (G0, G2), (G1, G3). Lower ID dials higher ID.
			treeDials := [][2]int{
				{0, 1}, // G0 dials G1
				{0, 2}, // G0 dials G2
				{1, 3}, // G1 dials G3
			}
			for _, edge := range treeDials {
				a, b := edge[0], edge[1]
				id := fmt.Sprintf("G%d", b)
				targetURL := "ws" + strings.TrimPrefix(servers[b].URL, "http") + "/inter-gw"
				if err := connectTree(hubs[a], id, targetURL); err != nil {
					t.Fatalf("failed to connect tree edge %d->%d: %v", a, b, err)
				}
			}

			// Verify all 4 gateways reach ready state with exact expected peers
			expectedPeersMap := map[string][]string{
				"G0": {"G1", "G2"},
				"G1": {"G0", "G3"},
				"G2": {"G0"},
				"G3": {"G1"},
			}
			for i, h := range hubs {
				gid := fmt.Sprintf("G%d", i)
				want := expectedPeersMap[gid]
				eventually(t, func() bool {
					h.peerLock.RLock()
					defer h.peerLock.RUnlock()
					ids, ready := peerStatus(h)
					if !ready || len(ids) != len(want) {
						return false
					}
					for idx, p := range want {
						if ids[idx] != p {
							return false
						}
					}
					return true
				})
			}

			// Connect 2 clients to each gateway (8 clients total)
			clients := make([]*websocket.Conn, 8)
			for i := range clients {
				c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(servers[i/2].URL, "http")+fmt.Sprintf("/ws?name=c%d", i), nil)
				if err != nil {
					t.Fatalf("failed to connect client c%d: %v", i, err)
				}
				clients[i] = c
				defer c.Close()
			}

			// Prepare payloads for 1 publication from each gateway
			payloads := map[string]string{}
			for i := 0; i < 4; i++ {
				s := fmt.Sprintf("G%d", i)
				payloads[s] = "  " + s + " tree msg\n<data>  "
				if structured {
					payloads[s] += strings.Repeat("y", 1500)
				}
			}

			// Publish 1 message from each gateway concurrently
			var wg sync.WaitGroup
			errors := make(chan error, 4)
			for g := 0; g < 4; g++ {
				g := g
				wg.Add(1)
				go func() {
					defer wg.Done()
					var err error
					source := fmt.Sprintf("G%d", g)
					if structured {
						err = clients[2*g].WriteJSON(Message{
							RunID:         "tree-test",
							MessageID:     fmt.Sprintf("m%d", g),
							SenderID:      fmt.Sprintf("c%d", 2*g),
							Payload:       payloads[source],
							SourceGateway: "spoofed",
							FromGateway:   "spoofed",
						})
					} else {
						err = clients[2*g].WriteMessage(websocket.TextMessage, []byte(payloads[source]))
					}
					if err != nil {
						errors <- err
					}
				}()
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				t.Fatal(err)
			}

			// Every client across all 4 gateways must receive all 4 messages
			for i, c := range clients {
				seen := map[string]bool{}
				for n := 0; n < 4; n++ {
					c.SetReadDeadline(time.Now().Add(3 * time.Second))
					var got Message
					if err := c.ReadJSON(&got); err != nil {
						t.Fatalf("client c%d failed to receive message #%d: %v", i, n, err)
					}
					if !validGatewayID(got.SourceGateway) || seen[got.SourceGateway] || got.Payload != payloads[got.SourceGateway] {
						t.Fatalf("client c%d bad delivery: %+v", i, got)
					}
					seen[got.SourceGateway] = true
					if structured && (got.RunID != "tree-test" || got.MessageID != "m"+strings.TrimPrefix(got.SourceGateway, "G")) {
						t.Fatalf("client c%d identity mismatch: %+v", i, got)
					}
				}
			}

			// Verify overlay metrics for Tree:
			// M = 4, m0 = 1, m1 = 1, m2 = 1, m3 = 1
			// G0: sent = m0 + M = 5, recv = M - m0 = 3
			// G1: sent = m1 + M = 5, recv = M - m1 = 3
			// G2: sent = m2 = 1,     recv = M - m2 = 3
			// G3: sent = m3 = 1,     recv = M - m3 = 3
			expectedSent := map[string]uint64{"G0": 5, "G1": 5, "G2": 1, "G3": 1}
			expectedRecv := map[string]uint64{"G0": 3, "G1": 3, "G2": 3, "G3": 3}
			for i, h := range hubs {
				gid := fmt.Sprintf("G%d", i)
				wantSent := expectedSent[gid]
				wantRecv := expectedRecv[gid]
				eventually(t, func() bool {
					return h.localPublications.Load() == 1 &&
						h.overlaySent.Load() == wantSent &&
						h.overlayReceived.Load() == wantRecv
				})
			}

			// Check for no duplicates
			for _, c := range clients {
				c.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
				if _, _, err := c.ReadMessage(); err == nil {
					t.Fatal("unexpected duplicate delivery")
				}
			}

			// Disconnect edge G0-G1: G0 and G1 must both become unready
			hubs[0].peerLock.RLock()
			edge := hubs[0].peerGateway["G1"]
			hubs[0].peerLock.RUnlock()
			edge.Close()
			for _, index := range []int{0, 1} {
				h := hubs[index]
				eventually(t, func() bool {
					h.peerLock.RLock()
					defer h.peerLock.RUnlock()
					_, ready := peerStatus(h)
					return !ready
				})
			}
		})
	}
}
