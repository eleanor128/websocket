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

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestDirectRouting(t *testing.T) {
	for _, source := range []string{"G0", "G1", "G2", "G3"} {
		strategy := NewDirectTopology(source)
		count := 0
		for _, target := range []string{"G0", "G1", "G2", "G3", "G4", ""} {
			got := strategy.ShouldForward(target, Message{SourceGateway: source})
			if got != (validGatewayID(target) && target != source) {
				t.Fatalf("bad local route %s->%s", source, target)
			}
			if got {
				count++
			}
			for _, from := range []string{"G0", "G1", "G2", "G3"} {
				if strategy.ShouldForward(target, Message{SourceGateway: source, FromGateway: from}) {
					t.Fatal("remote message reforwarded")
				}
			}
			if strategy.ShouldForward(target, Message{SourceGateway: "spoofed"}) {
				t.Fatal("wrong source accepted")
			}
		}
		if count != 3 {
			t.Fatal(count)
		}
	}
}

func TestDirectConfiguration(t *testing.T) {
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
			_, err := loadDirectConfig(path, id)
			if (err == nil) != (kind == "valid") {
				t.Fatal(kind, err)
			}
		})
	}
}

func TestDirectHandshakeAndReadiness(t *testing.T) {
	h := newHub("G2", "direct")
	server := httptest.NewServer(chatHandler(h))
	defer server.Close()
	base := "ws" + strings.TrimPrefix(server.URL, "http")
	for _, query := range []string{"from=G2&to=G2&topology=direct", "from=G3&to=G2&topology=direct", "from=G4&to=G2&topology=direct", "from=G0&to=G1&topology=direct", "from=G0&to=G2&topology=star"} {
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
	c, r, e := websocket.DefaultDialer.Dial(base+"/ws?name=a", nil)
	if e == nil {
		c.Close()
		t.Fatal("unready Direct accepted client")
	}
	if r == nil || r.StatusCode != 503 {
		t.Fatal(r, e)
	}
	r.Body.Close()
	source := newHub("G0", "direct")
	if err := connectDirect(source, "G1", base+"/inter-gw"); err == nil {
		t.Fatal("wrong destination accepted")
	}
	if err := connectDirect(source, "G2", base+"/inter-gw"); err != nil {
		t.Fatal(err)
	}
	source.peerLock.RLock()
	peer := source.peerGateway["G2"]
	source.peerLock.RUnlock()
	defer peer.Close()
	if err := connectDirect(source, "G2", base+"/inter-gw"); err == nil {
		t.Fatal("duplicate dial accepted")
	}
	dup, r, e := websocket.DefaultDialer.Dial(base+"/inter-gw?from=G0&to=G2&topology=direct", nil)
	if e == nil {
		dup.Close()
		t.Fatal("duplicate inbound accepted")
	}
	if r == nil || r.StatusCode != 409 {
		t.Fatal(r, e)
	}
	r.Body.Close()
	// A peer may not relay a third gateway's publication in Direct.
	if err := peer.WriteJSON(Message{MessageID: "bad", SenderID: "a", SourceGateway: "G1", FromGateway: "G0"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return h.rejectedPeerMessages.Load() == 1 })
	eventually(t, func() bool { h.peerLock.RLock(); defer h.peerLock.RUnlock(); return len(h.peerGateway) == 0 })
}

func TestDirectFourGatewayBroadcast(t *testing.T) {
	for _, structured := range []bool{false, true} {
		t.Run(fmt.Sprintf("benchmark=%t", structured), func(t *testing.T) {
			old := *benchmark
			*benchmark = structured
			defer func() { *benchmark = old }()
			hubs := make([]*Hub, 4)
			servers := make([]*httptest.Server, 4)
			for i := range hubs {
				hubs[i] = newHub(fmt.Sprintf("G%d", i), "direct")
				go hubs[i].run()
				servers[i] = httptest.NewServer(chatHandler(hubs[i]))
				defer servers[i].Close()
			}
			for a := 0; a < 4; a++ {
				for b := a + 1; b < 4; b++ {
					id := fmt.Sprintf("G%d", b)
					if err := connectDirect(hubs[a], id, "ws"+strings.TrimPrefix(servers[b].URL, "http")+"/inter-gw"); err != nil {
						t.Fatal(err)
					}
					hubs[a].peerLock.RLock()
					conn := hubs[a].peerGateway[id]
					hubs[a].peerLock.RUnlock()
					defer conn.Close()
				}
			}
			for _, h := range hubs {
				eventually(t, func() bool {
					h.peerLock.RLock()
					defer h.peerLock.RUnlock()
					ids, ready := peerStatus(h)
					return ready && len(ids) == 3
				})
			}
			clients := make([]*websocket.Conn, 8)
			for i := range clients {
				c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(servers[i/2].URL, "http")+fmt.Sprintf("/ws?name=c%d", i), nil)
				if err != nil {
					t.Fatal(err)
				}
				clients[i] = c
				defer c.Close()
			}
			payloads := map[string]string{}
			for i := 0; i < 4; i++ {
				s := fmt.Sprintf("G%d", i)
				payloads[s] = "  " + s + " 中文\n<hello>  "
				if structured {
					payloads[s] += strings.Repeat("x", 2000)
				}
			}
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
						err = clients[2*g].WriteJSON(Message{RunID: "direct-test", MessageID: fmt.Sprintf("m%d", g), SenderID: fmt.Sprintf("c%d", 2*g), Payload: payloads[source], SourceGateway: "spoofed", FromGateway: "spoofed"})
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
			for i, c := range clients {
				seen := map[string]bool{}
				for n := 0; n < 4; n++ {
					c.SetReadDeadline(time.Now().Add(3 * time.Second))
					var got Message
					if err := c.ReadJSON(&got); err != nil {
						t.Fatal(err)
					}
					if !validGatewayID(got.SourceGateway) || seen[got.SourceGateway] || got.Payload != payloads[got.SourceGateway] {
						t.Fatalf("bad delivery %+v", got)
					}
					seen[got.SourceGateway] = true
					if got.SourceGateway == fmt.Sprintf("G%d", i/2) {
						if got.FromGateway != "" {
							t.Fatal("local message took an overlay hop")
						}
					} else if got.FromGateway != got.SourceGateway {
						t.Fatal("remote delivery was relayed")
					}
					if structured && (got.RunID != "direct-test" || got.MessageID != "m"+strings.TrimPrefix(got.SourceGateway, "G")) {
						t.Fatal("workload identity changed")
					}
				}
			}
			for _, h := range hubs {
				eventually(t, func() bool {
					return h.localPublications.Load() == 1 && h.overlaySent.Load() == 3 && h.overlayReceived.Load() == 3
				})
				for id, count := range h.peerSends {
					want := uint64(1)
					if id == h.gatewayID {
						want = 0
					}
					if count.Load() != want {
						t.Fatal("wrong per-peer fanout", id, count.Load())
					}
				}
			}
			for _, c := range clients {
				c.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
				if _, _, err := c.ReadMessage(); err == nil {
					t.Fatal("duplicate delivery")
				}
			}
			// Disconnect one edge: both endpoints must become unready.
			hubs[0].peerLock.RLock()
			edge := hubs[0].peerGateway["G1"]
			hubs[0].peerLock.RUnlock()
			edge.Close()
			for _, index := range []int{0, 1} {
				h := hubs[index]
				eventually(t, func() bool { h.peerLock.RLock(); defer h.peerLock.RUnlock(); _, ready := peerStatus(h); return !ready })
			}
		})
	}
}
