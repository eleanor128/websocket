package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func fourFixture(t *testing.T) options {
	t.Helper()
	o := fixture(t)
	o.endpoint = ""
	o.gateways = filepath.Join(filepath.Dir(o.clients), "gateways.json")
	o.duration = 300 * time.Millisecond
	o.drain = 100 * time.Millisecond
	o.settle = 100 * time.Millisecond
	b := Bundle{Sets: map[string][]string{}}
	ids := []string{}
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("c%d", i)
		ids = append(ids, id)
		b.Clients = append(b.Clients, Client{ID: id, Gateway: fmt.Sprintf("G%d", i%4)})
	}
	b.Sets["all"] = ids
	if err := save(o.clients, b); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(o.trace)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for i := 0; i < 4; i++ {
		if err := enc.Encode(Event{ID: fmt.Sprintf("m%d", i), Sender: fmt.Sprintf("c%d", i), Gateway: fmt.Sprintf("G%d", i), Payload: "你好\n", Bytes: 7, Set: "all", Offset: int64(i) * int64(20*time.Millisecond)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return o
}

// Four actual WebSocket listeners with a simulated broadcast fabric. This tests
// generator routing/accounting, not the project's production overlay topology.
func TestFourGatewayReplay(t *testing.T) {
	for _, mode := range []string{"broadcast", "duplicate", "isolated", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			o := fourFixture(t)
			var mu sync.Mutex
			type peer struct {
				gateway int
				conn    *websocket.Conn
			}
			peers := map[string]peer{}
			origins := map[string]int{}
			badQuery := false
			endpoints := map[string]string{}
			for g := 0; g < 4; g++ {
				gateway := g
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					up := websocket.Upgrader{}
					c, err := up.Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer c.Close()
					id := r.URL.Query().Get("name")
					mu.Lock()
					peers[id] = peer{gateway, c}
					if r.URL.Query().Get("room") != "test" {
						badQuery = true
					}
					mu.Unlock()
					for {
						_, data, err := c.ReadMessage()
						if err != nil {
							return
						}
						var message Wire
						if json.Unmarshal(data, &message) != nil {
							return
						}
						mu.Lock()
						origins[message.ID] = gateway
						for _, p := range peers {
							if mode == "isolated" && p.gateway != gateway {
								continue
							}
							if mode == "disconnect" && p.gateway == 3 {
								p.conn.Close()
								continue
							}
							p.conn.SetWriteDeadline(time.Now().Add(time.Second))
							p.conn.WriteMessage(websocket.TextMessage, data)
							if mode == "duplicate" && p.gateway == 2 {
								p.conn.WriteMessage(websocket.TextMessage, data)
							}
						}
						mu.Unlock()
					}
				}))
				defer server.Close()
				endpoints[fmt.Sprintf("G%d", gateway)] = "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?room=test"
			}
			if err := save(o.gateways, endpoints); err != nil {
				t.Fatal(err)
			}
			if err := run(o); err != nil {
				t.Fatal(err)
			}
			var result struct {
				Mode        string                  `json:"mode"`
				Expected    int                     `json:"expected_deliveries"`
				Unique      int                     `json:"unique_deliveries"`
				Missing     int                     `json:"missing"`
				Duplicates  int                     `json:"duplicates"`
				Disconnects int                     `json:"disconnects"`
				Gateways    map[string]GatewayStats `json:"gateway_stats"`
			}
			if err := readJSON(filepath.Join(o.output, "summary.json"), &result); err != nil {
				t.Fatal(err)
			}
			if result.Mode != "four-gateway" || result.Expected != 32 || result.Unique+result.Missing != 32 {
				t.Fatalf("bad summary: %+v", result)
			}
			mu.Lock()
			defer mu.Unlock()
			if badQuery {
				t.Fatal("lost endpoint query parameters")
			}
			for i := 0; i < 8; i++ {
				if peers[fmt.Sprintf("c%d", i)].gateway != i%4 {
					t.Fatal("client connected to wrong gateway")
				}
			}
			if mode != "disconnect" {
				for i := 0; i < 4; i++ {
					g, ok := origins[fmt.Sprintf("m%d", i)]
					if !ok || g != i {
						t.Fatal("publication originated at wrong gateway")
					}
				}
			}
			switch mode {
			case "broadcast":
				if result.Unique != 32 || result.Duplicates != 0 {
					t.Fatal(result)
				}
			case "duplicate":
				if result.Unique != 32 || result.Duplicates != 8 {
					t.Fatal(result)
				}
			case "isolated":
				if result.Unique != 8 || result.Missing != 24 {
					t.Fatal(result)
				}
			case "disconnect":
				if result.Disconnects != 2 || result.Gateways["G3"].Missing != 8 {
					t.Fatal(result)
				}
			}
			for _, s := range result.Gateways {
				if s.Clients != 2 || s.Expected != 8 || s.Expected != s.Unique+s.Missing {
					t.Fatal(s)
				}
			}
		})
	}
}

func TestGatewayConfiguration(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "extra", "invalid_scheme", "duplicate", "unknown_client_gateway", "unequal_clients", "both_modes"} {
		t.Run(kind, func(t *testing.T) {
			o := fourFixture(t)
			endpoints := map[string]string{}
			for i := 0; i < 4; i++ {
				endpoints[fmt.Sprintf("G%d", i)] = fmt.Sprintf("ws://localhost:%d/ws", 9000+i)
			}
			var b Bundle
			readJSON(o.clients, &b)
			switch kind {
			case "missing":
				delete(endpoints, "G3")
			case "extra":
				endpoints["G4"] = "ws://localhost:9004/ws"
			case "invalid_scheme":
				endpoints["G2"] = "http://localhost:9002/ws"
			case "duplicate":
				endpoints["G2"] = endpoints["G1"] + "?name=ignored"
			case "unknown_client_gateway":
				b.Clients[0].Gateway = "G4"
			case "unequal_clients":
				b.Clients[0].Gateway = "G1"
			case "both_modes":
				o.endpoint = "ws://localhost:8080/ws"
			}
			save(o.gateways, endpoints)
			_, _, _, err := destinations(o, b)
			if (err == nil) != (kind == "valid") {
				t.Fatalf("%s: %v", kind, err)
			}
		})
	}
}

func TestGatewayConnectionFailure(t *testing.T) {
	o := fourFixture(t)
	reject := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer reject.Close()
	endpoints := map[string]string{}
	for i := 0; i < 4; i++ {
		endpoints[fmt.Sprintf("G%d", i)] = "ws" + strings.TrimPrefix(reject.URL, "http") + fmt.Sprintf("/g%d", i)
	}
	save(o.gateways, endpoints)
	if err := run(o); err == nil {
		t.Fatal("setup failure was ignored")
	}
	var failure map[string]any
	if err := readJSON(filepath.Join(o.output, "failure.json"), &failure); err != nil {
		t.Fatal(err)
	}
	if failure["gateway_id"] != "G0" || failure["mode"] != "four-gateway" {
		t.Fatal(failure)
	}
	if _, err := os.Stat(filepath.Join(o.output, "summary.json")); !os.IsNotExist(err) {
		t.Fatal("failed setup produced measurement summary")
	}
}
