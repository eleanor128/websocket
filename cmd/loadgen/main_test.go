package main

import (
	"encoding/json"
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

func fixture(t *testing.T) options {
	t.Helper()
	dir := t.TempDir()
	b := Bundle{Clients: []Client{{ID: "a", Gateway: "G0"}, {ID: "b", Gateway: "G1"}}, Sets: map[string][]string{"all": {"a", "b"}}}
	if err := save(filepath.Join(dir, "clients.json"), b); err != nil {
		t.Fatal(err)
	}
	ev := Event{ID: "m1", Offset: 0, Sender: "a", Payload: "你好\nhello", Bytes: len([]byte("你好\nhello")), Gateway: "G0", Set: "all"}
	data, _ := json.Marshal(ev)
	os.WriteFile(filepath.Join(dir, "trace.jsonl"), append(data, '\n'), 0644)
	return options{clients: filepath.Join(dir, "clients.json"), trace: filepath.Join(dir, "trace.jsonl"), output: filepath.Join(dir, "result"), duration: 100 * time.Millisecond, drain: 100 * time.Millisecond, settle: 50 * time.Millisecond, deadline: time.Second, queue: 2}
}

func TestReplayAccounting(t *testing.T) {
	for _, mode := range []string{"normal", "duplicate", "missing", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			peers := map[string]*websocket.Conn{}
			up := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := up.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer c.Close()
				mu.Lock()
				peers[r.URL.Query().Get("name")] = c
				mu.Unlock()
				for {
					_, data, err := c.ReadMessage()
					if err != nil {
						return
					}
					mu.Lock()
					for id, peer := range peers {
						if mode == "missing" && id == "b" {
							continue
						}
						if mode == "malformed" {
							peer.WriteMessage(websocket.TextMessage, []byte("bad json"))
							continue
						}
						peer.WriteMessage(websocket.TextMessage, data)
						if mode == "duplicate" {
							peer.WriteMessage(websocket.TextMessage, data)
						}
					}
					mu.Unlock()
				}
			}))
			defer server.Close()
			o := fixture(t)
			o.endpoint = "ws" + strings.TrimPrefix(server.URL, "http")
			if err := run(o); err != nil {
				t.Fatal(err)
			}
			var result map[string]any
			if err := readJSON(filepath.Join(o.output, "summary.json"), &result); err != nil {
				t.Fatal(err)
			}
			wantUnique, wantDup := float64(2), float64(0)
			if mode == "missing" {
				wantUnique = 1
			}
			if mode == "malformed" {
				wantUnique = 0
			}
			if mode == "duplicate" {
				wantDup = 2
			}
			if result["unique_deliveries"] != wantUnique || result["duplicates"] != wantDup || result["missing"] != 2-wantUnique {
				t.Fatalf("bad accounting: %+v", result)
			}
			if result["successful_writes"] != float64(1) {
				t.Fatal(result)
			}
			if result["mode"] != "single" {
				t.Fatal("single mode regressed")
			}
			if mode == "malformed" {
				reasons := result["unexpected_reasons"].(map[string]any)
				if reasons["invalid_json"] != float64(2) {
					t.Fatal("missing protocol diagnostics", reasons)
				}
			}
		})
	}
}

func TestInvalidTrace(t *testing.T) {
	o := fixture(t)
	o.duration = time.Nanosecond
	os.WriteFile(o.trace, []byte(`{"message_id":"x","scheduled_offset_ns":5}`), 0644)
	if _, _, err := load(o); err == nil {
		t.Fatal("accepted invalid trace")
	}
}

func TestPercentiles(t *testing.T) {
	if percentile([]int64{3000000, 1000000, 2000000}, .5) != 2 {
		t.Fatal("median")
	}
}
