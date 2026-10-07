package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStarTopologyBroadcast(t *testing.T) {
	for _, structured := range []bool{false, true} {
		t.Run(fmt.Sprintf("benchmark=%t", structured), func(t *testing.T) {
			old := *benchmark
			*benchmark = structured
			defer func() { *benchmark = old }()
			hubs := make([]*Hub, 4)
			servers := make([]*httptest.Server, 4)
			for i := range hubs {
				hubs[i] = newHub(fmt.Sprintf("G%d", i), "star")
				go hubs[i].run()
				servers[i] = httptest.NewServer(chatHandler(hubs[i]))
				defer servers[i].Close()
			}
			for i := 1; i < 4; i++ {
				conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(servers[0].URL, "http")+fmt.Sprintf("/inter-gw?from=G%d", i), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				hubs[i].peerLock.Lock()
				hubs[i].peerGateway["G0"] = conn
				hubs[i].peerLock.Unlock()
				go readPeerMessages(hubs[i], conn)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				hubs[0].peerLock.RLock()
				n := len(hubs[0].peerGateway)
				hubs[0].peerLock.RUnlock()
				if n == 3 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("peers not ready")
				}
				time.Sleep(time.Millisecond)
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
			// A real application publication/receipt acts as the registration barrier.
			for g := 0; g < 4; g++ {
				sender := g * 2
				payload := fmt.Sprintf("hello %d 中文\n", g)
				expected := Message{RunID: "test-run", MessageID: fmt.Sprintf("m%d", g), SenderID: fmt.Sprintf("c%d", sender), Payload: payload, SourceGateway: "spoofed", FromGateway: "spoofed"}
				var err error
				if structured {
					err = clients[sender].WriteJSON(expected)
				} else {
					err = clients[sender].WriteMessage(websocket.TextMessage, []byte(payload))
				}
				if err != nil {
					t.Fatal(err)
				}
				for i, c := range clients {
					c.SetReadDeadline(time.Now().Add(2 * time.Second))
					var got Message
					if err := c.ReadJSON(&got); err != nil {
						t.Fatalf("client %d: %v", i, err)
					}
					if got.Payload != payload || got.SenderID != expected.SenderID || got.SourceGateway != fmt.Sprintf("G%d", g) {
						t.Fatalf("wrong delivery %+v", got)
					}
					if structured && (got.RunID != expected.RunID || got.MessageID != expected.MessageID) {
						t.Fatalf("lost workload identity %+v", got)
					}
				}
			}
			// A bounce to the source or re-forward by a leaf would leave extra frames.
			for i, c := range clients {
				c.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
				if _, _, err := c.ReadMessage(); err == nil {
					t.Fatalf("duplicate at client %d", i)
				}
			}
		})
	}
}

func TestClientMessageValidation(t *testing.T) {
	for _, raw := range []string{`not json`, `{"message_id":"m","sender_id":"a"}`, `{"run_id":"r","message_id":"m","sender_id":"other"}`} {
		if _, err := clientMessage([]byte(raw), "a", "G1", true); err == nil {
			t.Fatal("accepted invalid benchmark message")
		}
	}
	raw := []byte(`{"run_id":"r","message_id":"m","sender_id":"a","payload":"  你好\n","source_gateway":"G3","from_gateway":"G0"}`)
	result, err := clientMessage(raw, "a", "G1", true)
	if err != nil {
		t.Fatal(err)
	}
	var got Message
	json.Unmarshal(result, &got)
	if got.Payload != "  你好\n" || got.SourceGateway != "G1" || got.FromGateway != "" || got.RunID != "r" {
		t.Fatal(got)
	}
}
