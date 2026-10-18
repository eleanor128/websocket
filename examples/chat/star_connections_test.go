package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestStarRejectsInvalidEdges(t *testing.T) {
	for _, local := range []string{"G0", "G1", "G2", "G3"} {
		h := newHub(local, "star")
		s := httptest.NewServer(chatHandler(h))
		for _, remote := range []string{"", "G0", "G1", "G2", "G3", "G4"} {
			conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/inter-gw?from="+remote, nil)
			permitted := local == "G0" && starLeaf(remote)
			if permitted {
				if err != nil {
					t.Fatalf("valid edge %s %s: %v", local, remote, err)
				}
				conn.Close()
			} else {
				if err == nil {
					conn.Close()
					t.Fatalf("accepted illegal %s <- %s", local, remote)
				}
				if response == nil || response.StatusCode != http.StatusForbidden {
					t.Fatalf("expected 403: %v", response)
				}
			}
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
		}
		s.Close()
	}
	for _, leaf := range []string{"G1", "G2", "G3"} {
		for _, target := range []string{"G1", "G2", "G3", "G4"} {
			if NewStarTopology(leaf).ShouldForward(target, Message{SourceGateway: leaf}) {
				t.Fatal("leaf forwards outside G0")
			}
		}
		if !NewStarTopology(leaf).ShouldForward("G0", Message{SourceGateway: leaf}) {
			t.Fatal("local publication cannot reach hub")
		}
		if NewStarTopology(leaf).ShouldForward("G0", Message{SourceGateway: "G2", FromGateway: "G0"}) {
			t.Fatal("leaf forwards remote publication")
		}
	}
}

func TestStarDuplicateAndDestination(t *testing.T) {
	hub := newHub("G0", "star")
	s := httptest.NewServer(chatHandler(hub))
	defer s.Close()
	address := "ws" + strings.TrimPrefix(s.URL, "http") + "/inter-gw"
	leaf := newHub("G1", "star")
	if err := connectGateway(leaf, address); err != nil {
		t.Fatal(err)
	}
	leaf.peerLock.RLock()
	conn := leaf.peerGateway["G0"]
	leaf.peerLock.RUnlock()
	defer conn.Close()
	duplicate, response, err := websocket.DefaultDialer.Dial(address+"?from=G1", nil)
	if err == nil {
		duplicate.Close()
		t.Fatal("duplicate accepted")
	}
	if response == nil || response.StatusCode != 409 {
		t.Fatal("duplicate not rejected with 409")
	}
	response.Body.Close()
	wrong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := upgrader.Upgrade(w, r, http.Header{"X-Gateway-Id": []string{"G2"}})
		if e == nil {
			defer c.Close()
			c.ReadMessage()
		}
	}))
	defer wrong.Close()
	if err := connectGateway(newHub("G3", "star"), "ws"+strings.TrimPrefix(wrong.URL, "http")); err == nil {
		t.Fatal("accepted destination identifying as G2")
	}
}

func TestStarStartupConfiguration(t *testing.T) {
	for _, tc := range []struct {
		id, peers string
		ok        bool
	}{
		{"G0", "", true}, {"G1", "ws://localhost:8081/inter-gw", true},
		{"G0", "ws://localhost:8082/inter-gw", false}, {"G4", "", false}, {"G2", "", false},
		{"G2", "ws://a,ws://b", false}, {"G3", "http://localhost", false},
	} {
		if err := validateStarConfig(tc.id, tc.peers); (err == nil) != tc.ok {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}
