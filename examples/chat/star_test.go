// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStarTopologyBroadcast(t *testing.T) {
	// 1. 建立 G0 (Hub) 與 G1, G2, G3 (Leaves)
	gateways := []*Hub{
		newHub("G0", "star"),
		newHub("G1", "star"),
		newHub("G2", "star"),
		newHub("G3", "star"),
	}
	for _, hub := range gateways {
		go hub.run()
	}

	servers := make([]*httptest.Server, 0, len(gateways))
	for _, hub := range gateways {
		servers = append(servers, httptest.NewServer(chatHandler(hub)))
		defer servers[len(servers)-1].Close()
	}

	// 2. 將 G1, G2, G3 都連線到 G0 的 /inter-gw
	g0URL := websocketURL(servers[0].URL) + "/inter-gw"
	connectPeer(t, gateways[1], g0URL, "G1")
	connectPeer(t, gateways[2], g0URL, "G2")
	connectPeer(t, gateways[3], g0URL, "G3")

	// 3. 等待 G0 成功對接 3 個 Peer
	waitForPeerCount(t, gateways[0], 3)

	// 4. 每個 Gateway 建立兩個 Client，並為每個 Client 開啟接收 goroutine
	clients := make(map[string]*websocket.Conn)
	clientGateways := []string{"G1", "G1", "G2", "G2", "G3", "G3"}
	for index, gatewayID := range clientGateways {
		name := fmt.Sprintf("%s_User%c", gatewayID, 'A'+rune(index%2))
		clients[name] = connectClient(t, servers[index/2+1].URL, name)
		defer clients[name].Close()
		startClientPrinter(t, name, clients[name])
	}
	for _, hub := range gateways[1:] {
		waitForClientCount(t, hub, 2)
	}

	// 5. 由 G1_UserA 發送訊息，六位 Client 都應該收到
	if err := clients["G1_UserA"].WriteMessage(websocket.TextMessage, []byte("hello from G1_UserA")); err != nil {
		t.Fatal(err)
	}
	waitForBroadcasts(t, 6, "hello from G1_UserA")

	// 6. 由 G3_UserB 回覆，六位 Client 也都應該收到
	if err := clients["G3_UserB"].WriteMessage(websocket.TextMessage, []byte("reply from G3_UserB")); err != nil {
		t.Fatal(err)
	}
	waitForBroadcasts(t, 6, "reply from G3_UserB")
}

type receivedMessage struct {
	client  string
	message Message
	err     error
}

var receivedMessages = make(chan receivedMessage, 32)

func startClientPrinter(t *testing.T, name string, conn *websocket.Conn) {
	t.Helper()
	go func() {
		for {
			var message Message
			if err := conn.ReadJSON(&message); err != nil {
				receivedMessages <- receivedMessage{client: name, err: err}
				return
			}
			fmt.Printf("[收到] %-10s <- %-10s (%s): %s\n", name, message.SenderID, message.SourceGateway, message.Payload)
			receivedMessages <- receivedMessage{client: name, message: message}
		}
	}()
}

func waitForBroadcasts(t *testing.T, expected int, payload string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for received := 0; received < expected; received++ {
		select {
		case result := <-receivedMessages:
			if result.err != nil {
				t.Fatalf("client %s stopped receiving: %v", result.client, result.err)
			}
			if result.message.Payload != payload {
				t.Fatalf("client %s received unexpected message: %+v", result.client, result.message)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %d clients to receive %q", expected, payload)
		}
	}
}

func connectPeer(t *testing.T, hub *Hub, peerURL, gatewayID string) {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(peerURL+"?from="+gatewayID, nil)
	if err != nil {
		t.Fatal(err)
	}
	hub.peerLock.Lock()
	hub.peerGateway[peerURL] = conn
	hub.peerLock.Unlock()
	go readPeerMessages(hub, conn)
}

func connectClient(t *testing.T, serverURL, name string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(websocketURL(serverURL)+"/ws?name="+name, nil)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func readMessage(t *testing.T, conn *websocket.Conn) Message {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var message Message
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatal(err)
	}
	return message
}

func waitForPeerCount(t *testing.T, hub *Hub, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.peerLock.RLock()
		count := len(hub.peerGateway)
		hub.peerLock.RUnlock()
		if count == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d peers", expected)
}

func waitForClientCount(t *testing.T, hub *Hub, expected int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(hub.clients) == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d local clients", expected)
}

func websocketURL(httpURL string) string {
	return strings.Replace(httpURL, "http", "ws", 1)
}
