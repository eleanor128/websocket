// hub.go
package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client

	gatewayID        string
	peerGateway      map[string]*websocket.Conn
	peerLock         sync.RWMutex
	topologyStrategy TopologyStrategy // 💡 改為注入介面
}

func newHub(gatewayID string, topology string) *Hub {
	var strategy TopologyStrategy

	// 根據傳入的字串動態掛載對應的拓樸物件
	switch topology {
	case "direct":
		strategy = NewDirectTopology(gatewayID)
	case "tree":
		strategy = NewTreeTopology(gatewayID)
	case "star":
		fallthrough
	default:
		strategy = NewStarTopology(gatewayID)
	}

	return &Hub{
		broadcast:        make(chan []byte),
		register:         make(chan *Client),
		unregister:       make(chan *Client),
		clients:          make(map[*Client]bool),
		gatewayID:        gatewayID,
		peerGateway:      make(map[string]*websocket.Conn),
		topologyStrategy: strategy,
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}

		case messageBytes := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- messageBytes:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}

			var msg Message
			if err := json.Unmarshal(messageBytes, &msg); err == nil {
				h.forwardToPeers(msg, messageBytes)
			}
		}
	}
}

func (h *Hub) forwardToPeers(msg Message, rawBytes []byte) {
	h.peerLock.RLock()
	defer h.peerLock.RUnlock()

	for peerID, conn := range h.peerGateway {
		// 💡 直接調用策略介面進行判斷
		if h.topologyStrategy.ShouldForward(peerID, msg) {
			forwardedMessage := msg
			forwardedMessage.FromGateway = h.gatewayID
			updatedBytes, err := json.Marshal(forwardedMessage)
			if err != nil {
				updatedBytes = rawBytes
			}
			if !*benchmark {
				fmt.Printf("[%s] --> 轉發訊息給 [%s] (Source: %s)\n", h.gatewayID, peerID, msg.SourceGateway)
			}
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			err = conn.WriteMessage(websocket.TextMessage, updatedBytes)
			if err != nil {
				fmt.Printf("[%s] 轉發至 [%s] 失敗: %v\n", h.gatewayID, peerID, err)
			}
		}
	}
}
