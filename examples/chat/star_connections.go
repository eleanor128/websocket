package main

import (
	"fmt"
	"github.com/gorilla/websocket"
	"net/url"
	"strings"
	"time"
)

func isStar(h *Hub) bool { _, ok := h.topologyStrategy.(*StarTopology); return ok }

// Star uses one connection per edge, initiated only by a leaf toward G0.
func validateStarConfig(id, peers string) error {
	if id != "G0" && !starLeaf(id) {
		return fmt.Errorf("Star gateway ID must be G0-G3")
	}
	if id == "G0" {
		if peers != "" {
			return fmt.Errorf("Star G0 accepts leaf connections; do not specify -peers")
		}
		return nil
	}
	if peers == "" || len(strings.Split(peers, ",")) != 1 {
		return fmt.Errorf("Star leaf requires exactly one G0 -peers URL")
	}
	u, err := url.Parse(peers)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		return fmt.Errorf("invalid Star peer URL")
	}
	return nil
}

func connectGateway(h *Hub, address string) error {
	if isStar(h) && !starLeaf(h.gatewayID) {
		return fmt.Errorf("only Star leaves may initiate peer connections")
	}
	u, err := url.Parse(address)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("from", h.gatewayID)
	u.RawQuery = q.Encode()
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, response, err := dialer.Dial(u.String(), nil)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return err
	}
	peerID := "G0" // Legacy behavior for the other topology prototypes.
	if isStar(h) && response.Header.Get("X-Gateway-ID") != "G0" {
		conn.Close()
		return fmt.Errorf("Star leaf destination did not identify as G0")
	}
	h.peerLock.Lock()
	if _, exists := h.peerGateway[peerID]; exists {
		h.peerLock.Unlock()
		conn.Close()
		return fmt.Errorf("peer %s already connected", peerID)
	}
	h.peerGateway[peerID] = conn
	h.peerLock.Unlock()
	go readPeerMessages(h, conn)
	return nil
}
