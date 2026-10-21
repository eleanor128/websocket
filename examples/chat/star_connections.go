package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
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

func startStarPeer(h *Hub, address string) {
	go maintainStarPeer(context.Background(), h, address)
}

// maintainStarPeer makes container startup order and transient overlay
// failures harmless. A Star leaf owns its single outgoing edge to G0, so it
// is also responsible for replacing that edge after a disconnect.
func maintainStarPeer(ctx context.Context, h *Hub, address string) {
	retry := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		h.peerLock.RLock()
		connected := h.peerGateway["G0"] != nil
		h.peerLock.RUnlock()
		if connected {
			retry = 200 * time.Millisecond
			if !waitStarRetry(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}

		if err := connectGateway(h, address); err == nil {
			log.Printf("Star connected %s <-> G0", h.gatewayID)
			retry = 200 * time.Millisecond
		} else {
			if !waitStarRetry(ctx, retry) {
				return
			}
			if retry < 2*time.Second {
				retry *= 2
			}
		}
	}
}

func waitStarRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
