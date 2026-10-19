package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func isTree(h *Hub) bool { _, ok := h.topologyStrategy.(*TreeTopology); return ok }

// loadTreeConfig validates that the config file provides valid /inter-gw endpoints for G0-G3.
func loadTreeConfig(path, id string) (map[string]string, error) {
	if !validGatewayID(id) {
		return nil, fmt.Errorf("Tree gateway ID must be G0-G3")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var peers map[string]string
	if err = json.Unmarshal(raw, &peers); err != nil {
		return nil, err
	}
	if len(peers) != 4 {
		return nil, fmt.Errorf("Tree configuration requires exactly G0-G3")
	}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("G%d", i)
		u, err := url.Parse(peers[key])
		if err != nil || u.Hostname() == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.Path != "/inter-gw" {
			return nil, fmt.Errorf("invalid overlay URL for %s (use ws[s]://host:port/inter-gw)", key)
		}
		if seen[u.String()] {
			return nil, fmt.Errorf("duplicate overlay endpoint")
		}
		seen[u.String()] = true
	}
	return peers, nil
}

// connectTree establishes a single bidirectional connection from lower-ID to higher-ID along a tree edge.
func connectTree(h *Hub, target, address string) error {
	if !isTree(h) || !validGatewayID(h.gatewayID) || !validGatewayID(target) {
		return fmt.Errorf("invalid Tree gateway IDs")
	}
	if !treeEdge(h.gatewayID, target) {
		return fmt.Errorf("%s and %s are not adjacent in Tree topology", h.gatewayID, target)
	}
	if h.gatewayID >= target {
		return fmt.Errorf("Tree dial must go from lower ID to higher ID")
	}
	h.peerLock.RLock()
	_, exists := h.peerGateway[target]
	h.peerLock.RUnlock()
	if exists {
		return fmt.Errorf("peer %s already connected", target)
	}
	u, err := url.Parse(address)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("from", h.gatewayID)
	q.Set("to", target)
	q.Set("topology", "tree")
	u.RawQuery = q.Encode()
	d := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, response, err := d.Dial(u.String(), nil)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return err
	}
	if response.Header.Get("X-Gateway-ID") != target || response.Header.Get("X-Topology") != "tree" {
		c.Close()
		return fmt.Errorf("peer handshake identity/topology mismatch")
	}
	h.peerLock.Lock()
	if _, exists = h.peerGateway[target]; exists {
		h.peerLock.Unlock()
		c.Close()
		return fmt.Errorf("peer %s already connected", target)
	}
	h.peerGateway[target] = c
	h.peerLock.Unlock()
	go readPeerMessages(h, c)
	return nil
}

// startTreePeers initiates connections to adjacent tree peers where this node has a lower ID.
func startTreePeers(h *Hub, peers map[string]string) {
	startTreePeersContext(context.Background(), h, peers)
}

func startTreePeersContext(ctx context.Context, h *Hub, peers map[string]string) {
	for id, address := range peers {
		if !treeEdge(h.gatewayID, id) || id <= h.gatewayID {
			continue
		}
		go maintainTreePeer(ctx, h, id, address)
	}
}

func maintainTreePeer(ctx context.Context, h *Hub, target, address string) {
	retry := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		h.peerLock.RLock()
		connected := h.peerGateway[target] != nil
		h.peerLock.RUnlock()
		if connected {
			retry = 200 * time.Millisecond
			if !waitTreeRetry(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}

		if err := connectTree(h, target, address); err == nil {
			log.Printf("Tree connected %s <-> %s", h.gatewayID, target)
			retry = 200 * time.Millisecond
		} else {
			if !waitTreeRetry(ctx, retry) {
				return
			}
			if retry < 2*time.Second {
				retry *= 2
			}
		}
	}
}

func waitTreeRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
