package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/gorilla/websocket"
)

func validGatewayID(id string) bool { return id == "G0" || starLeaf(id) }
func isDirect(h *Hub) bool          { _, ok := h.topologyStrategy.(*DirectTopology); return ok }

// Include all four overlay endpoints, including this process, in one config.
func loadDirectConfig(path, id string) (map[string]string, error) {
	if !validGatewayID(id) {
		return nil, fmt.Errorf("Direct gateway ID must be G0-G3")
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
		return nil, fmt.Errorf("Direct configuration requires exactly G0-G3")
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

// Exactly one dialer per unordered pair; the connection carries both directions.
func connectDirect(h *Hub, target, address string) error {
	if !isDirect(h) || !validGatewayID(h.gatewayID) || !validGatewayID(target) || h.gatewayID >= target {
		return fmt.Errorf("Direct dial must go from lower ID to higher ID")
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
	q.Set("topology", "direct")
	u.RawQuery = q.Encode()
	d := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	c, response, err := d.Dial(u.String(), nil)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return err
	}
	if response.Header.Get("X-Gateway-ID") != target || response.Header.Get("X-Topology") != "direct" {
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

func startDirectPeers(h *Hub, peers map[string]string) {
	for id, address := range peers {
		if id <= h.gatewayID {
			continue
		}
		go func(target, address string) {
			deadline := time.Now().Add(30 * time.Second)
			for {
				err := connectDirect(h, target, address)
				if err == nil {
					log.Printf("Direct connected %s <-> %s", h.gatewayID, target)
					return
				}
				if time.Now().After(deadline) {
					log.Printf("Direct peer %s unavailable: %v", target, err)
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}(id, address)
	}
}

// Caller holds peerLock. Readiness uses exact membership, not just a count.
func peerStatus(h *Hub) ([]string, bool) {
	ids := make([]string, 0, len(h.peerGateway))
	for id := range h.peerGateway {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if isDirect(h) {
		if len(ids) != 3 {
			return ids, false
		}
		for i := 0; i < 4; i++ {
			id := fmt.Sprintf("G%d", i)
			if id != h.gatewayID {
				if _, ok := h.peerGateway[id]; !ok {
					return ids, false
				}
			}
		}
		return ids, validGatewayID(h.gatewayID)
	}
	if isStar(h) {
		if h.gatewayID == "G0" {
			return ids, len(ids) == 3 && ids[0] == "G1" && ids[1] == "G2" && ids[2] == "G3"
		}
		return ids, starLeaf(h.gatewayID) && len(ids) == 1 && ids[0] == "G0"
	}
	return ids, false
}
