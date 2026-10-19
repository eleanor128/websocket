// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

var addr = flag.String("addr", ":8080", "http service address")

// === 新增：拓樸測試所需的 minimal 參數 ===
var gatewayID = flag.String("id", "G0", "gateway ID")
var topology = flag.String("topology", "star", "topology type")
var peers = flag.String("peers", "", "comma-separated peer ws URLs")
var directPeers = flag.String("direct-peers", "", "Direct JSON mapping G0-G3 to /inter-gw URLs")
var treePeers = flag.String("tree-peers", "", "Tree JSON mapping G0-G3 to /inter-gw URLs")

func serveHome(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL)
	if r.URL.Path != "/" {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.ServeFile(w, r, "home.html")
}

func main() {
	flag.Parse()
	var directConfig map[string]string
	var treeConfig map[string]string
	if *topology == "direct" {
		if *peers != "" || *treePeers != "" {
			log.Fatal("Direct uses -direct-peers, not -peers or -tree-peers")
		}
		var err error
		directConfig, err = loadDirectConfig(*directPeers, *gatewayID)
		if err != nil {
			log.Fatal(err)
		}
	} else if *directPeers != "" {
		log.Fatal("-direct-peers requires -topology direct")
	}
	if *topology == "tree" {
		if *peers != "" || *directPeers != "" {
			log.Fatal("Tree uses -tree-peers, not -peers or -direct-peers")
		}
		var err error
		treeConfig, err = loadTreeConfig(*treePeers, *gatewayID)
		if err != nil {
			log.Fatal(err)
		}
	} else if *treePeers != "" {
		log.Fatal("-tree-peers requires -topology tree")
	}
	if *topology == "star" {
		if *directPeers != "" || *treePeers != "" {
			log.Fatal("Star uses -peers (for leaves), not -direct-peers or -tree-peers")
		}
		if err := validateStarConfig(*gatewayID, *peers); err != nil {
			log.Fatal(err)
		}
	}

	// 傳入 ID 與 topology 初始化 hub
	hub := newHub(*gatewayID, *topology)
	go hub.run()
	if directConfig != nil {
		startDirectPeers(hub, directConfig)
	}
	if treeConfig != nil {
		startTreePeers(hub, treeConfig)
	}

	// 若有指定的 peers 則建立跨 Gateway 連線
	if *peers != "" {
		for _, peerAddr := range strings.Split(*peers, ",") {
			go func(url string) {
				err := connectGateway(hub, url)
				if err == nil {
					log.Printf("Connected to peer at %s\n", url)
				} else {
					log.Printf("Failed to connect to peer %s: %v\n", url, err)
				}
			}(peerAddr)
		}
	}

	err := http.ListenAndServe(*addr, chatHandler(hub))
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func chatHandler(hub *Hub) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		hub.peerLock.RLock()
		defer hub.peerLock.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		ids, ready := peerStatus(hub)
		json.NewEncoder(w).Encode(map[string]any{"gateway_id": hub.gatewayID, "topology": hub.topology, "peer_ids": ids, "ready": ready, "connected_peers": len(hub.peerGateway), "metrics": hub.metricsSnapshot()})
	})
	mux.HandleFunc("/", serveHome)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if isDirect(hub) || isTree(hub) {
			hub.peerLock.RLock()
			_, ready := peerStatus(hub)
			hub.peerLock.RUnlock()
			if !ready {
				http.Error(w, "peers not ready", http.StatusServiceUnavailable)
				return
			}
		}
		serveWs(hub, w, r)
	})

	mux.HandleFunc("/inter-gw", func(w http.ResponseWriter, r *http.Request) {
		from := r.URL.Query().Get("from")
		if isDirect(hub) && (!validGatewayID(from) || !validGatewayID(hub.gatewayID) || from >= hub.gatewayID || r.URL.Query().Get("to") != hub.gatewayID || r.URL.Query().Get("topology") != "direct") {
			http.Error(w, "Direct requires lower-ID peer dialing the declared higher-ID destination", http.StatusForbidden)
			return
		}
		if isTree(hub) && (!validGatewayID(from) || !validGatewayID(hub.gatewayID) || !treeEdge(from, hub.gatewayID) || from >= hub.gatewayID || r.URL.Query().Get("to") != hub.gatewayID || r.URL.Query().Get("topology") != "tree") {
			http.Error(w, "Tree requires lower-ID peer on tree edge dialing higher-ID destination", http.StatusForbidden)
			return
		}
		if isStar(hub) && (hub.gatewayID != "G0" || !starLeaf(from)) {
			http.Error(w, "Star permits only leaf-initiated connections to G0", http.StatusForbidden)
			return
		}
		// Check and register atomically, preventing duplicate live peer sockets.
		hub.peerLock.Lock()
		defer hub.peerLock.Unlock()
		if _, exists := hub.peerGateway[from]; exists {
			http.Error(w, "peer already connected", http.StatusConflict)
			return
		}
		conn, err := upgrader.Upgrade(w, r, http.Header{"X-Gateway-Id": []string{hub.gatewayID}, "X-Topology": []string{hub.topology}})
		if err != nil {
			return
		}
		hub.peerGateway[from] = conn
		go readPeerMessages(hub, conn)
		log.Println("Peer connected from:", from)
	})
	return mux
}

func readPeerMessages(hub *Hub, conn *websocket.Conn) {
	peerID := ""
	hub.peerLock.RLock()
	for id, c := range hub.peerGateway {
		if c == conn {
			peerID = id
			break
		}
	}
	hub.peerLock.RUnlock()
	defer func() {
		conn.Close()
		hub.peerLock.Lock()
		defer hub.peerLock.Unlock()
		for id, peer := range hub.peerGateway {
			if peer == conn {
				delete(hub.peerGateway, id)
			}
		}
	}()
	conn.SetReadLimit(2 * 1024 * 1024)
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if isDirect(hub) {
			var msg Message
			if json.Unmarshal(message, &msg) != nil || msg.MessageID == "" || msg.SenderID == "" || msg.SourceGateway != peerID || msg.FromGateway != peerID {
				hub.rejectedPeerMessages.Add(1)
				return
			}
		}
		if isTree(hub) {
			var msg Message
			if json.Unmarshal(message, &msg) != nil || msg.MessageID == "" || msg.SenderID == "" || !validGatewayID(msg.SourceGateway) || msg.FromGateway != peerID {
				hub.rejectedPeerMessages.Add(1)
				return
			}
		}
		hub.overlayReceived.Add(1)
		hub.broadcast <- message
	}
}
