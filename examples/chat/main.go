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
	if *topology == "star" {
		if err := validateStarConfig(*gatewayID, *peers); err != nil {
			log.Fatal(err)
		}
	}

	// 傳入 ID 與 topology 初始化 hub
	hub := newHub(*gatewayID, *topology)
	go hub.run()

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
		json.NewEncoder(w).Encode(map[string]any{"gateway_id": hub.gatewayID, "connected_peers": len(hub.peerGateway)})
	})
	mux.HandleFunc("/", serveHome)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	mux.HandleFunc("/inter-gw", func(w http.ResponseWriter, r *http.Request) {
		from := r.URL.Query().Get("from")
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
		conn, err := upgrader.Upgrade(w, r, http.Header{"X-Gateway-Id": []string{hub.gatewayID}})
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
		hub.broadcast <- message
	}
}
