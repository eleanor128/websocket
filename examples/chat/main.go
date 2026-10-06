// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
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

	// 傳入 ID 與 topology 初始化 hub
	hub := newHub(*gatewayID, *topology)
	go hub.run()

	// 若有指定的 peers 則建立跨 Gateway 連線
	if *peers != "" {
		for _, peerAddr := range strings.Split(*peers, ",") {
			go func(url string) {
				conn, _, err := websocket.DefaultDialer.Dial(url+"?from="+*gatewayID, nil)
				if err == nil {
					// 💡 關鍵修正：Star 架構下，Leaf 連線的目標 Hub 固定為 "G0"
					// 將 Key 存為 "G0" 而非原始 URL，與 shouldForward 的 ID 邏輯一致！
					peerID := "G0"

					hub.peerLock.Lock()
					hub.peerGateway[peerID] = conn
					hub.peerLock.Unlock()

					go readPeerMessages(hub, conn)
					log.Printf("Connected to peer [%s] at %s\n", peerID, url)
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
	mux.HandleFunc("/", serveHome)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	mux.HandleFunc("/inter-gw", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		from := r.URL.Query().Get("from")
		hub.peerLock.Lock()
		hub.peerGateway[from] = conn // ✅ 這邊原本就寫對了 (存 from，即 "G1"/"G2"/"G3")
		hub.peerLock.Unlock()
		go readPeerMessages(hub, conn)
		log.Println("Peer connected from:", from)
	})
	return mux
}

func readPeerMessages(hub *Hub, conn *websocket.Conn) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		hub.broadcast <- message
	}
}
