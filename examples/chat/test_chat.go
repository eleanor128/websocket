package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type ClientConfig struct {
	Name string
	Addr string
}

// 測試用進入點
func runTestChat() {
	// 定義要建立的使用者與對應 Gateway
	configs := []ClientConfig{
		{Name: "G1_UserA", Addr: "localhost:8081"},
		{Name: "G1_UserB", Addr: "localhost:8081"},
		{Name: "G2_UserA", Addr: "localhost:8082"},
		{Name: "G2_UserB", Addr: "localhost:8082"},
		{Name: "G3_UserA", Addr: "localhost:8083"},
		{Name: "G3_UserB", Addr: "localhost:8083"},
	}

	conns := make(map[string]*websocket.Conn)
	var wg sync.WaitGroup

	fmt.Println("🚀 正在建立各 Gateway 的 Client 連線...")

	// 1. 為所有 Client 建立 WebSocket 連線並開啟背景接收
	for _, cfg := range configs {
		u := url.URL{Scheme: "ws", Host: cfg.Addr, Path: "/ws", RawQuery: "name=" + cfg.Name}
		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			log.Printf("❌ [%s] 連線失敗 (請確認 %s 是否已啟動): %v", cfg.Name, cfg.Addr, err)
			continue
		}
		conns[cfg.Name] = conn
		defer conn.Close()

		wg.Add(1)
		go func(name string, c *websocket.Conn) {
			defer wg.Done()
			for {
				_, raw, err := c.ReadMessage()
				if err != nil {
					return
				}

				var msg Message
				if err := json.Unmarshal(raw, &msg); err == nil {
					fmt.Printf("📩 [%s 收到] 來自 %s (%s): %s\n", name, msg.SenderID, msg.SourceGateway, msg.Payload)
				} else {
					fmt.Printf("📩 [%s 收到純文字]: %s\n", name, string(raw))
				}
			}
		}(cfg.Name, conn)

		fmt.Printf("✅ [%s] 成功連線至 %s\n", cfg.Name, cfg.Addr)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("\n--- 測試廣播開始 ---")

	// 2. 模擬 G1_UserA 發送訊息
	if conn, ok := conns["G1_UserA"]; ok {
		testMsg := "Hello World from G1!"
		fmt.Printf("\n📤 [G1_UserA] 發送訊息: '%s'\n", testMsg)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(testMsg)); err != nil {
			log.Printf("發送失敗: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	// 3. 模擬 G3_UserB 回覆訊息
	if conn, ok := conns["G3_UserB"]; ok {
		replyMsg := "Received loud and clear from G3!"
		fmt.Printf("\n📤 [G3_UserB] 發送回覆: '%s'\n", replyMsg)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(replyMsg)); err != nil {
			log.Printf("發送失敗: %v", err)
		}
	}

	time.Sleep(2 * time.Second)
	fmt.Println("\n--- 測試結束 ---")
}
