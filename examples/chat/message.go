package main

type Message struct {
	RunID         string `json:"run_id,omitempty"`
	MessageID     string `json:"message_id"`     // 訊息 UUID
	SenderID      string `json:"sender_id"`      // 發送者 ID
	SourceGateway string `json:"source_gateway"` // 原始 Gateway (如 G1)
	FromGateway   string `json:"from_gateway"`   // 上一個轉發者 Gateway
	Payload       string `json:"payload"`        // 聊天內容
	Timestamp     int64  `json:"timestamp"`      // 發送時間戳記 (Unix ms)
}
