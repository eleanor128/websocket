package main

import (
	"testing"
)

func TestHubCountsClientQueueOverflow(t *testing.T) {
	h := newHub("G0", "tree")
	go h.run()
	client := &Client{hub: h, send: make(chan []byte, 1)}
	h.register <- client
	h.broadcast <- []byte("not-json")
	h.broadcast <- []byte("not-json")
	eventually(t, func() bool { return h.clientQueueOverflows.Load() == 1 })
	if got := h.clientQueueOverflows.Load(); got != 1 {
		t.Fatalf("queue overflow count=%d, want 1", got)
	}
}
