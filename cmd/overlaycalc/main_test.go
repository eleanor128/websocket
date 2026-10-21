package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSerializationMatchesCurrentProtocol(t *testing.T) {
	value := event{
		MessageID: "m<&>", SenderID: "sender", Payload: "hello <&> 你好\n",
		PayloadBytes: len([]byte("hello <&> 你好\n")), SourceGateway: "G2",
	}
	runID := "run-0000000000000000000"
	ingress, err := encodedIngressSize(wire{runID, value.MessageID, value.SenderID, value.Payload})
	if err != nil {
		t.Fatal(err)
	}
	ingressMarshal, _ := json.Marshal(wire{runID, value.MessageID, value.SenderID, value.Payload})
	if ingress != len(ingressMarshal)+1 {
		t.Fatalf("WriteJSON size=%d, json.Marshal size=%d; expected trailing newline", ingress, len(ingressMarshal))
	}
	local, err := encodedMessage(message{
		RunID: runID, MessageID: value.MessageID, SenderID: value.SenderID,
		SourceGateway: "G2", FromGateway: "", Payload: value.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	overlay, err := encodedMessage(message{
		RunID: runID, MessageID: value.MessageID, SenderID: value.SenderID,
		SourceGateway: "G2", FromGateway: "G2", Payload: value.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(overlay) != len(local)+2 {
		t.Fatalf("overlay=%d local=%d; setting from_gateway to a two-byte ID should add two bytes", len(overlay), len(local))
	}
}

func TestTopologyHops(t *testing.T) {
	tests := []struct {
		topology string
		source   string
		wantBy   map[string]int
	}{
		{"direct", "G2", map[string]int{"G2": 3}},
		{"star", "G2", map[string]int{"G2": 1, "G0": 2}},
		{"tree", "G3", map[string]int{"G3": 1, "G1": 1, "G0": 1}},
	}
	for _, test := range tests {
		hops, err := hopsFor(test.topology, test.source)
		if err != nil {
			t.Fatal(err)
		}
		if len(hops) != 3 {
			t.Fatalf("%s: got %d hops", test.topology, len(hops))
		}
		got := map[string]int{}
		for _, hop := range hops {
			got[hop.From]++
		}
		for gateway, count := range test.wantBy {
			if got[gateway] != count {
				t.Errorf("%s: %s sends=%d, want %d", test.topology, gateway, got[gateway], count)
			}
		}
	}
}

func TestWebSocketFrameSize(t *testing.T) {
	if got := websocketFrameSize(100, false); got != 102 {
		t.Fatalf("unmasked short frame=%d", got)
	}
	if got := websocketFrameSize(200, false); got != 204 {
		t.Fatalf("unmasked extended frame=%d", got)
	}
	if got := websocketFrameSize(200, true); got != 208 {
		t.Fatalf("masked extended frame=%d", got)
	}
}

func TestAnalyzeTraceCountsTopologySenders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rate-2.jsonl")
	events := []event{
		{MessageID: "m0", SenderID: "u0", Payload: "a", PayloadBytes: 1, SourceGateway: "G0"},
		{MessageID: "m1", SenderID: "u1", Payload: "bb", PayloadBytes: 2, SourceGateway: "G3"},
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, value := range events {
		if err := encoder.Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := analyzeTrace(path, manifestTrace{
		File: "rate-2.jsonl", Messages: 2,
		PublicationsByGateway: map[string]int{"G0": 1, "G1": 0, "G2": 0, "G3": 1},
	}, "run-0000000000000000000", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, topology := range []string{"direct", "star", "tree"} {
		if got := report.Topologies[topology].ClusterOverlayMessages; got != 6 {
			t.Errorf("%s cluster transmissions=%d, want 6", topology, got)
		}
	}
	if got := report.Topologies["star"].Nodes["G0"].OverlayMessages; got != 5 {
		t.Errorf("star G0 transmissions=%d, want 5", got)
	}
	if got := report.Topologies["tree"].Nodes["G1"].OverlayMessages; got != 2 {
		t.Errorf("tree G1 transmissions=%d, want 2", got)
	}
}
