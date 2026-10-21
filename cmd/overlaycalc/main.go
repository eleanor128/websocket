// Command overlaycalc reconstructs the exact JSON envelopes produced by the
// current load generator and gateway, then computes per-node overlay demand for
// Direct, Star and Tree from every event in a workload manifest.
package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var gateways = []string{"G0", "G1", "G2", "G3"}

type event struct {
	MessageID     string `json:"message_id"`
	SenderID      string `json:"sender_id"`
	Payload       string `json:"payload"`
	PayloadBytes  int    `json:"payload_bytes"`
	SourceGateway string `json:"source_gateway"`
}

// wire matches cmd/loadgen.Wire. Gorilla's WriteJSON uses json.Encoder.Encode,
// so the ingress byte count includes its trailing newline.
type wire struct {
	RunID     string `json:"run_id"`
	MessageID string `json:"message_id"`
	SenderID  string `json:"sender_id"`
	Payload   string `json:"payload"`
}

// message matches examples/chat.Message. Keep field order and tags identical:
// encoding/json preserves struct field order, which affects exact byte output.
type message struct {
	RunID         string `json:"run_id,omitempty"`
	MessageID     string `json:"message_id"`
	SenderID      string `json:"sender_id"`
	SourceGateway string `json:"source_gateway"`
	FromGateway   string `json:"from_gateway"`
	Payload       string `json:"payload"`
	Timestamp     int64  `json:"timestamp"`
}

type manifest struct {
	DurationSeconds string          `json:"duration_seconds"`
	Traces          []manifestTrace `json:"traces"`
}

type manifestTrace struct {
	File                  string         `json:"file"`
	Messages              int            `json:"messages"`
	PublicationsByGateway map[string]int `json:"publications_by_gateway"`
}

type sizeStats struct {
	Count     int     `json:"count"`
	MeanBytes float64 `json:"mean_bytes"`
	MinBytes  int     `json:"min_bytes"`
	P50Bytes  int     `json:"p50_bytes"`
	P95Bytes  int     `json:"p95_bytes"`
	P99Bytes  int     `json:"p99_bytes"`
	MaxBytes  int     `json:"max_bytes"`
}

type nodeDemand struct {
	OverlayMessages      int     `json:"overlay_messages"`
	SerializedJSONBytes  int64   `json:"serialized_json_bytes"`
	WebSocketFrameBytes  int64   `json:"websocket_frame_bytes"`
	AverageJSONMbps      float64 `json:"average_json_mbps"`
	AverageWebSocketMbps float64 `json:"average_websocket_mbps"`
}

type topologyDemand struct {
	Nodes                   map[string]*nodeDemand `json:"nodes"`
	ClusterOverlayMessages  int                    `json:"cluster_overlay_messages"`
	ClusterSerializedBytes  int64                  `json:"cluster_serialized_json_bytes"`
	ClusterWebSocketBytes   int64                  `json:"cluster_websocket_frame_bytes"`
	ClusterJSONMbps         float64                `json:"cluster_average_json_mbps"`
	ClusterWebSocketMbps    float64                `json:"cluster_average_websocket_mbps"`
	BottleneckGateway       string                 `json:"bottleneck_gateway"`
	BottleneckJSONMbps      float64                `json:"bottleneck_json_mbps"`
	BottleneckWebSocketMbps float64                `json:"bottleneck_websocket_mbps"`
}

type traceReport struct {
	Trace                 string                     `json:"trace"`
	TargetRate            int                        `json:"target_rate_messages_per_second"`
	ActualRate            float64                    `json:"actual_rate_messages_per_second"`
	DurationSeconds       float64                    `json:"duration_seconds"`
	Messages              int                        `json:"messages"`
	PublicationsByGateway map[string]int             `json:"publications_by_gateway"`
	Payload               sizeStats                  `json:"payload_bytes"`
	IngressJSON           sizeStats                  `json:"loadgen_ingress_json_bytes"`
	LocalEnvelopeJSON     sizeStats                  `json:"gateway_local_envelope_json_bytes"`
	OverlayEnvelopeJSON   sizeStats                  `json:"overlay_envelope_json_bytes"`
	Topologies            map[string]*topologyDemand `json:"topologies"`
}

type report struct {
	SchemaVersion               int               `json:"schema_version"`
	RunID                       string            `json:"run_id_used_for_size"`
	RunIDBytes                  int               `json:"run_id_bytes"`
	AnalyzedMessages            int               `json:"analyzed_messages"`
	AllTracesWeightedMeanSBytes float64           `json:"all_traces_weighted_mean_s_bytes"`
	Traces                      []traceReport     `json:"traces"`
	Definitions                 map[string]string `json:"definitions"`
	Limitations                 []string          `json:"limitations"`
}

type hop struct {
	From string
	To   string
}

func encodedIngressSize(value wire) (int, error) {
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(value); err != nil {
		return 0, err
	}
	return buffer.Len(), nil
}

func encodedMessage(value message) ([]byte, error) {
	return json.Marshal(value)
}

func stats(values []int) sizeStats {
	if len(values) == 0 {
		return sizeStats{}
	}
	ordered := append([]int(nil), values...)
	sort.Ints(ordered)
	total := int64(0)
	for _, value := range ordered {
		total += int64(value)
	}
	nearest := func(p float64) int {
		index := int(math.Ceil(p*float64(len(ordered)))) - 1
		if index < 0 {
			index = 0
		}
		return ordered[index]
	}
	return sizeStats{
		Count: len(ordered), MeanBytes: float64(total) / float64(len(ordered)),
		MinBytes: ordered[0], P50Bytes: nearest(.50), P95Bytes: nearest(.95),
		P99Bytes: nearest(.99), MaxBytes: ordered[len(ordered)-1],
	}
}

func websocketFrameSize(payloadBytes int, masked bool) int {
	header := 2
	if payloadBytes > 125 && payloadBytes <= 65535 {
		header = 4
	} else if payloadBytes > 65535 {
		header = 10
	}
	if masked {
		header += 4
	}
	return payloadBytes + header
}

func validGateway(value string) bool {
	for _, gateway := range gateways {
		if value == gateway {
			return true
		}
	}
	return false
}

func hopsFor(topology, source string) ([]hop, error) {
	if !validGateway(source) {
		return nil, fmt.Errorf("invalid source gateway %q", source)
	}
	switch topology {
	case "direct":
		result := make([]hop, 0, 3)
		for _, target := range gateways {
			if target != source {
				result = append(result, hop{From: source, To: target})
			}
		}
		return result, nil
	case "star":
		if source == "G0" {
			return []hop{{"G0", "G1"}, {"G0", "G2"}, {"G0", "G3"}}, nil
		}
		result := []hop{{source, "G0"}}
		for _, target := range []string{"G1", "G2", "G3"} {
			if target != source {
				result = append(result, hop{"G0", target})
			}
		}
		return result, nil
	case "tree":
		adjacent := map[string][]string{
			"G0": {"G1", "G2"},
			"G1": {"G0", "G3"},
			"G2": {"G0"},
			"G3": {"G1"},
		}
		visited := map[string]bool{source: true}
		queue := []string{source}
		result := make([]hop, 0, 3)
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, target := range adjacent[current] {
				if visited[target] {
					continue
				}
				visited[target] = true
				result = append(result, hop{From: current, To: target})
				queue = append(queue, target)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown topology %q", topology)
	}
}

// Lower-ID gateways dial higher-ID gateways in Direct and Tree. Star leaves
// dial G0. WebSocket clients mask frames; the accepting side does not.
func senderMasksFrame(topology string, transmission hop) bool {
	if topology == "star" {
		return transmission.From != "G0"
	}
	return transmission.From < transmission.To
}

func readEvents(path string) ([]event, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 2*1024*1024)
	var events []event
	for line := 1; scanner.Scan(); line++ {
		var value event
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		if value.MessageID == "" || value.SenderID == "" || !validGateway(value.SourceGateway) {
			return nil, fmt.Errorf("%s:%d: missing ID or invalid source gateway", path, line)
		}
		if len([]byte(value.Payload)) != value.PayloadBytes {
			return nil, fmt.Errorf("%s:%d: payload_bytes=%d, actual=%d", path, line, value.PayloadBytes, len([]byte(value.Payload)))
		}
		events = append(events, value)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("empty trace: %s", path)
	}
	return events, nil
}

func newTopologyDemand() *topologyDemand {
	nodes := map[string]*nodeDemand{}
	for _, gateway := range gateways {
		nodes[gateway] = &nodeDemand{}
	}
	return &topologyDemand{Nodes: nodes}
}

func analyzeTrace(path string, entry manifestTrace, runID string, duration float64) (traceReport, error) {
	events, err := readEvents(path)
	if err != nil {
		return traceReport{}, err
	}
	if entry.Messages != 0 && entry.Messages != len(events) {
		return traceReport{}, fmt.Errorf("%s: manifest messages=%d, trace messages=%d", path, entry.Messages, len(events))
	}
	published := map[string]int{"G0": 0, "G1": 0, "G2": 0, "G3": 0}
	payloadSizes := make([]int, 0, len(events))
	ingressSizes := make([]int, 0, len(events))
	localSizes := make([]int, 0, len(events))
	overlaySizes := make([]int, 0, len(events)*3)
	demands := map[string]*topologyDemand{
		"direct": newTopologyDemand(), "star": newTopologyDemand(), "tree": newTopologyDemand(),
	}
	for _, value := range events {
		published[value.SourceGateway]++
		payloadSizes = append(payloadSizes, value.PayloadBytes)
		ingressSize, err := encodedIngressSize(wire{runID, value.MessageID, value.SenderID, value.Payload})
		if err != nil {
			return traceReport{}, err
		}
		ingressSizes = append(ingressSizes, ingressSize)
		base := message{
			RunID: runID, MessageID: value.MessageID, SenderID: value.SenderID,
			SourceGateway: value.SourceGateway, FromGateway: "", Payload: value.Payload, Timestamp: 0,
		}
		localBytes, err := encodedMessage(base)
		if err != nil {
			return traceReport{}, err
		}
		localSizes = append(localSizes, len(localBytes))
		for _, topology := range []string{"direct", "star", "tree"} {
			transmissions, err := hopsFor(topology, value.SourceGateway)
			if err != nil {
				return traceReport{}, err
			}
			if len(transmissions) != 3 {
				return traceReport{}, fmt.Errorf("%s %s: got %d hops, want 3", topology, value.MessageID, len(transmissions))
			}
			for _, transmission := range transmissions {
				forwarded := base
				forwarded.FromGateway = transmission.From
				serialized, err := encodedMessage(forwarded)
				if err != nil {
					return traceReport{}, err
				}
				if topology == "direct" {
					overlaySizes = append(overlaySizes, len(serialized))
				}
				node := demands[topology].Nodes[transmission.From]
				node.OverlayMessages++
				node.SerializedJSONBytes += int64(len(serialized))
				node.WebSocketFrameBytes += int64(websocketFrameSize(len(serialized), senderMasksFrame(topology, transmission)))
			}
		}
	}
	for _, gateway := range gateways {
		if expected, ok := entry.PublicationsByGateway[gateway]; ok && expected != published[gateway] {
			return traceReport{}, fmt.Errorf("%s: %s publications=%d, manifest=%d", path, gateway, published[gateway], expected)
		}
	}
	for _, topology := range []string{"direct", "star", "tree"} {
		demand := demands[topology]
		for _, gateway := range gateways {
			node := demand.Nodes[gateway]
			node.AverageJSONMbps = float64(node.SerializedJSONBytes*8) / duration / 1_000_000
			node.AverageWebSocketMbps = float64(node.WebSocketFrameBytes*8) / duration / 1_000_000
			demand.ClusterOverlayMessages += node.OverlayMessages
			demand.ClusterSerializedBytes += node.SerializedJSONBytes
			demand.ClusterWebSocketBytes += node.WebSocketFrameBytes
			if demand.BottleneckGateway == "" || node.AverageJSONMbps > demand.BottleneckJSONMbps {
				demand.BottleneckGateway = gateway
				demand.BottleneckJSONMbps = node.AverageJSONMbps
				demand.BottleneckWebSocketMbps = node.AverageWebSocketMbps
			}
		}
		demand.ClusterJSONMbps = float64(demand.ClusterSerializedBytes*8) / duration / 1_000_000
		demand.ClusterWebSocketMbps = float64(demand.ClusterWebSocketBytes*8) / duration / 1_000_000
	}
	ratePattern := regexp.MustCompile(`rate-(\d+)\.jsonl$`)
	targetRate := 0
	if match := ratePattern.FindStringSubmatch(entry.File); match != nil {
		targetRate, _ = strconv.Atoi(match[1])
	}
	return traceReport{
		Trace: entry.File, TargetRate: targetRate, ActualRate: float64(len(events)) / duration,
		DurationSeconds: duration, Messages: len(events), PublicationsByGateway: published,
		Payload: stats(payloadSizes), IngressJSON: stats(ingressSizes),
		LocalEnvelopeJSON: stats(localSizes), OverlayEnvelopeJSON: stats(overlaySizes),
		Topologies: demands,
	}, nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func writeCSV(path string, traces []traceReport) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	header := []string{
		"trace", "target_rate", "actual_rate", "mean_overlay_json_bytes", "topology", "gateway",
		"overlay_messages", "serialized_json_bytes", "websocket_frame_bytes", "average_json_mbps",
		"average_websocket_mbps", "bottleneck_gateway",
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, trace := range traces {
		for _, topology := range []string{"direct", "star", "tree"} {
			demand := trace.Topologies[topology]
			for _, gateway := range gateways {
				node := demand.Nodes[gateway]
				row := []string{
					trace.Trace, strconv.Itoa(trace.TargetRate), fmt.Sprintf("%.9f", trace.ActualRate),
					fmt.Sprintf("%.6f", trace.OverlayEnvelopeJSON.MeanBytes), topology, gateway,
					strconv.Itoa(node.OverlayMessages), strconv.FormatInt(node.SerializedJSONBytes, 10),
					strconv.FormatInt(node.WebSocketFrameBytes, 10), fmt.Sprintf("%.9f", node.AverageJSONMbps),
					fmt.Sprintf("%.9f", node.AverageWebSocketMbps), demand.BottleneckGateway,
				}
				if err := writer.Write(row); err != nil {
					return err
				}
			}
		}
	}
	return writer.Error()
}

func printSummary(traces []traceReport) {
	fmt.Println("Serialized message sizes (bytes)")
	fmt.Println("trace\tmessages\tpayload mean\tingress mean\tlocal mean\toverlay S mean\toverlay P95\toverlay max")
	for _, trace := range traces {
		fmt.Printf("%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t%d\n",
			trace.Trace, trace.Messages, trace.Payload.MeanBytes, trace.IngressJSON.MeanBytes,
			trace.LocalEnvelopeJSON.MeanBytes, trace.OverlayEnvelopeJSON.MeanBytes,
			trace.OverlayEnvelopeJSON.P95Bytes, trace.OverlayEnvelopeJSON.MaxBytes)
	}
	fmt.Println("\nAverage application-layer overlay demand (Mbps by sending gateway)")
	fmt.Println("trace\ttopology\tG0\tG1\tG2\tG3\tbottleneck")
	for _, trace := range traces {
		for _, topology := range []string{"direct", "star", "tree"} {
			demand := trace.Topologies[topology]
			fmt.Printf("%s\t%s\t%.6f\t%.6f\t%.6f\t%.6f\t%s %.6f\n",
				trace.Trace, topology,
				demand.Nodes["G0"].AverageJSONMbps, demand.Nodes["G1"].AverageJSONMbps,
				demand.Nodes["G2"].AverageJSONMbps, demand.Nodes["G3"].AverageJSONMbps,
				demand.BottleneckGateway, demand.BottleneckJSONMbps)
		}
	}
}

func run(manifestPath, output, runID string) error {
	if output == "" {
		return errors.New("-output is required")
	}
	if runID == "" {
		return errors.New("-run-id must not be empty")
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		return fmt.Errorf("output must not exist: %s", output)
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var bundle manifest
	if err := json.Unmarshal(data, &bundle); err != nil {
		return err
	}
	duration, err := strconv.ParseFloat(bundle.DurationSeconds, 64)
	if err != nil || duration <= 0 {
		return fmt.Errorf("invalid manifest duration_seconds %q", bundle.DurationSeconds)
	}
	base := filepath.Dir(manifestPath)
	result := report{
		SchemaVersion: 1, RunID: runID, RunIDBytes: len([]byte(runID)),
		Definitions: map[string]string{
			"S":                      "bytes in one JSON overlay WebSocket message payload after gateway metadata stamping",
			"average_json_mbps":      "serialized overlay JSON bytes times 8 divided by trace duration",
			"average_websocket_mbps": "JSON plus WebSocket data-frame header/mask; excludes TCP/IP, ACKs, retransmissions and TLS",
		},
		Limitations: []string{
			"The configured run ID affects S through its UTF-8 byte length; pass the actual summary run_id for exact replay accounting.",
			"WebSocket estimates exclude TCP/IP headers, ACKs, retransmissions, connection setup, ping/pong and TLS.",
			"Traffic-control counters remain the authority for actual bytes seen on eth1.",
		},
	}
	for _, entry := range bundle.Traces {
		trace, err := analyzeTrace(filepath.Join(base, entry.File), entry, runID, duration)
		if err != nil {
			return err
		}
		result.Traces = append(result.Traces, trace)
	}
	if len(result.Traces) == 0 {
		return errors.New("manifest contains no traces")
	}
	var overlayBytes float64
	var overlayCopies int
	for _, trace := range result.Traces {
		result.AnalyzedMessages += trace.Messages
		overlayBytes += trace.OverlayEnvelopeJSON.MeanBytes * float64(trace.OverlayEnvelopeJSON.Count)
		overlayCopies += trace.OverlayEnvelopeJSON.Count
	}
	result.AllTracesWeightedMeanSBytes = overlayBytes / float64(overlayCopies)
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(output, "overlay-demand.json"), result); err != nil {
		return err
	}
	if err := writeCSV(filepath.Join(output, "overlay-demand.csv"), result.Traces); err != nil {
		return err
	}
	printSummary(result.Traces)
	fmt.Printf("\nSaved %s and %s\n", filepath.Join(output, "overlay-demand.json"), filepath.Join(output, "overlay-demand.csv"))
	return nil
}

func main() {
	manifestPath := flag.String("manifest", "workloads/generated/pilot-v1/manifest.json", "workload manifest whose trace files are analyzed")
	output := flag.String("output", "", "new output directory for JSON and CSV")
	runID := flag.String("run-id", "run-0000000000000000000", "representative or actual loadgen run_id; byte length affects S")
	flag.Parse()
	if err := run(*manifestPath, *output, *runID); err != nil {
		fmt.Fprintln(os.Stderr, "overlaycalc:", err)
		os.Exit(1)
	}
}
