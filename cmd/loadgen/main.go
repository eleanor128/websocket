// Loadgen replays offline workload bundles using independent WebSocket clients.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	ID      string `json:"client_id"`
	Gateway string `json:"gateway_id"`
}
type Bundle struct {
	Clients []Client            `json:"clients"`
	Sets    map[string][]string `json:"recipient_sets"`
}
type Event struct {
	ID      string `json:"message_id"`
	Offset  int64  `json:"scheduled_offset_ns"`
	Sender  string `json:"sender_id"`
	Payload string `json:"payload"`
	Bytes   int    `json:"payload_bytes"`
	Gateway string `json:"source_gateway"`
	Set     string `json:"recipient_set_id"`
}
type Wire struct {
	Run     string `json:"run_id"`
	ID      string `json:"message_id"`
	Sender  string `json:"sender_id"`
	Payload string `json:"payload"`
}
type Send struct {
	ID        string `json:"message_id"`
	Scheduled int64  `json:"scheduled_ns"`
	Start     int64  `json:"actual_start_ns"`
	End       int64  `json:"write_end_ns"`
	Error     string `json:"error,omitempty"`
}
type Receipt struct {
	ID     string `json:"message_id"`
	Client string `json:"recipient_id"`
	At     int64  `json:"received_ns"`
	Copies int    `json:"copies"`
}
type options struct {
	endpoint, clients, trace, output  string
	gateways                          string
	duration, drain, settle, deadline time.Duration
	queue                             int
}

type GatewayStats struct {
	Clients     int `json:"clients"`
	Scheduled   int `json:"scheduled_messages"`
	Successful  int `json:"successful_writes"`
	Failed      int `json:"failed_or_unsent"`
	Expected    int `json:"expected_deliveries"`
	Unique      int `json:"unique_deliveries"`
	Missing     int `json:"missing"`
	Duplicates  int `json:"duplicates"`
	Disconnects int `json:"disconnects"`
}

func parseEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("invalid WebSocket endpoint %q", raw)
	}
	return u, nil
}

// Resolve and validate every destination before opening any connections.
func destinations(o options, b Bundle) (map[string]*url.URL, map[string]string, string, error) {
	urls := map[string]*url.URL{}
	configured := map[string]string{}
	if o.gateways == "" {
		raw := o.endpoint
		if raw == "" {
			raw = "ws://localhost:8080/ws"
		}
		u, err := parseEndpoint(raw)
		if err != nil {
			return nil, nil, "", err
		}
		for _, c := range b.Clients {
			urls[c.ID] = u
		}
		configured["single"] = raw
		return urls, configured, "single", nil
	}
	if o.endpoint != "" {
		return nil, nil, "", fmt.Errorf("-url and -gateways are mutually exclusive")
	}
	if err := readJSON(o.gateways, &configured); err != nil {
		return nil, nil, "", err
	}
	if len(configured) != 4 {
		return nil, nil, "", fmt.Errorf("gateway configuration must contain exactly G0, G1, G2, G3")
	}
	parsed := map[string]*url.URL{}
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("G%d", i)
		u, err := parseEndpoint(configured[id])
		if err != nil {
			return nil, nil, "", fmt.Errorf("%s: %w", id, err)
		}
		canonical := *u
		q := canonical.Query()
		q.Del("name")
		canonical.RawQuery = q.Encode()
		if seen[canonical.String()] {
			return nil, nil, "", fmt.Errorf("gateways must use distinct endpoints (ignoring client name)")
		}
		seen[canonical.String()] = true
		parsed[id] = u
	}
	counts := map[string]int{}
	for _, c := range b.Clients {
		u, ok := parsed[c.Gateway]
		if !ok {
			return nil, nil, "", fmt.Errorf("client %s has unknown gateway %q", c.ID, c.Gateway)
		}
		urls[c.ID] = u
		counts[c.Gateway]++
	}
	for id := range parsed {
		if counts[id] == 0 || counts[id]*4 != len(b.Clients) {
			return nil, nil, "", fmt.Errorf("four-gateway mode requires equal nonzero client counts on G0-G3")
		}
	}
	return urls, configured, "four-gateway", nil
}

func readJSON(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func hash(path string) string {
	b, _ := os.ReadFile(path)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func save(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}
func load(o options) (Bundle, []Event, error) {
	var b Bundle
	if e := readJSON(o.clients, &b); e != nil {
		return b, nil, e
	}
	ids := map[string]string{}
	for _, c := range b.Clients {
		if c.ID == "" {
			return b, nil, fmt.Errorf("empty client ID")
		}
		if _, ok := ids[c.ID]; ok {
			return b, nil, fmt.Errorf("duplicate client")
		}
		ids[c.ID] = c.Gateway
	}
	if len(ids) == 0 {
		return b, nil, fmt.Errorf("no clients")
	}
	for _, set := range b.Sets {
		seen := map[string]bool{}
		for _, id := range set {
			if _, ok := ids[id]; !ok || seen[id] {
				return b, nil, fmt.Errorf("invalid recipient set")
			}
			seen[id] = true
		}
	}
	f, e := os.Open(o.trace)
	if e != nil {
		return b, nil, e
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 4096), 2*1024*1024)
	var events []Event
	seen := map[string]bool{}
	var last int64 = -1
	for s.Scan() {
		var v Event
		if e := json.Unmarshal(s.Bytes(), &v); e != nil {
			return b, nil, e
		}
		g, ok := ids[v.Sender]
		if !ok || g != v.Gateway || v.ID == "" || seen[v.ID] || v.Offset < last || v.Offset < 0 || v.Offset >= int64(o.duration) || len(b.Sets[v.Set]) == 0 || len([]byte(v.Payload)) != v.Bytes {
			return b, nil, fmt.Errorf("invalid trace event %q", v.ID)
		}
		seen[v.ID] = true
		last = v.Offset
		events = append(events, v)
	}
	if e := s.Err(); e != nil {
		return b, nil, e
	}
	if len(events) == 0 {
		return b, nil, fmt.Errorf("empty trace")
	}
	return b, events, nil
}
func percentile(v []int64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return float64(v[int(math.Ceil(p*float64(len(v))))-1]) / 1e6
}

func run(o options) error {
	if o.duration <= 0 || o.drain < 0 || o.settle < 0 || o.deadline <= 0 || o.queue < 1 {
		return fmt.Errorf("invalid duration/queue configuration")
	}
	b, events, e := load(o)
	if e != nil {
		return e
	}
	dests, configured, mode, e := destinations(o, b)
	if e != nil {
		return e
	}
	// Refuse overwriting previous results, including partial/failed runs.
	if _, e = os.Stat(o.output); !os.IsNotExist(e) {
		return fmt.Errorf("output must not exist: %s", o.output)
	}
	if e = os.MkdirAll(o.output, 0755); e != nil {
		return e
	}
	runID := fmt.Sprintf("run-%d", time.Now().UnixNano())
	var mu sync.Mutex
	conns := map[string]*websocket.Conn{}
	queues := map[string]chan int{}
	sends := make([]Send, len(events))
	received := map[string]map[string]*Receipt{}
	index := map[string]int{}
	allowed := map[string]map[string]bool{}
	for name, set := range b.Sets {
		allowed[name] = map[string]bool{}
		for _, id := range set {
			allowed[name][id] = true
		}
	}
	for i, v := range events {
		index[v.ID] = i
		sends[i] = Send{ID: v.ID, Scheduled: v.Offset, Start: -1, End: -1}
	}
	var readers, writers sync.WaitGroup
	stopping := false
	unexpected, disconnects := 0, 0
	unexpectedReasons := map[string]int{}
	disconnectReasons := map[string]string{}
	stats := map[string]*GatewayStats{}
	clientGateway := map[string]string{}
	for _, c := range b.Clients {
		g := c.Gateway
		if mode == "single" {
			g = "single"
		}
		clientGateway[c.ID] = g
		if stats[g] == nil {
			stats[g] = &GatewayStats{}
		}
		stats[g].Clients++
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for _, client := range b.Clients {
		u := *dests[client.ID]
		q := u.Query()
		q.Set("name", client.ID)
		u.RawQuery = q.Encode()
		d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
		c, _, err := d.Dial(u.String(), nil)
		if err != nil {
			save(filepath.Join(o.output, "failure.json"), map[string]any{"run_id": runID, "error": err.Error(), "connected_clients": len(conns), "client_id": client.ID, "gateway_id": clientGateway[client.ID], "endpoint": dests[client.ID].String(), "mode": mode})
			return fmt.Errorf("connect %s: %w", client.ID, err)
		}
		c.SetReadLimit(2 * 1024 * 1024)
		conns[client.ID] = c
		queues[client.ID] = make(chan int, o.queue)
	}
	// Immutable future start, shared monotonic clock for all send/receive timestamps.
	start := time.Now().Add(o.settle)
	cutoff := o.duration + o.drain
	for _, client := range b.Clients {
		id := client.ID
		c := conns[id]
		q := queues[id]
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				_, data, err := c.ReadMessage()
				at := time.Since(start)
				if err != nil {
					mu.Lock()
					if !stopping {
						disconnects++
						stats[clientGateway[id]].Disconnects++
						disconnectReasons[id] = err.Error()
					}
					mu.Unlock()
					return
				}
				var w Wire
				err = json.Unmarshal(data, &w)
				mu.Lock()
				i, ok := index[w.ID]
				if err != nil || w.Run != runID || !ok {
					unexpected++
					if err != nil {
						unexpectedReasons["invalid_json"]++
					} else if w.Run != runID {
						unexpectedReasons["wrong_run"]++
					} else {
						unexpectedReasons["unknown_message_id"]++
					}
					mu.Unlock()
					continue
				}
				ev := events[i]
				if !allowed[ev.Set][id] || w.Payload != ev.Payload || w.Sender != ev.Sender || at < 0 || at >= cutoff {
					unexpected++
					unexpectedReasons["recipient_payload_sender_or_time_mismatch"]++
					mu.Unlock()
					continue
				}
				if received[w.ID] == nil {
					received[w.ID] = map[string]*Receipt{}
				}
				if r := received[w.ID][id]; r != nil {
					r.Copies++
				} else {
					received[w.ID][id] = &Receipt{ID: w.ID, Client: id, At: int64(at), Copies: 1}
				}
				mu.Unlock()
			}
		}()
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range q {
				v := events[i]
				at := time.Since(start)
				if at >= o.duration {
					mu.Lock()
					sends[i].Error = "measurement ended before write"
					mu.Unlock()
					continue
				}
				mu.Lock()
				sends[i].Start = int64(at)
				mu.Unlock()
				until := time.Now().Add(o.deadline)
				if end := start.Add(o.duration); until.After(end) {
					until = end
				}
				c.SetWriteDeadline(until)
				err := c.WriteJSON(Wire{Run: runID, ID: v.ID, Sender: v.Sender, Payload: v.Payload})
				mu.Lock()
				sends[i].End = int64(time.Since(start))
				if err != nil {
					sends[i].Error = err.Error()
				}
				mu.Unlock()
			}
		}()
	}
	for i, v := range events {
		if wait := time.Until(start.Add(time.Duration(v.Offset))); wait > 0 {
			time.Sleep(wait)
		}
		select {
		case queues[v.Sender] <- i:
		default:
			mu.Lock()
			sends[i].Error = "sender queue full"
			mu.Unlock()
		}
	}
	for _, q := range queues {
		close(q)
	}
	writers.Wait()
	if wait := time.Until(start.Add(cutoff)); wait > 0 {
		time.Sleep(wait)
	}
	mu.Lock()
	stopping = true
	mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	readers.Wait()
	var expected, unique, onTime, late, duplicates, inWindow, success, failed int
	var latencies, lags, actualLatencies []int64
	receiptFile, err := os.Create(filepath.Join(o.output, "receipts.jsonl"))
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(receiptFile)
	enc := json.NewEncoder(bw)
	for i, v := range events {
		expected += len(b.Sets[v.Set])
		sourceStats := stats[clientGateway[v.Sender]]
		sourceStats.Scheduled++
		s := sends[i]
		if s.Start >= 0 {
			lags = append(lags, s.Start-s.Scheduled)
		}
		if s.Error == "" && s.Start >= 0 {
			success++
			sourceStats.Successful++
		} else {
			failed++
			sourceStats.Failed++
		}
		for _, id := range b.Sets[v.Set] {
			destStats := stats[clientGateway[id]]
			destStats.Expected++
			r := received[v.ID][id]
			if r == nil {
				destStats.Missing++
				continue
			}
			unique++
			destStats.Unique++
			destStats.Duplicates += r.Copies - 1
			duplicates += r.Copies - 1
			lat := r.At - v.Offset
			latencies = append(latencies, lat)
			if s.Start >= 0 {
				actualLatencies = append(actualLatencies, r.At-s.Start)
			}
			if lat <= int64(time.Second) {
				onTime++
			} else {
				late++
			}
			if r.At < int64(o.duration) {
				inWindow++
			}
			if err = enc.Encode(r); err != nil {
				receiptFile.Close()
				return err
			}
		}
	}
	if err = bw.Flush(); err != nil {
		receiptFile.Close()
		return err
	}
	if err = receiptFile.Close(); err != nil {
		return err
	}
	if err = save(filepath.Join(o.output, "sends.json"), sends); err != nil {
		return err
	}
	summary := map[string]any{"run_id": runID, "endpoint": o.endpoint, "clients": len(b.Clients), "trace_sha256": hash(o.trace), "clients_sha256": hash(o.clients), "duration_seconds": o.duration.Seconds(), "drain_seconds": o.drain.Seconds(), "settle_seconds": o.settle.Seconds(), "sender_queue_capacity": o.queue, "write_timeout_seconds": o.deadline.Seconds(), "scheduled_messages": len(events), "successful_writes": success, "failed_or_unsent": failed, "scheduled_messages_per_second": float64(len(events)) / o.duration.Seconds(), "successful_writes_per_second": float64(success) / o.duration.Seconds(), "expected_deliveries": expected, "unique_deliveries": unique, "on_time": onTime, "late": late, "missing": expected - unique, "duplicates": duplicates, "unexpected_messages": unexpected, "disconnects": disconnects, "on_time_fraction": float64(onTime) / float64(expected), "unique_deliveries_per_second": float64(inWindow) / o.duration.Seconds(), "drain_unique_deliveries": unique - inWindow, "scheduled_latency_p50_ms": percentile(latencies, .5), "scheduled_latency_p95_ms": percentile(latencies, .95), "scheduled_latency_p99_ms": percentile(latencies, .99), "actual_send_latency_p95_ms": percentile(actualLatencies, .95), "send_lag_p95_ms": percentile(lags, .95), "send_lag_max_ms": percentile(lags, 1), "latency_sample_count": len(latencies), "transport_clean": failed == 0 && disconnects == 0 && unexpected == 0, "warmup": "connection settling only; no message warmup"}
	summary["mode"] = mode
	summary["endpoints"] = configured
	summary["gateway_stats"] = stats
	summary["unexpected_reasons"] = unexpectedReasons
	summary["disconnect_errors"] = disconnectReasons
	if mode == "single" {
		summary["endpoint"] = configured["single"]
	} else {
		delete(summary, "endpoint")
	}
	if err = save(filepath.Join(o.output, "summary.json"), summary); err != nil {
		return err
	}
	fmt.Printf("%s: %d/%d unique deliveries, %d missing, %d duplicates, %d failed sends\n", o.output, unique, expected, expected-unique, duplicates, failed)
	return nil
}
func main() {
	var o options
	flag.StringVar(&o.endpoint, "url", "", "single server WebSocket endpoint (default ws://localhost:8080/ws)")
	flag.StringVar(&o.gateways, "gateways", "", "JSON file mapping G0-G3 to WebSocket endpoints; mutually exclusive with -url")
	flag.StringVar(&o.clients, "clients", "workloads/generated/pilot-v1/clients.json", "client table")
	flag.StringVar(&o.trace, "trace", "workloads/generated/pilot-v1/rate-10.jsonl", "JSONL trace")
	flag.StringVar(&o.output, "output", "results/run", "new results directory")
	flag.DurationVar(&o.duration, "duration", 60*time.Second, "measurement window (must match trace)")
	flag.DurationVar(&o.drain, "drain", 10*time.Second, "post-publication receive window")
	flag.DurationVar(&o.settle, "settle", time.Second, "connection settling time")
	flag.DurationVar(&o.deadline, "write-timeout", 5*time.Second, "maximum write wait")
	flag.IntVar(&o.queue, "queue", 256, "bounded queue per sender")
	flag.Parse()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
