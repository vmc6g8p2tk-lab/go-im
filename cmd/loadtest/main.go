// Command loadtest drives authenticated WebSocket clients against a running IM
// server. It reports measurements from this run only; no synthetic results.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type identity struct {
	User struct {
		ID int64 `json:"id"`
	} `json:"user"`
	Token string `json:"token"`
}

type frame struct {
	Type            string `json:"type"`
	Code            string `json:"code"`
	ClientMessageID string `json:"client_message_id"`
	MessageID       int64  `json:"message_id"`
	Message         struct {
		ID              int64  `json:"id"`
		ClientMessageID string `json:"client_message_id"`
	} `json:"message"`
}

type outgoing struct {
	Type            string `json:"type"`
	RecipientID     int64  `json:"recipient_id,omitempty"`
	ClientMessageID string `json:"client_message_id,omitempty"`
	Body            string `json:"body,omitempty"`
	MessageID       int64  `json:"message_id,omitempty"`
}

type client struct {
	conn   *websocket.Conn
	writes chan outgoing
	closed atomic.Bool
	seen   map[int64]struct{} // read goroutine only
}

type trace struct {
	sent      time.Time
	acked     bool
	delivered bool
}

type counters struct {
	mu                    sync.Mutex
	traces                map[string]*trace
	sent                  int
	serverACK             int
	uniqueDelivery        int
	deliveredDuringSend   int
	countUntil            time.Time
	duplicateFrame        int
	additionalDeviceFrame int
	failures              int
	disconnections        int
	latencies             []float64
	ackLatencies          []float64
	errors                map[string]int
}

type resourceSample struct {
	CPUSeconds float64 `json:"cpu_seconds,omitempty"`
	RSSBytes   float64 `json:"rss_bytes,omitempty"`
	Goroutines float64 `json:"goroutines,omitempty"`
}

type result struct {
	BaseURL                string         `json:"base_url"`
	StartedAt              time.Time      `json:"started_at"`
	DurationSeconds        float64        `json:"duration_seconds"`
	ConnectionsRequested   int            `json:"connections_requested"`
	ConnectionsSucceeded   int            `json:"connections_succeeded"`
	ConnectionSuccessRate  float64        `json:"connection_success_rate"`
	Users                  int            `json:"users"`
	TargetRate             int            `json:"target_rate_msg_per_sec"`
	Sent                   int            `json:"sent"`
	ServerACK              int            `json:"server_ack"`
	UniqueDelivered        int            `json:"unique_delivered"`
	DeliveredDuringSend    int            `json:"delivered_during_send"`
	DuplicateFrames        int            `json:"duplicate_frames"`
	AdditionalDeviceFrames int            `json:"additional_device_frames"`
	Failures               int            `json:"failures"`
	UnexpectedDisconnects  int            `json:"unexpected_disconnects"`
	Throughput             float64        `json:"delivered_msg_per_sec"`
	LatencySamples         int            `json:"latency_samples"`
	LatencyMeanMS          float64        `json:"delivery_mean_ms"`
	LatencyP50MS           float64        `json:"delivery_p50_ms"`
	LatencyP95MS           float64        `json:"delivery_p95_ms"`
	LatencyP99MS           float64        `json:"delivery_p99_ms"`
	ACKP95MS               float64        `json:"server_ack_p95_ms"`
	CPUPercent             *float64       `json:"server_cpu_percent,omitempty"`
	PeakRSSBytes           *float64       `json:"server_peak_rss_bytes,omitempty"`
	PeakGoroutines         *float64       `json:"server_peak_goroutines,omitempty"`
	Errors                 map[string]int `json:"errors"`
	Notes                  []string       `json:"notes"`
}

func main() {
	base := flag.String("base", "http://127.0.0.1:8080", "HTTP base URL")
	connections := flag.Int("connections", 100, "number of WebSocket connections")
	users := flag.Int("users", 10, "number of distinct authenticated users")
	rate := flag.Int("rate", 50, "target messages per second, globally")
	duration := flag.Duration("duration", 30*time.Second, "send duration")
	output := flag.String("output", "", "optional JSON output path")
	flag.Parse()
	if *connections < 2 || *users < 2 || *users > *connections || *rate < 1 || *duration < time.Second || *rate > 10000 {
		fatal(errors.New("require connections>=2, 2<=users<=connections, 1<=rate<=10000, duration>=1s"))
	}
	if err := run(strings.TrimRight(*base, "/"), *connections, *users, *rate, *duration, *output); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func run(base string, connectionCount, userCount, rate int, duration time.Duration, output string) error {
	parsed, err := url.Parse(base)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("base must be an absolute http(s) URL")
	}
	wsURL := *parsed
	if parsed.Scheme == "https" {
		wsURL.Scheme = "wss"
	} else {
		wsURL.Scheme = "ws"
	}
	wsURL.Path = "/v1/ws"
	transport := &http.Client{Timeout: 10 * time.Second}
	runID := randomID()
	identities := make([]identity, userCount)
	for i := range identities {
		username := fmt.Sprintf("load_%s_%d", runID[:12], i)
		if len(username) > 32 {
			username = username[:32]
		}
		if err := createIdentity(transport, base, username, &identities[i]); err != nil {
			return fmt.Errorf("prepare user %d: %w", i, err)
		}
	}
	stats := &counters{traces: make(map[string]*trace), errors: make(map[string]int)}
	clients := make([]*client, 0, connectionCount)
	var workers sync.WaitGroup
	defer func() {
		for _, c := range clients {
			c.closed.Store(true)
			_ = c.conn.Close()
		}
		workers.Wait()
	}()
	for i := 0; i < connectionCount; i++ {
		id := identities[i%userCount]
		conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
		if err != nil {
			return fmt.Errorf("connect %d/%d: %w", i+1, connectionCount, err)
		}
		if err := conn.WriteJSON(map[string]string{"type": "auth", "token": id.Token}); err != nil {
			_ = conn.Close()
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var auth frame
		if err := conn.ReadJSON(&auth); err != nil || auth.Type != "auth.ok" {
			_ = conn.Close()
			return fmt.Errorf("authenticate connection %d: frame=%s err=%v", i+1, auth.Type, err)
		}
		_ = conn.SetReadDeadline(time.Time{})
		c := &client{conn: conn, writes: make(chan outgoing, 256), seen: make(map[int64]struct{})}
		clients = append(clients, c)
		workers.Add(2)
		go func() { defer workers.Done(); writer(c, stats) }()
		go func() { defer workers.Done(); reader(c, stats) }()
	}
	baseline, _ := scrape(base)
	peak := baseline
	started := time.Now()
	deadline := started.Add(duration)
	stats.mu.Lock()
	stats.countUntil = deadline
	stats.mu.Unlock()
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	monitorDone := make(chan struct{})
	var monitor sync.WaitGroup
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-monitorDone:
				return
			case <-t.C:
				s, err := scrape(base)
				if err == nil {
					if s.RSSBytes > peak.RSSBytes {
						peak.RSSBytes = s.RSSBytes
					}
					if s.Goroutines > peak.Goroutines {
						peak.Goroutines = s.Goroutines
					}
				}
			}
		}
	}()
	sendIndex := 0
sendLoop:
	for {
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				break sendLoop
			}
			sender := sendIndex % len(clients)
			recipient := identities[(sender%userCount+1)%userCount].User.ID
			msg := outgoing{Type: "message.send", RecipientID: recipient, ClientMessageID: uuid(), Body: "load test message"}
			select {
			case clients[sender].writes <- msg:
			default:
				stats.mu.Lock()
				stats.failures++
				stats.errors["client_write_queue_full"]++
				stats.mu.Unlock()
			}
			sendIndex++
		case <-time.After(time.Until(deadline)):
			break sendLoop
		}
	}
	ended := time.Now()
	// Allow an explicit, bounded drain interval for in-flight ACKs and delivery.
	time.Sleep(3 * time.Second)
	close(monitorDone)
	monitor.Wait()
	final, _ := scrape(base)
	if final.RSSBytes > peak.RSSBytes {
		peak.RSSBytes = final.RSSBytes
	}
	if final.Goroutines > peak.Goroutines {
		peak.Goroutines = final.Goroutines
	}
	stats.mu.Lock()
	latencies := append([]float64(nil), stats.latencies...)
	ackLatencies := append([]float64(nil), stats.ackLatencies...)
	errorsCopy := make(map[string]int, len(stats.errors))
	for k, v := range stats.errors {
		errorsCopy[k] = v
	}
	result := result{BaseURL: base, StartedAt: started, DurationSeconds: ended.Sub(started).Seconds(),
		ConnectionsRequested: connectionCount, ConnectionsSucceeded: len(clients), ConnectionSuccessRate: float64(len(clients)) / float64(connectionCount), Users: userCount, TargetRate: rate,
		Sent: stats.sent, ServerACK: stats.serverACK, UniqueDelivered: stats.uniqueDelivery,
		DeliveredDuringSend: stats.deliveredDuringSend,
		DuplicateFrames:     stats.duplicateFrame, AdditionalDeviceFrames: stats.additionalDeviceFrame, Failures: stats.failures, UnexpectedDisconnects: stats.disconnections,
		LatencySamples: len(latencies), Errors: errorsCopy,
		Notes: []string{"delivery latency measures immediately before client WriteJSON to first recipient message.new; repeat frames on the same socket are excluded", "additional_device_frames counts first copies on other sockets of the same user", "throughput counts first deliveries during the send interval divided by send duration; a 3-second drain contributes to unique_delivered but not throughput", "CPU and RSS are server process metrics from /metrics when available; load generator and PostgreSQL are excluded"}}
	stats.mu.Unlock()
	result.Throughput = float64(result.DeliveredDuringSend) / result.DurationSeconds
	result.LatencyMeanMS, result.LatencyP50MS, result.LatencyP95MS, result.LatencyP99MS = summarize(latencies)
	_, _, result.ACKP95MS, _ = summarize(ackLatencies)
	if final.CPUSeconds >= baseline.CPUSeconds && final.CPUSeconds > 0 {
		cpu := (final.CPUSeconds - baseline.CPUSeconds) / time.Since(started).Seconds() * 100
		result.CPUPercent = &cpu
	}
	if peak.RSSBytes > 0 {
		result.PeakRSSBytes = &peak.RSSBytes
	}
	if peak.Goroutines > 0 {
		result.PeakGoroutines = &peak.Goroutines
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if output != "" {
		if err := os.WriteFile(output, append(data, '\n'), 0600); err != nil {
			return err
		}
	}
	fmt.Println(string(data))
	return nil
}

func createIdentity(client *http.Client, base, username string, out *identity) error {
	body, _ := json.Marshal(map[string]string{"username": username, "password": "load-password-123"})
	for _, endpoint := range []string{"register", "login"} {
		for attempt := 0; ; attempt++ {
			request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/v1/auth/"+endpoint, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			if err != nil {
				return err
			}
			if response.StatusCode == http.StatusTooManyRequests && attempt < 30 {
				_ = response.Body.Close()
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if endpoint == "register" && response.StatusCode != http.StatusCreated || endpoint == "login" && response.StatusCode != http.StatusOK {
				limited, _ := io.ReadAll(io.LimitReader(response.Body, 256))
				_ = response.Body.Close()
				return fmt.Errorf("%s status %d: %s", endpoint, response.StatusCode, limited)
			}
			if endpoint == "login" {
				err = json.NewDecoder(response.Body).Decode(out)
				_ = response.Body.Close()
				if err != nil {
					return err
				}
				if out.User.ID <= 0 || out.Token == "" {
					return errors.New("login response missing user or token")
				}
			} else {
				_ = response.Body.Close()
			}
			break
		}
	}
	return nil
}

func writer(c *client, stats *counters) {
	for {
		select {
		case msg := <-c.writes:
			if c.closed.Load() {
				return
			}
			var sentAt time.Time
			if msg.Type == "message.send" {
				sentAt = time.Now()
				stats.mu.Lock()
				stats.traces[msg.ClientMessageID] = &trace{sent: sentAt}
				stats.mu.Unlock()
			}
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.conn.WriteJSON(msg); err != nil {
				stats.mu.Lock()
				stats.failures++
				stats.errors["write_error"]++
				stats.mu.Unlock()
				c.closed.Store(true)
				_ = c.conn.Close()
				return
			}
			if !sentAt.IsZero() {
				stats.mu.Lock()
				stats.sent++
				stats.mu.Unlock()
			}
		case <-time.After(100 * time.Millisecond):
			if c.closed.Load() {
				return
			}
		}
	}
}

func reader(c *client, stats *counters) {
	for {
		var f frame
		if err := c.conn.ReadJSON(&f); err != nil {
			if !c.closed.Swap(true) {
				stats.mu.Lock()
				stats.disconnections++
				stats.errors["read_error"]++
				stats.mu.Unlock()
			}
			_ = c.conn.Close()
			return
		}
		now := time.Now()
		stats.mu.Lock()
		switch f.Type {
		case "server_ack":
			if tr := stats.traces[f.ClientMessageID]; tr != nil && !tr.acked {
				tr.acked = true
				stats.serverACK++
				stats.ackLatencies = append(stats.ackLatencies, now.Sub(tr.sent).Seconds()*1000)
			}
		case "message.new":
			if tr := stats.traces[f.Message.ClientMessageID]; tr != nil {
				if _, repeated := c.seen[f.Message.ID]; repeated {
					stats.duplicateFrame++
				} else {
					c.seen[f.Message.ID] = struct{}{}
					if tr.delivered {
						stats.additionalDeviceFrame++
					} else {
						tr.delivered = true
						stats.uniqueDelivery++
						if now.Before(stats.countUntil) {
							stats.deliveredDuringSend++
						}
						stats.latencies = append(stats.latencies, now.Sub(tr.sent).Seconds()*1000)
						select {
						case c.writes <- outgoing{Type: "delivery.ack", MessageID: f.Message.ID}:
						default:
							stats.failures++
							stats.errors["ack_queue_full"]++
						}
					}
				}
			}
		case "error":
			stats.failures++
			stats.errors[f.Code]++
		}
		stats.mu.Unlock()
	}
}

func scrape(base string) (resourceSample, error) {
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(base + "/metrics")
	if err != nil {
		return resourceSample{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return resourceSample{}, fmt.Errorf("metrics status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return resourceSample{}, err
	}
	var sample resourceSample
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconvParse(fields[1])
		if err != nil {
			continue
		}
		switch fields[0] {
		case "process_cpu_seconds_total":
			sample.CPUSeconds = value
		case "process_resident_memory_bytes":
			sample.RSSBytes = value
		case "go_goroutines":
			sample.Goroutines = value
		}
	}
	return sample, nil
}

func strconvParse(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

func summarize(samples []float64) (mean, p50, p95, p99 float64) {
	if len(samples) == 0 {
		return
	}
	sort.Float64s(samples)
	for _, sample := range samples {
		mean += sample
	}
	mean /= float64(len(samples))
	quantile := func(q float64) float64 {
		index := int(math.Ceil(float64(len(samples))*q)) - 1
		if index < 0 {
			index = 0
		}
		return samples[index]
	}
	return mean, quantile(.50), quantile(.95), quantile(.99)
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		fatal(err)
	}
	return hex.EncodeToString(value[:])
}

func uuid() string {
	value := randomID()
	return value[:8] + "-" + value[8:12] + "-4" + value[13:16] + "-a" + value[17:20] + "-" + value[20:]
}
