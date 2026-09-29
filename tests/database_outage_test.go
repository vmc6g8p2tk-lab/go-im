package tests

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A TCP proxy drops all PostgreSQL connections while the real database stays
// available to the test. This exercises an actual server-side DB network loss
// and recovery without stopping a shared PostgreSQL instance.
func TestDatabaseConnectionOutageAndRecovery(t *testing.T) {
	binary, databaseURL, secret := os.Getenv("INTEGRATION_SERVER_BINARY"), os.Getenv("DATABASE_URL"), os.Getenv("INTEGRATION_JWT_SECRET")
	if binary == "" || databaseURL == "" || secret == "" {
		t.Skip("set INTEGRATION_SERVER_BINARY, DATABASE_URL and INTEGRATION_JWT_SECRET")
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := newDBProxy(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	parsed.Host = proxy.listener.Addr().String()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	cmd := exec.Command(binary)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.Env = append(os.Environ(), "DATABASE_URL="+parsed.String(), "JWT_SECRET="+secret,
		"HTTP_ADDR="+address, "REDIS_ADDR=", "KAFKA_BROKERS=", "PPROF_ADDR=")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	base := "http://" + address
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(base + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("proxy-backed IM server did not become healthy")
		}
		time.Sleep(30 * time.Millisecond)
	}
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("outage_a%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("outage_b%d", suffix))
	ac := connect(t, "ws"+strings.TrimPrefix(base, "http")+"/v1/ws", a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, "ws"+strings.TrimPrefix(base, "http")+"/v1/ws", b.Token, b.User.ID)
	defer bc.Close()
	clientID := newClientMessageID(t)
	proxy.setAvailable(false)
	send := func() error {
		return ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID,
			"client_message_id": clientID, "body": "recover from DB outage"})
	}
	if err := send(); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrameWithin(ac, 8*time.Second); err != nil || frame.Type != "error" || frame.Code != "internal_error" {
		t.Fatalf("during DB outage frame=%+v err=%v", frame, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, clientID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed send persisted rows=%d err=%v", count, err)
	}
	proxy.setAvailable(true)
	var messageID int64
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := send(); err != nil {
			t.Fatal(err)
		}
		frame, err := readTypedFrameWithin(ac, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Type == "server_ack" {
			messageID = frame.MessageID
			break
		}
		if frame.Type != "error" || frame.Code != "internal_error" {
			t.Fatalf("recovery frame=%+v", frame)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if messageID <= 0 {
		t.Fatal("server did not recover after database connection returned")
	}
	if got, err := readMessageIDWithin(bc, 5*time.Second); err != nil || got != messageID {
		t.Fatalf("recipient after recovery id=%d want=%d err=%v", got, messageID, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, clientID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("recovered send persisted rows=%d err=%v", count, err)
	}
}

type dbProxy struct {
	listener  net.Listener
	upstream  string
	mu        sync.Mutex
	available bool
	active    map[net.Conn]struct{}
}

func newDBProxy(upstream string) (*dbProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &dbProxy{listener: listener, upstream: upstream, available: true, active: make(map[net.Conn]struct{})}
	go p.serve()
	return p, nil
}

func (p *dbProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.forward(client)
	}
}

func (p *dbProxy) forward(client net.Conn) {
	p.mu.Lock()
	available := p.available
	p.mu.Unlock()
	if !available {
		_ = client.Close()
		return
	}
	upstream, err := net.DialTimeout("tcp", p.upstream, time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if !p.available {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.active[client], p.active[upstream] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, upstream); done <- struct{}{} }()
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
	p.mu.Lock()
	delete(p.active, client)
	delete(p.active, upstream)
	p.mu.Unlock()
}

func (p *dbProxy) setAvailable(value bool) {
	p.mu.Lock()
	p.available = value
	if !value {
		for conn := range p.active {
			_ = conn.Close()
		}
	}
	p.mu.Unlock()
}

func (p *dbProxy) close() {
	_ = p.listener.Close()
	p.setAvailable(false)
}
