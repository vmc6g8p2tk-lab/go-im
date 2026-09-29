package tests

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/presence"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Real separate OS processes, real PostgreSQL, simulated Redis, no Kafka.
// The database retry loop provides the cross-process recovery path here.
func TestTwoProcessesDatabaseFallbackAndReconnect(t *testing.T) {
	binary := os.Getenv("INTEGRATION_SERVER_BINARY")
	secret := os.Getenv("INTEGRATION_JWT_SECRET")
	databaseURL := os.Getenv("DATABASE_URL")
	if binary == "" || secret == "" || databaseURL == "" {
		t.Skip("set INTEGRATION_SERVER_BINARY, INTEGRATION_JWT_SECRET and DATABASE_URL")
	}
	redisServer := miniredis.RunT(t)
	redisStore := presence.New(redisServer.Addr(), "")
	defer redisStore.Close()
	start := func(instance string) (string, *exec.Cmd) {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		cmd := exec.Command(binary)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmd.Env = append(os.Environ(), "DATABASE_URL="+databaseURL, "JWT_SECRET="+secret,
			"HTTP_ADDR="+address, "REDIS_ADDR="+redisServer.Addr(), "INSTANCE_ID="+instance, "KAFKA_BROKERS=")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})
		base := "http://" + address
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			response, err := http.Get(base + "/healthz")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return base, cmd
				}
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatal("process did not become healthy: " + instance)
		return "", nil
	}
	baseA, _ := start("process-a")
	baseB, processB := start("process-b")
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, baseA, fmt.Sprintf("p%d", suffix))
	b := registerAndLogin(t, baseB, fmt.Sprintf("q%d", suffix))
	ac := connect(t, "ws"+strings.TrimPrefix(baseA, "http")+"/v1/ws", a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, "ws"+strings.TrimPrefix(baseB, "http")+"/v1/ws", b.Token, b.User.ID)
	defer bc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	routes, err := redisStore.Instances(ctx, b.User.ID)
	if err != nil || len(routes) != 1 || routes[0] != "process-b" {
		t.Fatalf("initial route=%v err=%v", routes, err)
	}
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID,
		"client_message_id": newClientMessageID(t), "body": "first cross-process"}); err != nil {
		t.Fatal(err)
	}
	firstID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readMessageIDWithin(bc, 10*time.Second); err != nil || got != firstID {
		t.Fatalf("cross-process ID=%d want=%d err=%v", got, firstID, err)
	}
	if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": firstID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(bc); err != nil || frame.Type != "delivery.ack.ok" {
		t.Fatalf("ACK=%+v err=%v", frame, err)
	}
	if err := processB.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = processB.Wait()
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID,
		"client_message_id": newClientMessageID(t), "body": "after process crash"}); err != nil {
		t.Fatal(err)
	}
	secondID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	reconnected := connect(t, "ws"+strings.TrimPrefix(baseA, "http")+"/v1/ws", b.Token, b.User.ID)
	defer reconnected.Close()
	if got, err := readMessageID(reconnected); err != nil || got != secondID {
		t.Fatalf("reconnected ID=%d want=%d err=%v", got, secondID, err)
	}
	routes, err = redisStore.Instances(ctx, b.User.ID)
	if err != nil || !containsString(routes, "process-a") {
		t.Fatalf("route after reconnect=%v err=%v", routes, err)
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE id IN ($1,$2)`, firstID, secondID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("persisted rows=%d err=%v", count, err)
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
