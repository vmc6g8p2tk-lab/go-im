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

	"github.com/jackc/pgx/v5/pgxpool"
)

// Set INTEGRATION_SERVER_BINARY, DATABASE_URL and INTEGRATION_JWT_SECRET.
// A separate server process is killed and restarted against the same database.
func TestIdempotencySurvivesServerRestart(t *testing.T) {
	binary, databaseURL, secret := os.Getenv("INTEGRATION_SERVER_BINARY"), os.Getenv("DATABASE_URL"), os.Getenv("INTEGRATION_JWT_SECRET")
	if binary == "" || databaseURL == "" || secret == "" {
		t.Skip("set INTEGRATION_SERVER_BINARY, DATABASE_URL and INTEGRATION_JWT_SECRET")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	start := func() *exec.Cmd {
		cmd := exec.Command(binary)
		cmd.Env = append(os.Environ(), "DATABASE_URL="+databaseURL, "JWT_SECRET="+secret, fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", port))
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			response, err := http.Get(base + "/healthz")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == 200 {
					return cmd
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("test server did not become healthy")
		return nil
	}
	stop := func(cmd *exec.Cmd) {
		_ = cmd.Process.Kill() // Deliberately simulate abrupt instance failure.
		_ = cmd.Wait()
	}
	server := start()
	defer func() {
		if server != nil {
			stop(server)
		}
	}()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("g%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("h%d", suffix))
	clientID := newClientMessageID(t)
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	firstConn := connect(t, url, a.Token, a.User.ID)
	if err := firstConn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "survives restart", "client_message_id": clientID}); err != nil {
		t.Fatal(err)
	}
	originalID, err := readMessageID(firstConn)
	if err != nil {
		t.Fatal(err)
	}
	stop(server)
	server = nil
	_ = firstConn.Close()
	server = start()
	retryConn := connect(t, url, a.Token, a.User.ID)
	defer retryConn.Close()
	if err := retryConn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "survives restart", "client_message_id": clientID}); err != nil {
		t.Fatal(err)
	}
	retryID, err := readMessageID(retryConn)
	if err != nil || retryID != originalID {
		t.Fatalf("retry after restart id=%d original=%d err=%v", retryID, originalID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, clientID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("after restart database count=%d err=%v", count, err)
	}
}
