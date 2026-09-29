package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VMC6G8P2TK-Lab/go-im/internal/auth"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/repository"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/service"
	imws "github.com/VMC6G8P2TK-Lab/go-im/internal/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHistoryAuthorization(t *testing.T) {
	base, _ := integrationEnv(t)
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("hist%d_a", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("hist%d_b", suffix))
	c := registerAndLogin(t, base, fmt.Sprintf("hist%d_c", suffix))
	conn := connect(t, "ws"+strings.TrimPrefix(base, "http")+"/v1/ws", a.Token, a.User.ID)
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID,
		"body": "private conversation", "client_message_id": newClientMessageID(t)}); err != nil {
		t.Fatal(err)
	}
	if _, err := readMessageID(conn); err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("%s/v1/messages?peer_id=%d", base, b.User.ID)
	response, err := http.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous status=%d", response.StatusCode)
	}
	// C is authenticated but cannot supply A's identity through a parameter.
	request, _ := http.NewRequest(http.MethodGet, endpoint+fmt.Sprintf("&user_id=%d", a.User.ID), nil)
	request.Header.Set("Authorization", "Bearer "+c.Token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(result.Messages) != 0 {
		t.Fatalf("third-party history status=%d messages=%d", response.StatusCode, len(result.Messages))
	}
}

func TestWebSocketRateLimitAndSenderSpoofRejected(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("set DATABASE_URL for PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &repository.Store{DB: pool}
	manager := auth.New([]byte(strings.Repeat("x", 32)))
	svc := &service.Service{Store: store, Auth: manager}
	server := httptest.NewServer(&imws.Server{Auth: manager, Service: svc, Hub: imws.NewHub(), ConnectionRate: 0.001, ConnectionBurst: 1})
	defer server.Close()
	suffix := time.Now().UnixNano()
	a, err := store.CreateUser(ctx, fmt.Sprintf("secure%d_a", suffix), "hash")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.CreateUser(ctx, fmt.Sprintf("secure%d_b", suffix), "hash")
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := manager.Issue(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	conn := connect(t, "ws"+strings.TrimPrefix(server.URL, "http"), token, a.ID)
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.ID,
		"sender_id": b.ID, "body": "spoof", "client_message_id": newClientMessageID(t)}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(conn); err != nil || frame.Type != "error" || frame.Code != "invalid_message" {
		t.Fatalf("spoof response=%+v err=%v", frame, err)
	}
	for i := 0; i < 2; i++ {
		if err := conn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.ID,
			"body": fmt.Sprintf("allowed %d", i), "client_message_id": newClientMessageID(t)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := readMessageID(conn); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(conn); err != nil || frame.Type != "error" || frame.Code != "rate_limited" {
		t.Fatalf("rate response=%+v err=%v", frame, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND recipient_id=$2`, a.ID, b.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("stored messages=%d err=%v", count, err)
	}
}
