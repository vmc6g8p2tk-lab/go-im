package tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMessageIdempotency(t *testing.T) {
	base, databaseURL := os.Getenv("INTEGRATION_BASE_URL"), os.Getenv("DATABASE_URL")
	if base == "" || databaseURL == "" {
		t.Skip("set INTEGRATION_BASE_URL and DATABASE_URL for end-to-end test")
	}
	base = strings.TrimRight(base, "/")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("e%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("f%d", suffix))
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, url, a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, url, b.Token, b.User.ID)
	firstClientID := newClientMessageID(t)
	var originalID int64
	for i := range 3 {
		if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "send three times", "client_message_id": firstClientID}); err != nil {
			t.Fatal(err)
		}
		id, err := readMessageID(ac)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			originalID = id
		} else if id != originalID {
			t.Fatalf("retry returned id=%d, original=%d", id, originalID)
		}
	}
	if id, err := readMessageID(bc); err != nil || id != originalID {
		t.Fatalf("recipient delivery id=%d err=%v", id, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, firstClientID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("three sends created count=%d err=%v", count, err)
	}
	// Reusing the same ID for different content must not silently return success.
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "different body", "client_message_id": firstClientID}); err != nil {
		t.Fatal(err)
	}
	var conflict struct {
		Type string `json:"type"`
		Code string `json:"code"`
	}
	_ = ac.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ac.ReadJSON(&conflict); err != nil || conflict.Type != "error" || conflict.Code != "idempotency_conflict" {
		t.Fatalf("expected idempotency conflict, got %+v err=%v", conflict, err)
	}
	_ = bc.Close()

	// Eight connections of the same user race to create one logical message.
	const workers = 8
	secondClientID := newClientMessageID(t)
	receiver := connect(t, url, b.Token, b.User.ID)
	defer receiver.Close()
	senders := make([]*websocket.Conn, workers)
	for i := range senders {
		senders[i] = connect(t, url, a.Token, a.User.ID)
		defer senders[i].Close()
	}
	var wg sync.WaitGroup
	results := make(chan int64, workers)
	errors := make(chan error, workers)
	for _, conn := range senders {
		wg.Add(1)
		go func(conn *websocket.Conn) {
			defer wg.Done()
			if err := conn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "concurrent retry", "client_message_id": secondClientID}); err != nil {
				errors <- err
				return
			}
			id, err := readMessageID(conn)
			if err != nil {
				errors <- err
				return
			}
			results <- id
		}(conn)
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	var concurrentID int64
	for id := range results {
		if concurrentID == 0 {
			concurrentID = id
		} else if concurrentID != id {
			t.Errorf("concurrent retry returned %d and %d", concurrentID, id)
		}
	}
	if concurrentID == 0 {
		t.Fatal("no successful concurrent send")
	}
	if id, err := readMessageID(receiver); err != nil || id != concurrentID {
		t.Fatalf("concurrent recipient delivery id=%d err=%v", id, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, secondClientID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("concurrent sends created count=%d err=%v", count, err)
	}
}

func newClientMessageID(t *testing.T) string {
	t.Helper()
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}

func readMessageID(conn *websocket.Conn) (int64, error) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var frame struct {
		Type    string `json:"type"`
		Message struct {
			ID int64 `json:"id"`
		} `json:"message"`
	}
	if err := conn.ReadJSON(&frame); err != nil {
		return 0, err
	}
	if frame.Type != "message.new" || frame.Message.ID <= 0 {
		return 0, fmt.Errorf("expected message.new, got %+v", frame)
	}
	return frame.Message.ID, nil
}
