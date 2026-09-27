package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDeliveryAcknowledgmentAndDuplicateACK(t *testing.T) {
	base, databaseURL := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("i%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("j%d", suffix))
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, url, a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, url, b.Token, b.User.ID)
	defer bc.Close()
	clientID := newClientMessageID(t)
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "ack check", "client_message_id": clientID}); err != nil {
		t.Fatal(err)
	}
	messageID, err := readMessageID(ac) // server_ack: database persisted
	if err != nil {
		t.Fatal(err)
	}
	if receivedID, err := readMessageID(bc); err != nil || receivedID != messageID {
		t.Fatalf("recipient frame id=%d err=%v", receivedID, err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT delivery_status FROM messages WHERE id=$1`, messageID).Scan(&status); err != nil || status != "created" {
		t.Fatalf("status before recipient ACK=%q err=%v", status, err)
	}
	// The sender cannot forge an ACK on behalf of the recipient.
	if err := ac.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(ac); err != nil || frame.Type != "error" || frame.Code != "message_not_found" {
		t.Fatalf("sender ACK result=%+v err=%v", frame, err)
	}
	if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(bc); err != nil || frame.Type != "delivery.ack.ok" || frame.MessageID != messageID {
		t.Fatalf("recipient ACK result=%+v err=%v", frame, err)
	}
	if frame, err := readTypedFrame(ac); err != nil || frame.Type != "message.status" || frame.MessageID != messageID || frame.Status != "delivered" {
		t.Fatalf("sender status=%+v err=%v", frame, err)
	}
	var deliveredAt time.Time
	if err := pool.QueryRow(ctx, `SELECT delivery_status, delivered_at FROM messages WHERE id=$1`, messageID).Scan(&status, &deliveredAt); err != nil || status != "delivered" || deliveredAt.IsZero() {
		t.Fatalf("persisted delivery status=%q at=%v err=%v", status, deliveredAt, err)
	}
	if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(bc); err != nil || frame.Type != "delivery.ack.ok" || frame.MessageID != messageID {
		t.Fatalf("duplicate ACK result=%+v err=%v", frame, err)
	}
	var secondDeliveredAt time.Time
	if err := pool.QueryRow(ctx, `SELECT delivered_at FROM messages WHERE id=$1`, messageID).Scan(&secondDeliveredAt); err != nil || !secondDeliveredAt.Equal(deliveredAt) {
		t.Fatalf("duplicate ACK changed delivered_at: %v -> %v, err=%v", deliveredAt, secondDeliveredAt, err)
	}
}

func TestOfflineReconnectAndLostACK(t *testing.T) {
	base, databaseURL := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("k%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("l%d", suffix))
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, url, a.Token, a.User.ID)
	defer ac.Close()
	clientID := newClientMessageID(t)
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "offline message", "client_message_id": clientID}); err != nil {
		t.Fatal(err)
	}
	messageID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	first := connect(t, url, b.Token, b.User.ID)
	if id, err := readMessageID(first); err != nil || id != messageID {
		t.Fatalf("first offline sync id=%d err=%v", id, err)
	}
	_ = first.Close() // The delivery ACK is deliberately lost.
	second := connect(t, url, b.Token, b.User.ID)
	defer second.Close()
	if id, err := readMessageID(second); err != nil || id != messageID {
		t.Fatalf("reconnect sync id=%d err=%v", id, err)
	}
	if err := second.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(second); err != nil || frame.Type != "delivery.ack.ok" {
		t.Fatalf("reconnect ACK=%+v err=%v", frame, err)
	}
	if frame, err := readTypedFrame(ac); err != nil || frame.Type != "message.status" || frame.MessageID != messageID {
		t.Fatalf("sender status=%+v err=%v", frame, err)
	}
	var count int
	var status string
	if err := pool.QueryRow(ctx, `SELECT count(*), max(delivery_status) FROM messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, clientID).Scan(&count, &status); err != nil || count != 1 || status != "delivered" {
		t.Fatalf("offline persisted count=%d status=%s err=%v", count, status, err)
	}
}

func TestDelayedACKCausesRetry(t *testing.T) {
	base, databaseURL := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("m%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("n%d", suffix))
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, url, a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, url, b.Token, b.User.ID)
	defer bc.Close()
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": "slow ACK", "client_message_id": newClientMessageID(t)}); err != nil {
		t.Fatal(err)
	}
	messageID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := readMessageID(bc); err != nil || id != messageID {
		t.Fatalf("initial delivery id=%d err=%v", id, err)
	}
	// No ACK: the single server retry loop claims a due row after five seconds.
	if id, err := readMessageIDWithin(bc, 10*time.Second); err != nil || id != messageID {
		t.Fatalf("retry delivery id=%d err=%v", id, err)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT delivery_attempts FROM messages WHERE id=$1`, messageID).Scan(&attempts); err != nil || attempts < 1 {
		t.Fatalf("retry attempts=%d err=%v", attempts, err)
	}
	if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(bc); err != nil || frame.Type != "delivery.ack.ok" {
		t.Fatalf("delayed ACK=%+v err=%v", frame, err)
	}
}

func TestOfflineSyncAcrossBatches(t *testing.T) {
	base, databaseURL := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("o%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("p%d", suffix))
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, url, a.Token, a.User.ID)
	defer ac.Close()
	const total = 120 // More than the 32-message login batch and 100-message retry batch.
	for i := range total {
		if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID, "body": fmt.Sprintf("offline-%d", i), "client_message_id": newClientMessageID(t)}); err != nil {
			t.Fatal(err)
		}
		if _, err := readMessageID(ac); err != nil {
			t.Fatal(err)
		}
	}
	bc := connect(t, url, b.Token, b.User.ID)
	defer bc.Close()
	seen := make(map[int64]struct{}, total)
	confirmed := make(map[int64]struct{}, total)
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < total || len(confirmed) < total {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("offline sync stopped at %d/%d messages, %d/%d ACKs", len(seen), total, len(confirmed), total)
		}
		frame, err := readTypedFrameWithin(bc, remaining)
		if err != nil {
			t.Fatalf("offline sync stopped at %d/%d: %v", len(seen), total, err)
		}
		switch frame.Type {
		case "message.new":
			var message struct {
				ID int64 `json:"id"`
			}
			if err := json.Unmarshal(frame.Message, &message); err != nil {
				t.Fatal(err)
			}
			id := message.ID
			if id <= 0 {
				t.Fatalf("invalid message: %+v", frame)
			}
			seen[id] = struct{}{}
			if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": id}); err != nil {
				t.Fatal(err)
			}
		case "delivery.ack.ok":
			confirmed[frame.MessageID] = struct{}{}
		default:
			t.Fatalf("unexpected batch frame: %+v", frame)
		}
	}
	var delivered int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE sender_id=$1 AND recipient_id=$2 AND delivery_status='delivered'`, a.User.ID, b.User.ID).Scan(&delivered); err != nil || delivered != total {
		t.Fatalf("batch delivered=%d want=%d err=%v", delivered, total, err)
	}
}

func integrationEnv(t *testing.T) (string, string) {
	t.Helper()
	base, databaseURL := os.Getenv("INTEGRATION_BASE_URL"), os.Getenv("DATABASE_URL")
	if base == "" || databaseURL == "" {
		t.Skip("set INTEGRATION_BASE_URL and DATABASE_URL for end-to-end test")
	}
	return strings.TrimRight(base, "/"), databaseURL
}

type typedFrame struct {
	Type      string          `json:"type"`
	Code      string          `json:"code"`
	MessageID int64           `json:"message_id"`
	Status    string          `json:"status"`
	Message   json.RawMessage `json:"message"`
}

func readTypedFrame(conn *websocket.Conn) (typedFrame, error) {
	return readTypedFrameWithin(conn, 5*time.Second)
}

func readTypedFrameWithin(conn *websocket.Conn, timeout time.Duration) (typedFrame, error) {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var frame typedFrame
	err := conn.ReadJSON(&frame)
	return frame, err
}
