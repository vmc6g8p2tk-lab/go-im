package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Requires a real two-instance Compose deployment with Kafka enabled. This
// test is skipped in the local PostgreSQL-only setup, never counted as passed.
func TestRealKafkaCrossInstanceDeliveryAndStatus(t *testing.T) {
	second := os.Getenv("INTEGRATION_SECOND_BASE_URL")
	if second == "" {
		t.Skip("set INTEGRATION_SECOND_BASE_URL with two Kafka-enabled instances")
	}
	first, databaseURL := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, first, fmt.Sprintf("kafka_a%d", suffix))
	b := registerAndLogin(t, first, fmt.Sprintf("kafka_b%d", suffix))
	ac := connect(t, "ws"+strings.TrimPrefix(first, "http")+"/v1/ws", a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, "ws"+strings.TrimPrefix(strings.TrimRight(second, "/"), "http")+"/v1/ws", b.Token, b.User.ID)
	defer bc.Close()
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.User.ID,
		"client_message_id": newClientMessageID(t), "body": "real Kafka cross-instance"}); err != nil {
		t.Fatal(err)
	}
	messageID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readMessageIDWithin(bc, 10*time.Second); err != nil || got != messageID {
		t.Fatalf("recipient received id=%d want=%d err=%v", got, messageID, err)
	}
	if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrameWithin(bc, 10*time.Second); err != nil || frame.Type != "delivery.ack.ok" {
		t.Fatalf("recipient ACK frame=%+v err=%v", frame, err)
	}
	if frame, err := readTypedFrameWithin(ac, 10*time.Second); err != nil || frame.Type != "message.status" || frame.MessageID != messageID || frame.Status != "delivered" {
		t.Fatalf("cross-instance sender status frame=%+v err=%v", frame, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var status string
		var attempts, messagePublished, statusPublished int
		err := pool.QueryRow(ctx, `SELECT m.delivery_status, m.delivery_attempts,
			CASE WHEN mo.published_at IS NOT NULL THEN 1 ELSE 0 END,
			CASE WHEN so.published_at IS NOT NULL THEN 1 ELSE 0 END
			FROM messages m JOIN message_outbox mo ON mo.message_id=m.id
			JOIN delivery_status_outbox so ON so.message_id=m.id WHERE m.id=$1`, messageID).
			Scan(&status, &attempts, &messagePublished, &statusPublished)
		if err != nil {
			t.Fatal(err)
		}
		if status == "delivered" && messagePublished == 1 && statusPublished == 1 {
			if attempts != 0 {
				t.Fatalf("delivery_attempts=%d: recipient may have received database fallback, not Kafka", attempts)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kafka outboxes not published: status=%s message=%d status_event=%d", status, messagePublished, statusPublished)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
