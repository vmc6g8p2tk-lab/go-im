package tests

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/example/go-im/internal/auth"
	"github.com/example/go-im/internal/presence"
	"github.com/example/go-im/internal/repository"
	"github.com/example/go-im/internal/service"
	imws "github.com/example/go-im/internal/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This tests two independent HTTP/WebSocket servers and hubs, with one shared
// PostgreSQL database. Kafka delivery is injected at its consumer boundary;
// it is not a real broker or a separate-process end-to-end test.
func TestCrossInstanceConsumerDelivery(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("set DATABASE_URL for integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &repository.Store{DB: pool}
	manager := auth.New([]byte(strings.Repeat("x", 32)))
	svc := &service.Service{Store: store, Auth: manager}
	redisServer := miniredis.RunT(t)
	presenceStore := presence.New(redisServer.Addr(), "")
	defer presenceStore.Close()
	aHub, bHub := imws.NewHub(), imws.NewHub()
	aServer := &imws.Server{Auth: manager, Service: svc, Hub: aHub, Presence: presenceStore, InstanceID: "a", AsyncDelivery: true}
	bServer := &imws.Server{Auth: manager, Service: svc, Hub: bHub, Presence: presenceStore, InstanceID: "b", AsyncDelivery: true}
	aHTTP := httptest.NewServer(aServer)
	defer aHTTP.Close()
	bHTTP := httptest.NewServer(bServer)
	defer bHTTP.Close()
	defer aHub.CloseAll()
	defer bHub.CloseAll()
	suffix := time.Now().UnixNano()
	a, err := store.CreateUser(ctx, fmt.Sprintf("multi%d_a", suffix), "hash")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.CreateUser(ctx, fmt.Sprintf("multi%d_b", suffix), "hash")
	if err != nil {
		t.Fatal(err)
	}
	aToken, _, err := manager.Issue(a.ID)
	if err != nil {
		t.Fatal(err)
	}
	bToken, _, err := manager.Issue(b.ID)
	if err != nil {
		t.Fatal(err)
	}
	ac := connect(t, "ws"+strings.TrimPrefix(aHTTP.URL, "http"), aToken, a.ID)
	defer ac.Close()
	bc := connect(t, "ws"+strings.TrimPrefix(bHTTP.URL, "http"), bToken, b.ID)
	defer bc.Close()
	routes, err := presenceStore.Instances(ctx, b.ID)
	if err != nil || len(routes) != 1 || routes[0] != "b" {
		t.Fatalf("recipient route=%v err=%v", routes, err)
	}
	if err := ac.WriteJSON(map[string]any{"type": "message.send", "recipient_id": b.ID, "body": "across instances", "client_message_id": newClientMessageID(t)}); err != nil {
		t.Fatal(err)
	}
	messageID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	event := repository.OutboxEvent{MessageID: messageID, RecipientID: b.ID}
	if err := bServer.HandleOutboxEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	if got, err := readMessageID(bc); err != nil || got != messageID {
		t.Fatalf("cross-instance frame=%d err=%v", got, err)
	}
	if err := bc.WriteJSON(map[string]any{"type": "delivery.ack", "message_id": messageID}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(bc); err != nil || frame.Type != "delivery.ack.ok" {
		t.Fatalf("delivery ACK=%+v err=%v", frame, err)
	}
	// Replayed Kafka events may resend the same frame; the row and its ACK stay
	// singular. The client protocol requires deduplication by message.id.
	if err := bServer.HandleOutboxEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	if got, err := readMessageID(bc); err != nil || got != messageID {
		t.Fatalf("replay frame=%d err=%v", got, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE id=$1 AND delivery_status='delivered'`, messageID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("persisted logical message count=%d err=%v", count, err)
	}
}
