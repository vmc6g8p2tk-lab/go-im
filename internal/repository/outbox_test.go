package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOutboxAtomicPublishAndRetry(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("set DATABASE_URL for PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &Store{DB: pool}
	name := fmt.Sprintf("outbox%d", time.Now().UnixNano())
	a, err := store.CreateUser(ctx, name+"a", "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.CreateUser(ctx, name+"b", "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	message, _, err := store.SaveMessage(ctx, a.ID, b.ID, "durable event", "")
	if err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM message_outbox WHERE message_id=$1 AND published_at IS NULL`, message.ID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("message and outbox not atomically persisted: pending=%d err=%v", pending, err)
	}
	// Lock other pending rows so this worker can only claim our test event.
	// Rollback releases these locks without changing earlier test data.
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	rows, err := lockTx.Query(ctx, `SELECT message_id FROM message_outbox WHERE published_at IS NULL AND message_id <> $1 FOR UPDATE`, message.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	failed := errors.New("broker unavailable")
	_, err = store.PublishNext(ctx, func(_ context.Context, event OutboxEvent) error { return failed })
	if !errors.Is(err, failed) {
		t.Fatalf("publisher error=%v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM message_outbox WHERE message_id=$1 AND published_at IS NULL`, message.ID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("failed publication lost row: pending=%d err=%v", pending, err)
	}
	seen := false
	found, err := store.PublishNext(ctx, func(_ context.Context, event OutboxEvent) error {
		seen = event.MessageID == message.ID && event.RecipientID == b.ID
		return nil
	})
	if err != nil || !found || !seen {
		t.Fatalf("publish test event: found=%v seen=%v err=%v", found, seen, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM message_outbox WHERE message_id=$1 AND published_at IS NOT NULL`, message.ID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("published row missing: count=%d err=%v", pending, err)
	}
}

func TestDeliveryStatusOutboxPublishAndRetry(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("set DATABASE_URL for PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &Store{DB: pool}
	name := fmt.Sprintf("statusoutbox%d", time.Now().UnixNano())
	a, err := store.CreateUser(ctx, name+"a", "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.CreateUser(ctx, name+"b", "test-hash")
	if err != nil {
		t.Fatal(err)
	}
	message, _, err := store.SaveMessage(ctx, a.ID, b.ID, "status event", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := store.AcknowledgeDelivery(ctx, b.ID, message.ID); err != nil || !changed {
		t.Fatalf("ack changed=%v err=%v", changed, err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	rows, err := lockTx.Query(ctx, `SELECT message_id FROM delivery_status_outbox WHERE published_at IS NULL AND message_id <> $1 FOR UPDATE`, message.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	failed := errors.New("broker unavailable")
	_, err = store.PublishNextStatus(ctx, func(_ context.Context, event OutboxEvent) error { return failed })
	if !errors.Is(err, failed) {
		t.Fatalf("publisher error=%v", err)
	}
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_status_outbox WHERE message_id=$1 AND published_at IS NULL`, message.ID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("failed publish lost status: pending=%d err=%v", pending, err)
	}
	found, err := store.PublishNextStatus(ctx, func(_ context.Context, event OutboxEvent) error {
		if event.MessageID != message.ID || event.SenderID != a.ID || event.Status != "delivered" {
			t.Fatalf("unexpected status event: %+v", event)
		}
		return nil
	})
	if err != nil || !found {
		t.Fatalf("publish status found=%v err=%v", found, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_status_outbox WHERE message_id=$1 AND published_at IS NOT NULL`, message.ID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("published status missing: count=%d err=%v", pending, err)
	}
}
