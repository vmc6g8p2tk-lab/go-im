package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// PublishNextStatus keeps the status event durable until Kafka accepts it.
// Duplicate publication is possible after a crash; clients deduplicate by ID.
func (s *Store) PublishNextStatus(ctx context.Context, publish func(context.Context, OutboxEvent) error) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var event OutboxEvent
	err = tx.QueryRow(ctx, `SELECT message_id, sender_id FROM delivery_status_outbox
		WHERE published_at IS NULL ORDER BY created_at, message_id
		LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&event.MessageID, &event.SenderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	event.Status = "delivered"
	if err := publish(ctx, event); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE delivery_status_outbox SET published_at = now() WHERE message_id = $1`, event.MessageID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
