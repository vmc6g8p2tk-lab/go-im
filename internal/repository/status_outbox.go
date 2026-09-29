package repository

import (
	"context"
	"fmt"
)

// PublishNextStatus keeps the status event durable until Kafka accepts it.
// Duplicate publication is possible after a crash; clients deduplicate by ID.
func (s *Store) PublishNextStatus(ctx context.Context, publish func(context.Context, OutboxEvent) error) (bool, error) {
	return s.PublishNextStatusBatch(ctx, 1, func(ctx context.Context, events []OutboxEvent) error {
		return publish(ctx, events[0])
	})
}

// PublishNextStatusBatch uses the same at-least-once boundary as message Outbox.
func (s *Store) PublishNextStatusBatch(ctx context.Context, limit int, publish func(context.Context, []OutboxEvent) error) (bool, error) {
	if limit < 1 {
		return false, fmt.Errorf("status outbox batch limit must be positive")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	rows, err := tx.Query(ctx, `SELECT message_id, sender_id FROM delivery_status_outbox
		WHERE published_at IS NULL ORDER BY created_at, message_id
		LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return false, err
	}
	events := make([]OutboxEvent, 0, limit)
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.MessageID, &event.SenderID); err != nil {
			rows.Close()
			return false, err
		}
		event.Status = "delivered"
		events = append(events, event)
		ids = append(ids, event.MessageID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}
	if err := publish(ctx, events); err != nil {
		return false, err
	}
	command, err := tx.Exec(ctx, `UPDATE delivery_status_outbox SET published_at = now()
		WHERE message_id = ANY($1::bigint[]) AND published_at IS NULL`, ids)
	if err != nil {
		return false, err
	}
	if command.RowsAffected() != int64(len(ids)) {
		return false, fmt.Errorf("status outbox marked %d of %d published", command.RowsAffected(), len(ids))
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
