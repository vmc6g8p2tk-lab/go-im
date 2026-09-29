package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/example/go-im/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("already exists")
var ErrIdempotencyConflict = errors.New("client_message_id already used with different content")

type Store struct{ DB *pgxpool.Pool }

// OutboxEvent is created by the database trigger in the same transaction as
// its message. It is a durable notification, never the message source of truth.
type OutboxEvent struct {
	MessageID   int64  `json:"message_id"`
	RecipientID int64  `json:"recipient_id,omitempty"`
	GroupID     int64  `json:"group_id,omitempty"`
	SenderID    int64  `json:"sender_id,omitempty"`
	Status      string `json:"status,omitempty"`
}

// PublishNext keeps the row locked until Kafka confirms the write. A crash
// after Kafka accepts it but before commit may publish it twice; consumers
// must therefore be idempotent by message ID.
func (s *Store) PublishNext(ctx context.Context, publish func(context.Context, OutboxEvent) error) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var event OutboxEvent
	err = tx.QueryRow(ctx, `SELECT message_id, recipient_id FROM message_outbox
		WHERE published_at IS NULL ORDER BY created_at, message_id
		LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&event.MessageID, &event.RecipientID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := publish(ctx, event); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE message_outbox SET published_at = now() WHERE message_id = $1`, event.MessageID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) MessageByID(ctx context.Context, id int64) (model.Message, error) {
	var message model.Message
	err := s.DB.QueryRow(ctx, `SELECT id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at
		FROM messages WHERE id = $1`, id).Scan(&message.ID, &message.ClientMessageID, &message.SenderID,
		&message.RecipientID, &message.Body, &message.CreatedAt, &message.DeliveryStatus, &message.DeliveredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Message{}, ErrNotFound
	}
	return message, err
}

func (s *Store) CreateUser(ctx context.Context, username, passwordHash string) (model.User, error) {
	var user model.User
	err := s.DB.QueryRow(ctx, `INSERT INTO users (username, password_hash) VALUES ($1, $2)
		RETURNING id, username, created_at`, username, passwordHash).
		Scan(&user.ID, &user.Username, &user.CreatedAt)
	if isUniqueViolation(err) {
		return model.User{}, ErrConflict
	}
	return user, err
}

func (s *Store) UserByUsername(ctx context.Context, username string) (model.User, error) {
	var user model.User
	err := s.DB.QueryRow(ctx, `SELECT id, username, password_hash, created_at FROM users WHERE username = $1`, username).
		Scan(&user.ID, &user.Username, &user.PasswordHash, &user.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.User{}, ErrNotFound
	}
	return user, err
}

func (s *Store) UserExists(ctx context.Context, id int64) (bool, error) {
	var exists bool
	err := s.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

func (s *Store) SaveMessage(ctx context.Context, senderID, recipientID int64, body, clientMessageID string) (model.Message, bool, error) {
	var message model.Message
	if clientMessageID == "" {
		// Existing phase-one clients omit the ID. PostgreSQL assigns a fresh one;
		// only clients that resend their own stable ID get idempotent behavior.
		err := s.DB.QueryRow(ctx, `INSERT INTO messages (sender_id, recipient_id, body) VALUES ($1, $2, $3)
			RETURNING id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at`, senderID, recipientID, body).
			Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt, &message.DeliveryStatus, &message.DeliveredAt)
		return message, true, err
	}
	err := s.DB.QueryRow(ctx, `INSERT INTO messages (sender_id, recipient_id, body, client_message_id)
		VALUES ($1, $2, $3, $4::uuid) ON CONFLICT (sender_id, client_message_id) DO NOTHING
		RETURNING id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at`, senderID, recipientID, body, clientMessageID).
		Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt, &message.DeliveryStatus, &message.DeliveredAt)
	if err == nil {
		return message, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return model.Message{}, false, err
	}
	// The unique index serializes concurrent inserts. This separate statement
	// sees the winner under PostgreSQL's default READ COMMITTED isolation.
	err = s.DB.QueryRow(ctx, `SELECT id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at
		FROM messages WHERE sender_id = $1 AND client_message_id = $2::uuid`, senderID, clientMessageID).
		Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt, &message.DeliveryStatus, &message.DeliveredAt)
	if err != nil {
		return model.Message{}, false, err
	}
	if message.RecipientID != recipientID || message.Body != body {
		return model.Message{}, false, ErrIdempotencyConflict
	}
	return message, false, nil
}

func (s *Store) History(ctx context.Context, userID, peerID, beforeID int64, limit int) ([]model.Message, error) {
	var cursor any
	if beforeID > 0 {
		cursor = beforeID
	}
	rows, err := s.DB.Query(ctx, `SELECT id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at FROM messages
		WHERE ((sender_id = $1 AND recipient_id = $2) OR (sender_id = $2 AND recipient_id = $1))
		AND ($3::bigint IS NULL OR id < $3)
		ORDER BY id DESC LIMIT $4`, userID, peerID, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]model.Message, 0, limit)
	for rows.Next() {
		var m model.Message
		if err := rows.Scan(&m.ID, &m.ClientMessageID, &m.SenderID, &m.RecipientID, &m.Body, &m.CreatedAt, &m.DeliveryStatus, &m.DeliveredAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Return the selected page in display order; the cursor still uses its smallest ID.
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
	return messages, nil
}

// AcknowledgeDelivery only trusts the authenticated recipient, never a sender_id
// carried in a frame. Repeated ACKs return the same row without changing time.
func (s *Store) AcknowledgeDelivery(ctx context.Context, recipientID, messageID int64) (model.Message, bool, error) {
	var message model.Message
	err := s.DB.QueryRow(ctx, `UPDATE messages SET delivery_status = 'delivered', delivered_at = now()
		WHERE id = $1 AND recipient_id = $2 AND delivery_status = 'created'
		RETURNING id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at`, messageID, recipientID).
		Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt, &message.DeliveryStatus, &message.DeliveredAt)
	if err == nil {
		return message, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return model.Message{}, false, err
	}
	err = s.DB.QueryRow(ctx, `SELECT id, client_message_id, sender_id, recipient_id, body, created_at, delivery_status, delivered_at
		FROM messages WHERE id = $1 AND recipient_id = $2`, messageID, recipientID).
		Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt, &message.DeliveryStatus, &message.DeliveredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Message{}, false, ErrNotFound
	}
	return message, false, err
}

// ClaimPending advances the retry clock atomically, so concurrent sockets or
// instances do not all select the same due messages in one retry interval.
func (s *Store) ClaimPending(ctx context.Context, recipientID int64, force bool, limit int) ([]model.Message, error) {
	rows, err := s.DB.Query(ctx, `WITH due AS (
		SELECT id FROM messages WHERE recipient_id = $1 AND delivery_status = 'created'
		AND ($2::boolean OR next_delivery_at <= now()) ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED
	) UPDATE messages AS m SET delivery_attempts = m.delivery_attempts + 1,
		next_delivery_at = now() + interval '5 seconds' FROM due WHERE m.id = due.id
	RETURNING m.id, m.client_message_id, m.sender_id, m.recipient_id, m.body,
		m.created_at, m.delivery_status, m.delivered_at`, recipientID, force, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]model.Message, 0, limit)
	for rows.Next() {
		var m model.Message
		if err := rows.Scan(&m.ID, &m.ClientMessageID, &m.SenderID, &m.RecipientID, &m.Body, &m.CreatedAt, &m.DeliveryStatus, &m.DeliveredAt); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].ID < messages[j].ID })
	return messages, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
