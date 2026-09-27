package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/example/go-im/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("already exists")
var ErrIdempotencyConflict = errors.New("client_message_id already used with different content")

type Store struct{ DB *pgxpool.Pool }

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
			RETURNING id, client_message_id, sender_id, recipient_id, body, created_at`, senderID, recipientID, body).
			Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt)
		return message, true, err
	}
	err := s.DB.QueryRow(ctx, `INSERT INTO messages (sender_id, recipient_id, body, client_message_id)
		VALUES ($1, $2, $3, $4::uuid) ON CONFLICT (sender_id, client_message_id) DO NOTHING
		RETURNING id, client_message_id, sender_id, recipient_id, body, created_at`, senderID, recipientID, body, clientMessageID).
		Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt)
	if err == nil {
		return message, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return model.Message{}, false, err
	}
	// The unique index serializes concurrent inserts. This separate statement
	// sees the winner under PostgreSQL's default READ COMMITTED isolation.
	err = s.DB.QueryRow(ctx, `SELECT id, client_message_id, sender_id, recipient_id, body, created_at
		FROM messages WHERE sender_id = $1 AND client_message_id = $2::uuid`, senderID, clientMessageID).
		Scan(&message.ID, &message.ClientMessageID, &message.SenderID, &message.RecipientID, &message.Body, &message.CreatedAt)
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
	rows, err := s.DB.Query(ctx, `SELECT id, client_message_id, sender_id, recipient_id, body, created_at FROM messages
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
		if err := rows.Scan(&m.ID, &m.ClientMessageID, &m.SenderID, &m.RecipientID, &m.Body, &m.CreatedAt); err != nil {
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

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
