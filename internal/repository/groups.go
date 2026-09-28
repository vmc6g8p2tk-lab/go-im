package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/example/go-im/internal/model"
	"github.com/jackc/pgx/v5"
)

var ErrForbidden = errors.New("forbidden")
var ErrGroupFull = errors.New("group is full")

func (s *Store) CreateGroup(ctx context.Context, ownerID int64, name string) (model.Group, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return model.Group{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var group model.Group
	err = tx.QueryRow(ctx, `INSERT INTO chat_groups(owner_id,name) VALUES($1,$2)
		RETURNING id,owner_id,name,created_at`, ownerID, name).
		Scan(&group.ID, &group.OwnerID, &group.Name, &group.CreatedAt)
	if err != nil {
		return model.Group{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO group_members(group_id,user_id,role) VALUES($1,$2,'owner')`, group.ID, ownerID); err != nil {
		return model.Group{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Group{}, err
	}
	return group, nil
}

func (s *Store) JoinGroup(ctx context.Context, groupID, userID int64) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var id int64
	if err := tx.QueryRow(ctx, `SELECT id FROM chat_groups WHERE id=$1 FOR UPDATE`, groupID).Scan(&id); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	var member bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id=$1 AND user_id=$2)`, groupID, userID).Scan(&member); err != nil {
		return err
	}
	if member {
		return nil
	}
	var count int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM group_members WHERE group_id=$1`, groupID).Scan(&count); err != nil {
		return err
	}
	if count >= 100 {
		return ErrGroupFull
	}
	if _, err := tx.Exec(ctx, `INSERT INTO group_members(group_id,user_id,role) VALUES($1,$2,'member')`, groupID, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) LeaveGroup(ctx context.Context, groupID, userID int64) error {
	var role string
	if err := s.DB.QueryRow(ctx, `SELECT role FROM group_members WHERE group_id=$1 AND user_id=$2`, groupID, userID).Scan(&role); errors.Is(err, pgx.ErrNoRows) {
		return ErrForbidden
	} else if err != nil {
		return err
	}
	if role == "owner" {
		return ErrForbidden
	}
	_, err := s.DB.Exec(ctx, `DELETE FROM group_members WHERE group_id=$1 AND user_id=$2`, groupID, userID)
	return err
}

func (s *Store) RemoveGroupMember(ctx context.Context, groupID, actorID, targetID int64) error {
	var ownerID int64
	if err := s.DB.QueryRow(ctx, `SELECT owner_id FROM chat_groups WHERE id=$1`, groupID).Scan(&ownerID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}
	if ownerID != actorID || targetID == ownerID {
		return ErrForbidden
	}
	_, err := s.DB.Exec(ctx, `DELETE FROM group_members WHERE group_id=$1 AND user_id=$2`, groupID, targetID)
	return err
}

func (s *Store) GroupMemberIDs(ctx context.Context, groupID int64) ([]int64, error) {
	rows, err := s.DB.Query(ctx, `SELECT user_id FROM group_members WHERE group_id=$1 ORDER BY user_id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) ListGroupMembers(ctx context.Context, groupID, viewerID int64) ([]model.GroupMember, error) {
	var member bool
	if err := s.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id=$1 AND user_id=$2)`, groupID, viewerID).Scan(&member); err != nil {
		return nil, err
	}
	if !member {
		return nil, ErrForbidden
	}
	rows, err := s.DB.Query(ctx, `SELECT user_id,role,joined_at FROM group_members WHERE group_id=$1 ORDER BY user_id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := make([]model.GroupMember, 0)
	for rows.Next() {
		var m model.GroupMember
		if err := rows.Scan(&m.UserID, &m.Role, &m.JoinedAt); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

func (s *Store) SaveGroupMessage(ctx context.Context, groupID, senderID int64, body, clientID string) (model.GroupMessage, bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return model.GroupMessage{}, false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var member int64
	if err := tx.QueryRow(ctx, `SELECT user_id FROM group_members WHERE group_id=$1 AND user_id=$2 FOR SHARE`, groupID, senderID).Scan(&member); errors.Is(err, pgx.ErrNoRows) {
		return model.GroupMessage{}, false, ErrForbidden
	} else if err != nil {
		return model.GroupMessage{}, false, err
	}
	var m model.GroupMessage
	var created bool
	var row pgx.Row
	if clientID == "" {
		row = tx.QueryRow(ctx, `INSERT INTO group_messages(group_id,sender_id,body) VALUES($1,$2,$3)
			RETURNING id,group_id,sender_id,client_message_id,body,created_at`, groupID, senderID, body)
	} else {
		row = tx.QueryRow(ctx, `INSERT INTO group_messages(group_id,sender_id,body,client_message_id) VALUES($1,$2,$3,$4::uuid)
			ON CONFLICT(sender_id,client_message_id) DO NOTHING
			RETURNING id,group_id,sender_id,client_message_id,body,created_at`, groupID, senderID, body, clientID)
	}
	err = row.Scan(&m.ID, &m.GroupID, &m.SenderID, &m.ClientMessageID, &m.Body, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) && clientID != "" {
		err = tx.QueryRow(ctx, `SELECT id,group_id,sender_id,client_message_id,body,created_at FROM group_messages
			WHERE sender_id=$1 AND client_message_id=$2::uuid`, senderID, clientID).
			Scan(&m.ID, &m.GroupID, &m.SenderID, &m.ClientMessageID, &m.Body, &m.CreatedAt)
		if err == nil && (m.GroupID != groupID || m.Body != body) {
			return model.GroupMessage{}, false, ErrIdempotencyConflict
		}
	} else if err == nil {
		created = true
	}
	if err != nil {
		return model.GroupMessage{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.GroupMessage{}, false, err
	}
	return m, created, nil
}

func (s *Store) GroupMessageByID(ctx context.Context, id int64) (model.GroupMessage, error) {
	var m model.GroupMessage
	err := s.DB.QueryRow(ctx, `SELECT id,group_id,sender_id,client_message_id,body,created_at FROM group_messages WHERE id=$1`, id).
		Scan(&m.ID, &m.GroupID, &m.SenderID, &m.ClientMessageID, &m.Body, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.GroupMessage{}, ErrNotFound
	}
	return m, err
}

func (s *Store) GroupHistory(ctx context.Context, groupID, viewerID, beforeID int64, limit int) ([]model.GroupMessage, error) {
	var member bool
	if err := s.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id=$1 AND user_id=$2)`, groupID, viewerID).Scan(&member); err != nil {
		return nil, err
	}
	if !member {
		return nil, ErrForbidden
	}
	var before any
	if beforeID > 0 {
		before = beforeID
	}
	rows, err := s.DB.Query(ctx, `SELECT id,group_id,sender_id,client_message_id,body,created_at FROM group_messages
		WHERE group_id=$1 AND ($2::bigint IS NULL OR id<$2) ORDER BY id DESC LIMIT $3`, groupID, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := make([]model.GroupMessage, 0, limit)
	for rows.Next() {
		var m model.GroupMessage
		if err := rows.Scan(&m.ID, &m.GroupID, &m.SenderID, &m.ClientMessageID, &m.Body, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan group message: %w", err)
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}
	return messages, nil
}

func (s *Store) PublishNextGroup(ctx context.Context, publish func(context.Context, OutboxEvent) error) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var event OutboxEvent
	err = tx.QueryRow(ctx, `SELECT message_id,group_id FROM group_message_outbox
		WHERE published_at IS NULL ORDER BY created_at,message_id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&event.MessageID, &event.GroupID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := publish(ctx, event); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE group_message_outbox SET published_at=now() WHERE message_id=$1`, event.MessageID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
