package service

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/VMC6G8P2TK-Lab/go-im/internal/auth"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/model"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/repository"
)

var ErrInvalidInput = errors.New("invalid input")
var ErrInvalidCredentials = errors.New("invalid credentials")
var ErrRecipientNotFound = errors.New("recipient not found")

var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{3,32}$`)
var clientMessageIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Service struct {
	Store *repository.Store
	Auth  *auth.Manager
}

func (s *Service) Register(ctx context.Context, username, password string) (model.User, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	if !usernamePattern.MatchString(username) || len(password) < 8 || len(password) > 72 {
		return model.User{}, ErrInvalidInput
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return model.User{}, err
	}
	return s.Store.CreateUser(ctx, username, hash)
}

func (s *Service) Login(ctx context.Context, username, password string) (model.User, string, time.Time, error) {
	username = strings.ToLower(strings.TrimSpace(username))
	if !usernamePattern.MatchString(username) || password == "" {
		return model.User{}, "", time.Time{}, ErrInvalidCredentials
	}
	user, err := s.Store.UserByUsername(ctx, username)
	if errors.Is(err, repository.ErrNotFound) {
		return model.User{}, "", time.Time{}, ErrInvalidCredentials
	}
	if err != nil {
		return model.User{}, "", time.Time{}, err
	}
	if !auth.CheckPassword(user.PasswordHash, password) {
		return model.User{}, "", time.Time{}, ErrInvalidCredentials
	}
	token, expires, err := s.Auth.Issue(user.ID)
	return user, token, expires, err
}

func (s *Service) SendMessage(ctx context.Context, senderID, recipientID int64, body, clientMessageID string) (model.Message, bool, error) {
	clientMessageID = strings.ToLower(clientMessageID)
	if senderID <= 0 || recipientID <= 0 || !utf8.ValidString(body) || len(body) > 4000 || strings.TrimSpace(body) == "" || (clientMessageID != "" && !clientMessageIDPattern.MatchString(clientMessageID)) {
		return model.Message{}, false, ErrInvalidInput
	}
	exists, err := s.Store.UserExists(ctx, recipientID)
	if err != nil {
		return model.Message{}, false, err
	}
	if !exists {
		return model.Message{}, false, ErrRecipientNotFound
	}
	return s.Store.SaveMessage(ctx, senderID, recipientID, body, clientMessageID)
}

func (s *Service) History(ctx context.Context, userID, peerID, beforeID int64, limit int) ([]model.Message, error) {
	if userID <= 0 || peerID <= 0 || beforeID < 0 || limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	return s.Store.History(ctx, userID, peerID, beforeID, limit)
}

func (s *Service) AcknowledgeDelivery(ctx context.Context, recipientID, messageID int64) (model.Message, bool, error) {
	if recipientID <= 0 || messageID <= 0 {
		return model.Message{}, false, ErrInvalidInput
	}
	return s.Store.AcknowledgeDelivery(ctx, recipientID, messageID)
}

func (s *Service) ClaimPending(ctx context.Context, recipientID int64, force bool, limit int) ([]model.Message, error) {
	if recipientID <= 0 || limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	return s.Store.ClaimPending(ctx, recipientID, force, limit)
}

func (s *Service) CreateGroup(ctx context.Context, ownerID int64, name string) (model.Group, error) {
	name = strings.TrimSpace(name)
	if ownerID <= 0 || name == "" || !utf8.ValidString(name) || len(name) > 100 {
		return model.Group{}, ErrInvalidInput
	}
	return s.Store.CreateGroup(ctx, ownerID, name)
}

func (s *Service) JoinGroup(ctx context.Context, groupID, userID int64) error {
	if groupID <= 0 || userID <= 0 {
		return ErrInvalidInput
	}
	return s.Store.JoinGroup(ctx, groupID, userID)
}

func (s *Service) LeaveGroup(ctx context.Context, groupID, userID int64) error {
	if groupID <= 0 || userID <= 0 {
		return ErrInvalidInput
	}
	return s.Store.LeaveGroup(ctx, groupID, userID)
}

func (s *Service) RemoveGroupMember(ctx context.Context, groupID, actorID, targetID int64) error {
	if groupID <= 0 || actorID <= 0 || targetID <= 0 {
		return ErrInvalidInput
	}
	return s.Store.RemoveGroupMember(ctx, groupID, actorID, targetID)
}

func (s *Service) GroupMembers(ctx context.Context, groupID, viewerID int64) ([]model.GroupMember, error) {
	if groupID <= 0 || viewerID <= 0 {
		return nil, ErrInvalidInput
	}
	return s.Store.ListGroupMembers(ctx, groupID, viewerID)
}

func (s *Service) SendGroupMessage(ctx context.Context, groupID, senderID int64, body, clientID string) (model.GroupMessage, bool, error) {
	clientID = strings.ToLower(clientID)
	if groupID <= 0 || senderID <= 0 || !utf8.ValidString(body) || len(body) > 4000 || strings.TrimSpace(body) == "" || (clientID != "" && !clientMessageIDPattern.MatchString(clientID)) {
		return model.GroupMessage{}, false, ErrInvalidInput
	}
	return s.Store.SaveGroupMessage(ctx, groupID, senderID, body, clientID)
}

func (s *Service) GroupHistory(ctx context.Context, groupID, viewerID, beforeID int64, limit int) ([]model.GroupMessage, error) {
	if groupID <= 0 || viewerID <= 0 || beforeID < 0 || limit < 1 || limit > 100 {
		return nil, ErrInvalidInput
	}
	return s.Store.GroupHistory(ctx, groupID, viewerID, beforeID, limit)
}
