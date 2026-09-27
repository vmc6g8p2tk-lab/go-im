package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/example/go-im/internal/auth"
	"github.com/example/go-im/internal/model"
	"github.com/example/go-im/internal/presence"
	"github.com/example/go-im/internal/ratelimit"
	"github.com/example/go-im/internal/repository"
	"github.com/example/go-im/internal/service"
	ws "github.com/gorilla/websocket"
)

const (
	maxFrameBytes = 8192
	authTimeout   = 5 * time.Second
	pongWait      = 60 * time.Second
	pingInterval  = 25 * time.Second
	writeTimeout  = 5 * time.Second
)

type Server struct {
	Auth            *auth.Manager
	Service         *service.Service
	Hub             *Hub
	AllowedOrigins  map[string]struct{}
	Presence        *presence.RedisStore
	InstanceID      string
	AsyncDelivery   bool
	UserLimiter     *ratelimit.Limiter
	ConnectionRate  float64
	ConnectionBurst int
}

type client struct {
	conn     *ws.Conn
	userID   int64
	send     chan []byte
	done     chan struct{}
	once     sync.Once
	slowOnce sync.Once
	onSlow   func()
	limiter  *ratelimit.Limiter
}

func (c *client) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

type incoming struct {
	Type            string `json:"type"`
	Token           string `json:"token,omitempty"`
	RecipientID     int64  `json:"recipient_id,omitempty"`
	Body            string `json:"body,omitempty"`
	ClientMessageID string `json:"client_message_id,omitempty"`
	MessageID       int64  `json:"message_id,omitempty"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upgrader := ws.Upgrader{CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" { // Non-browser clients do not send Origin.
			return true
		}
		_, allowed := s.AllowedOrigins[origin]
		return allowed
	}}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(maxFrameBytes)
	_ = conn.SetReadDeadline(time.Now().Add(authTimeout))
	var authFrame incoming
	if err := conn.ReadJSON(&authFrame); err != nil || authFrame.Type != "auth" {
		writeBeforeStart(conn, "unauthorized", "auth frame required")
		_ = conn.Close()
		return
	}
	userID, err := s.Auth.Verify(authFrame.Token)
	if err != nil {
		writeBeforeStart(conn, "unauthorized", "invalid token")
		_ = conn.Close()
		return
	}
	c := &client{conn: conn, userID: userID, send: make(chan []byte, 64), done: make(chan struct{}),
		onSlow: func() { s.Hub.slowConsumers.Add(1) }}
	if s.ConnectionRate > 0 && s.ConnectionBurst > 0 {
		c.limiter = ratelimit.New(s.ConnectionRate, s.ConnectionBurst)
	}
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongWait)) })
	s.Hub.register(c)
	if s.Presence != nil {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		if err := s.Presence.Touch(ctx, userID, s.InstanceID); err != nil {
			slog.Warn("Redis presence unavailable; local chat continues", "error", err)
		}
		cancel()
	}
	var initial []model.Message
	if s.Service != nil {
		// Claim before auth.ok. A caller that starts sending immediately after
		// auth.ok cannot race this initial snapshot into a duplicate frame.
		initial = s.claimPending(r.Context(), userID, true, 32)
	}
	// Register before auth.ok so a client that immediately sees the reply is
	// already visible to shutdown and message delivery.
	if err := conn.WriteJSON(map[string]any{"type": "auth.ok", "user_id": userID}); err != nil {
		c.close()
		s.Hub.unregister(c)
		return
	}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		c.writeLoop()
	}()
	defer func() {
		c.close()
		s.Hub.unregister(c)
		<-writerDone
	}()
	for _, message := range initial {
		payload, err := messagePayload(message)
		if err == nil {
			c.enqueue(payload)
		}
	}
	for {
		messageType, frame, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != ws.TextMessage {
			c.enqueueError("invalid_message", "text JSON frame required")
			continue
		}
		var input incoming
		decoder := json.NewDecoder(bytes.NewReader(frame))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			c.enqueueError("invalid_message", "valid JSON frame required")
			continue
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			c.enqueueError("invalid_message", "valid JSON frame required")
			continue
		}
		if input.Type == "delivery.ack" {
			s.handleDeliveryAck(r.Context(), c, input.MessageID)
			continue
		}
		if input.Type != "message.send" {
			c.enqueueError("invalid_message", "message.send or delivery.ack required")
			continue
		}
		if (c.limiter != nil && !c.limiter.Allow("send")) || (s.UserLimiter != nil && !s.UserLimiter.Allow(strconv.FormatInt(c.userID, 10))) {
			c.enqueueError("rate_limited", "message rate exceeded")
			continue
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		message, created, err := s.Service.SendMessage(ctx, c.userID, input.RecipientID, input.Body, input.ClientMessageID)
		cancel()
		if errors.Is(err, service.ErrInvalidInput) {
			c.enqueueError("invalid_message", "recipient_id and nonempty body of at most 4000 bytes required")
			continue
		}
		if errors.Is(err, service.ErrRecipientNotFound) {
			c.enqueueError("recipient_not_found", "recipient does not exist")
			continue
		}
		if errors.Is(err, repository.ErrIdempotencyConflict) {
			c.enqueueError("idempotency_conflict", "client_message_id already belongs to different content")
			continue
		}
		if err != nil {
			slog.Error("save message failed", "error", err)
			c.enqueueError("internal_error", "message was not saved")
			continue
		}
		payload, err := messagePayload(message)
		if err != nil {
			slog.Error("encode message failed", "error", err)
			c.enqueueError("internal_error", "message was saved; query history to recover")
			continue
		}
		if input.ClientMessageID != "" {
			ack, _ := json.Marshal(map[string]any{
				"type": "server_ack", "client_message_id": message.ClientMessageID,
				"message_id": message.ID, "status": "persisted", "duplicate": !created,
			})
			c.enqueue(ack)
		} else {
			// Preserve the phase-one sender echo for legacy clients.
			s.Hub.deliver(c.userID, payload)
		}
		if created && message.RecipientID != c.userID && !s.AsyncDelivery {
			s.Hub.deliver(message.RecipientID, payload)
		}
	}
}

// HandleOutboxEvent resolves the durable row again; Kafka only carries its ID.
// Replays have no database side effect. Clients deduplicate repeated frames by
// message.id, since a crash can replay a published event.
func (s *Server) HandleOutboxEvent(ctx context.Context, event repository.OutboxEvent) error {
	message, err := s.Service.Store.MessageByID(ctx, event.MessageID)
	if err != nil {
		return err
	}
	if message.RecipientID != event.RecipientID {
		return errors.New("Kafka recipient does not match message")
	}
	if len(s.Hub.UserIDs()) == 0 {
		return nil
	}
	payload, err := messagePayload(message)
	if err != nil {
		return err
	}
	s.Hub.deliver(message.RecipientID, payload)
	return nil
}

func writeBeforeStart(conn *ws.Conn, code, message string) {
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = conn.WriteJSON(map[string]any{"type": "error", "code": code, "message": message})
}

func (c *client) enqueueError(code, message string) {
	payload, _ := json.Marshal(map[string]any{"type": "error", "code": code, "message": message})
	c.enqueue(payload)
}

func (c *client) enqueue(payload []byte) {
	select {
	case <-c.done:
		return
	default:
	}
	select {
	case c.send <- payload:
	case <-c.done:
	default:
		c.slowOnce.Do(func() {
			if c.onSlow != nil {
				c.onSlow()
			}
		})
		c.close()
	}
}

func messagePayload(message model.Message) ([]byte, error) {
	return json.Marshal(map[string]any{"type": "message.new", "message": message})
}

func (s *Server) handleDeliveryAck(parent context.Context, c *client, messageID int64) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	message, changed, err := s.Service.AcknowledgeDelivery(ctx, c.userID, messageID)
	if errors.Is(err, service.ErrInvalidInput) {
		c.enqueueError("invalid_message", "positive message_id required")
		return
	}
	if errors.Is(err, repository.ErrNotFound) {
		c.enqueueError("message_not_found", "message does not belong to this recipient")
		return
	}
	if err != nil {
		slog.Error("delivery ack failed", "error", err)
		c.enqueueError("internal_error", "delivery acknowledgment was not saved")
		return
	}
	confirmation, _ := json.Marshal(map[string]any{"type": "delivery.ack.ok", "message_id": message.ID, "status": message.DeliveryStatus})
	c.enqueue(confirmation)
	if changed {
		status, _ := json.Marshal(map[string]any{"type": "message.status", "message_id": message.ID, "status": "delivered", "delivered_at": message.DeliveredAt})
		s.Hub.deliver(message.SenderID, status)
	}
}

// One retry worker per server scans only users with local connections. The
// database claim bounds each pass; ACKs stop future claims.
func (s *Server) RunRetryLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, userID := range s.Hub.UserIDs() {
				s.syncPending(ctx, userID, false)
			}
		}
	}
}

func (s *Server) RunPresenceLoop(ctx context.Context) {
	if s.Presence == nil {
		return
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.Presence.TouchMany(operationCtx, s.Hub.UserIDs(), s.InstanceID); err != nil {
				slog.Warn("Redis presence heartbeat failed", "error", err)
			}
			cancel()
		}
	}
}

func (s *Server) RemoveLocalRoutes(ctx context.Context) {
	if s.Presence == nil {
		return
	}
	for _, userID := range s.Hub.UserIDs() {
		operationCtx, cancel := context.WithTimeout(ctx, time.Second)
		if err := s.Presence.Remove(operationCtx, userID, s.InstanceID); err != nil {
			slog.Warn("Redis presence cleanup failed", "error", err)
		}
		cancel()
	}
}

func (s *Server) syncPending(parent context.Context, userID int64, force bool) {
	// Stay below the 64-frame per-connection queue. Later ticks advance the
	// cursor through additional due rows without a per-message goroutine.
	messages := s.claimPending(parent, userID, force, 32)
	for _, message := range messages {
		payload, err := messagePayload(message)
		if err != nil {
			slog.Error("encode pending message failed", "message_id", message.ID, "error", err)
			continue
		}
		s.Hub.deliver(userID, payload)
	}
}

func (s *Server) claimPending(parent context.Context, userID int64, force bool, limit int) []model.Message {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	messages, err := s.Service.ClaimPending(ctx, userID, force, limit)
	if err != nil {
		slog.Error("pending sync failed", "user_id", userID, "error", err)
		return nil
	}
	return messages
}

// Gorilla permits one concurrent reader and one concurrent writer per connection.
// This loop owns every post-auth write, including Ping control frames.
func (c *client) writeLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	defer c.close()
	for {
		select {
		case payload := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(ws.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(ws.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}
