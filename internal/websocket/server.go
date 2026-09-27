package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/example/go-im/internal/auth"
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
	Auth           *auth.Manager
	Service        *service.Service
	Hub            *Hub
	AllowedOrigins map[string]struct{}
}

type client struct {
	conn   *ws.Conn
	userID int64
	send   chan []byte
	done   chan struct{}
	once   sync.Once
}

func (c *client) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

type incoming struct {
	Type        string `json:"type"`
	Token       string `json:"token,omitempty"`
	RecipientID int64  `json:"recipient_id,omitempty"`
	Body        string `json:"body,omitempty"`
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
	c := &client{conn: conn, userID: userID, send: make(chan []byte, 64), done: make(chan struct{})}
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(pongWait)) })
	s.Hub.register(c)
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
		if err := json.Unmarshal(frame, &input); err != nil || input.Type != "message.send" {
			c.enqueueError("invalid_message", "message.send JSON frame required")
			continue
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		message, err := s.Service.SendMessage(ctx, c.userID, input.RecipientID, input.Body)
		cancel()
		if errors.Is(err, service.ErrInvalidInput) {
			c.enqueueError("invalid_message", "recipient_id and nonempty body of at most 4000 bytes required")
			continue
		}
		if errors.Is(err, service.ErrRecipientNotFound) {
			c.enqueueError("recipient_not_found", "recipient does not exist")
			continue
		}
		if err != nil {
			slog.Error("save message failed", "error", err)
			c.enqueueError("internal_error", "message was not saved")
			continue
		}
		payload, err := json.Marshal(map[string]any{"type": "message.new", "message": message})
		if err != nil {
			slog.Error("encode message failed", "error", err)
			c.enqueueError("internal_error", "message was saved; query history to recover")
			continue
		}
		s.Hub.deliver(c.userID, payload)
		if message.RecipientID != c.userID {
			s.Hub.deliver(message.RecipientID, payload)
		}
	}
}

func writeBeforeStart(conn *ws.Conn, code, message string) {
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = conn.WriteJSON(map[string]any{"type": "error", "code": code, "message": message})
}

func (c *client) enqueueError(code, message string) {
	payload, _ := json.Marshal(map[string]any{"type": "error", "code": code, "message": message})
	select {
	case c.send <- payload:
	default:
		c.close()
	}
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
