package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/auth"
	"github.com/VMC6G8P2TK-Lab/go-im/internal/presence"
	ws "github.com/gorilla/websocket"
)

func TestCloseAllReleasesAuthenticatedConnection(t *testing.T) {
	manager := auth.New([]byte(strings.Repeat("x", 32)))
	hub := NewHub()
	server := httptest.NewServer(&Server{Auth: manager, Hub: hub, AllowedOrigins: map[string]struct{}{}})
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	token, _, err := manager.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(map[string]string{"type": "auth", "token": token}); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Type string `json:"type"`
	}
	if err := conn.ReadJSON(&reply); err != nil || reply.Type != "auth.ok" {
		t.Fatalf("auth reply: %+v, %v", reply, err)
	}
	hub.CloseAll()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("socket remained open after hub shutdown")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.RLock()
		remaining := len(hub.clients)
		hub.mu.RUnlock()
		if remaining == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("connection was not removed from hub")
}

func TestDisallowedOriginRejected(t *testing.T) {
	server := httptest.NewServer(&Server{AllowedOrigins: map[string]struct{}{"http://localhost:3000": {}}})
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	header := http.Header{"Origin": []string{"https://evil.example"}}
	conn, response, err := ws.DefaultDialer.Dial(url, header)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("unexpected origin result: response=%v err=%v", response, err)
	}
}

func TestDuplicateLoginAndUnexpectedDisconnect(t *testing.T) {
	manager := auth.New([]byte(strings.Repeat("x", 32)))
	hub := NewHub()
	server := httptest.NewServer(&Server{Auth: manager, Hub: hub})
	defer server.Close()
	defer hub.CloseAll()
	token, _, err := manager.Issue(88)
	if err != nil {
		t.Fatal(err)
	}
	open := func() *ws.Conn {
		conn, _, err := ws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteJSON(map[string]string{"type": "auth", "token": token}); err != nil {
			t.Fatal(err)
		}
		var reply struct {
			Type string `json:"type"`
		}
		if err := conn.ReadJSON(&reply); err != nil || reply.Type != "auth.ok" {
			t.Fatalf("auth reply=%+v err=%v", reply, err)
		}
		return conn
	}
	first, second := open(), open()
	defer first.Close()
	defer second.Close()
	if got := len(hub.UserIDs()); got != 1 {
		t.Fatalf("user count=%d", got)
	}
	_ = first.Close() // abrupt client disconnect: no close frame
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.RLock()
		remaining := len(hub.clients[88])
		hub.mu.RUnlock()
		if remaining == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	hub.mu.RLock()
	remaining := len(hub.clients[88])
	hub.mu.RUnlock()
	if remaining != 1 {
		t.Fatalf("remaining connections after first disconnect=%d", remaining)
	}
	_ = second.Close()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(hub.UserIDs()) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("hub retained disconnected user")
}

func TestAuthenticatedConnectionUpdatesRedisRoute(t *testing.T) {
	redisServer := miniredis.RunT(t)
	store := presence.New(redisServer.Addr(), "")
	defer store.Close()
	manager := auth.New([]byte(strings.Repeat("x", 32)))
	hub := NewHub()
	server := httptest.NewServer(&Server{Auth: manager, Hub: hub, Presence: store, InstanceID: "instance-1"})
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	token, _, err := manager.Issue(77)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(map[string]string{"type": "auth", "token": token}); err != nil {
		t.Fatal(err)
	}
	var reply struct {
		Type string `json:"type"`
	}
	if err := conn.ReadJSON(&reply); err != nil || reply.Type != "auth.ok" {
		t.Fatalf("auth reply=%+v err=%v", reply, err)
	}
	routes, err := store.Instances(context.Background(), 77)
	if err != nil || len(routes) != 1 || routes[0] != "instance-1" {
		t.Fatalf("routes=%v err=%v", routes, err)
	}
	redisServer.Close()
	hub.mu.RLock()
	localConnections := len(hub.clients[77])
	hub.mu.RUnlock()
	if localConnections != 1 {
		t.Fatalf("Redis outage removed local connection: %d", localConnections)
	}
	hub.CloseAll()
}
