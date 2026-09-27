package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/go-im/internal/auth"
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
