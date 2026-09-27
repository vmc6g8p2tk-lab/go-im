package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
)

func TestFullSendQueueClosesSlowConnectionOnce(t *testing.T) {
	hub := NewHub()
	clients := make(chan *client, 1)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		c := &client{conn: conn, userID: 42, send: make(chan []byte, 1), done: make(chan struct{}),
			onSlow: func() { hub.slowConsumers.Add(1) }}
		hub.register(c)
		clients <- c
		<-c.done // deliberately no writer: deterministic slow client
		hub.unregister(c)
	}))
	defer httpServer.Close()
	conn, _, err := ws.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := <-clients
	hub.deliver(42, []byte(`{"type":"message.new"}`))
	if len(c.send) != 1 {
		t.Fatalf("queue depth=%d", len(c.send))
	}
	hub.deliver(42, []byte(`{"type":"message.new"}`))
	hub.deliver(42, []byte(`{"type":"message.new"}`))
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("slow connection stayed open")
	}
	if got := hub.SlowConsumerDisconnects(); got != 1 {
		t.Fatalf("slow disconnect count=%d", got)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("client socket remained open")
	}
}
