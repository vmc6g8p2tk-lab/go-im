package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Set INTEGRATION_BASE_URL and DATABASE_URL to run against the real Compose stack.
func TestTwoUsersWebSocketAndPersistence(t *testing.T) {
	base := os.Getenv("INTEGRATION_BASE_URL")
	databaseURL := os.Getenv("DATABASE_URL")
	if base == "" || databaseURL == "" {
		t.Skip("set INTEGRATION_BASE_URL and DATABASE_URL for end-to-end test")
	}
	base = strings.TrimRight(base, "/")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("a%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("b%d", suffix))
	var storedHash string
	if err := pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE id = $1`, a.User.ID).Scan(&storedHash); err != nil || storedHash == "test-password-123" || !strings.HasPrefix(storedHash, "$2") {
		t.Fatalf("password not stored as bcrypt hash: err=%v", err)
	}
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, wsURL, a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, wsURL, b.Token, b.User.ID)
	defer bc.Close()
	checkDelivery := func(sender, recipient *websocket.Conn, senderID, recipientID int64, body string) int64 {
		t.Helper()
		if err := sender.WriteJSON(map[string]any{"type": "message.send", "recipient_id": recipientID, "body": body}); err != nil {
			t.Fatal(err)
		}
		var first, second struct {
			Type    string `json:"type"`
			Message struct {
				ID          int64  `json:"id"`
				SenderID    int64  `json:"sender_id"`
				RecipientID int64  `json:"recipient_id"`
				Body        string `json:"body"`
			} `json:"message"`
		}
		_ = sender.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := sender.ReadJSON(&first); err != nil {
			t.Fatal(err)
		}
		_ = recipient.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := recipient.ReadJSON(&second); err != nil {
			t.Fatal(err)
		}
		if first.Type != "message.new" || second.Type != "message.new" || first.Message.ID <= 0 || first.Message.ID != second.Message.ID || first.Message.SenderID != senderID || second.Message.RecipientID != recipientID || second.Message.Body != body {
			t.Fatalf("unexpected delivery: sender=%+v recipient=%+v", first, second)
		}
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE id = $1 AND sender_id = $2 AND recipient_id = $3 AND body = $4`, first.Message.ID, senderID, recipientID, body).Scan(&count); err != nil || count != 1 {
			t.Fatalf("message missing from PostgreSQL: count=%d err=%v", count, err)
		}
		return first.Message.ID
	}
	firstID := checkDelivery(ac, bc, a.User.ID, b.User.ID, "alice to bob")
	secondID := checkDelivery(bc, ac, b.User.ID, a.User.ID, "bob to alice")
	request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/messages?peer_id=%d", base, b.User.ID), nil)
	request.Header.Set("Authorization", "Bearer "+a.Token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var history struct {
		Messages []struct {
			ID int64 `json:"id"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(response.Body).Decode(&history); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || len(history.Messages) != 2 || history.Messages[0].ID != firstID || history.Messages[1].ID != secondID {
		t.Fatalf("history mismatch: status=%d history=%+v", response.StatusCode, history)
	}
	pageRequest, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/v1/messages?peer_id=%d&before_id=%d&limit=1", base, b.User.ID, secondID), nil)
	pageRequest.Header.Set("Authorization", "Bearer "+a.Token)
	pageResponse, err := http.DefaultClient.Do(pageRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer pageResponse.Body.Close()
	var page struct {
		Messages []struct {
			ID int64 `json:"id"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(pageResponse.Body).Decode(&page); err != nil || pageResponse.StatusCode != 200 || len(page.Messages) != 1 || page.Messages[0].ID != firstID {
		t.Fatalf("history cursor mismatch: status=%d page=%+v err=%v", pageResponse.StatusCode, page, err)
	}
}

func TestConcurrentBidirectionalDelivery(t *testing.T) {
	base := os.Getenv("INTEGRATION_BASE_URL")
	databaseURL := os.Getenv("DATABASE_URL")
	if base == "" || databaseURL == "" {
		t.Skip("set INTEGRATION_BASE_URL and DATABASE_URL for end-to-end test")
	}
	base = strings.TrimRight(base, "/")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("c%d", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("d%d", suffix))
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, wsURL, a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, wsURL, b.Token, b.User.ID)
	defer bc.Close()
	const perSide = 20
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	send := func(conn *websocket.Conn, recipientID int64) {
		defer wg.Done()
		for i := range perSide {
			if err := conn.WriteJSON(map[string]any{"type": "message.send", "recipient_id": recipientID, "body": fmt.Sprintf("parallel-%d", i)}); err != nil {
				errCh <- err
				return
			}
		}
	}
	read := func(conn *websocket.Conn) {
		defer wg.Done()
		seen := make(map[int64]struct{}, perSide*2)
		_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		for range perSide * 2 {
			var frame struct {
				Type    string `json:"type"`
				Message struct {
					ID int64 `json:"id"`
				} `json:"message"`
			}
			if err := conn.ReadJSON(&frame); err != nil {
				errCh <- err
				return
			}
			if frame.Type != "message.new" || frame.Message.ID <= 0 {
				errCh <- fmt.Errorf("unexpected frame: %+v", frame)
				return
			}
			seen[frame.Message.ID] = struct{}{}
		}
		if len(seen) != perSide*2 {
			errCh <- fmt.Errorf("received %d unique IDs, want %d", len(seen), perSide*2)
		}
	}
	wg.Add(4)
	go send(ac, b.User.ID)
	go send(bc, a.User.ID)
	go read(ac)
	go read(bc)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE (sender_id = $1 AND recipient_id = $2) OR (sender_id = $2 AND recipient_id = $1)`, a.User.ID, b.User.ID).Scan(&count)
	if err != nil || count != perSide*2 {
		t.Fatalf("PostgreSQL count=%d, want %d: %v", count, perSide*2, err)
	}
}

type identity struct {
	User struct {
		ID int64 `json:"id"`
	} `json:"user"`
	Token string `json:"token"`
}

func registerAndLogin(t *testing.T, base, username string) identity {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"username": username, "password": "test-password-123"})
	response, err := http.Post(base+"/v1/auth/register", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 201 {
		t.Fatalf("register %s: status=%d", username, response.StatusCode)
	}
	_ = response.Body.Close()
	response, err = http.Post(base+"/v1/auth/login", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result identity
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || result.User.ID <= 0 || result.Token == "" {
		t.Fatalf("login %s: status=%d", username, response.StatusCode)
	}
	return result
}

func connect(t *testing.T, url, token string, expectedID int64) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(map[string]string{"type": "auth", "token": token}); err != nil {
		t.Fatal(err)
	}
	var frame struct {
		Type   string `json:"type"`
		UserID int64  `json:"user_id"`
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "auth.ok" || frame.UserID != expectedID {
		t.Fatalf("auth frame: %+v", frame)
	}
	return conn
}
