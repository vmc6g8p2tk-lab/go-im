package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGroupMembershipFanoutHistoryAndIdempotency(t *testing.T) {
	base, databaseURL := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := time.Now().UnixNano()
	a := registerAndLogin(t, base, fmt.Sprintf("group%d_a", suffix))
	b := registerAndLogin(t, base, fmt.Sprintf("group%d_b", suffix))
	c := registerAndLogin(t, base, fmt.Sprintf("group%d_c", suffix))
	call := func(method, path, token, body string, want int) map[string]json.RawMessage {
		t.Helper()
		request, err := http.NewRequest(method, base+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("%s %s status=%d want=%d", method, path, response.StatusCode, want)
		}
		var result map[string]json.RawMessage
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	result := call("POST", "/v1/groups", a.Token, `{"name":"engineering"}`, 201)
	var group struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(result["group"], &group); err != nil || group.ID <= 0 {
		t.Fatalf("group=%+v err=%v", group, err)
	}
	path := fmt.Sprintf("/v1/groups/%d", group.ID)
	call("POST", path+"/join", b.Token, "", 200)
	call("POST", path+"/join", c.Token, "", 200)
	call("GET", path+"/members", c.Token, "", 200)
	call("DELETE", fmt.Sprintf("%s/members/%d", path, c.User.ID), b.Token, "", 403)
	call("DELETE", fmt.Sprintf("%s/members/%d", path, c.User.ID), a.Token, "", 200)
	call("GET", path+"/members", c.Token, "", 403)
	url := "ws" + strings.TrimPrefix(base, "http") + "/v1/ws"
	ac := connect(t, url, a.Token, a.User.ID)
	defer ac.Close()
	bc := connect(t, url, b.Token, b.User.ID)
	defer bc.Close()
	cc := connect(t, url, c.Token, c.User.ID)
	defer cc.Close()
	if err := cc.WriteJSON(map[string]any{"type": "group.send", "group_id": group.ID, "body": "forbidden"}); err != nil {
		t.Fatal(err)
	}
	if frame, err := readTypedFrame(cc); err != nil || frame.Type != "error" || frame.Code != "forbidden" {
		t.Fatalf("nonmember send=%+v err=%v", frame, err)
	}
	clientID := newClientMessageID(t)
	input := map[string]any{"type": "group.send", "group_id": group.ID, "body": "hello group", "client_message_id": clientID}
	if err := ac.WriteJSON(input); err != nil {
		t.Fatal(err)
	}
	messageID, err := readMessageID(ac)
	if err != nil {
		t.Fatal(err)
	}
	for _, conn := range []struct {
		name string
		read func() (typedFrame, error)
	}{
		{"sender", func() (typedFrame, error) { return readTypedFrame(ac) }},
		{"recipient", func() (typedFrame, error) { return readTypedFrame(bc) }},
	} {
		frame, err := conn.read()
		if err != nil || frame.Type != "group.message.new" {
			t.Fatalf("%s group frame=%+v err=%v", conn.name, frame, err)
		}
		var message struct {
			ID      int64 `json:"id"`
			GroupID int64 `json:"group_id"`
		}
		if err := json.Unmarshal(frame.Message, &message); err != nil || message.ID != messageID || message.GroupID != group.ID {
			t.Fatalf("%s message=%+v err=%v", conn.name, message, err)
		}
	}
	if err := ac.WriteJSON(input); err != nil {
		t.Fatal(err)
	}
	if duplicateID, err := readMessageID(ac); err != nil || duplicateID != messageID {
		t.Fatalf("duplicate ID=%d err=%v", duplicateID, err)
	}
	history := call("GET", path+"/messages?limit=10", b.Token, "", 200)
	var messages []struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(history["messages"], &messages); err != nil || len(messages) != 1 || messages[0].ID != messageID {
		t.Fatalf("history=%+v err=%v", messages, err)
	}
	call("GET", path+"/messages", c.Token, "", 403)
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM group_messages WHERE sender_id=$1 AND client_message_id=$2::uuid`, a.User.ID, clientID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("group message rows=%d err=%v", count, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM group_message_outbox WHERE message_id=$1`, messageID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("group outbox rows=%d err=%v", count, err)
	}
	call("DELETE", path+"/members/me", b.Token, "", 200)
	call("GET", path+"/messages", b.Token, "", 403)
	call("DELETE", path+"/members/me", a.Token, "", 403)
}
