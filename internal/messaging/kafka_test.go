package messaging

import (
	"testing"

	"github.com/example/go-im/internal/repository"
)

func TestValidEventTargets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event repository.OutboxEvent
		valid bool
	}{
		{"direct", repository.OutboxEvent{MessageID: 1, RecipientID: 2}, true},
		{"group", repository.OutboxEvent{MessageID: 1, GroupID: 2}, true},
		{"status", repository.OutboxEvent{MessageID: 1, SenderID: 2, Status: "delivered"}, true},
		{"missing target", repository.OutboxEvent{MessageID: 1}, false},
		{"mixed targets", repository.OutboxEvent{MessageID: 1, SenderID: 2, RecipientID: 3, Status: "delivered"}, false},
		{"wrong status", repository.OutboxEvent{MessageID: 1, SenderID: 2, Status: "created"}, false},
		{"status on direct", repository.OutboxEvent{MessageID: 1, RecipientID: 2, Status: "delivered"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := validEvent(tc.event); got != tc.valid {
				t.Fatalf("validEvent(%+v)=%v want=%v", tc.event, got, tc.valid)
			}
		})
	}
}
