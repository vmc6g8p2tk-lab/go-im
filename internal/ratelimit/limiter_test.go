package ratelimit

import (
	"testing"
	"time"
)

func TestTokenBucketRefillAndIsolation(t *testing.T) {
	now := time.Now()
	l := New(2, 2)
	l.now = func() time.Time { return now }
	if !l.Allow("a") || !l.Allow("a") || l.Allow("a") {
		t.Fatal("burst was not enforced")
	}
	if !l.Allow("b") {
		t.Fatal("keys were not isolated")
	}
	now = now.Add(500 * time.Millisecond)
	if !l.Allow("a") || l.Allow("a") {
		t.Fatal("refill was not enforced")
	}
}
