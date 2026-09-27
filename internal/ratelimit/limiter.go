package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter is a bounded, in-process token bucket table. It is suitable for
// local abuse protection; a cluster-wide quota needs Redis or a gateway.
type Limiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	entries map[string]bucket
	now     func() time.Time
	calls   uint64
}

func New(perSecond float64, burst int) *Limiter {
	return &Limiter{rate: perSecond, burst: float64(burst), entries: make(map[string]bucket), now: time.Now}
}

func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.calls++
	if l.calls%1024 == 0 || len(l.entries) >= 100000 {
		for k, b := range l.entries {
			if now.Sub(b.last) > 10*time.Minute {
				delete(l.entries, k)
			}
		}
	}
	b, exists := l.entries[key]
	if !exists {
		if len(l.entries) >= 100000 {
			return false
		}
		b = bucket{tokens: l.burst, last: now}
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens = min(l.burst, b.tokens+elapsed*l.rate)
	}
	b.last = now
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	l.entries[key] = b
	return allowed
}
