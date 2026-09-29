package middleware

import (
	"encoding/json"
	"net"
	"net/http"

	"github.com/VMC6G8P2TK-Lab/go-im/internal/ratelimit"
)

func LimitIP(limiter *ratelimit.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		// X-Forwarded-For is intentionally ignored until a trusted proxy is configured.
		if !limiter.Allow(ip) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{
				"code": "rate_limited", "message": "too many requests"}})
			return
		}
		next(w, r)
	}
}
