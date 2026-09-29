package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VMC6G8P2TK-Lab/go-im/internal/ratelimit"
)

func TestLimitIPIgnoresUntrustedForwardedFor(t *testing.T) {
	handler := LimitIP(ratelimit.New(0.001, 1), func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	for i := 0; i < 2; i++ {
		request := httptest.NewRequest(http.MethodPost, "/v1/auth/login", nil)
		request.RemoteAddr = "192.0.2.10:1234"
		request.Header.Set("X-Forwarded-For", "198.51.100.1")
		response := httptest.NewRecorder()
		handler(response, request)
		wanted := http.StatusNoContent
		if i == 1 {
			wanted = http.StatusTooManyRequests
		}
		if response.Code != wanted {
			t.Fatalf("request %d status=%d", i, response.Code)
		}
	}
}
