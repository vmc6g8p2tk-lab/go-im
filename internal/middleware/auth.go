package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/VMC6G8P2TK-Lab/go-im/internal/auth"
)

type userIDKey struct{}

func UserID(ctx context.Context) int64 {
	id, _ := ctx.Value(userIDKey{}).(int64)
	return id
}

func RequireAuth(manager *auth.Manager, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			unauthorized(w)
			return
		}
		id, err := manager.Verify(parts[1])
		if err != nil {
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDKey{}, id)))
	})
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "unauthorized", "message": "valid bearer token required"}})
}
