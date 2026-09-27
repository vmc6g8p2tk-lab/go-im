package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/example/go-im/internal/middleware"
	"github.com/example/go-im/internal/repository"
	"github.com/example/go-im/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
)

type API struct {
	Service *service.Service
	DB      *pgxpool.Pool
}

func (a *API) Register(w http.ResponseWriter, r *http.Request) {
	var input credentials
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, 400, "bad_request", "invalid JSON body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, err := a.Service.Register(ctx, input.Username, input.Password)
	if errors.Is(err, service.ErrInvalidInput) {
		writeError(w, 400, "invalid_credentials_format", "username must be 3-32 lowercase letters, digits or underscores; password must be 8-72 bytes")
		return
	}
	if errors.Is(err, repository.ErrConflict) {
		writeError(w, 409, "username_taken", "username already exists")
		return
	}
	if err != nil {
		slog.Error("register failed", "error", err)
		writeError(w, 500, "internal_error", "internal server error")
		return
	}
	writeJSON(w, 201, map[string]any{"user": user})
}

func (a *API) Login(w http.ResponseWriter, r *http.Request) {
	var input credentials
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, 400, "bad_request", "invalid JSON body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, token, expires, err := a.Service.Login(ctx, input.Username, input.Password)
	if errors.Is(err, service.ErrInvalidCredentials) {
		writeError(w, 401, "invalid_credentials", "invalid username or password")
		return
	}
	if err != nil {
		slog.Error("login failed", "error", err)
		writeError(w, 500, "internal_error", "internal server error")
		return
	}
	writeJSON(w, 200, map[string]any{"user": user, "token": token, "expires_at": expires})
}

func (a *API) History(w http.ResponseWriter, r *http.Request) {
	peerID, err := strconv.ParseInt(r.URL.Query().Get("peer_id"), 10, 64)
	if err != nil || peerID <= 0 {
		writeError(w, 400, "bad_request", "peer_id must be a positive integer")
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, 400, "bad_request", "limit must be between 1 and 100")
			return
		}
	}
	var beforeID int64
	if raw := r.URL.Query().Get("before_id"); raw != "" {
		beforeID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || beforeID <= 0 {
			writeError(w, 400, "bad_request", "before_id must be a positive integer")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	messages, err := a.Service.History(ctx, middleware.UserID(r.Context()), peerID, beforeID, limit)
	if err != nil {
		slog.Error("history query failed", "error", err)
		writeError(w, 500, "internal_error", "internal server error")
		return
	}
	writeJSON(w, 200, map[string]any{"messages": messages})
}

func (a *API) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.DB.Ping(ctx); err != nil {
		writeError(w, 503, "database_unavailable", "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func decodeJSON(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("multiple JSON values")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write response failed", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
