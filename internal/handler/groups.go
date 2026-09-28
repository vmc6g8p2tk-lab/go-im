package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/example/go-im/internal/middleware"
	"github.com/example/go-im/internal/repository"
	"github.com/example/go-im/internal/service"
)

func groupID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, service.ErrInvalidInput
	}
	return id, nil
}

func groupError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(w, 400, "bad_request", "invalid group request")
	case errors.Is(err, repository.ErrNotFound):
		writeError(w, 404, "group_not_found", "group does not exist")
	case errors.Is(err, repository.ErrForbidden):
		writeError(w, 403, "forbidden", "group membership required")
	case errors.Is(err, repository.ErrGroupFull):
		writeError(w, 409, "group_full", "group has 100 members")
	default:
		writeError(w, 500, "internal_error", "internal server error")
	}
}

func (a *API) CreateGroup(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, 400, "bad_request", "invalid JSON body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	group, err := a.Service.CreateGroup(ctx, middleware.UserID(r.Context()), input.Name)
	if err != nil {
		groupError(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"group": group})
}

func (a *API) JoinGroup(w http.ResponseWriter, r *http.Request) {
	id, err := groupID(r)
	if err != nil {
		groupError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.Service.JoinGroup(ctx, id, middleware.UserID(r.Context())); err != nil {
		groupError(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "joined"})
}

func (a *API) LeaveGroup(w http.ResponseWriter, r *http.Request) {
	id, err := groupID(r)
	if err != nil {
		groupError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.Service.LeaveGroup(ctx, id, middleware.UserID(r.Context())); err != nil {
		groupError(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "left"})
}

func (a *API) RemoveGroupMember(w http.ResponseWriter, r *http.Request) {
	id, err := groupID(r)
	if err != nil {
		groupError(w, err)
		return
	}
	targetID, err := strconv.ParseInt(r.PathValue("user_id"), 10, 64)
	if err != nil || targetID <= 0 {
		groupError(w, service.ErrInvalidInput)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.Service.RemoveGroupMember(ctx, id, middleware.UserID(r.Context()), targetID); err != nil {
		groupError(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "removed"})
}

func (a *API) GroupMembers(w http.ResponseWriter, r *http.Request) {
	id, err := groupID(r)
	if err != nil {
		groupError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	members, err := a.Service.GroupMembers(ctx, id, middleware.UserID(r.Context()))
	if err != nil {
		groupError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"members": members})
}

func (a *API) GroupHistory(w http.ResponseWriter, r *http.Request) {
	id, err := groupID(r)
	if err != nil {
		groupError(w, err)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			groupError(w, service.ErrInvalidInput)
			return
		}
	}
	var beforeID int64
	if raw := r.URL.Query().Get("before_id"); raw != "" {
		beforeID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || beforeID <= 0 {
			groupError(w, service.ErrInvalidInput)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	messages, err := a.Service.GroupHistory(ctx, id, middleware.UserID(r.Context()), beforeID, limit)
	if err != nil {
		groupError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"messages": messages})
}
