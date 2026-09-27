package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/go-im/internal/auth"
	"github.com/example/go-im/internal/config"
	"github.com/example/go-im/internal/handler"
	"github.com/example/go-im/internal/middleware"
	"github.com/example/go-im/internal/migration"
	"github.com/example/go-im/internal/repository"
	"github.com/example/go-im/internal/service"
	"github.com/example/go-im/internal/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(connectCtx); err != nil {
		return err
	}
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		return migration.Apply(ctx, pool, "migrations")
	}
	manager := auth.New(config.JWTSecret)
	store := &repository.Store{DB: pool}
	svc := &service.Service{Store: store, Auth: manager}
	api := &handler.API{Service: svc, DB: pool}
	hub := websocket.NewHub()
	wsServer := &websocket.Server{Auth: manager, Service: svc, Hub: hub, AllowedOrigins: config.AllowedOrigins}
	retryCtx, retryCancel := context.WithCancel(ctx)
	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		wsServer.RunRetryLoop(retryCtx)
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/auth/register", api.Register)
	mux.HandleFunc("POST /v1/auth/login", api.Login)
	mux.Handle("GET /v1/messages", middleware.RequireAuth(manager, http.HandlerFunc(api.History)))
	mux.Handle("GET /v1/ws", wsServer)
	mux.HandleFunc("GET /healthz", api.Health)
	server := &http.Server{
		Addr:              config.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	slog.Info("server listening", "addr", config.Addr)
	var listenErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			listenErr = err
		}
	}
	retryCancel()
	<-retryDone
	// Upgraded sockets are outside http.Server.Shutdown; close them explicitly.
	hub.CloseAll()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return listenErr
}
