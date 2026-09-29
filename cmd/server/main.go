package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/go-im/internal/auth"
	"github.com/example/go-im/internal/config"
	"github.com/example/go-im/internal/handler"
	"github.com/example/go-im/internal/messaging"
	"github.com/example/go-im/internal/metrics"
	"github.com/example/go-im/internal/middleware"
	"github.com/example/go-im/internal/migration"
	"github.com/example/go-im/internal/presence"
	"github.com/example/go-im/internal/ratelimit"
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
	observability := metrics.New()
	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return err
	}
	poolConfig.ConnConfig.Tracer = metrics.DBTracer{Metrics: observability}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
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
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return err
		}
		instanceID = hex.EncodeToString(random[:])
	}
	var presenceStore *presence.RedisStore
	if config.RedisAddr != "" {
		presenceStore = presence.New(config.RedisAddr, config.RedisPassword)
		presenceStore.Metrics = observability
		defer presenceStore.Close()
		pingCtx, pingCancel := context.WithTimeout(ctx, time.Second)
		if err := presenceStore.Ping(pingCtx); err != nil {
			slog.Warn("Redis unavailable at startup; local chat continues", "error", err)
		}
		pingCancel()
	}
	store := &repository.Store{DB: pool}
	svc := &service.Service{Store: store, Auth: manager}
	api := &handler.API{Service: svc, DB: pool}
	hub := websocket.NewHub()
	hub.Metrics = observability
	observability.RegisterHubGauges(func() float64 { connections, _ := hub.Stats(); return connections }, func() float64 { _, depth := hub.Stats(); return depth })
	wsServer := &websocket.Server{Auth: manager, Service: svc, Hub: hub, AllowedOrigins: config.AllowedOrigins,
		Presence: presenceStore, InstanceID: instanceID,
		UserLimiter: ratelimit.New(100, 200), ConnectionRate: 100, ConnectionBurst: 200, Metrics: observability}
	var broker *messaging.Kafka
	if config.KafkaBrokers != "" {
		broker = messaging.New(config.KafkaBrokers, instanceID)
		broker.Metrics = observability
		wsServer.AsyncDelivery = true
	}
	retryCtx, retryCancel := context.WithCancel(ctx)
	defer retryCancel()
	kafkaPublisherDone := make(chan struct{})
	groupPublisherDone := make(chan struct{})
	statusPublisherDone := make(chan struct{})
	kafkaConsumerDone := make(chan struct{})
	if broker != nil {
		go func() { defer close(kafkaPublisherDone); messaging.RunPublisher(retryCtx, store, broker.Publish) }()
		go func() { defer close(kafkaConsumerDone); broker.RunConsumer(retryCtx, wsServer.HandleOutboxEvent) }()
		go func() { defer close(groupPublisherDone); messaging.RunGroupPublisher(retryCtx, store, broker.Publish) }()
		go func() {
			defer close(statusPublisherDone)
			messaging.RunStatusPublisher(retryCtx, store, broker.Publish)
		}()
	} else {
		close(kafkaPublisherDone)
		close(groupPublisherDone)
		close(statusPublisherDone)
		close(kafkaConsumerDone)
	}
	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		wsServer.RunRetryLoop(retryCtx)
	}()
	presenceDone := make(chan struct{})
	go func() {
		defer close(presenceDone)
		wsServer.RunPresenceLoop(retryCtx)
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/auth/register", middleware.LimitIP(ratelimit.New(2, 100), api.Register))
	mux.HandleFunc("POST /v1/auth/login", middleware.LimitIP(ratelimit.New(2, 100), api.Login))
	mux.Handle("GET /v1/messages", middleware.RequireAuth(manager, http.HandlerFunc(api.History)))
	mux.Handle("POST /v1/groups", middleware.RequireAuth(manager, http.HandlerFunc(api.CreateGroup)))
	mux.Handle("POST /v1/groups/{id}/join", middleware.RequireAuth(manager, http.HandlerFunc(api.JoinGroup)))
	mux.Handle("DELETE /v1/groups/{id}/members/me", middleware.RequireAuth(manager, http.HandlerFunc(api.LeaveGroup)))
	mux.Handle("DELETE /v1/groups/{id}/members/{user_id}", middleware.RequireAuth(manager, http.HandlerFunc(api.RemoveGroupMember)))
	mux.Handle("GET /v1/groups/{id}/members", middleware.RequireAuth(manager, http.HandlerFunc(api.GroupMembers)))
	mux.Handle("GET /v1/groups/{id}/messages", middleware.RequireAuth(manager, http.HandlerFunc(api.GroupHistory)))
	mux.Handle("GET /v1/ws", wsServer)
	mux.HandleFunc("GET /healthz", api.Health)
	mux.Handle("GET /metrics", observability.Handler())
	server := &http.Server{
		Addr:              config.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	// Profiling is opt-in and listens only on loopback so profile data is not
	// exposed through the public API or Docker port mapping.
	var profileServer *http.Server
	if address := os.Getenv("PPROF_ADDR"); address != "" {
		host, _, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" && host != "localhost" && host != "::1" {
			return errors.New("PPROF_ADDR must be a loopback host:port")
		}
		profileMux := http.NewServeMux()
		profileMux.HandleFunc("/debug/pprof/", pprof.Index)
		profileMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		profileMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		profileMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		profileMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		profileServer = &http.Server{Addr: address, Handler: profileMux, ReadHeaderTimeout: 5 * time.Second}
		profileListener, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}
		defer profileListener.Close()
		go func() {
			if err := profileServer.Serve(profileListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("pprof server stopped", "error", err)
			}
		}()
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
	<-presenceDone
	<-kafkaPublisherDone
	<-groupPublisherDone
	<-statusPublisherDone
	<-kafkaConsumerDone
	if broker != nil {
		broker.Close()
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
	wsServer.RemoveLocalRoutes(cleanupCtx)
	cleanupCancel()
	// Upgraded sockets are outside http.Server.Shutdown; close them explicitly.
	hub.CloseAll()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if profileServer != nil {
		if err := profileServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return listenErr
}
