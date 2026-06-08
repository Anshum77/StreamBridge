// Package main boots the StreamBridge server: config → DB → Redis → HTTP → graceful shutdown.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"github.com/Anshum77/StreamBridge/cache"
	"github.com/Anshum77/StreamBridge/config"
	"github.com/Anshum77/StreamBridge/database"
	"github.com/Anshum77/StreamBridge/internal/handler"
	"github.com/Anshum77/StreamBridge/internal/hub"
	"github.com/Anshum77/StreamBridge/internal/middleware"
)

func main() {
	// Fail fast if required env vars (DATABASE_URL, etc.) are missing.
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	// Single structured logger shared across all components for consistent output.
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "streambridge").
		Logger()

	if cfg.AppEnv == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	ctx := context.Background()

	// Connection pool avoids per-request dial overhead (max 10 persistent conns).
	dbPool, err := database.NewPool(ctx, cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("connect postgres")
	}
	defer dbPool.Close()

	// Auto-apply pending migrations so schema stays in sync with code.
	if err := database.RunMigrations(ctx, dbPool, cfg.MigrationsPath, logger); err != nil {
		logger.Fatal().Err(err).Msg("run migrations")
	}

	// Redis backs the distributed sliding-window rate limiter.
	redisClient, err := cache.NewClient(ctx, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("connect redis")
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Error().Err(err).Msg("close redis")
		}
	}()

	// gin.New() over gin.Default() — we supply our own zerolog middleware.
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(middleware.RequestLogger(logger))

	// WebSocket hub — central coordinator for all real-time connections.
	wsHub := hub.NewHub(logger)

	serverHandler := handler.New(dbPool, redisClient, wsHub, logger)
	serverHandler.RegisterRoutes(router)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: router,
	}

	// Serve in background; forward startup errors (e.g. port conflict) via channel.
	errCh := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", cfg.HTTPAddr).Msg("server listening")
		errCh <- server.ListenAndServe()
	}()

	// Block until SIGINT/SIGTERM, then trigger graceful shutdown.
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-stopCtx.Done():
		logger.Info().Msg("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Err(err).Msg("server stopped unexpectedly")
		}
	}

	// Drain in-flight requests; force-exit after ShutdownTimeout (default 15s).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("server shutdown timed out")
		return
	}

	logger.Info().Msg("server stopped")
}
