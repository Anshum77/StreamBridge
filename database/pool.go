// Package database manages the PostgreSQL connection pool and schema migrations.
package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/Anshum77/StreamBridge/config"
)

// NewPool creates a pgx connection pool with sensible defaults.
// Pooling reuses connections instead of dialing per-request — critical at high throughput.
func NewPool(ctx context.Context, cfg config.Config, logger zerolog.Logger) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	// Ping verifies the connection is live; fail early if DB is unreachable.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	logger.Info().Msg("connected to postgres")
	return pool, nil
}
