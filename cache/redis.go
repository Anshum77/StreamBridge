// Package cache provides the Redis client used for distributed rate limiting.
package cache

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/Anshum77/StreamBridge/config"
)

// NewClient creates a Redis connection and verifies it with a PING.
// Returns an error immediately if Redis is unreachable — fail-fast on startup.
func NewClient(ctx context.Context, cfg config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	return client, nil
}
