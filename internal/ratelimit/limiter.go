// Package ratelimit provides a Redis-backed sliding window rate limiter.
// Uses a sorted set (ZSET) to track individual request timestamps,
// giving an accurate count over any rolling time window.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// luaScript performs the sliding window check atomically in a single round trip.
// Returns {count, allowed, oldest_score} — oldest_score enables precise Retry-After
// calculation (time until the oldest request exits the window).
var luaScript = redis.NewScript(`
	local key = KEYS[1]
	local window = tonumber(ARGV[1])
	local limit = tonumber(ARGV[2])
	local now = tonumber(ARGV[3])

	-- Prune entries outside the sliding window.
	redis.call("ZREMRANGEBYSCORE", key, 0, now - window)

	-- Count requests still within the window.
	local count = redis.call("ZCARD", key)

	if count < limit then
		-- Under limit: record this request with its timestamp as score.
		redis.call("ZADD", key, now, now .. "-" .. math.random(1000000))
		redis.call("EXPIRE", key, window)
		return {count + 1, 1, 0}
	end

	-- Over limit: find the oldest entry so caller knows when a slot opens.
	local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
	redis.call("EXPIRE", key, window)
	return {count, 0, oldest[2]}
`)

// Result holds the outcome of a rate limit check.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

// Limiter enforces per-client request limits using a sliding window over Redis.
type Limiter struct {
	client *redis.Client
	max    int
	window time.Duration
}

// NewLimiter creates a rate limiter that allows max requests per rolling window.
func NewLimiter(client *redis.Client, max int, window time.Duration) *Limiter {
	return &Limiter{
		client: client,
		max:    max,
		window: window,
	}
}

// Allow checks if the given key is within the rate limit.
func (l *Limiter) Allow(ctx context.Context, key string) (Result, error) {
	redisKey := fmt.Sprintf("ratelimit:%s", key)
	windowMs := l.window.Milliseconds()
	nowMs := time.Now().UnixMilli()

	raw, err := luaScript.Run(ctx, l.client, []string{redisKey}, windowMs, l.max, nowMs).Int64Slice()
	if err != nil {
		return Result{}, err
	}

	count := int(raw[0])
	allowed := raw[1] == 1

	remaining := l.max - count
	if remaining < 0 {
		remaining = 0
	}

	// Compute precise retry time: when the oldest request leaves the window.
	var retryAfter time.Duration
	if !allowed {
		oldestMs := raw[2]
		expiresAt := oldestMs + windowMs
		retryAfter = time.Duration(expiresAt-nowMs) * time.Millisecond
		if retryAfter < 0 {
			retryAfter = 0
		}
	}

	return Result{
		Allowed:    allowed,
		Limit:      l.max,
		Remaining:  remaining,
		RetryAfter: retryAfter,
	}, nil
}
