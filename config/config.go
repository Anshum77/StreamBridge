// Package config loads application settings from environment variables with .env fallback.
// No external config library (viper, etc.) — keeps the dependency tree lean.
package config

import (
	"bufio"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings injected via environment variables.
type Config struct {
	AppEnv          string
	HTTPAddr        string
	DatabaseURL     string
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	RateLimit       int
	RateWindow      time.Duration
	AdminAPIKey     string
	MigrationsPath  string
	ShutdownTimeout time.Duration
}

// Load reads config from env vars, falling back to .env file for local dev.
func Load() (Config, error) {
	_ = loadDotEnv(".env")

	redisDB, err := intEnv("REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}

	shutdownTimeout, err := durationEnv("SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	rateLimit, err := intEnv("RATE_LIMIT", 100)
	if err != nil {
		return Config{}, err
	}

	rateWindow, err := durationEnv("RATE_WINDOW", time.Minute)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		AppEnv:          stringEnv("APP_ENV", "development"),
		HTTPAddr:        stringEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:     stringEnv("DATABASE_URL", "postgres://streambridge:streambridge@localhost:5432/streambridge?sslmode=disable"),
		RedisAddr:       stringEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   stringEnv("REDIS_PASSWORD", ""),
		RedisDB:         redisDB,
		RateLimit:       rateLimit,
		RateWindow:      rateWindow,
		AdminAPIKey:     stringEnv("ADMIN_API_KEY", "super-secret-admin-key"),
		MigrationsPath:  stringEnv("MIGRATIONS_PATH", "migrations"),
		ShutdownTimeout: shutdownTimeout,
	}

	// Validate required fields — fail early rather than at first query.
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return Config{}, errors.New("REDIS_ADDR is required")
	}

	return cfg, nil
}

// stringEnv returns the env var value or the fallback if unset/empty.
func stringEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// intEnv parses an integer env var with a fallback default.
func intEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

// durationEnv parses a Go duration string (e.g. "15s", "1m") from env.
func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

// loadDotEnv is a minimal .env parser. It only sets vars that aren't already
// defined in the real environment, so real env vars always take precedence.
func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" || os.Getenv(key) != "" {
			continue
		}

		_ = os.Setenv(key, value)
	}

	return scanner.Err()
}
