// Package middleware provides Gin middleware for cross-cutting concerns (logging, auth, rate limiting).
package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// RequestLogger logs every HTTP request with method, path, status, and latency.
// Runs after the handler (c.Next()) so it captures the final response status.
func RequestLogger(logger zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		startedAt := time.Now()

		c.Next()

		logger.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Int("bytes", c.Writer.Size()).
			Dur("duration", time.Since(startedAt)).
			Msg("http request")
	}
}
