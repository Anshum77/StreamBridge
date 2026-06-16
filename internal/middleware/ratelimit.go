package middleware

import (
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/Anshum77/StreamBridge/internal/ratelimit"
)

// RateLimiter enforces per-IP request quotas using a Redis-backed sliding window.
// Exceeding the quota results in a 429 Too Many Requests response.
func RateLimiter(limiter *ratelimit.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()

		res, err := limiter.Allow(c.Request.Context(), ip)
		if err != nil {
			// Fail-close to protect downstream services.
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "rate limiter unavailable"})
			return
		}

		// Attach standard visibility headers.
		c.Header("X-RateLimit-Limit", strconv.Itoa(res.Limit))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))

		if !res.Allowed {
			// Retry-After strictly requires integer seconds.
			retrySec := int(math.Ceil(res.RetryAfter.Seconds()))
			if retrySec < 1 {
				retrySec = 1
			}

			c.Header("Retry-After", strconv.Itoa(retrySec))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "too many requests",
				"retry_after": fmt.Sprintf("%ds", retrySec),
			})
			return
		}

		c.Next()
	}
}
