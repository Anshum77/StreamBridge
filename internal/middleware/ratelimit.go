package middleware

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Anshum77/StreamBridge/internal/metrics"
	"github.com/Anshum77/StreamBridge/internal/model"
	"github.com/Anshum77/StreamBridge/internal/ratelimit"
)

// RateLimiter enforces per-tenant request quotas using a Redis-backed sliding window.
// It extracts the authenticated tenant from the context and applies their specific quota.
func RateLimiter(limiter *ratelimit.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		val, exists := c.Get("tenant")
		if !exists {
			// Fail-close to protect downstream services if auth middleware was bypassed.
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "tenant context missing"})
			return
		}
		tenant, ok := val.(*model.Tenant)
		if !ok {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "invalid tenant context"})
			return
		}

		// Convert tenant's rate window (stored as seconds) to time.Duration
		window := time.Duration(tenant.RateWindow) * time.Second

		res, err := limiter.Allow(c.Request.Context(), tenant.ID, tenant.RateLimit, window)
		if err != nil {
			// Fail-close if Redis is unreachable
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
			
			metrics.RateLimitHits.Inc()
			
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       "too many requests",
				"retry_after": fmt.Sprintf("%ds", retrySec),
			})
			return
		}

		c.Next()
	}
}
