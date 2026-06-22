package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/Anshum77/StreamBridge/internal/repository"
)

// RequireAPIKey enforces authentication via Bearer token and injects the resolved Tenant into the request context.
func RequireAPIKey(repo *repository.TenantRepo) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization format"})
			return
		}

		rawKey := parts[1]
		tenant, err := repo.GetTenantByAPIKey(c.Request.Context(), rawKey)
		if err != nil {
			// Suppress internal resolution errors to prevent leaking database state.
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
			return
		}

		// Attach the authenticated tenant entity for downstream handlers.
		c.Set("tenant", tenant)
		c.Next()
	}
}
