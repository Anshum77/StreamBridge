package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type createTenantRequest struct {
	Name         string `json:"name" binding:"required"`
	ChannelLimit int    `json:"channel_limit" binding:"required"`
	WSLimit      int    `json:"ws_limit" binding:"required"`
	RateLimit    int    `json:"rate_limit" binding:"required"`
	RateWindow   int    `json:"rate_window" binding:"required"`
}

// createTenant provisions a new isolated environment with defined resource quotas.
func (h *Handler) createTenant(c *gin.Context) {
	var req createTenantRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tenant, err := h.tenants.CreateTenant(c.Request.Context(), req.Name, req.ChannelLimit, req.WSLimit, req.RateLimit, req.RateWindow)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to create tenant")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to provision tenant"})
		return
	}

	c.JSON(http.StatusCreated, tenant)
}

// generateAPIKey issues a new authentication token for a specific tenant.
func (h *Handler) generateAPIKey(c *gin.Context) {
	tenantID := c.Param("id")

	rawKey, err := h.tenants.CreateAPIKey(c.Request.Context(), tenantID)
	if err != nil {
		h.logger.Error().Err(err).Str("tenant_id", tenantID).Msg("failed to generate api key")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to issue key"})
		return
	}

	// The plaintext key is returned exactly once; we only store the hash.
	c.JSON(http.StatusCreated, gin.H{
		"tenant_id": tenantID,
		"api_key":   rawKey,
	})
}
