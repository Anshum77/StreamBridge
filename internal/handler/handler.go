// Package handler implements HTTP route handlers for the StreamBridge REST API.
// All handlers receive shared dependencies (DB pool, Redis, logger) via the Handler struct.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/Anshum77/StreamBridge/internal/hub"
	"github.com/Anshum77/StreamBridge/internal/metrics"
	"github.com/Anshum77/StreamBridge/internal/middleware"
	"github.com/Anshum77/StreamBridge/internal/model"
	"github.com/Anshum77/StreamBridge/internal/ratelimit"
	"github.com/Anshum77/StreamBridge/internal/repository"
)

// Handler holds shared dependencies injected at server startup.
type Handler struct {
	db       *pgxpool.Pool
	redis    *redis.Client
	hub      *hub.Hub
	events   *repository.EventRepo
	tenants  *repository.TenantRepo
	channels *repository.ChannelRepo
	limiter  *ratelimit.Limiter
	adminKey string
	logger   zerolog.Logger
}

// New creates a Handler with all required dependencies.
func New(db *pgxpool.Pool, redisClient *redis.Client, wsHub *hub.Hub, eventRepo *repository.EventRepo, tenantRepo *repository.TenantRepo, channelRepo *repository.ChannelRepo, limiter *ratelimit.Limiter, adminKey string, logger zerolog.Logger) *Handler {
	return &Handler{
		db:       db,
		redis:    redisClient,
		hub:      wsHub,
		events:   eventRepo,
		tenants:  tenantRepo,
		channels: channelRepo,
		limiter:  limiter,
		adminKey: adminKey,
		logger:   logger,
	}
}

// RegisterRoutes maps all API endpoints to their handler methods.
func (h *Handler) RegisterRoutes(router *gin.Engine) {
	router.GET("/health", h.health)
	router.GET("/ready", h.ready)

	channels := router.Group("/channels")
	channels.Use(middleware.RequireAPIKey(h.tenants))
	channels.GET("", h.listChannels)
	channels.POST("", h.createChannel)
	channels.GET("/:id", h.getChannel)
	channels.PUT("/:id", h.updateChannel)
	channels.DELETE("/:id", h.deleteChannel)
	channels.GET("/:id/ws", h.subscribeWS)
	channels.GET("/:id/events", h.replayEvents)
	channels.POST("/:id/events", middleware.RateLimiter(h.limiter), h.publishEvent)

	// Admin API (provisioning tenants and keys)
	admin := router.Group("/admin")
	admin.Use(func(c *gin.Context) {
		auth := c.GetHeader("Authorization")
		if auth != "Bearer "+h.adminKey {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized admin"})
			return
		}
		c.Next()
	})
	admin.POST("/tenants", h.createTenant)
	admin.POST("/tenants/:id/keys", h.generateAPIKey)
}

// getTenantFromContext securely extracts the authenticated tenant from the Gin context.
func (h *Handler) getTenantFromContext(c *gin.Context) (*model.Tenant, bool) {
	val, exists := c.Get("tenant")
	if !exists {
		h.logger.Error().Msg("tenant missing from context")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return nil, false
	}
	tenant, ok := val.(*model.Tenant)
	if !ok {
		h.logger.Error().Msg("tenant in context is invalid type")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
		return nil, false
	}
	return tenant, true
}

// health is a lightweight liveness probe — returns 200 if the process is running.
func (h *Handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// ready is a readiness probe — returns 200 only if both Postgres and Redis are reachable.
func (h *Handler) ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "postgres": "down"})
		return
	}
	if err := h.redis.Ping(ctx).Err(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "redis": "down"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// listChannels returns all channels belonging to the authenticated tenant, ordered by newest first.
func (h *Handler) listChannels(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	channels, err := h.channels.List(c.Request.Context(), tenant.ID)
	if err != nil {
		h.internalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"channels": channels})
}

// createChannel provisions a new channel under the authenticated tenant's namespace.
func (h *Handler) createChannel(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	var req channelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel name is required"})
		return
	}

	channel, err := h.channels.Create(c.Request.Context(), tenant.ID, name)
	if err != nil {
		h.internalError(c, err)
		return
	}

	c.JSON(http.StatusCreated, channel)
}

// getChannel looks up a single channel by its UUID, enforcing tenant ownership.
func (h *Handler) getChannel(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	channel, err := h.channels.Get(c.Request.Context(), tenant.ID, c.Param("id"))
	if err != nil {
		h.handleLookupError(c, err)
		return
	}

	c.JSON(http.StatusOK, channel)
}

// updateChannel replaces the channel name, enforcing tenant ownership.
func (h *Handler) updateChannel(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	var req channelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel name is required"})
		return
	}

	channel, err := h.channels.Update(c.Request.Context(), tenant.ID, c.Param("id"), name)
	if err != nil {
		h.handleLookupError(c, err)
		return
	}

	c.JSON(http.StatusOK, channel)
}

// deleteChannel removes a channel by UUID, enforcing tenant ownership.
func (h *Handler) deleteChannel(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	rowsAffected, err := h.channels.Delete(c.Request.Context(), tenant.ID, c.Param("id"))
	if err != nil {
		h.internalError(c, err)
		return
	}
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}

	c.Status(http.StatusNoContent)
}


// handleLookupError maps pgx.ErrNoRows → 404, everything else → 500.
func (h *Handler) handleLookupError(c *gin.Context, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}

	h.internalError(c, err)
}

// internalError logs the real error and returns a generic 500 to the client.
func (h *Handler) internalError(c *gin.Context, err error) {
	h.logger.Error().Err(err).Msg("handler error")
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
}

type channelRequest struct {
	Name string `json:"name"`
}

const defaultReplayLimit = 100

// subscribeWS upgrades the HTTP connection to WebSocket for real-time event delivery.
func (h *Handler) subscribeWS(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	channelID := c.Param("id")
	if _, err := h.channels.Get(c.Request.Context(), tenant.ID, channelID); err != nil {
		h.handleLookupError(c, err)
		return
	}

	var missedEvents [][]byte
	lastSeenOffsetStr := c.Query("last_seen_offset")
	
	if lastSeenOffsetStr != "" {
		lastSeenOffset, err := strconv.ParseInt(lastSeenOffsetStr, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid last_seen_offset parameter"})
			return
		}
		
		events, fetchErr := h.events.ListAfterOffset(c.Request.Context(), channelID, lastSeenOffset, defaultReplayLimit)
		if fetchErr != nil {
			h.internalError(c, fetchErr)
			return
		}
		
		for _, ev := range events {
			if payload, marshalErr := json.Marshal(ev); marshalErr == nil {
				missedEvents = append(missedEvents, payload)
			}
		}
	}

	hub.ServeWS(h.hub, channelID, c.Writer, c.Request, missedEvents, h.logger)
}

// publishEvent persists an event to Postgres, then broadcasts to WebSocket subscribers.
// Persist-first guarantees durability — even if the broadcast fails, clients can replay.
func (h *Handler) publishEvent(c *gin.Context) {
	var req publishRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	channelID := c.Param("id")
	if _, err := h.channels.Get(c.Request.Context(), tenant.ID, channelID); err != nil {
		h.handleLookupError(c, err)
		return
	}

	// Persist first — durable before broadcast.
	event, err := h.events.Insert(c.Request.Context(), channelID, req.Payload)
	if err != nil {
		h.internalError(c, err)
		return
	}

	// Broadcast to live WebSocket subscribers.
	wsPayload, _ := json.Marshal(event)
	h.hub.Broadcast(channelID, wsPayload)

	metrics.EventsPublished.Inc()

	c.JSON(http.StatusCreated, event)
}

type publishRequest struct {
	Payload json.RawMessage `json:"payload" binding:"required"`
}

// replayEvents returns persisted events for a channel after a given offset.
// Clients call this on reconnect to catch up on missed events.
func (h *Handler) replayEvents(c *gin.Context) {
	tenant, ok := h.getTenantFromContext(c)
	if !ok {
		return
	}

	channelID := c.Param("id")
	if _, err := h.channels.Get(c.Request.Context(), tenant.ID, channelID); err != nil {
		h.handleLookupError(c, err)
		return
	}

	afterOffset, _ := strconv.ParseInt(c.DefaultQuery("after_offset", "0"), 10, 64)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))

	events, err := h.events.ListAfterOffset(c.Request.Context(), channelID, afterOffset, limit)
	if err != nil {
		h.internalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"events": events, "count": len(events)})
}
