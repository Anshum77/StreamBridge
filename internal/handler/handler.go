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
	"github.com/Anshum77/StreamBridge/internal/model"
	"github.com/Anshum77/StreamBridge/internal/repository"
)

// Handler holds shared dependencies injected at server startup.
type Handler struct {
	db       *pgxpool.Pool
	redis    *redis.Client
	hub      *hub.Hub
	events   *repository.EventRepo
	logger   zerolog.Logger
}

// New creates a Handler with all required dependencies.
func New(db *pgxpool.Pool, redisClient *redis.Client, wsHub *hub.Hub, eventRepo *repository.EventRepo, logger zerolog.Logger) *Handler {
	return &Handler{
		db:     db,
		redis:  redisClient,
		hub:    wsHub,
		events: eventRepo,
		logger: logger,
	}
}

// RegisterRoutes maps all API endpoints to their handler methods.
func (h *Handler) RegisterRoutes(router *gin.Engine) {
	router.GET("/health", h.health)
	router.GET("/ready", h.ready)

	channels := router.Group("/channels")
	channels.GET("", h.listChannels)
	channels.POST("", h.createChannel)
	channels.GET("/:id", h.getChannel)
	channels.PUT("/:id", h.updateChannel)
	channels.DELETE("/:id", h.deleteChannel)
	channels.GET("/:id/ws", h.subscribeWS)        // WebSocket upgrade endpoint
	channels.GET("/:id/events", h.replayEvents)    // Replay missed events by offset
	channels.POST("/:id/events", h.publishEvent)   // Persist + broadcast
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

// listChannels returns all channels ordered by newest first.
func (h *Handler) listChannels(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	rows, err := h.db.Query(ctx, `
		SELECT id, name, created_at, updated_at
		FROM channels
		ORDER BY created_at DESC
	`)
	if err != nil {
		h.internalError(c, err)
		return
	}
	defer rows.Close()

	channels := make([]model.Channel, 0)
	for rows.Next() {
		var channel model.Channel
		if err := rows.Scan(&channel.ID, &channel.Name, &channel.CreatedAt, &channel.UpdatedAt); err != nil {
			h.internalError(c, err)
			return
		}
		channels = append(channels, channel)
	}
	if err := rows.Err(); err != nil {
		h.internalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"channels": channels})
}

// createChannel inserts a new channel and returns it with the generated UUID.
func (h *Handler) createChannel(c *gin.Context) {
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

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	var channel model.Channel
	err := h.db.QueryRow(ctx, `
		INSERT INTO channels (name)
		VALUES ($1)
		RETURNING id, name, created_at, updated_at
	`, name).Scan(&channel.ID, &channel.Name, &channel.CreatedAt, &channel.UpdatedAt)
	if err != nil {
		h.internalError(c, err)
		return
	}

	c.JSON(http.StatusCreated, channel)
}

// getChannel looks up a single channel by its UUID.
func (h *Handler) getChannel(c *gin.Context) {
	channel, err := h.findChannel(c.Request.Context(), c.Param("id"))
	if err != nil {
		h.handleLookupError(c, err)
		return
	}

	c.JSON(http.StatusOK, channel)
}

// updateChannel replaces the channel name (PUT semantics = full replace).
func (h *Handler) updateChannel(c *gin.Context) {
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

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	var channel model.Channel
	err := h.db.QueryRow(ctx, `
		UPDATE channels
		SET name = $2, updated_at = now()
		WHERE id = $1
		RETURNING id, name, created_at, updated_at
	`, c.Param("id"), name).Scan(&channel.ID, &channel.Name, &channel.CreatedAt, &channel.UpdatedAt)
	if err != nil {
		h.handleLookupError(c, err)
		return
	}

	c.JSON(http.StatusOK, channel)
}

// deleteChannel removes a channel by UUID. Returns 204 on success, 404 if not found.
func (h *Handler) deleteChannel(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	result, err := h.db.Exec(ctx, "DELETE FROM channels WHERE id = $1", c.Param("id"))
	if err != nil {
		h.internalError(c, err)
		return
	}
	if result.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}

	c.Status(http.StatusNoContent)
}

// findChannel is a shared lookup used by get and update handlers.
func (h *Handler) findChannel(parent context.Context, id string) (model.Channel, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()

	var channel model.Channel
	err := h.db.QueryRow(ctx, `
		SELECT id, name, created_at, updated_at
		FROM channels
		WHERE id = $1
	`, id).Scan(&channel.ID, &channel.Name, &channel.CreatedAt, &channel.UpdatedAt)
	if err != nil {
		return model.Channel{}, err
	}

	return channel, nil
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

// subscribeWS upgrades the HTTP connection to WebSocket for real-time event delivery.
func (h *Handler) subscribeWS(c *gin.Context) {
	channelID := c.Param("id")
	hub.ServeWS(h.hub, channelID, c.Writer, c.Request, h.logger)
}

// publishEvent persists an event to Postgres, then broadcasts to WebSocket subscribers.
// Persist-first guarantees durability — even if the broadcast fails, clients can replay.
func (h *Handler) publishEvent(c *gin.Context) {
	var req publishRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	channelID := c.Param("id")

	// Persist first — durable before broadcast.
	event, err := h.events.Insert(c.Request.Context(), channelID, req.Payload)
	if err != nil {
		h.internalError(c, err)
		return
	}

	// Broadcast to live WebSocket subscribers.
	wsPayload, _ := json.Marshal(event)
	h.hub.Broadcast(channelID, wsPayload)

	c.JSON(http.StatusCreated, event)
}

type publishRequest struct {
	Payload json.RawMessage `json:"payload" binding:"required"`
}

// replayEvents returns persisted events for a channel after a given offset.
// Clients call this on reconnect to catch up on missed events.
func (h *Handler) replayEvents(c *gin.Context) {
	channelID := c.Param("id")

	afterOffset, _ := strconv.ParseInt(c.DefaultQuery("after_offset", "0"), 10, 64)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))

	events, err := h.events.ListAfterOffset(c.Request.Context(), channelID, afterOffset, limit)
	if err != nil {
		h.internalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"events": events, "count": len(events)})
}
