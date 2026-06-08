package hub

import "github.com/rs/zerolog"

// Hub is the central coordinator for all WebSocket connections.
// It tracks connected clients per channel and handles register/unregister/broadcast.
// Full implementation comes in Step 3 — for now it just holds the logger.
type Hub struct {
	logger zerolog.Logger
}

// NewHub creates a Hub instance. Called once at server startup.
func NewHub(logger zerolog.Logger) *Hub {
	return &Hub{
		logger: logger,
	}
}
