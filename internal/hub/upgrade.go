package hub

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

// upgrader handles the HTTP → WebSocket protocol switch.
// ReadBufferSize/WriteBufferSize control the I/O buffer per connection (4KB each).
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for local dev; restrict in production.
	},
}

// ServeWS upgrades an HTTP request to a WebSocket connection and starts
// the read/write pump goroutines for bidirectional communication.
func ServeWS(hub *Hub, channelID string, w http.ResponseWriter, r *http.Request, logger zerolog.Logger) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error().Err(err).Msg("websocket upgrade failed")
		return
	}

	client := &Client{
		hub:       hub,
		conn:      conn,
		channelID: channelID,
		send:      make(chan []byte, 256),
		logger:    logger.With().Str("channel", channelID).Logger(),
	}

	client.logger.Info().
		Str("remote", conn.RemoteAddr().String()).
		Msg("websocket client connected")

	// Each pump runs in its own goroutine. readPump detects disconnect,
	// writePump delivers messages. They share nothing except the send channel.
	go client.writePump()
	go client.readPump()
}
