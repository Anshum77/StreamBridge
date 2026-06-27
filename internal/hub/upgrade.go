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
// Replays any missed events before registering the client to the live broadcast.
func ServeWS(hub *Hub, channelID string, w http.ResponseWriter, r *http.Request, missedEvents [][]byte, logger zerolog.Logger) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error().Err(err).Msg("websocket upgrade failed")
		return
	}

	client := &Client{
		hub:       hub,
		conn:      conn,
		channelID: channelID,
		send:      make(chan []byte, 4096),
		logger:    logger.With().Str("channel", channelID).Logger(),
	}

	client.logger.Info().
		Str("remote", conn.RemoteAddr().String()).
		Msg("websocket client connected")

	// Push missed events into the buffer before launching pumps to ensure strict ordering
	for _, payload := range missedEvents {
		client.send <- payload
	}

	// Launch bidirectional I/O pumps.
	go client.writePump()
	go client.readPump()

	// Register with the Hub so this client receives broadcasts for its channel.
	hub.register <- client
}
