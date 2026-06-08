package hub

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

// upgrader handles the HTTP → WebSocket protocol switch.
// ReadBufferSize/WriteBufferSize control the I/O buffer per connection (4KB each).
// CheckOrigin allows all origins in development — restrict this in production.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for local dev; lock down in production.
	},
}

// ServeWS upgrades an HTTP request to a WebSocket connection.
// This is the entry point for every real-time subscriber — called once per client.
//
// What happens here:
//  1. HTTP GET arrives with "Upgrade: websocket" headers
//  2. gorilla/websocket sends back "101 Switching Protocols"
//  3. The same TCP connection is now a full-duplex WebSocket
//  4. We wrap it in a Client and log the connection
//
// In Step 2, we'll start readPump/writePump goroutines here.
func ServeWS(hub *Hub, channelID string, w http.ResponseWriter, r *http.Request, logger zerolog.Logger) {
	// Upgrade performs the HTTP → WebSocket handshake. If it fails (e.g. missing
	// headers, bad origin), gorilla writes the error response automatically.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error().Err(err).Msg("websocket upgrade failed")
		return
	}

	client := &Client{
		hub:       hub,
		conn:      conn,
		channelID: channelID,
		send:      make(chan []byte, 256), // Buffered: absorbs short bursts without blocking.
		logger:    logger.With().Str("channel", channelID).Logger(),
	}

	client.logger.Info().
		Str("remote", conn.RemoteAddr().String()).
		Msg("websocket client connected")

	// For now, just hold the connection open. In Step 2, we'll launch
	// readPump and writePump goroutines here to handle bidirectional I/O.
}
