// Package hub manages WebSocket connections and real-time event fan-out.
// Each client gets a persistent, full-duplex connection to receive events
// the moment they are published — no polling required.
package hub

import (
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Maximum message size allowed from peer (64KB).
	// Prevents a single client from flooding the server with oversized payloads.
	maxMessageSize = 64 * 1024
)

// Client wraps a single WebSocket connection and the channel it subscribed to.
// Each client runs two goroutines (readPump + writePump) for concurrent I/O.
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	channelID string
	send      chan []byte
	logger    zerolog.Logger
}

// readPump runs in its own goroutine, one per client.
// It continuously reads from the WebSocket connection. Its real job isn't to
// process messages — it's to detect when the client disconnects so we can
// clean up the connection and free resources.
func (c *Client) readPump() {
	defer func() {
		// Client disconnected — clean up the connection and free resources.
		c.conn.Close()
		c.logger.Info().Msg("client disconnected")
	}()

	c.conn.SetReadLimit(maxMessageSize)

	for {
		// ReadMessage blocks until a message arrives or the connection breaks.
		// We don't use the message content yet — the purpose is to detect
		// disconnect (err != nil) which breaks the loop and triggers cleanup.
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.logger.Warn().Err(err).Msg("unexpected close")
			}
			return
		}
	}
}

// writePump runs in its own goroutine, one per client.
// It drains the send channel and writes each message to the WebSocket connection.
// This is the only goroutine that writes to the conn — gorilla/websocket requires
// at most one concurrent writer.
func (c *Client) writePump() {
	defer c.conn.Close()

	for message := range c.send {
		// Set a deadline so a slow/dead client doesn't block the writer forever.
		c.conn.SetWriteDeadline(time.Now().Add(writeWait))

		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
			c.logger.Warn().Err(err).Msg("write failed")
			return
		}
	}

	// send channel was closed (by Hub or readPump cleanup) — send a close frame
	// to gracefully inform the client before dropping the connection.
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	c.conn.WriteMessage(websocket.CloseMessage, []byte{})
}
