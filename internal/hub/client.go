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

	// Time allowed to read the next pong message from the peer.
	// If no pong arrives within this window, the connection is considered dead.
	pongWait = 30 * time.Second

	// Pings are sent at this interval. Must be less than pongWait so the
	// client has time to respond before the read deadline expires.
	pingPeriod = (pongWait * 9) / 10 // 27s

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
// It continuously reads from the WebSocket connection to detect disconnects
// and handles pong responses to keep the heartbeat alive.
func (c *Client) readPump() {
	defer func() {
		// Notify the Hub to remove this client from the subscriber map.
		// The Hub will close the send channel, which stops writePump.
		c.hub.unregister <- c
		c.conn.Close()
		c.logger.Info().Msg("client disconnected")
	}()

	c.conn.SetReadLimit(maxMessageSize)

	// Set the initial read deadline. If no data (including pong) arrives
	// within pongWait, ReadMessage returns an error and we clean up.
	c.conn.SetReadDeadline(time.Now().Add(pongWait))

	// When a pong arrives, push the read deadline forward.
	// This keeps the connection alive as long as the client is responsive.
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
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
// It drains the send channel and periodically pings the client to detect
// dead connections. This is the only goroutine that writes to the conn.
func (c *Client) writePump() {
	// Ticker fires every pingPeriod to send heartbeat pings.
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				// Hub closed the send channel — send a close frame and exit.
				c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				c.logger.Warn().Err(err).Msg("write failed")
				return
			}

		case <-ticker.C:
			// Send a ping frame. If the write deadline expires (client is
			// unreachable), the next ReadMessage in readPump will also fail
			// and trigger cleanup.
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
