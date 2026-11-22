package engineio_test

import (
	"context"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// client is one open session seen from both ends: the server-side recorder and
// the client-side connection. ws is nil for a polling client.
type client struct {
	h   *harness
	rec *recorder
	sid string
	ws  *wsConn
}

type transportCase struct {
	name    string
	connect func(*harness) *client
}

var transportCases = []transportCase{
	{"polling", func(h *harness) *client {
		sid, rec := h.open()
		return &client{h: h, rec: rec, sid: sid}
	}},
	{"websocket", func(h *harness) *client {
		ws, open := h.openWS(nil)
		return &client{h: h, rec: h.recorder(open.SID), sid: open.SID, ws: ws}
	}},
}

// write sends the packets and, over polling, waits for the POST to be answered.
func (c *client) write(packets ...string) {
	c.h.t.Helper()
	if c.ws != nil {
		for _, p := range packets {
			c.ws.write(p)
		}
		return
	}
	if r := c.h.post(c.sid, strings.Join(packets, "\x1e")); r.status != http.StatusOK || r.body != "ok" {
		c.h.t.Fatalf("POST %q = %d %q, want 200 \"ok\"", packets, r.status, r.body)
	}
}

// writeAsync sends one packet without waiting for the POST to be answered.
func (c *client) writeAsync(packet string) {
	c.h.t.Helper()
	if c.ws != nil {
		c.ws.write(packet)
		return
	}
	c.h.async(http.MethodPost, c.h.pollingURL(c.sid), packet)
}

// hangUp sends the close packet and may be called from any goroutine.
func (c *client) hangUp() {
	if c.ws != nil {
		c.ws.WriteMessage(websocket.TextMessage, []byte("1"))
		return
	}
	c.h.send(context.Background(), http.MethodPost, c.h.pollingURL(c.sid), "1", nil)
}
