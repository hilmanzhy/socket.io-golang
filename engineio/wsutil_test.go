package engineio_test

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type wsConn struct {
	*websocket.Conn
	t testing.TB
}

func (h *harness) connect(query string, header http.Header) (*wsConn, *http.Response, error) {
	dialer := websocket.Dialer{HandshakeTimeout: wait}
	conn, resp, err := dialer.Dial(h.wsURL(query), header)
	if err != nil {
		return nil, resp, err
	}
	h.t.Cleanup(func() { conn.Close() })
	return &wsConn{Conn: conn, t: h.t}, resp, nil
}

func (h *harness) openWS(header http.Header) (*wsConn, openPacket) {
	h.t.Helper()
	ws, _, err := h.connect(socketQuery, header)
	if err != nil {
		h.t.Fatalf("dial: %v", err)
	}
	return ws, parseOpen(h.t, ws.text())
}

func (h *harness) dialStatus(query string, header http.Header) int {
	h.t.Helper()
	ws, resp, err := h.connect(query, header)
	if err == nil {
		ws.Close()
		return http.StatusSwitchingProtocols
	}
	if resp == nil {
		h.t.Fatalf("dial %q: %v", query, err)
	}
	return resp.StatusCode
}

func (h *harness) connectUpgrade(sid string) *wsConn {
	h.t.Helper()
	ws, _, err := h.connect(socketQuery+"&sid="+sid, nil)
	if err != nil {
		h.t.Fatalf("dial upgrade: %v", err)
	}
	return ws
}

// upgrade runs the whole polling to WebSocket upgrade for sid and returns the
// WebSocket once the server has accepted the final upgrade packet.
func (h *harness) upgrade(sid string) *wsConn {
	h.t.Helper()
	pending := h.async(http.MethodGet, h.pollingURL(sid), "")
	ws := h.connectUpgrade(sid)
	ws.write("2probe")
	if got := ws.text(); got != "3probe" {
		h.t.Fatalf("probe answer = %q, want 3probe", got)
	}
	if r := pending.wait(h.t); r.status != http.StatusOK || r.body != "6" {
		h.t.Fatalf("pending poll = %d %q, want 200 \"6\"", r.status, r.body)
	}
	ws.write("5")
	return ws
}

func (c *wsConn) frame() (int, []byte) {
	c.t.Helper()
	c.SetReadDeadline(time.Now().Add(wait))
	kind, data, err := c.ReadMessage()
	if err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	return kind, data
}

func (c *wsConn) text() string {
	c.t.Helper()
	kind, data := c.frame()
	if kind != websocket.TextMessage {
		c.t.Fatalf("frame type = %d (%q), want text", kind, data)
	}
	return string(data)
}

func (c *wsConn) binary() []byte {
	c.t.Helper()
	kind, data := c.frame()
	if kind != websocket.BinaryMessage {
		c.t.Fatalf("frame type = %d (%q), want binary", kind, data)
	}
	return data
}

func (c *wsConn) write(s string) {
	c.t.Helper()
	c.SetWriteDeadline(time.Now().Add(wait))
	if err := c.WriteMessage(websocket.TextMessage, []byte(s)); err != nil {
		c.t.Fatalf("write %q: %v", s, err)
	}
}

func (c *wsConn) writeBinary(b []byte) {
	c.t.Helper()
	c.SetWriteDeadline(time.Now().Add(wait))
	if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		c.t.Fatalf("write binary: %v", err)
	}
}

// readError discards frames until the connection fails and returns that error.
// It fails the test when the failure is only the wait running out. The timeout
// is found through net.Error: gorilla hides the deadline error from errors.Is.
func (c *wsConn) readError() error {
	c.t.Helper()
	c.SetReadDeadline(time.Now().Add(wait))
	for {
		_, _, err := c.ReadMessage()
		if err == nil {
			continue
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			c.t.Fatal("connection still open after the deadline")
		}
		return err
	}
}
