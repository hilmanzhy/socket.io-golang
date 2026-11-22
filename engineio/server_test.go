package engineio_test

import (
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nuriontech/socket.io-golang/engineio"
)

func TestServerClose(t *testing.T) {
	t.Parallel()
	t.Run("closes polling and WebSocket sessions", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		polling := transportCases[0].connect(h)
		socket := transportCases[1].connect(h)
		h.server.Close()
		for _, c := range []*client{polling, socket} {
			if got := c.rec.reason(t); got != engineio.ReasonServerShutdown {
				t.Errorf("reason = %q, want %q", got, engineio.ReasonServerShutdown)
			}
			if err := c.rec.session.Send(msg("x")); !errors.Is(err, engineio.ErrClosed) {
				t.Errorf("Send after Server.Close = %v, want ErrClosed", err)
			}
		}
		if r := h.poll(polling.sid); r.status != http.StatusOK || r.body != "1" {
			t.Errorf("poll = %d %q, want 200 \"1\"", r.status, r.body)
		}
		socket.ws.SetReadDeadline(time.Now().Add(wait))
		if _, _, err := socket.ws.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			t.Errorf("WebSocket read = %v, want a normal closure", err)
		}
		polling.rec.ended(t)
		socket.rec.ended(t)
		if r := h.poll(polling.sid); r.status != http.StatusBadRequest {
			t.Errorf("poll after the close packet = %d, want 400", r.status)
		}
		h.server.Close()
		polling.rec.closedOnce(t)
		socket.rec.closedOnce(t)
	})
	t.Run("polling client collects queued messages first", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		c := transportCases[0].connect(h)
		if err := c.rec.session.Send(msg("a"), msg("b")); err != nil {
			t.Fatal(err)
		}
		h.server.Close()
		if r := h.poll(c.sid); r.status != http.StatusOK || r.body != "4a\x1e4b" {
			t.Errorf("first poll = %d %q, want the queued messages", r.status, r.body)
		}
		if r := h.poll(c.sid); r.status != http.StatusOK || r.body != "1" {
			t.Errorf("second poll = %d %q, want 200 \"1\"", r.status, r.body)
		}
		c.rec.ended(t)
	})
	t.Run("wakes a pending poll", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		c := transportCases[0].connect(h)
		pending := h.startPoll(c.sid)
		if !pending.running(settle) {
			t.Fatalf("poll answered at once: %+v", pending.reply)
		}
		h.server.Close()
		if r := pending.wait(t); r.status != http.StatusOK || r.body != "1" {
			t.Errorf("pending poll = %d %q, want 200 \"1\"", r.status, r.body)
		}
	})
	t.Run("flushes a WebSocket queue before closing", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		c := transportCases[1].connect(h)
		c.rec.session.Send(msg("last"))
		h.server.Close()
		if got := c.ws.text(); got != "4last" {
			t.Errorf("frame = %q, want 4last", got)
		}
		c.ws.SetReadDeadline(time.Now().Add(wait))
		if _, _, err := c.ws.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			t.Errorf("read after the queue = %v, want a normal closure", err)
		}
	})
	t.Run("a server without sessions closes quietly", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		h.server.Close()
		h.server.Close()
	})
}

func TestHandshakeAfterServerClose(t *testing.T) {
	t.Parallel()
	t.Run("polling", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		h.server.Close()
		h.poll("")
		rec := h.sole()
		if got := rec.reason(t); got != engineio.ReasonServerShutdown {
			t.Errorf("reason = %q, want %q", got, engineio.ReasonServerShutdown)
		}
		rec.ended(t)
		if err := rec.session.Send(msg("x")); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("Send = %v, want ErrClosed", err)
		}
		rec.closedOnce(t)
	})
	t.Run("websocket", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		h.server.Close()
		ws, _, err := h.connect(socketQuery, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if err := ws.readError(); err == nil {
			t.Error("connection stayed open")
		}
		rec := h.sole()
		if got := rec.reason(t); got != engineio.ReasonServerShutdown {
			t.Errorf("reason = %q, want %q", got, engineio.ReasonServerShutdown)
		}
		rec.ended(t)
		rec.closedOnce(t)
	})
}

func TestSessionRequest(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, rec *recorder, method string) {
		t.Helper()
		req := rec.session.Request()
		if req.Method != method {
			t.Errorf("Method = %q, want %q", req.Method, method)
		}
		if got := req.Header.Get("X-Test"); got != "handshake" {
			t.Errorf("X-Test = %q, want the handshake value", got)
		}
		if got := req.URL.Query().Get("token"); got != "abc" {
			t.Errorf("token = %q, want abc", got)
		}
		if got := req.URL.Path; got != "/engine.io/" {
			t.Errorf("Path = %q, want /engine.io/", got)
		}
		if req.Host == "" || req.RemoteAddr == "" {
			t.Errorf("Host = %q, RemoteAddr = %q, want both set", req.Host, req.RemoteAddr)
		}
		if req.Body != nil {
			if body, err := io.ReadAll(req.Body); err != nil || len(body) != 0 {
				t.Errorf("Body = %q, %v, want an empty body", body, err)
			}
		}
		if err := req.Context().Err(); err != nil {
			t.Errorf("request context is done: %v", err)
		}
	}
	t.Run("polling", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		header := http.Header{"X-Test": {"handshake"}}
		r := h.request(http.MethodGet, h.url(pollingQuery+"&token=abc"), "", header)
		rec := h.recorder(parseOpen(t, split(r.body)[0]).SID)
		later := http.Header{"X-Test": {"later request"}}
		h.request(http.MethodPost, h.pollingURL(rec.session.ID())+"&token=other", "3", later)
		check(t, rec, http.MethodGet)
	})
	t.Run("websocket", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		ws, _, err := h.connect(socketQuery+"&token=abc", http.Header{"X-Test": {"handshake"}})
		if err != nil {
			t.Fatal(err)
		}
		parseOpen(t, ws.text())
		rec := h.sole()
		ws.Close()
		rec.reason(t)
		rec.ended(t)
		check(t, rec, http.MethodGet)
	})
}

func TestSessionIdentity(t *testing.T) {
	t.Parallel()
	want := map[string]engineio.Transport{"polling": engineio.Polling, "websocket": engineio.WebSocket}
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			c := tr.connect(h)
			s := c.rec.session
			if s.ID() == "" || s.ID() != c.sid {
				t.Errorf("ID = %q, want the sid %q of the open packet", s.ID(), c.sid)
			}
			if got := s.Transport(); got != want[tr.name] {
				t.Errorf("Transport = %q, want %q", got, want[tr.name])
			}
			if h.accepts.Load() != 1 {
				t.Errorf("accept called %d times, want 1", h.accepts.Load())
			}
		})
	}
}
