package engineio_test

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hilmanzhy/socket.io-golang/engineio"
)

func (h *harness) expectRejected(sid string) {
	h.t.Helper()
	ws, resp, err := h.connect(socketQuery+"&sid="+sid, nil)
	if err != nil {
		if resp == nil || resp.StatusCode < http.StatusBadRequest {
			h.t.Fatalf("second WebSocket failed oddly: %v", err)
		}
		return
	}
	ws.WriteMessage(websocket.TextMessage, []byte("2probe"))
	ws.SetReadDeadline(time.Now().Add(wait))
	kind, data, err := ws.ReadMessage()
	if err == nil {
		h.t.Fatalf("second WebSocket was served: %d %q", kind, data)
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		h.t.Fatal("second WebSocket was left open")
	}
}

func TestUpgrade(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	if got := rec.session.Transport(); got != engineio.Polling {
		t.Fatalf("transport before the upgrade = %q, want polling", got)
	}
	ws := h.upgrade(sid)
	eventually(t, "the session to report the WebSocket transport", func() bool {
		return rec.session.Transport() == engineio.WebSocket
	})
	ws.write("4hello")
	if got := ws.text(); got != "4hello" {
		t.Errorf("echo over WebSocket = %q, want 4hello", got)
	}
	if err := rec.session.Send(msg("pushed")); err != nil {
		t.Fatal(err)
	}
	if got := ws.text(); got != "4pushed" {
		t.Errorf("server message over WebSocket = %q, want 4pushed", got)
	}
	ws.writeBinary([]byte{1, 2, 3})
	if got := ws.binary(); len(got) != 3 || got[2] != 3 {
		t.Errorf("binary echo = %v", got)
	}
}

func TestUpgradeEndsPolling(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	ws := h.upgrade(sid)
	eventually(t, "the upgrade to complete", func() bool {
		return rec.session.Transport() == engineio.WebSocket
	})
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("GET after the upgrade = %d, want 400", r.status)
	}
	if r := h.post(sid, "4x"); r.status != http.StatusBadRequest {
		t.Errorf("POST after the upgrade = %d, want 400", r.status)
	}
	if !rec.open() {
		t.Fatal("a rejected polling request ended the upgraded session")
	}
	ws.write("4still here")
	if got := ws.text(); got != "4still here" {
		t.Errorf("echo = %q", got)
	}
}

func TestUpgradeSecondWebSocket(t *testing.T) {
	t.Parallel()
	t.Run("after the upgrade", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{}, withEcho())
		sid, rec := h.open()
		ws := h.upgrade(sid)
		eventually(t, "the upgrade to complete", func() bool {
			return rec.session.Transport() == engineio.WebSocket
		})
		h.expectRejected(sid)
		ws.write("4alive")
		if got := ws.text(); got != "4alive" {
			t.Errorf("echo = %q", got)
		}
	})
	t.Run("during the probe", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{}, withEcho())
		sid, rec := h.open()
		first := h.connectUpgrade(sid)
		first.write("2probe")
		if got := first.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		h.expectRejected(sid)
		first.write("5")
		eventually(t, "the upgrade to complete", func() bool {
			return rec.session.Transport() == engineio.WebSocket
		})
		first.write("4alive")
		if got := first.text(); got != "4alive" {
			t.Errorf("echo = %q", got)
		}
	})
}

func TestUpgradeUnknownSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	if status := h.dialStatus(socketQuery+"&sid=unknown", nil); status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
	if n := h.accepts.Load(); n != 0 {
		t.Errorf("accept called %d times", n)
	}
}

func TestUpgradeKeepsMessages(t *testing.T) {
	t.Parallel()
	t.Run("sent during the probe", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		sid, rec := h.open()
		pending := h.async(http.MethodGet, h.pollingURL(sid), "")
		ws := h.connectUpgrade(sid)
		ws.write("2probe")
		if got := ws.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		if r := pending.wait(t); r.body != "6" {
			t.Fatalf("pending poll = %q, want 6", r.body)
		}
		rec.session.Send(msg("x"))
		rec.session.Send(msg("y"))
		ws.write("5")
		if got := ws.text(); got != "4x" {
			t.Errorf("first frame = %q, want 4x", got)
		}
		if got := ws.text(); got != "4y" {
			t.Errorf("second frame = %q, want 4y", got)
		}
	})
	t.Run("sent before the probe", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		sid, rec := h.open()
		rec.session.Send(msg("early"))
		ws := h.connectUpgrade(sid)
		ws.write("2probe")
		if got := ws.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		ws.write("5")
		if got := ws.text(); got != "4early" {
			t.Errorf("frame = %q, want 4early", got)
		}
	})
}

func TestUpgradeBadProbe(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		first func(*wsConn)
	}{
		{"text instead of the probe", func(ws *wsConn) { ws.write("hello") }},
		{"binary instead of the probe", func(ws *wsConn) { ws.writeBinary([]byte("2probe")) }},
		{"ping without the probe suffix", func(ws *wsConn) { ws.write("2") }},
		{"text frame starting with b", func(ws *wsConn) { ws.write("bAQID") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			sid, rec := h.open()
			ws := h.connectUpgrade(sid)
			tt.first(ws)
			if err := ws.readError(); err == nil {
				t.Fatal("upgrade connection stayed open")
			}
			if !rec.open() {
				t.Fatal("a failed upgrade ended the session")
			}
			h.post(sid, "4still polling")
			if got := rec.text(t); got != "still polling" {
				t.Errorf("received %q", got)
			}
			h.upgrade(sid)
		})
	}
}

func TestUpgradeBadConfirmation(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	pending := h.async(http.MethodGet, h.pollingURL(sid), "")
	ws := h.connectUpgrade(sid)
	ws.write("2probe")
	if got := ws.text(); got != "3probe" {
		t.Fatalf("probe answer = %q", got)
	}
	if r := pending.wait(t); r.body != "6" {
		t.Fatalf("pending poll = %q, want 6", r.body)
	}
	ws.write("hello")
	if err := ws.readError(); err == nil {
		t.Fatal("upgrade connection stayed open")
	}
	later := h.startPoll(sid)
	if !later.running(settle) {
		t.Fatalf("poll after the failed upgrade answered at once: %+v", later.reply)
	}
	rec.session.Send(msg("x"))
	if r := later.wait(t); r.body != "4x" {
		t.Errorf("poll = %q, want 4x", r.body)
	}
	h.post(sid, "4back")
	if got := rec.text(t); got != "back" {
		t.Errorf("received %q", got)
	}
}

func TestUpgradeBase64FrameInsteadOfConfirmation(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	pending := h.async(http.MethodGet, h.pollingURL(sid), "")
	ws := h.connectUpgrade(sid)
	ws.write("2probe")
	if got := ws.text(); got != "3probe" {
		t.Fatalf("probe answer = %q", got)
	}
	if r := pending.wait(t); r.body != "6" {
		t.Fatalf("pending poll = %q, want 6", r.body)
	}
	ws.write("bAQID")
	if err := ws.readError(); err == nil {
		t.Fatal("upgrade connection stayed open")
	}
	if !rec.open() {
		t.Fatal("a failed upgrade ended the session")
	}
	h.post(sid, "bAQID")
	if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
		t.Errorf("message = %+v, want binary 1,2,3 over polling", m)
	}
}

func TestUpgradeTimeout(t *testing.T) {
	t.Parallel()
	t.Run("no probe", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{UpgradeTimeout: 150 * time.Millisecond})
		sid, rec := h.open()
		ws := h.connectUpgrade(sid)
		if err := ws.readError(); err == nil {
			t.Fatal("silent upgrade connection stayed open")
		}
		rec.session.Send(msg("x"))
		if r := h.poll(sid); r.body != "4x" {
			t.Errorf("poll after the timeout = %q, want 4x", r.body)
		}
	})
	t.Run("no confirmation", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{UpgradeTimeout: 500 * time.Millisecond})
		sid, rec := h.open()
		pending := h.async(http.MethodGet, h.pollingURL(sid), "")
		ws := h.connectUpgrade(sid)
		ws.write("2probe")
		if got := ws.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		if r := pending.wait(t); r.body != "6" {
			t.Fatalf("pending poll = %q, want 6", r.body)
		}
		if err := ws.readError(); err == nil {
			t.Fatal("unconfirmed upgrade connection stayed open")
		}
		later := h.startPoll(sid)
		if !later.running(settle) {
			t.Fatalf("poll after the timeout answered at once: %+v", later.reply)
		}
		rec.session.Send(msg("x"))
		if r := later.wait(t); r.body != "4x" {
			t.Errorf("poll = %q, want 4x", r.body)
		}
	})
}

func TestUpgradedClientClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	ws := h.upgrade(sid)
	ws.write("1")
	if got := rec.reason(t); got != engineio.ReasonTransportClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportClose)
	}
	rec.ended(t)
	if err := ws.readError(); err == nil {
		t.Error("connection stayed open")
	}
}

func TestUpgradedGracefulClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	ws := h.upgrade(sid)
	rec.session.Send(msg("bye"))
	rec.session.Close(engineio.ReasonForcedClose)
	if got := ws.text(); got != "4bye" {
		t.Errorf("frame = %q, want 4bye", got)
	}
	ws.SetReadDeadline(time.Now().Add(wait))
	if _, _, err := ws.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Errorf("read after the queue = %v, want a normal closure", err)
	}
	if got := rec.reason(t); got != engineio.ReasonForcedClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
	}
}

func TestUpgradeWithoutWebSocketHeaders(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	r := h.request(http.MethodGet, h.url(socketQuery+"&sid="+sid), "", nil)
	if r.status < http.StatusBadRequest || r.status >= http.StatusInternalServerError {
		t.Fatalf("plain GET for the WebSocket transport = %d %q, want a 4xx", r.status, r.body)
	}
	if !rec.open() {
		t.Fatal("a refused upgrade ended the session")
	}
	ws := h.upgrade(sid)
	ws.write("4after")
	if got := ws.text(); got != "4after" {
		t.Errorf("echo after the retried upgrade = %q", got)
	}
}

// TestUpgradeProbeClosedWhenSessionEnds checks that the WebSocket of an upgrade
// probe is closed at once when the session ends, not when UpgradeTimeout runs
// out.
func TestUpgradeProbeClosedWhenSessionEnds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		probe func(*wsConn)
	}{
		{"before the probe packet", func(*wsConn) {}},
		{"after the probe answer", func(ws *wsConn) {
			ws.write("2probe")
			if got := ws.text(); got != "3probe" {
				ws.t.Fatalf("probe answer = %q, want 3probe", got)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{UpgradeTimeout: time.Minute})
			sid, rec := h.open()
			ws := h.connectUpgrade(sid)
			c.probe(ws)
			h.post(sid, "1")
			rec.ended(t)
			ws.readError()
		})
	}
}
