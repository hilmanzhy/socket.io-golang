package engineio_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nuriontech/socket.io-golang/engineio"
)

func TestHeartbeatPolling(t *testing.T) {
	h := newHarness(t, engineio.Options{PingInterval: 100 * time.Millisecond, PingTimeout: 400 * time.Millisecond})
	sid, rec := h.open()
	start := time.Now()
	for i := range 3 {
		r := h.poll(sid)
		if r.status != http.StatusOK || r.body != "2" {
			t.Fatalf("poll %d = %d %q, want 200 \"2\"", i, r.status, r.body)
		}
		if i == 0 {
			if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
				t.Errorf("first ping after %v, want about the 100ms ping interval", elapsed)
			}
		}
		if r := h.post(sid, "3"); r.status != http.StatusOK || r.body != "ok" {
			t.Fatalf("pong %d = %d %q, want 200 \"ok\"", i, r.status, r.body)
		}
	}
	if !rec.open() {
		t.Error("session ended although every ping was answered")
	}
}

func TestHeartbeatPollingTimeout(t *testing.T) {
	h := newHarness(t, engineio.Options{PingInterval: 50 * time.Millisecond, PingTimeout: 100 * time.Millisecond})
	sid, rec := h.open()
	if r := h.poll(sid); r.body != "2" {
		t.Fatalf("poll = %q, want the ping", r.body)
	}
	if got := rec.reason(t); got != engineio.ReasonPingTimeout {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonPingTimeout)
	}
	rec.ended(t)
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after the timeout = %d, want 400", r.status)
	}
	if r := h.post(sid, "3"); r.status != http.StatusBadRequest {
		t.Errorf("late pong = %d, want 400", r.status)
	}
	rec.closedOnce(t)
}

func TestHeartbeatPollingIdleClient(t *testing.T) {
	h := newHarness(t, engineio.Options{PingInterval: 50 * time.Millisecond, PingTimeout: 100 * time.Millisecond})
	sid, rec := h.open()
	if got := rec.reason(t); got != engineio.ReasonPingTimeout {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonPingTimeout)
	}
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after the timeout = %d, want 400", r.status)
	}
}

func TestHeartbeatWebSocket(t *testing.T) {
	h := newHarness(t, engineio.Options{PingInterval: 100 * time.Millisecond, PingTimeout: 400 * time.Millisecond})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	for i := range 3 {
		if got := ws.text(); got != "2" {
			t.Fatalf("frame %d = %q, want the ping", i, got)
		}
		ws.write("3")
	}
	if !rec.open() {
		t.Error("session ended although every ping was answered")
	}
}

func TestHeartbeatWebSocketTimeout(t *testing.T) {
	h := newHarness(t, engineio.Options{PingInterval: 50 * time.Millisecond, PingTimeout: 100 * time.Millisecond})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	if got := ws.text(); got != "2" {
		t.Fatalf("frame = %q, want the ping", got)
	}
	if err := ws.readError(); err == nil {
		t.Fatal("connection stayed open")
	}
	if got := rec.reason(t); got != engineio.ReasonPingTimeout {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonPingTimeout)
	}
	rec.ended(t)
}

func TestHeartbeatAfterUpgrade(t *testing.T) {
	h := newHarness(t, engineio.Options{PingInterval: 300 * time.Millisecond, PingTimeout: 400 * time.Millisecond})
	sid, rec := h.open()
	ws := h.upgrade(sid)
	for i := range 2 {
		if got := ws.text(); got != "2" {
			t.Fatalf("frame %d = %q, want the ping", i, got)
		}
		ws.write("3")
	}
	if !rec.open() {
		t.Error("session ended although every ping was answered")
	}
}

func TestUnsolicitedPongIgnored(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	if r := h.post(sid, "3"); r.status != http.StatusOK || r.body != "ok" {
		t.Fatalf("POST 3 = %d %q, want 200 \"ok\"", r.status, r.body)
	}
	if !rec.open() {
		t.Fatal("an unsolicited pong ended the session")
	}
	h.post(sid, "4after")
	if r := h.poll(sid); r.body != "4after" {
		t.Errorf("poll = %q, want 4after", r.body)
	}
}
