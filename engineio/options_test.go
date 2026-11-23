package engineio_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
)

// TestNegativeOptionsUseDefaults checks that a negative duration or size is
// treated like the zero value and gives the default, instead of a ping loop, a
// session that cannot upgrade or send, or a limit that is off.
func TestNegativeOptionsUseDefaults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		opts  engineio.Options
		check func(*testing.T, *harness)
	}{
		{"PingInterval", engineio.Options{PingInterval: -time.Second}, func(t *testing.T, h *harness) {
			r := h.poll("")
			packets := split(r.body)
			if open := parseOpen(t, packets[0]); open.PingInterval != 25000 {
				t.Errorf("pingInterval = %d, want the default 25000", open.PingInterval)
			}
			if len(packets) != 1 {
				t.Errorf("handshake body %q holds a ping already", r.body)
			}
			sid := parseOpen(t, packets[0]).SID
			if pending := h.startPoll(sid); !pending.running(settle) {
				t.Errorf("a ping arrived within %v of the handshake: %+v", settle, pending.reply)
			}
		}},
		{"PingTimeout", engineio.Options{PingTimeout: -time.Second}, func(t *testing.T, h *harness) {
			sid, open := h.handshake()
			if open.PingTimeout != 20000 {
				t.Errorf("pingTimeout = %d, want the default 20000", open.PingTimeout)
			}
			rec := h.recorder(sid)
			rec.session.Close(engineio.ReasonForcedClose)
			rec.reason(t)
			if !quiet(rec.session.Done(), settle) {
				t.Error("the session ended at once, before the client could collect its packets")
			}
		}},
		{"UpgradeTimeout", engineio.Options{UpgradeTimeout: -time.Second}, func(t *testing.T, h *harness) {
			sid, _ := h.handshake()
			h.upgrade(sid)
			session := h.recorder(sid).session
			eventually(t, "the WebSocket transport", func() bool { return session.Transport() == engineio.WebSocket })
		}},
		{"MaxPayload", engineio.Options{MaxPayload: -1}, func(t *testing.T, h *harness) {
			sid, open := h.handshake()
			if open.MaxPayload != 1000000 {
				t.Errorf("maxPayload = %d, want the default 1000000", open.MaxPayload)
			}
			if r := h.post(sid, "4a"); r.status != http.StatusOK {
				t.Errorf("POST = %d %q, want 200", r.status, r.body)
			}
			ws, socket := h.openWS(nil)
			rec := h.recorder(socket.SID)
			ws.write("4" + strings.Repeat("a", 1000000))
			ws.readError()
			rec.reason(t)
		}},
		{"MaxBuffer", engineio.Options{MaxBuffer: -1}, func(t *testing.T, h *harness) {
			_, rec := h.open()
			for i := range 2 {
				if err := rec.session.Send(msg("a")); err != nil {
					t.Fatalf("Send %d = %v, want nil", i, err)
				}
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.check(t, newHarness(t, c.opts))
		})
	}
}
