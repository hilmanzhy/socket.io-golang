package engineio_test

import (
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
)

func TestHandshakeOpenPacket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		opts engineio.Options
		want openPacket
	}{
		{
			name: "defaults",
			want: openPacket{Upgrades: []string{"websocket"}, PingInterval: 25000, PingTimeout: 20000, MaxPayload: 1000000},
		},
		{
			name: "custom",
			opts: engineio.Options{PingInterval: 300 * time.Millisecond, PingTimeout: 150 * time.Millisecond, MaxPayload: 4096},
			want: openPacket{Upgrades: []string{"websocket"}, PingInterval: 300, PingTimeout: 150, MaxPayload: 4096},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, tt.opts)
			r := h.poll("")
			if r.status != http.StatusOK {
				t.Fatalf("status = %d, want 200", r.status)
			}
			packets := split(r.body)
			if len(packets) != 1 {
				t.Fatalf("handshake body has %d packets, want only the open packet: %q", len(packets), r.body)
			}
			open := parseOpen(t, packets[0])
			if !slices.Equal(open.Upgrades, tt.want.Upgrades) {
				t.Errorf("upgrades = %v, want %v", open.Upgrades, tt.want.Upgrades)
			}
			if open.PingInterval != tt.want.PingInterval || open.PingTimeout != tt.want.PingTimeout || open.MaxPayload != tt.want.MaxPayload {
				t.Errorf("open packet = %+v, want intervals %d/%d and payload %d",
					open, tt.want.PingInterval, tt.want.PingTimeout, tt.want.MaxPayload)
			}
		})
	}
}

func TestHandshakeWebSocketOpenPacket(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{PingInterval: 40 * time.Second, PingTimeout: 30 * time.Second, MaxPayload: 2048})
	_, open := h.openWS(nil)
	if !strings.Contains(open.raw, `"upgrades":[]`) {
		t.Errorf("open packet %q must carry an empty upgrades array", open.raw)
	}
	if open.PingInterval != 40000 || open.PingTimeout != 30000 || open.MaxPayload != 2048 {
		t.Errorf("open packet = %+v", open)
	}
	if h.recorder(open.SID).session.Transport() != engineio.WebSocket {
		t.Error("session does not report the WebSocket transport")
	}
}

func TestHandshakeSessionIDs(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	urlSafe := regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	seen := map[string]bool{}
	for range 60 {
		sid, _ := h.handshake()
		if seen[sid] {
			t.Fatalf("session id %q issued twice", sid)
		}
		seen[sid] = true
		if !urlSafe.MatchString(sid) {
			t.Errorf("session id %q is not safe to put in a query string", sid)
		}
		if got := h.recorder(sid).session.ID(); got != sid {
			t.Errorf("Session.ID() = %q, open packet says %q", got, sid)
		}
	}
}

func TestHandshakeRejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		query  string
		code   int
	}{
		{"missing version", http.MethodGet, "transport=polling", 5},
		{"version 3", http.MethodGet, "EIO=3&transport=polling", 5},
		{"version text", http.MethodGet, "EIO=abc&transport=polling", 5},
		{"empty version", http.MethodGet, "EIO=&transport=polling", 5},
		{"future version", http.MethodGet, "EIO=5&transport=polling", 5},
		{"unknown transport", http.MethodGet, "EIO=4&transport=xhr", 0},
		{"empty transport", http.MethodGet, "EIO=4&transport=", 0},
		{"missing transport", http.MethodGet, "EIO=4", 0},
		{"post without session", http.MethodPost, "EIO=4&transport=polling", 2},
		{"post websocket without session", http.MethodPost, "EIO=4&transport=websocket", 2},
		{"unknown session get", http.MethodGet, "EIO=4&transport=polling&sid=nope", 1},
		{"unknown session post", http.MethodPost, "EIO=4&transport=polling&sid=nope", 1},
		{"unknown session websocket", http.MethodGet, "EIO=4&transport=websocket&sid=nope", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			r := h.request(tt.method, h.url(tt.query), "", nil)
			if r.status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %q)", r.status, r.body)
			}
			if f := parseFailure(t, r); f.Code != tt.code {
				t.Errorf("code = %d, want %d", f.Code, tt.code)
			}
			if n := h.accepts.Load(); n != 0 {
				t.Errorf("accept called %d times for a rejected request", n)
			}
		})
	}
}

func TestHandshakeWebSocketNeedsUpgradeHeaders(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	r := h.request(http.MethodGet, h.url(socketQuery), "", nil)
	if r.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.status)
	}
	if n := h.accepts.Load(); n != 0 {
		t.Errorf("accept called %d times without a WebSocket upgrade", n)
	}
}

func TestTransportsOption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		transports []engineio.Transport
		polling    bool
		websocket  bool
	}{
		{"default", nil, true, true},
		{"both", []engineio.Transport{engineio.Polling, engineio.WebSocket}, true, true},
		{"polling only", []engineio.Transport{engineio.Polling}, true, false},
		{"websocket only", []engineio.Transport{engineio.WebSocket}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{Transports: tt.transports})
			r := h.poll("")
			if tt.polling {
				if r.status != http.StatusOK {
					t.Fatalf("polling handshake status = %d, want 200", r.status)
				}
				open := parseOpen(t, split(r.body)[0])
				if got := slices.Equal(open.Upgrades, []string{"websocket"}); got != tt.websocket {
					t.Errorf("upgrades = %v, with websocket enabled = %v", open.Upgrades, tt.websocket)
				}
				if status := h.dialStatus(socketQuery+"&sid="+open.SID, nil); (status == http.StatusSwitchingProtocols) != tt.websocket {
					t.Errorf("upgrade status = %d, websocket enabled = %v", status, tt.websocket)
				}
			} else {
				if r.status != http.StatusBadRequest {
					t.Fatalf("disabled polling status = %d, want 400", r.status)
				}
				if f := parseFailure(t, r); f.Code != 0 {
					t.Errorf("code = %d, want 0 (transport unknown)", f.Code)
				}
			}
			status := h.dialStatus(socketQuery, nil)
			if tt.websocket && status != http.StatusSwitchingProtocols {
				t.Errorf("websocket dial status = %d, want 101", status)
			}
			if !tt.websocket && status != http.StatusBadRequest {
				t.Errorf("disabled websocket dial status = %d, want 400", status)
			}
		})
	}
}

func TestAllowRequestRefusal(t *testing.T) {
	t.Parallel()
	refuse := func(*http.Request) error { return errors.New("rate limited") }
	t.Run("polling", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{AllowRequest: refuse})
		r := h.poll("")
		if r.status != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", r.status)
		}
		if f := parseFailure(t, r); f.Code != 4 {
			t.Errorf("code = %d, want 4 (forbidden)", f.Code)
		}
		if n := h.accepts.Load(); n != 0 {
			t.Errorf("accept called %d times for a refused handshake", n)
		}
	})
	t.Run("websocket", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{AllowRequest: refuse})
		if status := h.dialStatus(socketQuery, nil); status != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", status)
		}
		if n := h.accepts.Load(); n != 0 {
			t.Errorf("accept called %d times for a refused handshake", n)
		}
	})
}

func TestAllowRequestSeesHandshakeOnly(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	seen := make(chan *http.Request, 8)
	h := newHarness(t, engineio.Options{AllowRequest: func(r *http.Request) error {
		calls.Add(1)
		seen <- r.Clone(r.Context())
		return nil
	}}, withEcho())
	header := http.Header{}
	header.Set("X-Client", "probe")
	r := h.request(http.MethodGet, h.url(pollingQuery+"&token=abc"), "", header)
	if r.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.status)
	}
	sid := parseOpen(t, split(r.body)[0]).SID
	h.post(sid, "4a")
	h.poll(sid)
	req := receive(t, seen, "AllowRequest call")
	if got := req.URL.Query().Get("token"); got != "abc" {
		t.Errorf("AllowRequest saw token %q, want abc", got)
	}
	if got := req.Header.Get("X-Client"); got != "probe" {
		t.Errorf("AllowRequest saw X-Client %q, want probe", got)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("AllowRequest called %d times for one handshake and two follow-up requests, want 1", n)
	}
}
