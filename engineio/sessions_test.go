package engineio_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
)

const refusedBody = `{"code":4,"message":"Forbidden"}`

func tryPolling(h *harness) (reply, error) {
	return h.send(context.Background(), http.MethodGet, h.pollingURL(""), "", nil)
}

func tryWebSocket(h *harness) (reply, error) {
	return dialWebSocket(h, nil)
}

// dialWebSocket dials the WebSocket transport and keeps the connection open
// until the test ends. It reports the status of the handshake answer, with the
// body of a refusal.
func dialWebSocket(h *harness, header http.Header) (reply, error) {
	_, resp, err := h.connect(socketQuery, header)
	if err == nil {
		return reply{status: resp.StatusCode}, nil
	}
	if resp == nil {
		return reply{}, err
	}
	body, _ := io.ReadAll(resp.Body)
	return reply{status: resp.StatusCode, body: string(body), header: resp.Header}, nil
}

var limitCases = []struct {
	transportCase
	try func(*harness) (reply, error)
}{
	{transportCases[0], tryPolling},
	{transportCases[1], tryWebSocket},
}

func (h *harness) expectRefused(try func(*harness) (reply, error)) {
	h.t.Helper()
	r, err := try(h)
	if err != nil {
		h.t.Fatalf("handshake: %v", err)
	}
	if r.status != http.StatusServiceUnavailable {
		h.t.Fatalf("status = %d, want 503 (body %q)", r.status, r.body)
	}
	if f := parseFailure(h.t, r); f.Code != 4 {
		h.t.Errorf("code = %d, want 4 (forbidden)", f.Code)
	}
	if r.body != refusedBody {
		h.t.Errorf("body = %q, want %q", r.body, refusedBody)
	}
}

func TestMaxSessionsRefusesHandshake(t *testing.T) {
	t.Parallel()
	const limit = 3
	for _, tc := range limitCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxSessions: limit})
			for range limit {
				tc.connect(h)
			}
			for range 2 * limit {
				h.expectRefused(tc.try)
			}
			if n := h.accepts.Load(); n != limit {
				t.Errorf("accept called %d times, want %d", n, limit)
			}
		})
	}
	t.Run("the transports share the limit", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{MaxSessions: 2})
		for _, tc := range limitCases {
			tc.connect(h)
		}
		for _, tc := range limitCases {
			h.expectRefused(tc.try)
		}
	})
}

func TestMaxSessionsFreedWhenSessionEnds(t *testing.T) {
	t.Parallel()
	enders := []struct {
		name string
		end  func(*client)
	}{
		{"client close", (*client).hangUp},
		{"Session.Close", func(c *client) {
			c.rec.session.Close(engineio.ReasonForcedClose)
			if c.ws == nil {
				c.h.poll(c.sid)
			}
		}},
	}
	for _, tc := range limitCases {
		for _, e := range enders {
			t.Run(tc.name+" "+e.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, engineio.Options{MaxSessions: 1})
				first := tc.connect(h)
				h.expectRefused(tc.try)
				e.end(first)
				first.rec.ended(t)
				tc.connect(h)
				h.expectRefused(tc.try)
				if n := h.accepts.Load(); n != 2 {
					t.Errorf("accept called %d times, want 2", n)
				}
			})
		}
	}
}

// TestMaxSessionsConcurrentHandshakes checks that handshakes racing for the
// last slots are granted exactly MaxSessions times. The 101 answer of a
// WebSocket handshake leaves the server before accept runs, so the test waits
// for the accepts of the granted handshakes before it counts them.
func TestMaxSessionsConcurrentHandshakes(t *testing.T) {
	t.Parallel()
	const limit, attempts = 5, 40
	picks := []struct {
		name string
		pick func(int) func(*harness) (reply, error)
	}{
		{"polling", func(int) func(*harness) (reply, error) { return tryPolling }},
		{"websocket", func(int) func(*harness) (reply, error) { return tryWebSocket }},
		{"mixed", func(i int) func(*harness) (reply, error) { return limitCases[i%len(limitCases)].try }},
	}
	for _, p := range picks {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxSessions: limit}, withAccept(func(*engineio.Session) { runtime.Gosched() }))
			statuses := make(chan int, attempts)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range attempts {
				try := p.pick(i)
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					r, err := try(h)
					if err != nil {
						t.Errorf("handshake %d: %v", i, err)
						return
					}
					statuses <- r.status
				}()
			}
			close(start)
			wg.Wait()
			close(statuses)
			granted, refused := 0, 0
			for status := range statuses {
				switch status {
				case http.StatusOK, http.StatusSwitchingProtocols:
					granted++
				case http.StatusServiceUnavailable:
					refused++
				default:
					t.Errorf("unexpected status %d", status)
				}
			}
			if granted != limit || refused != attempts-limit {
				t.Fatalf("%d handshakes granted and %d refused, want %d and %d", granted, refused, limit, attempts-limit)
			}
			eventually(t, "accept of every granted handshake", func() bool { return h.accepts.Load() >= limit })
			if n := h.accepts.Load(); n != limit {
				t.Errorf("accept called %d times, want %d", n, limit)
			}
		})
	}
}

func TestMaxSessionsFailedUpgradeReleasesSlot(t *testing.T) {
	t.Parallel()
	for _, tc := range limitCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxSessions: 1})
			for range 3 {
				r := h.request(http.MethodGet, h.url(socketQuery), "", nil)
				if r.status != http.StatusBadRequest {
					t.Fatalf("GET without upgrade headers = %d, want 400 (a leaked slot answers 503)", r.status)
				}
			}
			tc.connect(h)
			h.expectRefused(tc.try)
		})
	}
}

func TestMaxSessionsRefusedRequestKeepsSlot(t *testing.T) {
	t.Parallel()
	denied := http.Header{"X-Deny": {"yes"}}
	h := newHarness(t, engineio.Options{MaxSessions: 1, AllowRequest: func(r *http.Request) error {
		if r.Header.Get("X-Deny") != "" {
			return errors.New("denied")
		}
		return nil
	}})
	for range 3 {
		if r := h.request(http.MethodGet, h.pollingURL(""), "", denied); r.status != http.StatusForbidden {
			t.Fatalf("polling = %d, want 403", r.status)
		}
		if status := h.dialStatus(socketQuery, denied); status != http.StatusForbidden {
			t.Fatalf("websocket = %d, want 403", status)
		}
	}
	h.open()
}

func TestMaxSessionsUnlimitedByDefault(t *testing.T) {
	t.Parallel()
	const sessions = 30
	for _, limit := range []int{0, -1} {
		for _, tc := range limitCases {
			t.Run(fmt.Sprintf("%s limit %d", tc.name, limit), func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, engineio.Options{MaxSessions: limit})
				for range sessions {
					tc.connect(h)
				}
				if n := h.accepts.Load(); n != sessions {
					t.Errorf("accept called %d times, want %d", n, sessions)
				}
			})
		}
	}
}

// servePanicking serves one polling handshake straight through the server, with
// no network in between, and returns what the handler panicked with.
func servePanicking(h *harness) (panicked any) {
	defer func() { panicked = recover() }()
	h.server.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, h.pollingURL(""), nil))
	return nil
}

// TestMaxSessionsAcceptPanicReleasesSlot checks that an accept callback that
// panics neither keeps its session slot nor has its panic swallowed.
func TestMaxSessionsAcceptPanicReleasesSlot(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		fault func(*engineio.Session)
	}{
		{"panic", func(*engineio.Session) { panic(errBoom) }},
		{"panic after the session ended", func(s *engineio.Session) {
			s.Close(engineio.ReasonForcedClose)
			<-s.Done()
			panic(errBoom)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var once sync.Once
			h := newHarness(t, engineio.Options{MaxSessions: 1, PingTimeout: time.Millisecond}, withAccept(func(s *engineio.Session) {
				once.Do(func() { c.fault(s) })
			}))
			if got := servePanicking(h); got != errBoom {
				t.Fatalf("handshake panicked with %v, want the panic of accept", got)
			}
			if r := h.poll(""); r.status != http.StatusOK {
				t.Fatalf("handshake after the panic = %d, want 200: a panic must not keep a session slot", r.status)
			}
			h.expectRefused(tryPolling)
		})
	}
}

// TestWebSocketHandshakeAcceptPanicClosesConnection checks that the WebSocket of
// a handshake is closed at once when accept panics, and that its slot is
// returned.
func TestWebSocketHandshakeAcceptPanicClosesConnection(t *testing.T) {
	t.Parallel()
	var once sync.Once
	h := newHarness(t, engineio.Options{MaxSessions: 1}, withAccept(func(*engineio.Session) {
		once.Do(func() { panic(http.ErrAbortHandler) })
	}))
	ws, _, err := h.connect(socketQuery, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ws.readError()
	if r := h.poll(""); r.status != http.StatusOK {
		t.Fatalf("handshake after the panic = %d, want 200: a panic must not keep a session slot", r.status)
	}
}

// TestSessionEndedDuringAcceptIsForgotten checks that a session that ends
// before accept returns is not registered afterwards and keeps no slot.
func TestSessionEndedDuringAcceptIsForgotten(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{MaxSessions: 1, PingTimeout: time.Millisecond}, withAccept(func(s *engineio.Session) {
		s.Close(engineio.ReasonForcedClose)
		<-s.Done()
	}))
	if r := h.poll(""); r.status != http.StatusOK || r.body != "1" {
		t.Fatalf("handshake = %d %q, want 200 \"1\"", r.status, r.body)
	}
	sid := h.sole().session.ID()
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll of the ended session = %d %q, want 400: it is still registered", r.status, r.body)
	}
	if r := h.poll(""); r.status != http.StatusOK {
		t.Errorf("second handshake = %d, want 200: the ended session keeps its slot", r.status)
	}
}
