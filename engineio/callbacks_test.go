package engineio_test

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/engineio"
)

const (
	allowedOrigin = "https://a.example"
	boomOrigin    = "https://boom.example"
	panicMessage  = "engineio: callback panic"
)

var errBoom = errors.New("boom")

var panicValues = []struct {
	name  string
	value any
}{
	{"string", "boom"},
	{"error", errBoom},
}

type captureHandler struct {
	records chan slog.Record
}

func newCapture() *captureHandler {
	return &captureHandler{records: make(chan slog.Record, 16)}
}

func (c *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (c *captureHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case c.records <- r:
	default:
	}
	return nil
}

func (c *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *captureHandler) WithGroup(string) slog.Handler { return c }

func (c *captureHandler) expect(t testing.TB, callback string, panicked any) {
	t.Helper()
	r := receive(t, c.records, "the panic log record")
	if r.Level != slog.LevelError {
		t.Errorf("level = %v, want error", r.Level)
	}
	if r.Message != panicMessage {
		t.Errorf("message = %q, want %q", r.Message, panicMessage)
	}
	attrs := map[string]any{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	if attrs["callback"] != callback {
		t.Errorf("callback = %v, want %q", attrs["callback"], callback)
	}
	if attrs["panic"] != panicked {
		t.Errorf("panic = %v, want %v", attrs["panic"], panicked)
	}
	if stack, _ := attrs["stack"].(string); !strings.Contains(stack, "callbacks_test.go") {
		t.Errorf("stack does not reach the panicking callback: %q", stack)
	}
	if len(attrs) != 3 {
		t.Errorf("attributes = %v, want callback, panic and stack only", attrs)
	}
}

type callbackRequest struct {
	name   string
	do     func(*harness, http.Header) (reply, error)
	served int
}

func (cr callbackRequest) mustDo(t *testing.T, h *harness, header http.Header) reply {
	t.Helper()
	r, err := cr.do(h, header)
	if err != nil {
		t.Fatalf("%s: %v", cr.name, err)
	}
	return r
}

func pollingRequest(method string) func(*harness, http.Header) (reply, error) {
	return func(h *harness, header http.Header) (reply, error) {
		return h.send(context.Background(), method, h.pollingURL(""), "", header)
	}
}

var (
	pollingHandshake = callbackRequest{"polling", pollingRequest(http.MethodGet), http.StatusOK}
	socketHandshake  = callbackRequest{"websocket", dialWebSocket, http.StatusSwitchingProtocols}
	preflight        = callbackRequest{"preflight", pollingRequest(http.MethodOptions), http.StatusNoContent}
)

func expectRefusal(t *testing.T, got, want reply) {
	t.Helper()
	if got.status != http.StatusForbidden || got.body != refusedBody {
		t.Errorf("answer = %d %q, want 403 %q", got.status, got.body, refusedBody)
	}
	if got.status != want.status || got.body != want.body {
		t.Errorf("answer = %d %q, differs from the plain refusal %d %q", got.status, got.body, want.status, want.body)
	}
	if gotType, wantType := got.header.Get("Content-Type"), want.header.Get("Content-Type"); gotType != wantType {
		t.Errorf("Content-Type = %q, want %q", gotType, wantType)
	}
	if v := got.header.Get("Access-Control-Allow-Origin"); v != "" {
		t.Errorf("refusal carries Access-Control-Allow-Origin %q", v)
	}
}

func TestCheckOriginPanic(t *testing.T) {
	t.Parallel()
	for _, p := range panicValues {
		for _, req := range []callbackRequest{pollingHandshake, socketHandshake, preflight} {
			t.Run(p.name+" "+req.name, func(t *testing.T) {
				t.Parallel()
				logs := newCapture()
				h := newHarness(t, engineio.Options{Logger: slog.New(logs), CheckOrigin: func(r *http.Request) bool {
					if r.Header.Get("Origin") == boomOrigin {
						panic(p.value)
					}
					return true
				}})
				plain := newHarness(t, engineio.Options{CheckOrigin: func(*http.Request) bool { return false }})
				got := req.mustDo(t, h, originHeader(h, boomOrigin))
				want := req.mustDo(t, plain, originHeader(plain, boomOrigin))
				expectRefusal(t, got, want)
				logs.expect(t, "CheckOrigin", p.value)
				if n := h.accepts.Load(); n != 0 {
					t.Errorf("accept called %d times for a refused origin", n)
				}
				next := req.mustDo(t, h, originHeader(h, allowedOrigin))
				if next.status != req.served {
					t.Errorf("request after the panic = %d, want %d", next.status, req.served)
				}
				if n := len(logs.records); n != 0 {
					t.Errorf("%d log records for a request that did not panic", n)
				}
			})
		}
	}
}

func TestAllowRequestPanic(t *testing.T) {
	t.Parallel()
	boom := http.Header{"X-Boom": {"1"}}
	for _, p := range panicValues {
		for _, req := range []callbackRequest{pollingHandshake, socketHandshake} {
			t.Run(p.name+" "+req.name, func(t *testing.T) {
				t.Parallel()
				logs := newCapture()
				h := newHarness(t, engineio.Options{Logger: slog.New(logs), MaxSessions: 1, AllowRequest: func(r *http.Request) error {
					if r.Header.Get("X-Boom") != "" {
						panic(p.value)
					}
					return nil
				}})
				plain := newHarness(t, engineio.Options{AllowRequest: func(*http.Request) error { return errors.New("denied") }})
				want := req.mustDo(t, plain, boom)
				for range 3 {
					expectRefusal(t, req.mustDo(t, h, boom), want)
					logs.expect(t, "AllowRequest", p.value)
				}
				if n := h.accepts.Load(); n != 0 {
					t.Errorf("accept called %d times for a refused handshake", n)
				}
				if next := req.mustDo(t, h, nil); next.status != req.served {
					t.Errorf("request after the panics = %d, want %d: a panic must not keep a session slot", next.status, req.served)
				}
				if n := len(logs.records); n != 0 {
					t.Errorf("%d log records for a request that did not panic", n)
				}
			})
		}
	}
}

func TestPanicsGoToDefaultLoggerWhenNoneSet(t *testing.T) {
	logger, writer, flags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(logger)
		log.SetOutput(writer)
		log.SetFlags(flags)
	})
	logs := newCapture()
	slog.SetDefault(slog.New(logs))
	h := newHarness(t, engineio.Options{
		CheckOrigin:  func(*http.Request) bool { panic(errBoom) },
		AllowRequest: func(*http.Request) error { panic(errBoom) },
	})
	if r := pollingHandshake.mustDo(t, h, originHeader(h, boomOrigin)); r.status != http.StatusForbidden {
		t.Errorf("status = %d, want 403", r.status)
	}
	logs.expect(t, "CheckOrigin", errBoom)
	if r := pollingHandshake.mustDo(t, h, nil); r.status != http.StatusForbidden {
		t.Errorf("status = %d, want 403", r.status)
	}
	logs.expect(t, "AllowRequest", errBoom)
}
