package socketio_test

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
	"github.com/hilmanzhy/socket.io-golang/engineio"
)

func panicEntries(t *testing.T, logs *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, entry := range logEntries(t, logs) {
		if entry["msg"] == "engineio: callback panic" {
			out = append(out, entry)
		}
	}
	return out
}

func TestEngineCallbackPanicGoesToTheConfiguredLogger(t *testing.T) {
	t.Parallel()
	callbacks := []struct {
		name   string
		set    func(*engineio.Options)
		origin string
	}{
		{"CheckOrigin", func(o *engineio.Options) { o.CheckOrigin = func(*http.Request) bool { panic("boom") } }, "https://other.example"},
		{"AllowRequest", func(o *engineio.Options) { o.AllowRequest = func(*http.Request) error { panic("boom") } }, ""},
	}
	setups := []struct {
		name         string
		server       bool
		engine       bool
		wantOnServer bool
	}{
		{"only the server logger", true, false, true},
		{"only the engine logger", false, true, false},
		{"both loggers", true, true, false},
	}
	for _, cb := range callbacks {
		for _, setup := range setups {
			t.Run(cb.name+" with "+setup.name, func(t *testing.T) {
				t.Parallel()
				var serverLogs, engineLogs syncBuffer
				var opts socketio.Options
				if setup.server {
					opts.Logger = slog.New(slog.NewJSONHandler(&serverLogs, nil))
				}
				if setup.engine {
					opts.Engine.Logger = slog.New(slog.NewJSONHandler(&engineLogs, nil))
				}
				cb.set(&opts.Engine)
				_, ts := newServer(t, opts)

				req, err := http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=polling", nil)
				if err != nil {
					t.Fatal(err)
				}
				if cb.origin != "" {
					req.Header.Set("Origin", cb.origin)
				}
				res, err := ts.Client().Do(req)
				if err != nil {
					t.Fatalf("request: %v", err)
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != http.StatusForbidden {
					t.Fatalf("status = %d, want %d", res.StatusCode, http.StatusForbidden)
				}

				want, other := &engineLogs, &serverLogs
				if setup.wantOnServer {
					want, other = &serverLogs, &engineLogs
				}
				entries := panicEntries(t, want)
				if len(entries) != 1 {
					t.Fatalf("%d panic entries in the expected logger, want 1; log: %q", len(entries), want.String())
				}
				if entries[0]["callback"] != cb.name || entries[0]["level"] != slog.LevelError.String() || entries[0]["panic"] != "boom" {
					t.Errorf("entry = %v, want callback %q, level ERROR, panic boom", entries[0], cb.name)
				}
				if got := panicEntries(t, other); len(got) != 0 {
					t.Errorf("the other logger got %d panic entries: %v", len(got), got)
				}
			})
		}
	}
}
