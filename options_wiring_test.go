package socketio

import (
	"io"
	"log/slog"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/engineio"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

func discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestOptionsEngineLogger(t *testing.T) {
	t.Parallel()
	server, engine := discard(), discard()
	cases := []struct {
		name       string
		in         Options
		wantServer *slog.Logger
		wantEngine *slog.Logger
	}{
		{"nothing set", Options{}, slog.Default(), slog.Default()},
		{"only the server logger", Options{Logger: server}, server, server},
		{"only the engine logger", Options{Engine: engineio.Options{Logger: engine}}, slog.Default(), engine},
		{"both loggers", Options{Logger: server, Engine: engineio.Options{Logger: engine}}, server, engine},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.in.withDefaults()
			if got.Logger != tc.wantServer {
				t.Errorf("Logger = %p, want %p", got.Logger, tc.wantServer)
			}
			if got.Engine.Logger != tc.wantEngine {
				t.Errorf("Engine.Logger = %p, want %p", got.Engine.Logger, tc.wantEngine)
			}
			started := New(tc.in)
			if started.opts.Engine.Logger != tc.wantEngine {
				t.Errorf("Engine.Logger of the new server = %p, want %p", started.opts.Engine.Logger, tc.wantEngine)
			}
			if started.opts.Logger != tc.wantServer {
				t.Errorf("Logger of the new server = %p, want %p", started.opts.Logger, tc.wantServer)
			}
			started.Close()
		})
	}
}

func TestOptionsEngineLoggerLeavesTheCallersOptionsAlone(t *testing.T) {
	t.Parallel()
	in := Options{Logger: discard()}
	_ = in.withDefaults()
	New(in).Close()
	if in.Engine.Logger != nil {
		t.Error("the options of the caller got an Engine.Logger")
	}
}

func TestOptionsMaxArguments(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   int
		want int
	}{
		{"unset means 64", 0, 64},
		{"unset means the parser default", 0, parser.DefaultMaxArguments},
		{"an explicit value is kept", 5, 5},
		{"one is kept", 1, 1},
		{"a value above the default is kept", 1000, 1000},
		{"the default written out is kept", 64, 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := (Options{MaxArguments: tc.in}).withDefaults().MaxArguments; got != tc.want {
				t.Errorf("MaxArguments = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestOptionsMaxRoomsReachesEveryNamespace(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, 1, 7} {
		server := New(Options{MaxRooms: limit, Logger: discard()})
		for _, name := range []string{"/", "/chat", "chat", "/a/b"} {
			if got := server.Of(name).rooms.limit; got != limit {
				t.Errorf("MaxRooms %d: namespace %q has limit %d", limit, name, got)
			}
		}
		if got := server.Namespace.rooms.limit; got != limit {
			t.Errorf("MaxRooms %d: main namespace has limit %d", limit, got)
		}
		server.Close()
	}
}
