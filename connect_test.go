package socketio_test

import (
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

func TestConnectAnswersWithSid(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, namespace string }{{"main", "/"}, {"custom", "/custom"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			p := watch(server.Of(tc.namespace))
			c := dial(t, ts)
			sid := c.connect(tc.namespace, "")
			s := p.socket(t)
			if s.ID() != sid {
				t.Errorf("ID() = %q, sid sent to the client = %q", s.ID(), sid)
			}
			if got := s.Namespace().Name(); got != tc.namespace {
				t.Errorf("Namespace().Name() = %q, want %q", got, tc.namespace)
			}
			if !s.Connected() {
				t.Error("Connected() = false after the CONNECT answer")
			}
			c.ping(tc.namespace)
			p.noSocket(t)
		})
	}
}

func TestConnectAuth(t *testing.T) {
	t.Parallel()
	type credentials struct{ Token string }
	for _, tc := range []struct {
		name, namespace, auth string
		want                  credentials
	}{
		{"main", "/", `{"token":"123"}`, credentials{"123"}},
		{"custom", "/admin", `{"token":"x"}`, credentials{"x"}},
		{"absent", "/", ``, credentials{"untouched"}},
		{"empty object", "/", `{}`, credentials{"untouched"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			p := watch(server.Of(tc.namespace))
			c := dial(t, ts)
			c.connect(tc.namespace, tc.auth)
			got := credentials{"untouched"}
			if err := p.socket(t).Auth(&got); err != nil {
				t.Fatalf("Auth: %v", err)
			}
			if got != tc.want {
				t.Errorf("Auth = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestUnknownNamespaceIsRefusedAndConnectionStaysOpen(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	p := watch(server.Namespace)
	c := dial(t, ts)
	c.send("40/random,")
	c.expect(`44/random,{"message":"Invalid namespace"}`)
	c.expectOpen()
	c.connect("/", "")
	p.socket(t)
	c.send("40/random,")
	c.expect(`44/random,{"message":"Invalid namespace"}`)
	c.ping("/")
}

func TestNamespaceMiddlewareRefusal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		middleware func(*socketio.Socket) error
		body       string
		logged     string
	}{
		{"plain error", func(*socketio.Socket) error { return errors.New("not allowed") }, `{"message":"not allowed"}`, ""},
		{"connect error without data", func(*socketio.Socket) error { return &socketio.ConnectError{Message: "denied"} }, `{"message":"denied"}`, ""},
		{"connect error with data", func(*socketio.Socket) error {
			return &socketio.ConnectError{Message: "denied", Data: map[string]any{"code": 42}}
		}, `{"message":"denied","data":{"code":42}}`, ""},
		{"panic", func(*socketio.Socket) error { panic("middleware exploded") }, `{"message":"Internal server error"}`, "middleware exploded"},
	}
	for _, where := range []struct{ label, namespace string }{{"main", "/"}, {"admin", "/admin"}} {
		namespace := where.namespace
		for _, tc := range cases {
			t.Run(where.label+" "+tc.name, func(t *testing.T) {
				t.Parallel()
				logs := &syncBuffer{}
				server, ts := newServer(t, socketio.Options{Logger: slog.New(slog.NewTextHandler(logs, nil))})
				ns := server.Of(namespace)
				ns.Use(tc.middleware)
				p := watch(ns)
				free := watch(server.Of("/free"))
				c := dial(t, ts)
				c.send(pkt('0', namespace, ""))
				c.expect(pkt('4', namespace, tc.body))
				c.expectOpen()
				p.noSocket(t)
				if n := len(ns.Sockets()); n != 0 {
					t.Errorf("namespace has %d sockets after a refused connection", n)
				}
				if tc.logged != "" && !strings.Contains(logs.String(), tc.logged) {
					t.Errorf("log %q does not mention %q", logs.String(), tc.logged)
				}
				c.connect("/free", "")
				free.socket(t)
			})
		}
	}
}

func TestNamespaceMiddlewareSeesAnUnconnectedSocket(t *testing.T) {
	t.Parallel()
	type observation struct {
		connected bool
		rooms     []string
		sockets   int
		emitErr   error
		token     string
	}
	server, ts := newServer(t, socketio.Options{})
	seen := make(chan observation, 1)
	server.Use(func(s *socketio.Socket) error {
		var auth struct{ Token string }
		s.Auth(&auth)
		s.Join("early")
		seen <- observation{s.Connected(), s.Rooms(), len(server.Sockets()), s.Emit("early"), auth.Token}
		return nil
	})
	p := watch(server.Namespace)
	c := dial(t, ts)
	c.connect("/", `{"token":"abc"}`)
	got := receive(t, seen)
	if got.connected || len(got.rooms) != 0 || got.sockets != 0 {
		t.Errorf("inside the middleware: connected=%v rooms=%v sockets=%d, want a socket that is not connected", got.connected, got.rooms, got.sockets)
	}
	if !errors.Is(got.emitErr, socketio.ErrDisconnected) {
		t.Errorf("Emit inside the middleware = %v, want ErrDisconnected", got.emitErr)
	}
	if got.token != "abc" {
		t.Errorf("auth read by the middleware = %q, want abc", got.token)
	}
	s := p.socket(t)
	if want := []string{s.ID()}; !slices.Equal(s.Rooms(), want) {
		t.Errorf("Rooms() = %v, want only %v: Join inside the middleware must be a no-op", s.Rooms(), want)
	}
}

func TestNamespaceMiddlewareChain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		failAt    int
		wantCalls []int
		connected bool
	}{
		{"all pass", 0, []int{1, 2, 3}, true},
		{"first refuses", 1, []int{1}, false},
		{"second refuses", 2, []int{1, 2}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			var mu sync.Mutex
			var calls []int
			for i := 1; i <= 3; i++ {
				server.Use(func(*socketio.Socket) error {
					mu.Lock()
					calls = append(calls, i)
					mu.Unlock()
					if i == tc.failAt {
						return errors.New("refused")
					}
					return nil
				})
			}
			p := watch(server.Namespace)
			c := dial(t, ts)
			if tc.connected {
				c.connect("/", "")
				p.socket(t)
			} else {
				c.send("40")
				c.expect(`44{"message":"refused"}`)
				p.noSocket(t)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(calls, tc.wantCalls) {
				t.Errorf("middleware calls = %v, want %v", calls, tc.wantCalls)
			}
		})
	}
}

func TestOnConnectEmitArrivesAfterTheConnectAnswer(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		if err := s.Emit("hello", "world"); err != nil {
			t.Errorf("Emit inside OnConnect: %v", err)
		}
	})
	c := dial(t, f.ts)
	c.connect("/", "")
	c.expect(`42["hello","world"]`)
	f.probe.socket(t)
	f.probe.noSocket(t)
}

func TestOnConnectRunsOncePerSocketAndLastHandlerWins(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	first := make(chan string, 4)
	second := make(chan string, 4)
	server.OnConnect(func(s *socketio.Socket) { first <- s.ID() })
	server.OnConnect(func(s *socketio.Socket) { second <- s.ID() })
	want := []string{dial(t, ts).connect("/", ""), dial(t, ts).connect("/", "")}
	got := []string{receive(t, second), receive(t, second)}
	if !slices.Equal(sorted(got...), sorted(want...)) {
		t.Errorf("OnConnect ran for %v, want %v", got, want)
	}
	nothing(t, second)
	nothing(t, first)
}

func TestOneConnectionJoinsSeveralNamespaces(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	answer := func(s *socketio.Socket) {
		s.On("who", func(e *socketio.Event) { e.Ack(s.Namespace().Name()) })
	}
	probes := map[string]*probe{
		"/":  watch(server.Namespace, answer),
		"/a": watch(server.Of("/a"), answer),
		"/b": watch(server.Of("/b"), answer),
	}
	c := dial(t, ts)
	sids := map[string]string{}
	for _, namespace := range []string{"/", "/a", "/b"} {
		sids[namespace] = c.connect(namespace, "")
		if got := probes[namespace].socket(t); got.ID() != sids[namespace] || got.Namespace().Name() != namespace {
			t.Errorf("socket of %s = %s in %s, want %s", namespace, got.ID(), got.Namespace().Name(), sids[namespace])
		}
	}
	if sids["/"] == sids["/a"] || sids["/a"] == sids["/b"] || sids["/"] == sids["/b"] {
		t.Errorf("sockets share ids: %v", sids)
	}
	for _, namespace := range []string{"/", "/a", "/b"} {
		c.send(pkt('2', namespace, `5["who"]`))
		c.expect(pkt('3', namespace, `5["`+namespace+`"]`))
	}
}

func TestConnectTimeout(t *testing.T) {
	t.Parallel()
	_, ts := newServer(t, socketio.Options{ConnectTimeout: 50 * time.Millisecond})
	dial(t, ts).expectClosed()
}

func TestConnectTimeoutDoesNotApplyOnceNamespaceJoined(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{ConnectTimeout: 200 * time.Millisecond})
	c, _ := f.join(t)
	c.expectOpenFor(600 * time.Millisecond)
	c.ping("/")
}

func TestProtocolViolationsCloseTheConnection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts socketio.Options
		send func(*client)
	}{
		{"second connect to the same namespace", socketio.Options{}, func(c *client) { c.send("40") }},
		{"connect error sent by the client", socketio.Options{}, func(c *client) { c.send(`44{"message":"x"}`) }},
		{"unknown packet type letter", socketio.Options{}, func(c *client) { c.send("4abc") }},
		{"unknown packet type digit", socketio.Options{}, func(c *client) { c.send("47[]") }},
		{"empty packet", socketio.Options{}, func(c *client) { c.send("4") }},
		{"event payload is an object", socketio.Options{}, func(c *client) { c.send("42{}") }},
		{"event payload is not JSON", socketio.Options{}, func(c *client) { c.send(`42["a"`) }},
		{"event array is empty", socketio.Options{}, func(c *client) { c.send("42[]") }},
		{"event name is a number", socketio.Options{}, func(c *client) { c.send("42[1]") }},
		{"event name is an object", socketio.Options{}, func(c *client) { c.send(`42[{"a":1}]`) }},
		{"reserved name connect", socketio.Options{}, func(c *client) { c.send(`42["connect"]`) }},
		{"reserved name connect_error", socketio.Options{}, func(c *client) { c.send(`42["connect_error"]`) }},
		{"reserved name disconnect", socketio.Options{}, func(c *client) { c.send(`42["disconnect"]`) }},
		{"acknowledgement payload is an object", socketio.Options{}, func(c *client) { c.send("431{}") }},
		{"binary frame without a packet", socketio.Options{}, func(c *client) { c.sendBinary([]byte{1}) }},
		{"text frame while attachments are expected", socketio.Options{}, func(c *client) {
			c.send(`451-["a",{"_placeholder":true,"num":0}]`)
			c.send(`42["b"]`)
		}},
		{"placeholder points past the attachments", socketio.Options{}, func(c *client) {
			c.send(`451-["a",{"_placeholder":true,"num":3}]`)
			c.sendBinary([]byte{1})
		}},
		{"too many attachments", socketio.Options{MaxAttachments: 1}, func(c *client) {
			c.send(`452-["a",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1}]`)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, tc.opts)
			c, _ := f.join(t)
			tc.send(c)
			c.expectClosed()
			if got := f.probe.reason(t); got != socketio.ReasonParseError {
				t.Errorf("disconnect reason = %q, want %q", got, socketio.ReasonParseError)
			}
			f.probe.noReason(t)
		})
	}
}

func TestParseErrorDisconnectsEveryNamespaceOfTheConnection(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	probes := []*probe{watch(server.Namespace), watch(server.Of("/a")), watch(server.Of("/b"))}
	c := dial(t, ts)
	for _, namespace := range []string{"/", "/a", "/b"} {
		c.connect(namespace, "")
	}
	c.send("4abc")
	c.expectClosed()
	for i, p := range probes {
		if got := p.reason(t); got != socketio.ReasonParseError {
			t.Errorf("namespace %d: reason = %q, want %q", i, got, socketio.ReasonParseError)
		}
	}
}

func TestTrafficForAnUnjoinedNamespaceIsDroppedSilently(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		send func(*client)
	}{
		{"event for a namespace that exists", func(c *client) { c.send(`42/other,["x"]`) }},
		{"event for a namespace that does not exist", func(c *client) { c.send(`42/ghost,["x"]`) }},
		{"event asking for an acknowledgement", func(c *client) { c.send(`42/other,12["x"]`) }},
		{"acknowledgement", func(c *client) { c.send(`43/other,1[]`) }},
		{"disconnect", func(c *client) { c.send("41/other,") }},
		{"binary event", func(c *client) {
			c.send(`451-/other,["x",{"_placeholder":true,"num":0}]`)
			c.sendBinary([]byte{1, 2})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			other := watch(server.Of("/other"))
			main := watch(server.Namespace)
			c := dial(t, ts)
			c.connect("/", "")
			main.socket(t)
			tc.send(c)
			c.ping("/")
			c.expectOpen()
			other.noSocket(t)
			main.noReason(t)
		})
	}
}
