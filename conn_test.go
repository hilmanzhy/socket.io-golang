package socketio

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

func newIdleConn(t *testing.T, opts Options) (*Server, *conn) {
	t.Helper()
	opts.ConnectTimeout = time.Hour
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	server := New(opts)
	c := newConn(server, nil)
	t.Cleanup(func() { c.timer.Stop() })
	return server, c
}

func attach(c *conn, name string) *Socket {
	namespace := c.server.Of(name)
	s := &Socket{
		id:        "id-" + name,
		namespace: namespace,
		conn:      c,
		mailbox:   newMailbox(1, nil, c.server.recovered),
		acks:      make(map[uint64]chan Args),
	}
	c.mu.Lock()
	c.sockets[namespace.name] = s
	c.timer.Stop()
	c.mu.Unlock()
	namespace.rooms.add(s)
	return s
}

func armed(c *conn) bool {
	return c.timer.Stop()
}

func TestConnTimerRunsFromTheStart(t *testing.T) {
	t.Parallel()
	_, c := newIdleConn(t, Options{})
	if !armed(c) {
		t.Error("a new connection has no timer running")
	}
}

func TestConnTimerIsArmedAgainWhenTheLastSocketLeaves(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		sockets []string
		leave   []string
		armed   bool
	}{
		{"the only socket", []string{"/"}, []string{"/"}, true},
		{"the only socket of a custom namespace", []string{"/chat"}, []string{"/chat"}, true},
		{"one of two sockets", []string{"/", "/chat"}, []string{"/chat"}, false},
		{"the other one of two sockets", []string{"/", "/chat"}, []string{"/"}, false},
		{"both sockets one after the other", []string{"/", "/chat"}, []string{"/chat", "/"}, true},
		{"three sockets but not the last", []string{"/", "/a", "/b"}, []string{"/a", "/b"}, false},
		{"no socket leaves", []string{"/"}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, c := newIdleConn(t, Options{})
			sockets := map[string]*Socket{}
			for _, name := range tc.sockets {
				sockets[name] = attach(c, name)
			}
			if armed(c) {
				t.Fatal("the timer runs although a socket is connected")
			}
			for _, name := range tc.leave {
				sockets[name].leave(ReasonClientDisconnect)
			}
			if got := armed(c); got != tc.armed {
				t.Errorf("timer running = %v, want %v", got, tc.armed)
			}
		})
	}
}

func TestConnTimerIsArmedAgainOnEveryLeave(t *testing.T) {
	t.Parallel()
	_, c := newIdleConn(t, Options{})
	for round := range 3 {
		s := attach(c, "/")
		if armed(c) {
			t.Fatalf("round %d: the timer runs although a socket is connected", round)
		}
		s.leave(ReasonServerDisconnect)
		if !armed(c) {
			t.Fatalf("round %d: the timer does not run after the last socket left", round)
		}
	}
}

func TestConnTimerIgnoresASocketThatIsNotTheConnectedOne(t *testing.T) {
	t.Parallel()
	_, c := newIdleConn(t, Options{})
	s := attach(c, "/")
	stale := &Socket{id: "stale", namespace: s.namespace, conn: c}
	c.forget(stale)
	if armed(c) {
		t.Error("forgetting an unrelated socket armed the timer")
	}
	if c.socket("/") != s {
		t.Error("forgetting an unrelated socket removed the connected one")
	}
}

func TestConnTimerIsNotArmedOnceTheSessionIsClosed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		sockets []string
	}{
		{"one socket", []string{"/"}},
		{"two sockets", []string{"/", "/chat"}},
		{"no socket", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, c := newIdleConn(t, Options{})
			var sockets []*Socket
			for _, name := range tc.sockets {
				sockets = append(sockets, attach(c, name))
			}
			c.Closed(engineio.ReasonTransportClose)
			if armed(c) {
				t.Error("the timer runs after the session closed")
			}
			for _, s := range sockets {
				if s.Connected() {
					t.Errorf("socket of %s is still connected after the session closed", s.namespace.name)
				}
			}
		})
	}
}

func TestConnTimerIsNotArmedByALeaveAfterTheSessionClosed(t *testing.T) {
	t.Parallel()
	_, c := newIdleConn(t, Options{})
	s := attach(c, "/")
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	s.leave(ReasonServerDisconnect)
	if armed(c) {
		t.Error("a socket leaving a closed connection armed the timer")
	}
}

func TestConnClosedStopsTheTimer(t *testing.T) {
	t.Parallel()
	_, c := newIdleConn(t, Options{})
	c.Closed(engineio.ReasonPingTimeout)
	if armed(c) {
		t.Error("the timer still runs after the session closed")
	}
}

func eventPayload(elements int) string {
	parts := make([]string, elements)
	parts[0] = `"e"`
	for i := 1; i < elements; i++ {
		parts[i] = "0"
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func ackPayload(elements int) string {
	return "[" + strings.TrimSuffix(strings.Repeat("0,", elements), ",") + "]"
}

func TestConnDecoderRefusesMoreArgumentsThanTheOption(t *testing.T) {
	t.Parallel()
	const placeholder = `{"_placeholder":true,"num":0}`
	cases := []struct {
		name  string
		limit int
		frame string
		bad   bool
	}{
		{"an event name alone", 3, `2["e"]`, false},
		{"an event exactly at the limit", 3, `2["e",1,2]`, false},
		{"an event one over the limit", 3, `2["e",1,2,3]`, true},
		{"an event with an acknowledgement id at the limit", 3, `212["e",1,2]`, false},
		{"an event with an acknowledgement id over the limit", 3, `212["e",1,2,3]`, true},
		{"an event in a namespace at the limit", 3, `2/chat,["e",1,2]`, false},
		{"an event in a namespace over the limit", 3, `2/chat,["e",1,2,3]`, true},
		{"nested arrays and strings are not counted", 3, `2["e","a,b,c,d",[1,2,3,4]]`, false},
		{"nested values count as one element each", 3, `2["e","a,b,c,d",[1,2,3,4],{"k":[1,2,3,4]}]`, true},
		{"an object argument is not counted by its members", 3, `2["e",{"a":1,"b":2,"c":3,"d":4}]`, false},
		{"escaped quotes do not end a string", 3, `2["e","a\",b\",c",1]`, false},
		{"an acknowledgement exactly at the limit", 3, `3[1,2,3]`, false},
		{"an acknowledgement one over the limit", 3, `3[1,2,3,4]`, true},
		{"an acknowledgement with an id over the limit", 3, `31[1,2,3,4]`, true},
		{"a binary event at the limit", 2, `51-["e",` + placeholder + `]`, false},
		{"a binary event over the limit", 2, `51-["e",` + placeholder + `,1]`, true},
		{"a binary acknowledgement at the limit", 3, `61-1[` + placeholder + `,2,3]`, false},
		{"a binary acknowledgement over the limit", 3, `61-1[` + placeholder + `,2,3,4]`, true},
		{"a limit of one allows only the name", 1, `2["e"]`, false},
		{"a limit of one refuses an argument", 1, `2["e",1]`, true},
		{"a limit of one on an acknowledgement", 1, `3[1]`, false},
		{"a limit of one refuses two acknowledgement values", 1, `3[1,2]`, true},
		{"the default allows 64 elements", 0, "2" + eventPayload(64), false},
		{"the default refuses 65 elements", 0, "2" + eventPayload(65), true},
		{"the default allows 64 acknowledgement values", 0, "3" + ackPayload(64), false},
		{"the default refuses 65 acknowledgement values", 0, "3" + ackPayload(65), true},
		{"an explicit 64 is the default", 64, "2" + eventPayload(65), true},
		{"a raised limit allows what the default refuses", 100, "2" + eventPayload(65), false},
		{"a raised limit has its own edge", 100, "2" + eventPayload(101), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, c := newIdleConn(t, Options{MaxArguments: tc.limit})
			err := c.Receive(engineio.Message{Data: []byte(tc.frame)})
			if tc.bad {
				if !errors.Is(err, parser.ErrMalformed) {
					t.Errorf("Receive(%.60q) = %v, want an error matching ErrMalformed", tc.frame, err)
				}
				return
			}
			if err != nil {
				t.Errorf("Receive(%.60q) = %v, want nil", tc.frame, err)
			}
		})
	}
}

func TestConnDecoderGetsTheConfiguredLimits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		in          Options
		wantArgs    int
		wantAttachs int
	}{
		{"the defaults", Options{}, parser.DefaultMaxArguments, parser.DefaultMaxAttachments},
		{"explicit values", Options{MaxArguments: 7, MaxAttachments: 3}, 7, 3},
		{"only arguments", Options{MaxArguments: 9}, 9, parser.DefaultMaxAttachments},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, c := newIdleConn(t, tc.in)
			if c.decoder.MaxArguments != tc.wantArgs || c.decoder.MaxAttachments != tc.wantAttachs {
				t.Errorf("decoder limits = %d arguments, %d attachments; want %d and %d",
					c.decoder.MaxArguments, c.decoder.MaxAttachments, tc.wantArgs, tc.wantAttachs)
			}
		})
	}
}
