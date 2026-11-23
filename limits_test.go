package socketio_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

const placeholder = `{"_placeholder":true,"num":0}`

func elements(count int) string {
	parts := make([]string, count)
	parts[0] = `"e"`
	for i := 1; i < count; i++ {
		parts[i] = "0"
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func values(count int) string {
	return "[" + strings.TrimSuffix(strings.Repeat("0,", count), ",") + "]"
}

func TestMaxArgumentsOfReceivedEvents(t *testing.T) {
	t.Parallel()
	const refused = -1
	cases := []struct {
		name   string
		limit  int
		frame  string
		binary []byte
		want   int
	}{
		{"an event name alone", 3, `42["e"]`, nil, 0},
		{"an event exactly at the limit", 3, `42["e",1,2]`, nil, 2},
		{"an event one over the limit", 3, `42["e",1,2,3]`, nil, refused},
		{"an event far over the limit", 3, `42["e",1,2,3,4,5,6,7,8,9]`, nil, refused},
		{"an event with an acknowledgement id at the limit", 3, `427["e",1,2]`, nil, 2},
		{"an event with an acknowledgement id over the limit", 3, `427["e",1,2,3]`, nil, refused},
		{"an event in a custom namespace at the limit", 3, `42/chat,["e",1,2]`, nil, 2},
		{"an event in a custom namespace over the limit", 3, `42/chat,["e",1,2,3]`, nil, refused},
		{"an event for a namespace never joined over the limit", 3, `42/ghost,["e",1,2,3]`, nil, refused},
		{"an event with a binary argument at the limit", 3, `451-["e",` + placeholder + `,1]`, []byte{9}, 2},
		{"an event with a binary argument over the limit", 3, `451-["e",` + placeholder + `,1,2]`, []byte{9}, refused},
		{"nested values count once", 3, `42["e",[1,2,3,4,5],{"a":1,"b":2,"c":3}]`, nil, 2},
		{"commas inside strings are not counted", 3, `42["e","1,2,3,4,5","6,7,8"]`, nil, 2},
		{"a limit of one takes no argument", 1, `42["e",1]`, nil, refused},
		{"a limit of one takes the name", 1, `42["e"]`, nil, 0},
		{"the default allows 64 elements", 0, `42` + elements(64), nil, 63},
		{"the default refuses 65 elements", 0, `42` + elements(65), nil, refused},
		{"a raised limit allows what the default refuses", 100, `42` + elements(65), nil, 64},
		{"a raised limit has its own edge", 100, `42` + elements(101), nil, refused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{MaxArguments: tc.limit})
			got := make(chan int, 8)
			setup := func(s *socketio.Socket) {
				s.On("e", func(e *socketio.Event) { got <- e.Args.Len() })
			}
			main, chat := watch(server.Namespace, setup), watch(server.Of("/chat"), setup)
			c := dial(t, ts)
			c.connect("/", "")
			c.connect("/chat", "")
			main.socket(t)
			chat.socket(t)

			c.send(tc.frame)
			if tc.binary != nil {
				c.sendBinary(tc.binary)
			}
			if tc.want == refused {
				c.expectClosed()
				for _, p := range []*probe{main, chat} {
					if reason := p.reason(t); reason != socketio.ReasonParseError {
						t.Errorf("reason = %q, want %q", reason, socketio.ReasonParseError)
					}
				}
				nothing(t, got)
				return
			}
			if n := receive(t, got); n != tc.want {
				t.Errorf("handler got %d arguments, want %d", n, tc.want)
			}
			c.ping("/")
			c.expectOpen()
			main.noReason(t)
			chat.noReason(t)
		})
	}
}

func TestMaxArgumentsOfReceivedAcknowledgements(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		limit   int
		frame   string
		binary  []byte
		want    int
		refused bool
	}{
		{"at the limit", 3, `430[1,2,3]`, nil, 3, false},
		{"one over the limit", 3, `430[1,2,3,4]`, nil, 0, true},
		{"empty", 3, `430[]`, nil, 0, false},
		{"a binary value at the limit", 2, `461-0[` + placeholder + `,1]`, []byte{9}, 2, false},
		{"a binary value over the limit", 2, `461-0[` + placeholder + `,1,2]`, []byte{9}, 0, true},
		{"the default allows 64 values", 0, `430` + values(64), nil, 64, false},
		{"the default refuses 65 values", 0, `430` + values(65), nil, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{MaxArguments: tc.limit})
			c, s := f.join(t)
			type outcome struct {
				args socketio.Args
				err  error
			}
			answer := make(chan outcome, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*patience)
				defer cancel()
				args, err := s.EmitWithAck(ctx, "q")
				answer <- outcome{args, err}
			}()
			c.expect(`420["q"]`)

			c.send(tc.frame)
			if tc.binary != nil {
				c.sendBinary(tc.binary)
			}
			got := receive(t, answer)
			if tc.refused {
				if !errors.Is(got.err, socketio.ErrDisconnected) {
					t.Errorf("EmitWithAck = %v, want ErrDisconnected", got.err)
				}
				c.expectClosed()
				if reason := f.probe.reason(t); reason != socketio.ReasonParseError {
					t.Errorf("reason = %q, want %q", reason, socketio.ReasonParseError)
				}
				return
			}
			if got.err != nil {
				t.Fatalf("EmitWithAck = %v", got.err)
			}
			if got.args.Len() != tc.want {
				t.Errorf("acknowledgement has %d values, want %d", got.args.Len(), tc.want)
			}
			c.ping("/")
			c.expectOpen()
		})
	}
}
