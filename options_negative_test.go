package socketio_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

// binaryEvent returns the text frame of a BINARY_EVENT named "e" that carries
// count attachments.
func binaryEvent(count int) string {
	var args strings.Builder
	for i := range count {
		fmt.Fprintf(&args, `,{"_placeholder":true,"num":%d}`, i)
	}
	return fmt.Sprintf(`45%d-["e"%s]`, count, args.String())
}

func TestNegativeEventBacklogMeansTheDefault(t *testing.T) {
	t.Parallel()
	for _, backlog := range []int{0, -1, -100} {
		t.Run(fmt.Sprint(backlog), func(t *testing.T) {
			t.Parallel()
			got := make(chan string, 1)
			f := newFixture(t, socketio.Options{EventBacklog: backlog}, func(s *socketio.Socket) {
				s.On("go", func(e *socketio.Event) { got <- e.Name })
			})
			c, _ := f.join(t)
			c.send(`42["go"]`)
			if name := receive(t, got); name != "go" {
				t.Errorf("handler got event %q, want go", name)
			}
			c.ping("/")
		})
	}
}

func TestNegativeConnectTimeoutMeansTheDefault(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{0, -1, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{ConnectTimeout: timeout})
			c, _ := f.join(t)
			c.ping("/")
			c.send("41")
			f.probe.reason(t)
			c.expectOpenFor(quiet)
		})
	}
}

func TestNegativeParserLimitsMeanTheDefaults(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		opts    socketio.Options
		frame   string
		blobs   int
		refused bool
	}{
		{"arguments at the default", socketio.Options{MaxArguments: -1}, `42` + elements(64), 0, false},
		{"arguments over the default", socketio.Options{MaxArguments: -1}, `42` + elements(65), 0, true},
		{"attachments at the default", socketio.Options{MaxAttachments: -1}, binaryEvent(10), 10, false},
		{"attachments over the default", socketio.Options{MaxAttachments: -1}, binaryEvent(11), 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := make(chan int, 1)
			f := newFixture(t, tc.opts, func(s *socketio.Socket) {
				s.On("e", func(e *socketio.Event) { got <- e.Args.Len() })
			})
			c, _ := f.join(t)
			c.send(tc.frame)
			for range tc.blobs {
				c.sendBinary([]byte{1})
			}
			if tc.refused {
				c.expectClosed()
				return
			}
			receive(t, got)
			c.ping("/")
		})
	}
}

func TestNegativeMaxRoomsMeansNoLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{MaxRooms: -1})
	c, s := f.join(t)
	if err := s.Join("a", "b", "c", "d", "e"); err != nil {
		t.Errorf("Join = %v, want nil", err)
	}
	if got := len(s.Rooms()); got != 6 {
		t.Errorf("socket is in %d rooms, want 6", got)
	}
	c.barrier(s)
}
