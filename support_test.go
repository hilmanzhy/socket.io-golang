package socketio_test

import (
	"bytes"
	"slices"
	"sync"
	"testing"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

// eventually polls until condition holds or patience runs out.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(patience)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v: %s", patience, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func ids(sockets []*socketio.Socket) []string {
	out := make([]string, len(sockets))
	for i, s := range sockets {
		out[i] = s.ID()
	}
	slices.Sort(out)
	return out
}

func sorted(values ...string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return out
}

func text(s string) frame {
	return frame{data: []byte(s)}
}

func blob(data ...byte) frame {
	return frame{binary: true, data: data}
}

// delivered expects exactly the frames in want, in order. A marker event
// queued on s afterwards proves that nothing else reached the client before it.
func (c *client) delivered(s *socketio.Socket, want ...frame) {
	c.t.Helper()
	for _, w := range want {
		got := c.next()
		if got.binary != w.binary || !bytes.Equal(got.data, w.data) {
			c.t.Fatalf("frame = %s %q, want %s %q", kindName(got), got.data, kindName(w), w.data)
		}
	}
	c.barrier(s)
}

type guest struct {
	name string
	c    *client
	s    *socketio.Socket
}

// seat connects a new client to the main namespace and puts its socket in rooms.
func (f *fixture) seat(t *testing.T, name string, rooms ...string) guest {
	t.Helper()
	c, s := f.join(t)
	s.Join(rooms...)
	return guest{name: name, c: c, s: s}
}

// expectDelivery checks that the guests named in recipients received want and
// that every other guest received nothing.
func expectDelivery(t *testing.T, guests []guest, want []frame, recipients ...string) {
	t.Helper()
	for _, g := range guests {
		if slices.Contains(recipients, g.name) {
			g.c.delivered(g.s, want...)
		} else {
			g.c.delivered(g.s)
		}
	}
}

// buffered returns the text frames the client has received and not read yet.
// Once the connection has closed, that is everything the server sent after the
// last frame the test read.
func (c *client) buffered() []string {
	var out []string
	for {
		select {
		case f := <-c.frames:
			out = append(out, string(f.data))
		default:
			return out
		}
	}
}
