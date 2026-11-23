package socketio_test

import (
	"testing"
	"time"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

const rearmTimeout = 300 * time.Millisecond

type leaving struct {
	name  string
	leave func(c *client, s *socketio.Socket, namespace string)
}

func leavings() []leaving {
	return []leaving{
		{"client DISCONNECT", func(c *client, _ *socketio.Socket, namespace string) {
			c.send(pkt('1', namespace, ""))
		}},
		{"server Disconnect", func(c *client, s *socketio.Socket, namespace string) {
			s.Disconnect(false)
			c.expect(pkt('1', namespace, ""))
		}},
	}
}

func expectClosedAfter(t *testing.T, c *client, start time.Time, timeout time.Duration) {
	t.Helper()
	c.expectClosed()
	if elapsed := time.Since(start); elapsed < timeout {
		t.Errorf("the connection closed %v after the last namespace was left, want at least %v", elapsed, timeout)
	}
}

func TestLeavingTheLastNamespaceClosesTheConnectionAfterTheTimeout(t *testing.T) {
	t.Parallel()
	for _, l := range leavings() {
		for _, namespace := range []string{"/", "/chat"} {
			t.Run(l.name+" "+namespace, func(t *testing.T) {
				t.Parallel()
				server, ts := newServer(t, socketio.Options{ConnectTimeout: rearmTimeout})
				p := watch(server.Of(namespace))
				c := dial(t, ts)
				c.connect(namespace, "")
				s := p.socket(t)
				c.ping(namespace)

				start := time.Now()
				l.leave(c, s, namespace)
				expectClosedAfter(t, c, start, rearmTimeout)
				eventually(t, "the socket to be released", func() bool { return len(server.Sockets()) == 0 })
			})
		}
	}
}

func TestLeavingOneOfTwoNamespacesKeepsTheConnectionOpen(t *testing.T) {
	t.Parallel()
	for _, l := range leavings() {
		t.Run(l.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{ConnectTimeout: rearmTimeout})
			pm, pc := watch(server.Namespace), watch(server.Of("/chat"))
			c := dial(t, ts)
			c.connect("/", "")
			c.connect("/chat", "")
			main, chat := pm.socket(t), pc.socket(t)

			l.leave(c, chat, "/chat")
			c.expectOpenFor(2 * rearmTimeout)
			c.ping("/")

			start := time.Now()
			l.leave(c, main, "/")
			expectClosedAfter(t, c, start, rearmTimeout)
		})
	}
}

func TestRejoiningWithinTheTimeoutKeepsTheConnectionOpen(t *testing.T) {
	t.Parallel()
	for _, l := range leavings() {
		for _, rejoin := range []string{"/", "/chat"} {
			t.Run(l.name+" then "+rejoin, func(t *testing.T) {
				t.Parallel()
				server, ts := newServer(t, socketio.Options{ConnectTimeout: rearmTimeout})
				pm, pc := watch(server.Namespace), watch(server.Of("/chat"))
				c := dial(t, ts)
				c.connect("/", "")
				main := pm.socket(t)

				l.leave(c, main, "/")
				c.connect(rejoin, "")
				if rejoin == "/" {
					pm.socket(t)
				} else {
					pc.socket(t)
				}

				c.expectOpenFor(3 * rearmTimeout)
				c.ping(rejoin)
			})
		}
	}
}

func TestEveryLeaveOfTheLastNamespaceRestartsTheTimeout(t *testing.T) {
	t.Parallel()
	for _, l := range leavings() {
		t.Run(l.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{ConnectTimeout: rearmTimeout})
			p := watch(server.Namespace)
			c := dial(t, ts)
			for range 2 {
				c.connect("/", "")
				l.leave(c, p.socket(t), "/")
			}
			c.connect("/", "")
			s := p.socket(t)
			c.ping("/")

			start := time.Now()
			l.leave(c, s, "/")
			expectClosedAfter(t, c, start, rearmTimeout)
		})
	}
}
