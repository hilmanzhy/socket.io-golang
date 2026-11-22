package socketio_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	socketio "github.com/nuriontech/socket.io-golang"
	"github.com/nuriontech/socket.io-golang/engineio"
)

func assertGone(t *testing.T, s *socketio.Socket) {
	t.Helper()
	if s.Connected() {
		t.Error("Connected() = true after the disconnect")
	}
	if rooms := s.Rooms(); len(rooms) != 0 {
		t.Errorf("Rooms() = %v after the disconnect, want none", rooms)
	}
	for _, other := range s.Namespace().Sockets() {
		if other == s {
			t.Error("socket is still listed by Namespace.Sockets()")
		}
	}
	s.Join("late")
	if rooms := s.Rooms(); len(rooms) != 0 {
		t.Errorf("Join on a disconnected socket made Rooms() = %v", rooms)
	}
}

func TestClientDisconnect(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, namespace string }{{"main", "/"}, {"custom", "/custom"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			p := watch(server.Of(tc.namespace), func(s *socketio.Socket) { s.Join("a", "b") })
			c := dial(t, ts)
			c.connect(tc.namespace, "")
			s := p.socket(t)
			c.send(pkt('1', tc.namespace, ""))
			if got := p.reason(t); got != socketio.ReasonClientDisconnect {
				t.Errorf("reason = %q, want %q", got, socketio.ReasonClientDisconnect)
			}
			assertGone(t, s)
			c.expectOpen()
			c.close()
			p.noReason(t)
		})
	}
}

func TestDisconnectHandlerRunsAfterTheHandlersOfEarlierEvents(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(open)
	started := make(chan struct{}, 1)
	reasons := make(chan socketio.Reason, 2)
	var mu sync.Mutex
	var order []string
	note := func(entry string) {
		mu.Lock()
		order = append(order, entry)
		mu.Unlock()
	}
	server.OnConnect(func(s *socketio.Socket) {
		s.On("slow", func(*socketio.Event) {
			started <- struct{}{}
			<-gate
			note("event")
		})
		s.OnDisconnect(func(r socketio.Reason) {
			note("disconnect")
			reasons <- r
		})
	})
	c := dial(t, ts)
	c.connect("/", "")
	c.send(`42["slow"]`)
	receive(t, started)
	c.send("41")
	nothing(t, reasons)
	open()
	if got := receive(t, reasons); got != socketio.ReasonClientDisconnect {
		t.Errorf("reason = %q, want %q", got, socketio.ReasonClientDisconnect)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"event", "disconnect"}; !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
}

func TestClientDisconnectLeavesOtherNamespacesConnected(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("/custom"))
	c := dial(t, ts)
	c.connect("/", "")
	c.connect("/custom", "")
	main, custom := pm.socket(t), pc.socket(t)
	c.send("41/custom,")
	if got := pc.reason(t); got != socketio.ReasonClientDisconnect {
		t.Errorf("reason = %q, want %q", got, socketio.ReasonClientDisconnect)
	}
	c.send(`42/custom,9999["ping"]`)
	c.ping("/")
	c.expectOpen()
	pm.noReason(t)
	if !main.Connected() || custom.Connected() {
		t.Errorf("Connected: main %v, custom %v, want true and false", main.Connected(), custom.Connected())
	}
}

func TestTransportClose(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		close func(*client)
	}{
		{"close frame", (*client).close},
		{"dropped connection", (*client).drop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			pm := watch(server.Namespace, func(s *socketio.Socket) { s.Join("room") })
			pc := watch(server.Of("/custom"))
			c := dial(t, ts)
			c.connect("/", "")
			c.connect("/custom", "")
			main, custom := pm.socket(t), pc.socket(t)
			c.ping("/")
			c.ping("/custom")
			tc.close(c)
			for _, p := range []*probe{pm, pc} {
				if got := p.reason(t); got != socketio.ReasonTransportClose {
					t.Errorf("reason = %q, want %q", got, socketio.ReasonTransportClose)
				}
				p.noReason(t)
			}
			assertGone(t, main)
			assertGone(t, custom)
		})
	}
}

func TestServerDisconnectKeepsTheConnection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, namespace string }{{"main", "/"}, {"custom", "/custom"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			p := watch(server.Of(tc.namespace), func(s *socketio.Socket) { s.Join("a") })
			free := watch(server.Of("/free"))
			c := dial(t, ts)
			oldSid := c.connect(tc.namespace, "")
			c.connect("/free", "")
			s, _ := p.socket(t), free.socket(t)

			s.Disconnect(false)
			s.Disconnect(false)
			c.expect(pkt('1', tc.namespace, ""))
			if got := p.reason(t); got != socketio.ReasonServerDisconnect {
				t.Errorf("reason = %q, want %q", got, socketio.ReasonServerDisconnect)
			}
			c.ping("/free")
			c.expectOpen()
			p.noReason(t)
			assertGone(t, s)
			if err := s.Emit("late"); !errors.Is(err, socketio.ErrDisconnected) {
				t.Errorf("Emit after the disconnect = %v, want ErrDisconnected", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := s.EmitWithAck(ctx, "late"); !errors.Is(err, socketio.ErrDisconnected) {
				t.Errorf("EmitWithAck after the disconnect = %v, want ErrDisconnected", err)
			}

			if newSid := c.connect(tc.namespace, ""); newSid == oldSid || newSid != p.socket(t).ID() {
				t.Errorf("rejoining answered with sid %q, the first sid was %q", newSid, oldSid)
			}
		})
	}
}

func TestServerDisconnectClosingTheConnection(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("/custom"))
	other, po := dial(t, ts), watch(server.Of("/other"))
	c := dial(t, ts)
	c.connect("/", "")
	c.connect("/custom", "")
	other.connect("/other", "")
	main, custom, bystander := pm.socket(t), pc.socket(t), po.socket(t)

	main.Disconnect(true)
	c.expectSet("41", "41/custom,")
	c.expectClosed()
	for _, p := range []*probe{pm, pc} {
		if got := p.reason(t); got != socketio.ReasonServerDisconnect {
			t.Errorf("reason = %q, want %q", got, socketio.ReasonServerDisconnect)
		}
	}
	assertGone(t, main)
	assertGone(t, custom)
	other.ping("/other")
	if !bystander.Connected() {
		t.Error("a socket of another connection was disconnected")
	}
}

func TestServerCloseDisconnectsEverySocket(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("/custom"))
	c := dial(t, ts)
	c.connect("/", "")
	c.connect("/custom", "")
	main, custom := pm.socket(t), pc.socket(t)
	if err := server.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, p := range []*probe{pm, pc} {
		if got := p.reason(t); got != socketio.ReasonServerShutdown {
			t.Errorf("reason = %q, want %q", got, socketio.ReasonServerShutdown)
		}
	}
	c.expectClosed()
	assertGone(t, main)
	assertGone(t, custom)
}

func TestPingTimeoutReason(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{Engine: engineio.Options{PingInterval: 100 * time.Millisecond, PingTimeout: 150 * time.Millisecond}})
	c, _ := f.join(t, withoutPong())
	if got := f.probe.reason(t); got != socketio.ReasonPingTimeout {
		t.Errorf("reason = %q, want %q", got, socketio.ReasonPingTimeout)
	}
	c.expectClosed()
}
