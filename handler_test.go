package socketio_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	socketio "github.com/nuriontech/socket.io-golang"
)

func TestHandlersOfOneSocketRunOneAtATimeInOrder(t *testing.T) {
	t.Parallel()
	const total = 150
	var running, peak atomic.Int32
	var mu sync.Mutex
	var order []int
	finished := make(chan struct{})
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("n", func(e *socketio.Event) {
			now := running.Add(1)
			for seen := peak.Load(); now > seen && !peak.CompareAndSwap(seen, now); seen = peak.Load() {
			}
			var n int
			e.Bind(&n)
			runtime.Gosched()
			mu.Lock()
			order = append(order, n)
			mu.Unlock()
			running.Add(-1)
			if n == total-1 {
				close(finished)
			}
		})
	})
	c, _ := f.join(t)
	for i := 0; i < total; i++ {
		c.send(fmt.Sprintf(`42["n",%d]`, i))
	}
	receive(t, finished)
	mu.Lock()
	defer mu.Unlock()
	for i, n := range order {
		if n != i {
			t.Fatalf("handler %d ran for event %d: order %v", i, n, order)
		}
	}
	if len(order) != total || peak.Load() != 1 {
		t.Errorf("handled %d events with up to %d running at once, want %d events one at a time", len(order), peak.Load(), total)
	}
}

func TestSlowHandlerDoesNotDelayAnotherSocket(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	entered := make(chan struct{}, 1)
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		var auth struct{ Slow bool }
		s.Auth(&auth)
		s.On("work", func(e *socketio.Event) {
			if auth.Slow {
				entered <- struct{}{}
				<-release
			}
			e.Ack("done")
		})
	})
	t.Cleanup(unblock)
	slow := dial(t, f.ts)
	slow.connect("/", `{"slow":true}`)
	f.probe.socket(t)
	fast := dial(t, f.ts)
	fast.connect("/", `{"slow":false}`)
	f.probe.socket(t)

	slow.send(`421["work"]`)
	receive(t, entered)
	fast.send(`421["work"]`)
	fast.expect(`431["done"]`)
	slow.expectSilence()
	unblock()
	slow.expect(`431["done"]`)
}

func TestPanicsAreRecoveredAndLogged(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		value  string
		leaves bool
		arm    func(ns *socketio.Namespace)
		act    func(t *testing.T, c *client)
	}{
		{
			name: "event handler", value: "handler exploded",
			arm: func(ns *socketio.Namespace) {
				ns.OnConnect(func(s *socketio.Socket) {
					s.On("boom", func(*socketio.Event) { panic("handler exploded") })
					s.On("ping", func(e *socketio.Event) { e.Ack() })
				})
			},
			act: func(t *testing.T, c *client) {
				c.connect("/", "")
				c.send(`42["boom"]`)
				c.send(`42["boom"]`)
				c.ping("/")
			},
		},
		{
			name: "socket middleware", value: "event middleware exploded",
			arm: func(ns *socketio.Namespace) {
				ns.OnConnect(func(s *socketio.Socket) {
					s.Use(func(e *socketio.Event) error {
						if e.Name == "boom" {
							panic("event middleware exploded")
						}
						return nil
					})
					s.On("ping", func(e *socketio.Event) { e.Ack() })
				})
			},
			act: func(t *testing.T, c *client) {
				c.connect("/", "")
				c.send(`42["boom"]`)
				c.ping("/")
			},
		},
		{
			name: "connection handler", value: "connect handler exploded",
			arm: func(ns *socketio.Namespace) {
				ns.OnConnect(func(s *socketio.Socket) {
					s.On("ping", func(e *socketio.Event) { e.Ack() })
					panic("connect handler exploded")
				})
			},
			act: func(t *testing.T, c *client) {
				c.connect("/", "")
				c.ping("/")
			},
		},
		{
			name: "disconnect handler", value: "disconnect handler exploded", leaves: true,
			arm: func(ns *socketio.Namespace) {
				ns.OnConnect(func(s *socketio.Socket) {
					s.OnDisconnect(func(socketio.Reason) { panic("disconnect handler exploded") })
				})
			},
			act: func(t *testing.T, c *client) {
				c.connect("/", "")
				c.send("41")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logs := &syncBuffer{}
			server, ts := newServer(t, socketio.Options{Logger: slog.New(slog.NewTextHandler(logs, nil))})
			tc.arm(server.Namespace)
			c := dial(t, ts)
			tc.act(t, c)
			eventually(t, "the panic is logged", func() bool { return strings.Contains(logs.String(), tc.value) })
			if !tc.leaves {
				c.expectOpen()
			}
		})
	}
}

func TestBackpressureStopsReadingTheConnection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		queued  int
		blocked bool
	}{
		{"events fit in the backlog", 2, false},
		{"events overflow the backlog", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gate := make(chan struct{})
			var once sync.Once
			open := func() { once.Do(func() { close(gate) }) }
			started := make(chan struct{}, 1)
			handled := make(chan int, 16)
			f := newFixture(t, socketio.Options{EventBacklog: 2}, func(s *socketio.Socket) {
				s.On("n", func(e *socketio.Event) {
					var n int
					e.Bind(&n)
					if n == 0 {
						started <- struct{}{}
						<-gate
					}
					handled <- n
				})
			})
			t.Cleanup(open)
			c, s := f.join(t)

			answer := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, err := s.EmitWithAck(ctx, "question")
				answer <- err
			}()
			c.expect(`420["question"]`)

			c.send(`42["n",0]`)
			receive(t, started)
			for i := 1; i <= tc.queued; i++ {
				c.send(fmt.Sprintf(`42["n",%d]`, i))
			}
			c.send(`430[]`)

			if tc.blocked {
				nothing(t, answer)
				open()
			}
			if err := receive(t, answer); err != nil {
				t.Fatalf("EmitWithAck: %v", err)
			}
			if !tc.blocked {
				select {
				case n := <-handled:
					t.Fatalf("event %d was handled while the first handler was still blocked", n)
				default:
				}
				open()
			}
			var got []int
			for range tc.queued + 1 {
				got = append(got, receive(t, handled))
			}
			want := make([]int, tc.queued+1)
			for i := range want {
				want[i] = i
			}
			if !slices.Equal(got, want) {
				t.Errorf("events handled in order %v, want %v", got, want)
			}
		})
	}
}

func TestFullBacklogDoesNotKeepTheSocketOfAnEndedConnection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		end  func(f *fixture, c *client)
		want socketio.Reason
	}{
		{"the client vanishes", func(_ *fixture, c *client) { c.drop() }, ""},
		{"the server closes", func(f *fixture, _ *client) { f.server.Close() }, socketio.ReasonServerShutdown},
		{"the client stays and its pongs go unread", func(*fixture, *client) {}, socketio.ReasonPingTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := socketio.Options{EventBacklog: 2}
			opts.Engine.PingInterval, opts.Engine.PingTimeout = 100*time.Millisecond, 200*time.Millisecond
			answer := make(chan error, 1)
			f := newFixture(t, opts, func(s *socketio.Socket) {
				s.On("ask", func(*socketio.Event) {
					_, err := s.EmitWithAck(context.Background(), "question")
					answer <- err
				})
				s.On("n", func(*socketio.Event) {})
			})
			c, _ := f.join(t)
			c.send(`42["ask"]`)
			c.expect(`420["question"]`)
			for range 4 {
				c.send(`42["n"]`)
			}
			c.expectSilence()
			tc.end(f, c)
			if err := receive(t, answer); !errors.Is(err, socketio.ErrDisconnected) {
				t.Fatalf("EmitWithAck = %v, want ErrDisconnected", err)
			}
			if reason := f.probe.reason(t); tc.want != "" && reason != tc.want {
				t.Errorf("reason = %q, want %q", reason, tc.want)
			}
			eventually(t, "the socket to be released", func() bool { return len(f.server.Sockets()) == 0 })
		})
	}
}
