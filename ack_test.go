package socketio_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	socketio "github.com/nuriontech/socket.io-golang"
)

type answer struct {
	args socketio.Args
	err  error
}

// ask runs EmitWithAck in the background and delivers its outcome.
func ask(s *socketio.Socket, timeout time.Duration, event string, args ...any) <-chan answer {
	out := make(chan answer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		got, err := s.EmitWithAck(ctx, event, args...)
		out <- answer{got, err}
	}()
	return out
}

func TestEventAcknowledgement(t *testing.T) {
	t.Parallel()
	handlers := func(s *socketio.Socket) {
		s.On("none", func(e *socketio.Event) { e.Ack() })
		s.On("args", func(e *socketio.Event) { e.Ack("x", 1, map[string]bool{"a": true}) })
		s.On("bytes", func(e *socketio.Event) { e.Ack([]byte{1, 2}) })
		s.On("echo", func(e *socketio.Event) {
			var text string
			e.Bind(&text)
			e.Ack(text)
		})
	}
	cases := []struct {
		name      string
		namespace string
		frame     string
		want      string
		binary    []byte
	}{
		{"no arguments", "/", `4212["none"]`, `4312[]`, nil},
		{"several arguments", "/", `4212["args"]`, `4312["x",1,{"a":true}]`, nil},
		{"echo of an argument", "/", `427["echo","hi"]`, `437["hi"]`, nil},
		{"id zero", "/", `420["none"]`, `430[]`, nil},
		{"large id", "/", `42123456789["none"]`, `43123456789[]`, nil},
		{"custom namespace", "/chat", `42/chat,5["echo","hi"]`, `43/chat,5["hi"]`, nil},
		{"binary acknowledgement", "/", `4212["bytes"]`, `461-12[{"_placeholder":true,"num":0}]`, []byte{1, 2}},
		{"binary acknowledgement in a namespace", "/chat", `42/chat,3["bytes"]`, `461-/chat,3[{"_placeholder":true,"num":0}]`, []byte{1, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, ts := newServer(t, socketio.Options{})
			watch(server.Namespace, handlers)
			watch(server.Of("/chat"), handlers)
			c := dial(t, ts)
			c.connect(tc.namespace, "")
			c.send(tc.frame)
			c.expect(tc.want)
			if tc.binary != nil {
				c.expectBinary(tc.binary)
			}
			c.ping(tc.namespace)
		})
	}
}

func TestAckWithoutAnIDSendsNothing(t *testing.T) {
	t.Parallel()
	type outcome struct {
		wants bool
		err   error
	}
	got := make(chan outcome, 1)
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("plain", func(e *socketio.Event) { got <- outcome{e.WantsAck(), e.Ack("ignored")} })
	})
	c, _ := f.join(t)
	c.send(`42["plain"]`)
	if o := receive(t, got); o.wants || o.err != nil {
		t.Errorf("WantsAck = %v, Ack = %v, want false and nil", o.wants, o.err)
	}
	c.ping("/")
}

func TestOnlyTheFirstAckIsSent(t *testing.T) {
	t.Parallel()
	type outcome struct {
		wants         bool
		first, second error
	}
	got := make(chan outcome, 1)
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("twice", func(e *socketio.Event) {
			got <- outcome{e.WantsAck(), e.Ack("a"), e.Ack("b")}
		})
		s.On("race", func(e *socketio.Event) {
			var wg sync.WaitGroup
			for i := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					e.Ack(i)
				}()
			}
			wg.Wait()
		})
	})
	c, _ := f.join(t)
	c.send(`427["twice"]`)
	c.expect(`437["a"]`)
	if o := receive(t, got); !o.wants || o.first != nil || o.second != nil {
		t.Errorf("WantsAck = %v, Ack errors = %v, %v, want true, nil, nil", o.wants, o.first, o.second)
	}
	c.send(`428["race"]`)
	if text := string(c.next().data); !strings.HasPrefix(text, `438[`) {
		t.Errorf("acknowledgement of the concurrent calls = %q", text)
	}
	c.ping("/")
}

func TestEmitWithAckNumbersRequestsPerSocket(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pa := watch(server.Namespace), watch(server.Of("/a"))
	c := dial(t, ts)
	c.connect("/", "")
	c.connect("/a", "")
	primary, other := pm.socket(t), pa.socket(t)
	second := dial(t, ts)
	second.connect("/", "")
	secondSocket := pm.socket(t)

	first := ask(primary, 10*time.Second, "q", "a")
	c.expect(`420["q","a"]`)
	c.send(`430["r1",2]`)
	got := receive(t, first)
	var text string
	var count int
	if err := got.err; err != nil || got.args.Len() != 2 || got.args.Bind(&text, &count) != nil || text != "r1" || count != 2 {
		t.Errorf("first answer = %q %d, %v", text, count, got.err)
	}

	next := ask(primary, 10*time.Second, "q")
	c.expect(`421["q"]`)
	c.send(`431[]`)
	if got := receive(t, next); got.err != nil || got.args.Len() != 0 {
		t.Errorf("second answer = %d arguments, %v", got.args.Len(), got.err)
	}

	for _, target := range []struct {
		name   string
		socket *socketio.Socket
		reader *client
		frame  string
	}{
		{"other namespace of the connection", other, c, `42/a,0["q"]`},
		{"socket of another connection", secondSocket, second, `420["q"]`},
	} {
		done := ask(target.socket, 10*time.Second, "q")
		target.reader.expect(target.frame)
		if target.socket == other {
			c.send(`43/a,0[]`)
		} else {
			second.send(`430[]`)
		}
		if got := receive(t, done); got.err != nil {
			t.Errorf("%s: %v", target.name, got.err)
		}
	}
}

func TestEmitWithAckMatchesRepliesToRequests(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	one := ask(s, 10*time.Second, "one")
	c.expect(`420["one"]`)
	two := ask(s, 10*time.Second, "two")
	c.expect(`421["two"]`)
	c.send(`431["second"]`)
	var text string
	if got := receive(t, two); got.err != nil || got.args.Bind(&text) != nil || text != "second" {
		t.Errorf("answer of the second request = %q, %v", text, got.err)
	}
	nothing(t, one)
	c.send(`430["first"]`)
	if got := receive(t, one); got.err != nil || got.args.Bind(&text) != nil || text != "first" {
		t.Errorf("answer of the first request = %q, %v", text, got.err)
	}
}

func TestEmitWithAckCarriesBinary(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	done := ask(s, 10*time.Second, "upload", []byte{1})
	c.expect(`451-0["upload",{"_placeholder":true,"num":0}]`)
	c.expectBinary([]byte{1})
	c.send(`461-0[{"_placeholder":true,"num":0}]`)
	c.sendBinary([]byte{5, 6})
	got := receive(t, done)
	if got.err != nil || got.args.Len() != 1 || !got.args.IsBinary(0) || string(got.args.Bytes(0)) != "\x05\x06" {
		t.Errorf("answer = %d arguments, binary %v, bytes %v, err %v", got.args.Len(), got.args.IsBinary(0), got.args.Bytes(0), got.err)
	}
}

func TestEmitWithAckTimeout(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	slow := ask(s, 100*time.Millisecond, "slow")
	c.expect(`420["slow"]`)
	if got := receive(t, slow); !errors.Is(got.err, context.DeadlineExceeded) || got.args.Len() != 0 {
		t.Errorf("answer = %d arguments, %v, want the context deadline error", got.args.Len(), got.err)
	}
	c.send(`430["late"]`)
	next := ask(s, 10*time.Second, "next")
	c.expect(`421["next"]`)
	c.send(`431["ok"]`)
	if got := receive(t, next); got.err != nil {
		t.Errorf("request after the timeout: %v", got.err)
	}
	c.expectOpen()
}

func TestEmitWithAckFailsWhenTheSocketDisconnectsFirst(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		cause  func(c *client, s *socketio.Socket)
		expect func(c *client)
	}{
		{"client disconnect", func(c *client, _ *socketio.Socket) { c.send("41") }, func(*client) {}},
		{"server disconnect", func(_ *client, s *socketio.Socket) { s.Disconnect(false) }, func(c *client) { c.expect("41") }},
		{"transport close", func(c *client, _ *socketio.Socket) { c.close() }, func(*client) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			c, s := f.join(t)
			done := ask(s, 10*time.Second, "q")
			c.expect(`420["q"]`)
			tc.cause(c, s)
			tc.expect(c)
			if got := receive(t, done); !errors.Is(got.err, socketio.ErrDisconnected) {
				t.Errorf("EmitWithAck = %v, want ErrDisconnected", got.err)
			}
		})
	}
}

func TestEmitWithAckInsideAHandler(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("relay", func(e *socketio.Event) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reply, err := s.EmitWithAck(ctx, "question")
			var text string
			if err != nil || reply.Bind(&text) != nil {
				e.Ack("failed")
				return
			}
			e.Ack(text)
		})
	})
	c, _ := f.join(t)
	c.send(`4250["relay"]`)
	c.expect(`420["question"]`)
	c.send(`430["answer"]`)
	c.expect(`4350["answer"]`)
	c.ping("/")
}

func TestAcknowledgementWithoutAWaitingRequestIsIgnored(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	c, s := f.join(t)
	c.send(`43999["x"]`)
	c.send(`430[]`)
	c.ping("/")
	c.barrier(s)
}
