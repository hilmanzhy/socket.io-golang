package socketio_test

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	socketio "github.com/nuriontech/socket.io-golang"
)

func idsOf(guests []guest, names ...string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = named(guests, name).s.ID()
	}
	slices.Sort(out)
	return out
}

func TestBroadcastOperatorSockets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		target func(ns *socketio.Namespace) socketio.BroadcastOperator
		want   []string
	}{
		{"namespace", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.To() }, []string{"red", "both", "blue", "none"}},
		{"one room", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.To("red") }, []string{"red", "both"}},
		{"several rooms without duplicates", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.To("red", "blue") }, []string{"red", "both", "blue"}},
		{"Except", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.Except("red") }, []string{"blue", "none"}},
		{"To and Except", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.To("red").Except("blue") }, []string{"red"}},
		{"empty room", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.To("green") }, nil},
		{"Local", func(ns *socketio.Namespace) socketio.BroadcastOperator { return ns.Local().To("blue") }, []string{"both", "blue"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := crowd(t, f)
			if got, want := ids(tc.target(f.server.Namespace).Sockets()), idsOf(guests, tc.want...); !slices.Equal(got, want) {
				t.Errorf("Sockets() = %q, want %q", got, want)
			}
		})
	}
}

func TestNamespaceSockets(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	if got := f.server.Sockets(); len(got) != 0 {
		t.Fatalf("%d sockets before anybody connected", len(got))
	}
	guests := crowd(t, f)
	if got, want := ids(f.server.Sockets()), idsOf(guests, "red", "both", "blue", "none"); !slices.Equal(got, want) {
		t.Errorf("Sockets() = %q, want %q", got, want)
	}
	if got := f.server.Of("/elsewhere").Sockets(); len(got) != 0 {
		t.Errorf("another namespace lists %d sockets", len(got))
	}
}

func TestBroadcastOperatorJoinAndLeave(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		act     func(ns *socketio.Namespace) error
		room    string
		members []string
	}{
		{"Join for a room", func(ns *socketio.Namespace) error { return ns.To("red").Join("vip") }, "vip", []string{"red", "both"}},
		{"Join for everybody", func(ns *socketio.Namespace) error { return ns.To().Join("all") }, "all", []string{"red", "both", "blue", "none"}},
		{"Join with Except", func(ns *socketio.Namespace) error { return ns.To("red").Except("blue").Join("vip") }, "vip", []string{"red"}},
		{"Join for an empty room", func(ns *socketio.Namespace) error { return ns.To("green").Join("vip") }, "vip", nil},
		{"Join several rooms", func(ns *socketio.Namespace) error { return ns.To("blue").Join("a", "b") }, "b", []string{"both", "blue"}},
		{"Leave for a room", func(ns *socketio.Namespace) error { return ns.To("red").Leave("red") }, "red", nil},
		{"Leave with Except", func(ns *socketio.Namespace) error { return ns.To("red").Except("blue").Leave("red") }, "red", []string{"both"}},
		{"Leave for everybody", func(ns *socketio.Namespace) error { return ns.To().Leave("blue") }, "blue", nil},
		{"Leave a room only some targets joined", func(ns *socketio.Namespace) error { return ns.To("red").Leave("blue") }, "blue", []string{"blue"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := crowd(t, f)
			if err := tc.act(f.server.Namespace); err != nil {
				t.Fatalf("operator: %v", err)
			}
			if got, want := ids(f.server.To(tc.room).Sockets()), idsOf(guests, tc.members...); !slices.Equal(got, want) {
				t.Errorf("members of %q = %q, want %q", tc.room, got, want)
			}
			if err := f.server.To(tc.room).Emit("check"); err != nil {
				t.Fatal(err)
			}
			expectDelivery(t, guests, []frame{text(`42["check"]`)}, tc.members...)
		})
	}
}

func TestBroadcastOperatorDisconnect(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		close bool
	}{
		{"keeping the connection", false},
		{"closing the connection", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := crowd(t, f)
			if err := f.server.To("red").Disconnect(tc.close); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if got := f.probe.reason(t); got != socketio.ReasonServerDisconnect {
					t.Errorf("reason = %q, want %q", got, socketio.ReasonServerDisconnect)
				}
			}
			for _, name := range []string{"red", "both"} {
				g := named(guests, name)
				g.c.expect("41")
				if tc.close {
					g.c.expectClosed()
				} else {
					g.c.expectSilence()
					g.c.expectOpen()
				}
				if g.s.Connected() {
					t.Errorf("%s is still connected", name)
				}
			}
			for _, name := range []string{"blue", "none"} {
				g := named(guests, name)
				g.c.delivered(g.s)
				if !g.s.Connected() {
					t.Errorf("%s was disconnected", name)
				}
			}
			if got, want := ids(f.server.Sockets()), idsOf(guests, "blue", "none"); !slices.Equal(got, want) {
				t.Errorf("Sockets() = %q, want %q", got, want)
			}
		})
	}
}

func TestBroadcastDisconnectWithCloseReachesEveryNamespaceOfTheConnection(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("/chat"))
	c := dial(t, ts)
	c.connect("/", "")
	pm.socket(t)
	c.connect("/chat", "")
	chat := pc.socket(t)

	if err := server.Of("/chat").To().Disconnect(true); err != nil {
		t.Fatal(err)
	}
	c.expectSet("41", "41/chat,")
	c.expectClosed()
	if got := pm.reason(t); got != socketio.ReasonServerDisconnect {
		t.Errorf("main reason = %q, want %q", got, socketio.ReasonServerDisconnect)
	}
	if got := pc.reason(t); got != socketio.ReasonServerDisconnect {
		t.Errorf("chat reason = %q, want %q", got, socketio.ReasonServerDisconnect)
	}
	if chat.Connected() || len(server.Sockets()) != 0 {
		t.Error("a socket of the closed connection is still connected")
	}
}

func TestBroadcastBinary(t *testing.T) {
	t.Parallel()
	const placeholder0, placeholder1 = `{"_placeholder":true,"num":0}`, `{"_placeholder":true,"num":1}`
	cases := []struct {
		name       string
		send       func(ns *socketio.Namespace) error
		want       []frame
		recipients []string
	}{
		{
			"one attachment to everybody",
			func(ns *socketio.Namespace) error { return ns.Emit("file", []byte{1, 2}) },
			[]frame{text(`451-["file",` + placeholder0 + `]`), blob(1, 2)},
			[]string{"red", "both", "blue", "none"},
		},
		{
			"two attachments between other arguments",
			func(ns *socketio.Namespace) error { return ns.To("blue").Emit("mix", "a", []byte{1}, 5, []byte{2, 3}) },
			[]frame{text(`452-["mix","a",` + placeholder0 + `,5,` + placeholder1 + `]`), blob(1), blob(2, 3)},
			[]string{"both", "blue"},
		},
		{
			"an empty attachment",
			func(ns *socketio.Namespace) error { return ns.To("red").Emit("empty", []byte{}) },
			[]frame{text(`451-["empty",` + placeholder0 + `]`), blob()},
			[]string{"red", "both"},
		},
		{
			"no recipient",
			func(ns *socketio.Namespace) error { return ns.To("green").Emit("file", []byte{9}) },
			nil,
			nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := crowd(t, f)
			if err := tc.send(f.server.Namespace); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			expectDelivery(t, guests, tc.want, tc.recipients...)
		})
	}
}

func TestBinaryBroadcastInACustomNamespace(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	p := watch(server.Of("/chat"))
	c := dial(t, ts)
	c.connect("/chat", "")
	s := p.socket(t)
	if err := server.Of("/chat").Emit("file", []byte{7}); err != nil {
		t.Fatal(err)
	}
	c.delivered(s, text(`451-/chat,["file",{"_placeholder":true,"num":0}]`), blob(7))
}

func TestConcurrentBinaryEmitsStayPaired(t *testing.T) {
	t.Parallel()
	const senders, rounds = 6, 25
	cases := []struct {
		name string
		emit func(f *fixture, guests []guest, id int) error
	}{
		{"namespace broadcast", func(f *fixture, _ []guest, id int) error {
			return f.server.Emit("blob", id, []byte{byte(id)})
		}},
		{"socket emit", func(_ *fixture, guests []guest, id int) error {
			return guests[0].s.Emit("blob", id, []byte{byte(id)})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := []guest{f.seat(t, "one"), f.seat(t, "two"), f.seat(t, "three")}
			var wg sync.WaitGroup
			for sender := range senders {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for round := range rounds {
						if err := tc.emit(f, guests, sender*rounds+round); err != nil {
							t.Errorf("emit: %v", err)
						}
					}
				}()
			}
			wg.Wait()
			for i, g := range guests {
				if tc.name == "socket emit" && i > 0 {
					g.c.delivered(g.s)
					continue
				}
				seen := make(map[int]bool)
				for range senders * rounds {
					head := string(g.c.next().data)
					rest, ok := strings.CutPrefix(head, `451-["blob",`)
					number, _, found := strings.Cut(rest, ",")
					id, err := strconv.Atoi(number)
					if !ok || !found || err != nil {
						t.Fatalf("%s: header frame %q", g.name, head)
					}
					g.c.expectBinary([]byte{byte(id)})
					seen[id] = true
				}
				if len(seen) != senders*rounds {
					t.Errorf("%s received %d different messages, want %d", g.name, len(seen), senders*rounds)
				}
				g.c.barrier(g.s)
			}
		})
	}
}
