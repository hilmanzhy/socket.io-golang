package socketio_test

import (
	"slices"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

// crowd seats four guests: red is in room "red", both in "red" and "blue",
// blue in "blue", and none in no room but its own.
func crowd(t *testing.T, f *fixture) []guest {
	t.Helper()
	return []guest{
		f.seat(t, "red", "red"),
		f.seat(t, "both", "red", "blue"),
		f.seat(t, "blue", "blue"),
		f.seat(t, "none"),
	}
}

func named(guests []guest, name string) guest {
	for _, g := range guests {
		if g.name == name {
			return g
		}
	}
	panic("no guest named " + name)
}

func TestBroadcastTargets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		send       func(ns *socketio.Namespace) error
		recipients []string
	}{
		{"every socket of the namespace", func(ns *socketio.Namespace) error { return ns.Emit("news", 1) }, []string{"red", "both", "blue", "none"}},
		{"To without rooms", func(ns *socketio.Namespace) error { return ns.To().Emit("news", 1) }, []string{"red", "both", "blue", "none"}},
		{"To one room", func(ns *socketio.Namespace) error { return ns.To("red").Emit("news", 1) }, []string{"red", "both"}},
		{"To several rooms reaches a member of both once", func(ns *socketio.Namespace) error { return ns.To("red", "blue").Emit("news", 1) }, []string{"red", "both", "blue"}},
		{"To chained", func(ns *socketio.Namespace) error { return ns.To("red").To("blue").Emit("news", 1) }, []string{"red", "both", "blue"}},
		{"To a room nobody joined", func(ns *socketio.Namespace) error { return ns.To("green").Emit("news", 1) }, nil},
		{"Except one room", func(ns *socketio.Namespace) error { return ns.Except("red").Emit("news", 1) }, []string{"blue", "none"}},
		{"Except several rooms", func(ns *socketio.Namespace) error { return ns.Except("red", "blue").Emit("news", 1) }, []string{"none"}},
		{"Except chained", func(ns *socketio.Namespace) error { return ns.Except("red").Except("blue").Emit("news", 1) }, []string{"none"}},
		{"Except a room nobody joined", func(ns *socketio.Namespace) error { return ns.Except("green").Emit("news", 1) }, []string{"red", "both", "blue", "none"}},
		{"To then Except", func(ns *socketio.Namespace) error { return ns.To("blue").Except("red").Emit("news", 1) }, []string{"blue"}},
		{"Except then To", func(ns *socketio.Namespace) error { return ns.Except("red").To("blue").Emit("news", 1) }, []string{"blue"}},
		{"To and Except the same room", func(ns *socketio.Namespace) error { return ns.To("red").Except("red").Emit("news", 1) }, nil},
		{"Local without an adapter", func(ns *socketio.Namespace) error { return ns.Local().Emit("news", 1) }, []string{"red", "both", "blue", "none"}},
		{"Local with rooms", func(ns *socketio.Namespace) error { return ns.Local().To("blue").Emit("news", 1) }, []string{"both", "blue"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := crowd(t, f)
			if err := tc.send(f.server.Namespace); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			expectDelivery(t, guests, []frame{text(`42["news",1]`)}, tc.recipients...)
		})
	}
}

func TestBroadcastFromASocketSkipsTheSender(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		sender     string
		send       func(s *socketio.Socket) error
		recipients []string
	}{
		{"Broadcast", "both", func(s *socketio.Socket) error { return s.Broadcast().Emit("news", 1) }, []string{"red", "blue", "none"}},
		{"To a room the sender is in", "both", func(s *socketio.Socket) error { return s.To("red").Emit("news", 1) }, []string{"red"}},
		{"To a room the sender is not in", "none", func(s *socketio.Socket) error { return s.To("red").Emit("news", 1) }, []string{"red", "both"}},
		{"To several rooms", "both", func(s *socketio.Socket) error { return s.To("red", "blue").Emit("news", 1) }, []string{"red", "blue"}},
		{"To then Except", "none", func(s *socketio.Socket) error { return s.To("red", "blue").Except("blue").Emit("news", 1) }, []string{"red"}},
		{"Broadcast then Except", "none", func(s *socketio.Socket) error { return s.Broadcast().Except("red").Emit("news", 1) }, []string{"blue"}},
		{"Broadcast then To", "red", func(s *socketio.Socket) error { return s.Broadcast().To("red").Emit("news", 1) }, []string{"both"}},
		{"the sender is the only socket in the room", "blue", func(s *socketio.Socket) error { return s.To(s.ID()).Emit("news", 1) }, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			guests := crowd(t, f)
			if err := tc.send(named(guests, tc.sender).s); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			expectDelivery(t, guests, []frame{text(`42["news",1]`)}, tc.recipients...)
		})
	}
}

func TestBroadcastOperatorsAreImmutable(t *testing.T) {
	t.Parallel()
	event := []frame{text(`42["news"]`)}

	t.Run("To and Except return new operators", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, socketio.Options{})
		guests := crowd(t, f)
		base := f.server.To("red")
		wider, narrower := base.To("blue"), base.Except("blue")
		for _, step := range []struct {
			name       string
			operator   socketio.BroadcastOperator
			recipients []string
		}{
			{"wider", wider, []string{"red", "both", "blue"}},
			{"narrower", narrower, []string{"red"}},
			{"base", base, []string{"red", "both"}},
		} {
			if err := step.operator.Emit("news"); err != nil {
				t.Fatalf("%s: %v", step.name, err)
			}
			expectDelivery(t, guests, event, step.recipients...)
		}
	})

	t.Run("To on a slice with spare capacity", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, socketio.Options{})
		guests := crowd(t, f)
		rooms := make([]string, 1, 8)
		rooms[0] = "red"
		base := f.server.To(rooms...)
		first, second := base.To("blue"), base.To("green")
		for _, step := range []struct {
			name       string
			operator   socketio.BroadcastOperator
			recipients []string
		}{
			{"first", first, []string{"red", "both", "blue"}},
			{"second", second, []string{"red", "both"}},
			{"base", base, []string{"red", "both"}},
		} {
			if err := step.operator.Emit("news"); err != nil {
				t.Fatalf("%s: %v", step.name, err)
			}
			expectDelivery(t, guests, event, step.recipients...)
		}
	})

	t.Run("Except on a slice with spare capacity", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t, socketio.Options{})
		guests := crowd(t, f)
		rooms := make([]string, 1, 8)
		rooms[0] = "red"
		base := f.server.Except(rooms...)
		first, second := base.Except("blue"), base.Except("green")
		for _, step := range []struct {
			name       string
			operator   socketio.BroadcastOperator
			recipients []string
		}{
			{"first", first, []string{"none"}},
			{"second", second, []string{"blue", "none"}},
			{"base", base, []string{"blue", "none"}},
		} {
			if err := step.operator.Emit("news"); err != nil {
				t.Fatalf("%s: %v", step.name, err)
			}
			expectDelivery(t, guests, event, step.recipients...)
		}
	})
}

func TestBroadcastStaysInItsNamespace(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pc := watch(server.Namespace), watch(server.Of("/chat"))
	both, onlyChat := dial(t, ts), dial(t, ts)
	both.connect("/", "")
	bothMain := pm.socket(t)
	both.connect("/chat", "")
	bothChat := pc.socket(t)
	onlyChat.connect("/chat", "")
	onlyChatSocket := pc.socket(t)

	cases := []struct {
		name string
		send func() error
		both []string
		only []string
	}{
		{"server Emit", func() error { return server.Emit("a") }, []string{`42["a"]`}, nil},
		{"main namespace by its name", func() error { return server.Of("/").Emit("a") }, []string{`42["a"]`}, nil},
		{"custom namespace", func() error { return server.Of("/chat").Emit("b") }, []string{`42/chat,["b"]`}, []string{`42/chat,["b"]`}},
		{"custom namespace named without a slash", func() error { return server.Of("chat").Emit("b") }, []string{`42/chat,["b"]`}, []string{`42/chat,["b"]`}},
		{"custom namespace with a room", func() error { return server.Of("/chat").To(onlyChatSocket.ID()).Emit("b") }, nil, []string{`42/chat,["b"]`}},
		{"namespace nobody joined", func() error { return server.Of("/empty").Emit("c") }, nil, nil},
	}
	for _, tc := range cases {
		if err := tc.send(); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		for _, want := range tc.both {
			both.expect(want)
		}
		for _, want := range tc.only {
			onlyChat.expect(want)
		}
		both.barrier(bothMain)
		both.barrier(bothChat)
		onlyChat.barrier(onlyChatSocket)
	}
}

func TestBroadcastSkipsSocketsThatDisconnected(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pa := watch(server.Namespace), watch(server.Of("/aux"))
	left, stays := dial(t, ts), dial(t, ts)
	left.connect("/", "")
	pm.socket(t)
	left.connect("/aux", "")
	aux := pa.socket(t)
	stays.connect("/", "")
	stayed := pm.socket(t)

	left.send("41")
	pm.reason(t)
	if err := server.Emit("news"); err != nil {
		t.Fatal(err)
	}
	stays.delivered(stayed, text(`42["news"]`))
	left.delivered(aux)
	if got := ids(server.Sockets()); !slices.Equal(got, []string{stayed.ID()}) {
		t.Errorf("Sockets() = %q, want only the connected socket %q", got, stayed.ID())
	}
}

func TestBroadcastToNobodyIsNotAnError(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	for name, operator := range map[string]socketio.BroadcastOperator{
		"namespace without sockets": f.server.To(),
		"empty room":                f.server.To("empty"),
		"Except without rooms":      f.server.Except(),
		"local":                     f.server.Local(),
	} {
		if err := operator.Emit("news"); err != nil {
			t.Errorf("%s: Emit = %v", name, err)
		}
		if err := operator.Join("room"); err != nil {
			t.Errorf("%s: Join = %v", name, err)
		}
		if err := operator.Leave("room"); err != nil {
			t.Errorf("%s: Leave = %v", name, err)
		}
		if err := operator.Disconnect(true); err != nil {
			t.Errorf("%s: Disconnect = %v", name, err)
		}
		if got := operator.Sockets(); len(got) != 0 {
			t.Errorf("%s: %d sockets", name, len(got))
		}
	}
}
