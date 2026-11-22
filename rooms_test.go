package socketio_test

import (
	"slices"
	"testing"

	socketio "github.com/nuriontech/socket.io-golang"
)

func TestSocketStartsInItsOwnRoom(t *testing.T) {
	t.Parallel()
	inHandler := make(chan []string, 2)
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) { inHandler <- s.Rooms() })
	a := f.seat(t, "a")
	b := f.seat(t, "b")

	if got, want := receive(t, inHandler), []string{a.s.ID()}; !slices.Equal(got, want) {
		t.Errorf("Rooms() in the connect handler = %q, want %q", got, want)
	}
	if got, want := a.s.Rooms(), []string{a.s.ID()}; !slices.Equal(got, want) {
		t.Errorf("Rooms() = %q, want %q", got, want)
	}
	if a.s.ID() == b.s.ID() {
		t.Fatalf("two sockets share the id %q", a.s.ID())
	}
	if got := ids(f.server.To(a.s.ID()).Sockets()); !slices.Equal(got, []string{a.s.ID()}) {
		t.Errorf("sockets in the room named after the id = %q, want only that socket", got)
	}
	if err := f.server.To(a.s.ID()).Emit("direct", 1); err != nil {
		t.Fatal(err)
	}
	expectDelivery(t, []guest{a, b}, []frame{text(`42["direct",1]`)}, "a")
}

func TestJoinAndLeave(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		act  func(s *socketio.Socket)
		want []string
	}{
		{"join one room", func(s *socketio.Socket) { s.Join("a") }, []string{"a"}},
		{"join several rooms in one call", func(s *socketio.Socket) { s.Join("zeta", "alpha", "mid") }, []string{"alpha", "mid", "zeta"}},
		{"join in several calls", func(s *socketio.Socket) { s.Join("b"); s.Join("a") }, []string{"a", "b"}},
		{"join the same room twice", func(s *socketio.Socket) { s.Join("a"); s.Join("a") }, []string{"a"}},
		{"name a room twice in one call", func(s *socketio.Socket) { s.Join("a", "a") }, []string{"a"}},
		{"join no room", func(s *socketio.Socket) { s.Join() }, nil},
		{"leave one room", func(s *socketio.Socket) { s.Join("a", "b"); s.Leave("a") }, []string{"b"}},
		{"leave several rooms", func(s *socketio.Socket) { s.Join("a", "b", "c"); s.Leave("a", "c") }, []string{"b"}},
		{"leave a room never joined", func(s *socketio.Socket) { s.Join("a"); s.Leave("other") }, []string{"a"}},
		{"leave before joining", func(s *socketio.Socket) { s.Leave("a") }, nil},
		{"leave twice", func(s *socketio.Socket) { s.Join("a"); s.Leave("a"); s.Leave("a") }, nil},
		{"leave no room", func(s *socketio.Socket) { s.Join("a"); s.Leave() }, []string{"a"}},
		{"join again after leaving", func(s *socketio.Socket) { s.Join("a"); s.Leave("a"); s.Join("a") }, []string{"a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{})
			_, s := f.join(t)
			tc.act(s)
			got := s.Rooms()
			want := sorted(append([]string{s.ID()}, tc.want...)...)
			if !slices.Equal(got, want) {
				t.Errorf("Rooms() = %q, want %q", got, want)
			}
			if !slices.IsSorted(got) {
				t.Errorf("Rooms() = %q is not sorted", got)
			}
		})
	}
}

func TestLeaveStopsDelivery(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	stays := f.seat(t, "stays", "lobby")
	leaves := f.seat(t, "leaves", "lobby")
	guests := []guest{stays, leaves}
	event := []frame{text(`42["news"]`)}

	f.server.To("lobby").Emit("news")
	expectDelivery(t, guests, event, "stays", "leaves")
	leaves.s.Leave("lobby")
	f.server.To("lobby").Emit("news")
	expectDelivery(t, guests, event, "stays")
	if got := ids(f.server.To("lobby").Sockets()); !slices.Equal(got, []string{stays.s.ID()}) {
		t.Errorf("sockets in the room = %q, want %q", got, []string{stays.s.ID()})
	}
	leaves.s.Join("lobby")
	f.server.To("lobby").Emit("news")
	expectDelivery(t, guests, event, "stays", "leaves")
}

func TestJoinFromAHandler(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{}, func(s *socketio.Socket) {
		s.On("join", func(e *socketio.Event) {
			var room string
			e.Bind(&room)
			s.Join(room)
			e.Ack()
		})
		s.On("leave", func(e *socketio.Event) {
			var room string
			e.Bind(&room)
			s.Leave(room)
			e.Ack()
		})
	})
	member := f.seat(t, "member")
	other := f.seat(t, "other")
	guests := []guest{member, other}
	event := []frame{text(`42["news"]`)}

	member.c.send(`421["join","lobby"]`)
	member.c.expect(`431[]`)
	f.server.To("lobby").Emit("news")
	expectDelivery(t, guests, event, "member")
	member.c.send(`422["leave","lobby"]`)
	member.c.expect(`432[]`)
	f.server.To("lobby").Emit("news")
	expectDelivery(t, guests, event)
}

func TestRoomsBelongToTheirNamespace(t *testing.T) {
	t.Parallel()
	server, ts := newServer(t, socketio.Options{})
	pm, pa := watch(server.Namespace), watch(server.Of("/a"))
	c := dial(t, ts)
	c.connect("/", "")
	c.connect("/a", "")
	main, custom := pm.socket(t), pa.socket(t)
	main.Join("shared", "main-only")
	custom.Join("shared")

	if got, want := main.Rooms(), sorted(main.ID(), "shared", "main-only"); !slices.Equal(got, want) {
		t.Errorf("main Rooms() = %q, want %q", got, want)
	}
	if got, want := custom.Rooms(), sorted(custom.ID(), "shared"); !slices.Equal(got, want) {
		t.Errorf("custom Rooms() = %q, want %q", got, want)
	}
	cases := []struct {
		name string
		send func() error
		want string
	}{
		{"server room", func() error { return server.To("shared").Emit("hi") }, `42["hi"]`},
		{"custom namespace room", func() error { return server.Of("/a").To("shared").Emit("hi") }, `42/a,["hi"]`},
		{"room only in the main namespace", func() error { return server.Of("/a").To("main-only").Emit("hi") }, ""},
		{"custom namespace socket id", func() error { return server.To(custom.ID()).Emit("hi") }, ""},
	}
	for _, tc := range cases {
		if err := tc.send(); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if tc.want != "" {
			c.expect(tc.want)
		}
		c.barrier(main)
		c.barrier(custom)
	}
	main.Leave("shared")
	if got := custom.Rooms(); !slices.Contains(got, "shared") {
		t.Errorf("leaving a room in the main namespace changed the custom one: %q", got)
	}
}

func TestDisconnectClearsRooms(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	gone := f.seat(t, "gone", "lobby")
	stays := f.seat(t, "stays", "lobby")
	oldID := gone.s.ID()

	gone.c.send("41")
	f.probe.reason(t)
	if got := gone.s.Rooms(); len(got) != 0 {
		t.Errorf("Rooms() after the disconnect = %q, want none", got)
	}
	if got := ids(f.server.To("lobby").Sockets()); !slices.Equal(got, []string{stays.s.ID()}) {
		t.Errorf("sockets in lobby = %q, want %q", got, []string{stays.s.ID()})
	}
	if got := f.server.To(oldID).Sockets(); len(got) != 0 {
		t.Errorf("%d sockets in the room named after a disconnected socket", len(got))
	}
	if err := f.server.To("lobby").Emit("news"); err != nil {
		t.Fatal(err)
	}
	stays.c.delivered(stays.s, text(`42["news"]`))

	gone.c.connect("/", "")
	back := f.probe.socket(t)
	if got, want := back.Rooms(), []string{back.ID()}; back.ID() == oldID || !slices.Equal(got, want) {
		t.Errorf("rejoined socket %q has rooms %q, want only its own id", back.ID(), got)
	}
}

func TestClosingTheConnectionClearsRooms(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{})
	gone := f.seat(t, "gone", "lobby")
	stays := f.seat(t, "stays", "lobby")

	gone.c.close()
	f.probe.reason(t)
	eventually(t, "the room forgets the closed socket", func() bool { return len(f.server.To("lobby").Sockets()) == 1 })
	if got := gone.s.Rooms(); len(got) != 0 {
		t.Errorf("Rooms() after the connection closed = %q, want none", got)
	}
	f.server.To("lobby").Emit("news")
	stays.c.delivered(stays.s, text(`42["news"]`))
}
