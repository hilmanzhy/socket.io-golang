package socketio_test

import (
	"slices"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

type placement struct {
	name  string
	node  int
	rooms []string
}

// populate seats one guest per placement and returns them in order.
func populate(t *testing.T, nodes []*node, placements []placement) []guest {
	t.Helper()
	guests := make([]guest, len(placements))
	for i, p := range placements {
		guests[i] = nodes[p.node].seat(t, p.name, p.rooms...)
	}
	return guests
}

var spread = []placement{
	{"a-red", 0, []string{"red"}},
	{"a-both", 0, []string{"red", "blue"}},
	{"a-none", 0, nil},
	{"b-red", 1, []string{"red"}},
	{"b-blue", 1, []string{"blue"}},
	{"b-both", 1, []string{"red", "blue"}},
	{"b-none", 1, nil},
}

func TestClusterEmitReachesEveryServerOnce(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		emitter int
	}{{"from the first server", 0}, {"from the middle server", 1}, {"from the last server", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 3)
			guests := populate(t, nodes, []placement{{"one", 0, nil}, {"two", 1, nil}, {"three", 2, nil}, {"another", 2, nil}})
			if err := nodes[tc.emitter].server.Emit("news", 1); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			expectDelivery(t, guests, []frame{text(`42["news",1]`)}, "one", "two", "three", "another")
			nodes[tc.emitter].expectPublished(t, 1)
		})
	}
}

func TestClusterHonoursTargetsOnTheOtherServer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		send       func(ns *socketio.Namespace) error
		recipients []string
	}{
		{"one room", func(ns *socketio.Namespace) error { return ns.To("red").Emit("news") }, []string{"a-red", "a-both", "b-red", "b-both"}},
		{"another room", func(ns *socketio.Namespace) error { return ns.To("blue").Emit("news") }, []string{"a-both", "b-blue", "b-both"}},
		{"several rooms", func(ns *socketio.Namespace) error { return ns.To("red", "blue").Emit("news") }, []string{"a-red", "a-both", "b-red", "b-blue", "b-both"}},
		{"Except", func(ns *socketio.Namespace) error { return ns.Except("red").Emit("news") }, []string{"a-none", "b-blue", "b-none"}},
		{"To and Except", func(ns *socketio.Namespace) error { return ns.To("blue").Except("red").Emit("news") }, []string{"b-blue"}},
		{"a room nobody joined", func(ns *socketio.Namespace) error { return ns.To("green").Emit("news") }, nil},
		{"everybody", func(ns *socketio.Namespace) error { return ns.To().Emit("news") }, []string{"a-red", "a-both", "a-none", "b-red", "b-blue", "b-both", "b-none"}},
	}
	for _, tc := range cases {
		for _, emitter := range []int{0, 1} {
			t.Run(tc.name+" from server "+string(rune('A'+emitter)), func(t *testing.T) {
				t.Parallel()
				nodes := newCluster(t, 2)
				guests := populate(t, nodes, spread)
				if err := tc.send(nodes[emitter].server.Namespace); err != nil {
					t.Fatalf("Emit: %v", err)
				}
				expectDelivery(t, guests, []frame{text(`42["news"]`)}, tc.recipients...)
			})
		}
	}
}

func TestClusterReachesASocketOnAnotherServerByItsID(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	guests := populate(t, nodes, spread)
	target := named(guests, "b-none")
	if err := nodes[0].server.To(target.s.ID()).Emit("direct"); err != nil {
		t.Fatal(err)
	}
	expectDelivery(t, guests, []frame{text(`42["direct"]`)}, "b-none")
}

func TestClusterBroadcastFromASocketSkipsOnlyTheSender(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		sender     string
		send       func(s *socketio.Socket) error
		recipients []string
	}{
		{"Broadcast", "a-both", func(s *socketio.Socket) error { return s.Broadcast().Emit("news") }, []string{"a-red", "a-none", "b-red", "b-blue", "b-both", "b-none"}},
		{"To a room the sender is in", "a-both", func(s *socketio.Socket) error { return s.To("red").Emit("news") }, []string{"a-red", "b-red", "b-both"}},
		{"To a room the sender is not in", "b-none", func(s *socketio.Socket) error { return s.To("blue").Emit("news") }, []string{"a-both", "b-blue", "b-both"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 2)
			guests := populate(t, nodes, spread)
			if err := tc.send(named(guests, tc.sender).s); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			expectDelivery(t, guests, []frame{text(`42["news"]`)}, tc.recipients...)
		})
	}
}

func TestClusterLocalStaysOnTheServer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		send       func(ns *socketio.Namespace) error
		recipients []string
	}{
		{"Local", func(ns *socketio.Namespace) error { return ns.Local().Emit("news") }, []string{"a-red", "a-both", "a-none"}},
		{"Local then To", func(ns *socketio.Namespace) error { return ns.Local().To("blue").Emit("news") }, []string{"a-both"}},
		{"To then Local", func(ns *socketio.Namespace) error { return ns.To("red").Local().Emit("news") }, []string{"a-red", "a-both"}},
		{"To, Except and Local", func(ns *socketio.Namespace) error { return ns.To("red").Except("blue").Local().Emit("news") }, []string{"a-red"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 2)
			guests := populate(t, nodes, spread)
			if err := tc.send(nodes[0].server.Namespace); err != nil {
				t.Fatalf("Emit: %v", err)
			}
			expectDelivery(t, guests, []frame{text(`42["news"]`)}, tc.recipients...)
			nodes[0].expectPublished(t, 0)
		})
	}

	t.Run("Local leaves the operator it came from unchanged", func(t *testing.T) {
		t.Parallel()
		nodes := newCluster(t, 2)
		guests := populate(t, nodes, spread)
		base := nodes[0].server.To("red")
		if err := base.Local().Emit("first"); err != nil {
			t.Fatal(err)
		}
		expectDelivery(t, guests, []frame{text(`42["first"]`)}, "a-red", "a-both")
		if err := base.Emit("second"); err != nil {
			t.Fatal(err)
		}
		expectDelivery(t, guests, []frame{text(`42["second"]`)}, "a-red", "a-both", "b-red", "b-both")
		nodes[0].expectPublished(t, 1)
	})

	t.Run("rooms and disconnects", func(t *testing.T) {
		t.Parallel()
		nodes := newCluster(t, 2)
		guests := populate(t, nodes, spread)
		local := nodes[0].server.To("red").Local()
		hasRoom := func(name, room string) bool { return slices.Contains(named(guests, name).s.Rooms(), room) }

		if err := local.Join("vip"); err != nil {
			t.Fatal(err)
		}
		if !hasRoom("a-red", "vip") || !hasRoom("a-both", "vip") || hasRoom("b-red", "vip") || hasRoom("b-both", "vip") {
			t.Error("Join did not stay on the server")
		}
		if err := local.Leave("blue"); err != nil {
			t.Fatal(err)
		}
		if hasRoom("a-both", "blue") || !hasRoom("b-both", "blue") {
			t.Error("Leave did not stay on the server")
		}
		if err := local.Disconnect(false); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a-red", "a-both"} {
			named(guests, name).c.expect("41")
		}
		for _, name := range []string{"b-red", "b-both"} {
			if !named(guests, name).s.Connected() {
				t.Errorf("%s was disconnected by an operation that stayed local", name)
			}
		}
		nodes[0].expectPublished(t, 0)
	})
}

func TestClusterRoomOperations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		act  func(ns *socketio.Namespace) error
		room string
		want []string
	}{
		{"Join", func(ns *socketio.Namespace) error { return ns.To("red").Join("vip") }, "vip", []string{"a-red", "a-both", "b-red", "b-both"}},
		{"Join with Except", func(ns *socketio.Namespace) error { return ns.To("red").Except("blue").Join("vip") }, "vip", []string{"a-red", "b-red"}},
		{"Join everybody", func(ns *socketio.Namespace) error { return ns.To().Join("all") }, "all", []string{"a-red", "a-both", "a-none", "b-red", "b-blue", "b-both", "b-none"}},
		{"Leave", func(ns *socketio.Namespace) error { return ns.To("red").Leave("red") }, "red", nil},
		{"Leave with Except", func(ns *socketio.Namespace) error { return ns.To("red").Except("blue").Leave("red") }, "red", []string{"a-both", "b-both"}},
	}
	for _, tc := range cases {
		for _, actor := range []int{0, 1} {
			t.Run(tc.name+" from server "+string(rune('A'+actor)), func(t *testing.T) {
				t.Parallel()
				nodes := newCluster(t, 2)
				guests := populate(t, nodes, spread)
				if err := tc.act(nodes[actor].server.Namespace); err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, g := range guests {
					if slices.Contains(g.s.Rooms(), tc.room) {
						got = append(got, g.name)
					}
				}
				if !slices.Equal(sorted(got...), sorted(tc.want...)) {
					t.Errorf("members of %q = %q, want %q", tc.room, got, tc.want)
				}
			})
		}
	}
}

func TestClusterDisconnect(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		close bool
	}{{"keeping the connections", false}, {"closing the connections", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 2)
			guests := populate(t, nodes, spread)
			if err := nodes[0].server.To("blue").Disconnect(tc.close); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"a-both", "b-blue", "b-both"} {
				g := named(guests, name)
				g.c.expect("41")
				if tc.close {
					g.c.expectClosed()
				} else {
					g.c.expectSilence()
				}
				if g.s.Connected() {
					t.Errorf("%s is still connected", name)
				}
			}
			for _, name := range []string{"a-red", "a-none", "b-red", "b-none"} {
				g := named(guests, name)
				g.c.delivered(g.s)
			}
			for i, count := range []int{1, 2} {
				for range count {
					if reason := nodes[i].probe.reason(t); reason != socketio.ReasonServerDisconnect {
						t.Errorf("reason = %q, want %q", reason, socketio.ReasonServerDisconnect)
					}
				}
				nodes[i].probe.noReason(t)
			}
		})
	}
}

func TestClusterBinary(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	guests := populate(t, nodes, spread)
	payload := []byte{0, 255, 10, 13, 128}
	if err := nodes[0].server.To("blue").Emit("file", "x", payload, []byte{}); err != nil {
		t.Fatal(err)
	}
	expectDelivery(t, guests, []frame{
		text(`452-["file","x",{"_placeholder":true,"num":0},{"_placeholder":true,"num":1}]`),
		blob(payload...),
		blob(),
	}, "a-both", "b-blue", "b-both")
}

func TestClusterKeepsNamespacesApart(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	chats := make([]*probe, len(nodes))
	for i, n := range nodes {
		chats[i] = watch(n.server.Of("/chat"))
	}
	local := dial(t, nodes[0].ts)
	local.connect("/chat", "")
	localChat := chats[0].socket(t)
	remote := dial(t, nodes[1].ts)
	remote.connect("/", "")
	remoteMain := nodes[1].probe.socket(t)
	remote.connect("/chat", "")
	remoteChat := chats[1].socket(t)

	if err := nodes[0].server.Of("/chat").Emit("news"); err != nil {
		t.Fatal(err)
	}
	local.delivered(localChat, text(`42/chat,["news"]`))
	remote.expect(`42/chat,["news"]`)
	remote.barrier(remoteMain)
	remote.barrier(remoteChat)

	if err := nodes[0].server.Emit("main"); err != nil {
		t.Fatal(err)
	}
	local.delivered(localChat)
	remote.expect(`42["main"]`)
	remote.barrier(remoteMain)
	remote.barrier(remoteChat)
}
