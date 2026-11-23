package socketio_test

import (
	"errors"
	"slices"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

var errBoom = errors.New("bus is down")

func TestClusterIgnoresNamespacesItDoesNotHave(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	a, b := nodes[0], nodes[1]
	guest := b.seat(t, "b", "red")
	only := a.server.Of("/only-a").To()
	for name, err := range map[string]error{
		"Emit":       only.Emit("news"),
		"Join":       only.Join("red"),
		"Leave":      only.Leave("red"),
		"Disconnect": only.Disconnect(true),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	a.expectPublished(t, 4)
	ghost, err := parser.NewEvent("/ghost", "news", nil)
	if err != nil {
		t.Fatal(err)
	}
	b.adapter.inject(socketio.Operation{Kind: socketio.OpEmit, Namespace: "/ghost", Packet: ghost})
	b.adapter.inject(socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/ghost", Close: true})

	guest.c.delivered(guest.s)
	if rooms := guest.s.Rooms(); !slices.Contains(rooms, "red") || !guest.s.Connected() {
		t.Errorf("an operation for another namespace reached the main namespace: connected %v, rooms %q", guest.s.Connected(), rooms)
	}
	for _, name := range []string{"/only-a", "/ghost"} {
		guest.c.send(pkt('0', name, ""))
		guest.c.expect(pkt('4', name, `{"message":"Invalid namespace"}`))
	}
	guest.c.barrier(guest.s)
}

func TestClusterIgnoresPacketsThatAreNotEvents(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		packet *parser.Packet
		want   []frame
	}{
		{"CONNECT", &parser.Packet{Type: parser.Connect, Namespace: "/", Data: []byte(`{"sid":"x"}`)}, nil},
		{"DISCONNECT", &parser.Packet{Type: parser.Disconnect, Namespace: "/"}, nil},
		{"ACK", &parser.Packet{Type: parser.Ack, Namespace: "/", ID: 1, HasID: true, Data: []byte(`["x"]`)}, nil},
		{"BINARY_ACK", &parser.Packet{Type: parser.BinaryAck, Namespace: "/", ID: 1, HasID: true, Data: []byte(`[{"_placeholder":true,"num":0}]`), Attachments: [][]byte{{1}}}, nil},
		{"CONNECT_ERROR", &parser.Packet{Type: parser.ConnectError, Namespace: "/", Data: []byte(`{"message":"x"}`)}, nil},
		{"unknown type", &parser.Packet{Type: parser.Type(42), Namespace: "/", Data: []byte(`["x"]`)}, nil},
		{"no packet", nil, nil},
		{"EVENT as a control", &parser.Packet{Type: parser.Event, Namespace: "/", Data: []byte(`["x",1]`)}, []frame{text(`42["x",1]`)}},
		{"BINARY_EVENT as a control", &parser.Packet{Type: parser.BinaryEvent, Namespace: "/", Data: []byte(`["x",{"_placeholder":true,"num":0}]`), Attachments: [][]byte{{1}}}, []frame{text(`451-["x",{"_placeholder":true,"num":0}]`), blob(1)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 1)
			guest := nodes[0].seat(t, "guest")
			nodes[0].adapter.inject(socketio.Operation{Kind: socketio.OpEmit, Namespace: "/", Packet: tc.packet})
			guest.c.delivered(guest.s, tc.want...)
			if !guest.s.Connected() {
				t.Error("the socket was disconnected")
			}
		})
	}
}

func TestClusterPublishFailureIsReturnedAfterTheLocalEffect(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		act   func(ns *socketio.Namespace) error
		check func(t *testing.T, guests []guest)
	}{
		{
			"Emit",
			func(ns *socketio.Namespace) error { return ns.To("red").Emit("news") },
			func(t *testing.T, guests []guest) {
				expectDelivery(t, guests, []frame{text(`42["news"]`)}, "a-red", "a-both")
			},
		},
		{
			"Join",
			func(ns *socketio.Namespace) error { return ns.To("red").Join("vip") },
			func(t *testing.T, guests []guest) {
				for _, g := range guests {
					if got, want := slices.Contains(g.s.Rooms(), "vip"), slices.Contains([]string{"a-red", "a-both"}, g.name); got != want {
						t.Errorf("%s in vip = %v, want %v", g.name, got, want)
					}
				}
			},
		},
		{
			"Leave",
			func(ns *socketio.Namespace) error { return ns.To("red").Leave("red") },
			func(t *testing.T, guests []guest) {
				for _, g := range guests {
					if got, want := slices.Contains(g.s.Rooms(), "red"), g.name == "b-red" || g.name == "b-both"; got != want {
						t.Errorf("%s in red = %v, want %v", g.name, got, want)
					}
				}
			},
		},
		{
			"Disconnect",
			func(ns *socketio.Namespace) error { return ns.To("red").Disconnect(false) },
			func(t *testing.T, guests []guest) {
				for _, g := range guests {
					if got, want := g.s.Connected(), g.name != "a-red" && g.name != "a-both"; got != want {
						t.Errorf("%s connected = %v, want %v", g.name, got, want)
					}
				}
				named(guests, "a-red").c.expect("41")
				named(guests, "a-both").c.expect("41")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 2)
			guests := populate(t, nodes, spread)
			nodes[0].adapter.failWith(errBoom)
			if err := tc.act(nodes[0].server.Namespace); !errors.Is(err, errBoom) {
				t.Errorf("error = %v, want the error of Publish", err)
			}
			nodes[0].expectPublished(t, 1)
			tc.check(t, guests)
			for _, name := range []string{"b-red", "b-blue", "b-both", "b-none"} {
				if g := named(guests, name); !g.s.Connected() {
					t.Errorf("%s was reached although Publish failed", name)
				}
			}
		})
	}

	t.Run("Local does not publish", func(t *testing.T) {
		t.Parallel()
		nodes := newCluster(t, 2)
		nodes[0].adapter.failWith(errBoom)
		if err := nodes[0].server.Local().Emit("news"); err != nil {
			t.Errorf("Local().Emit = %v, want nil", err)
		}
		nodes[0].expectPublished(t, 0)
	})
}

func TestClusterPublishesNothingForInvalidEmits(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	guest := nodes[1].seat(t, "b")
	ns := nodes[0].server.Namespace
	for name, err := range map[string]error{
		"reserved name": ns.Emit("connect"),
		"channel":       ns.To("x").Emit("news", make(chan int)),
		"function":      ns.Emit("news", func() {}),
	} {
		if err == nil {
			t.Errorf("%s: Emit succeeded", name)
		}
	}
	nodes[0].expectPublished(t, 0)
	guest.c.delivered(guest.s)
}

func TestAdapterIsSubscribedOnceByNew(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	for i, n := range nodes {
		if subscribed, closed := n.adapter.counts(); subscribed != 1 || closed != 0 {
			t.Errorf("server %d: Subscribe called %d times and Close %d times, want 1 and 0", i, subscribed, closed)
		}
	}
}

func TestServerCloseClosesTheAdapter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{{"adapter closes cleanly", nil}, {"adapter fails to close", errBoom}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 2)
			guest := nodes[0].seat(t, "guest")
			nodes[0].adapter.closeWith(tc.err)
			err := nodes[0].server.Close()
			if !errors.Is(err, tc.err) {
				t.Errorf("Close = %v, want %v", err, tc.err)
			}
			if _, closed := nodes[0].adapter.counts(); closed != 1 {
				t.Errorf("adapter closed %d times, want 1", closed)
			}
			if _, closed := nodes[1].adapter.counts(); closed != 0 {
				t.Errorf("the adapter of another server was closed")
			}
			if reason := nodes[0].probe.reason(t); reason != socketio.ReasonServerShutdown {
				t.Errorf("reason = %q, want %q", reason, socketio.ReasonServerShutdown)
			}
			guest.c.expectClosed()
		})
	}
}

func TestPublishedOperations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		act  func(server *socketio.Server, s *socketio.Socket) error
		want socketio.Operation
		data string
	}{
		{
			"Emit with rooms",
			func(server *socketio.Server, _ *socketio.Socket) error {
				return server.To("a", "b").Except("c").Emit("news", 1)
			},
			socketio.Operation{Kind: socketio.OpEmit, Namespace: "/", Rooms: []string{"a", "b"}, Except: []string{"c"}},
			`["news",1]`,
		},
		{
			"Emit to everybody",
			func(server *socketio.Server, _ *socketio.Socket) error { return server.Emit("news") },
			socketio.Operation{Kind: socketio.OpEmit, Namespace: "/"},
			`["news"]`,
		},
		{
			"Emit in a custom namespace",
			func(server *socketio.Server, _ *socketio.Socket) error {
				return server.Of("/chat").Emit("news")
			},
			socketio.Operation{Kind: socketio.OpEmit, Namespace: "/chat"},
			`["news"]`,
		},
		{
			"Emit from a socket",
			func(_ *socketio.Server, s *socketio.Socket) error { return s.To("r").Emit("news") },
			socketio.Operation{Kind: socketio.OpEmit, Namespace: "/", Rooms: []string{"r"}},
			`["news"]`,
		},
		{
			"Join",
			func(server *socketio.Server, _ *socketio.Socket) error { return server.To("a").Join("x", "y") },
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"a"}, Targets: []string{"x", "y"}},
			"",
		},
		{
			"Leave",
			func(server *socketio.Server, _ *socketio.Socket) error { return server.Except("a").Leave("x") },
			socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Except: []string{"a"}, Targets: []string{"x"}},
			"",
		},
		{
			"Disconnect keeping the connection",
			func(server *socketio.Server, _ *socketio.Socket) error { return server.To("a").Disconnect(false) },
			socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{"a"}},
			"",
		},
		{
			"Disconnect closing the connection",
			func(server *socketio.Server, _ *socketio.Socket) error { return server.To("a").Disconnect(true) },
			socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{"a"}, Close: true},
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nodes := newCluster(t, 2)
			guest := nodes[0].seat(t, "guest")
			if err := tc.act(nodes[0].server, guest.s); err != nil {
				t.Fatal(err)
			}
			published := nodes[0].adapter.published()
			if len(published) != 1 {
				t.Fatalf("%d operations published, want 1", len(published))
			}
			got := published[0]
			packet := got.Packet
			got.Packet = nil
			if tc.name == "Emit from a socket" {
				tc.want.Except = []string{guest.s.ID()}
			}
			if got.Kind != tc.want.Kind || got.Namespace != tc.want.Namespace || got.Close != tc.want.Close ||
				!slices.Equal(got.Rooms, tc.want.Rooms) || !slices.Equal(got.Except, tc.want.Except) || !slices.Equal(got.Targets, tc.want.Targets) {
				t.Errorf("operation = %+v, want %+v", got, tc.want)
			}
			if tc.data == "" {
				if packet != nil {
					t.Errorf("a %s operation carries the packet %+v", got.Kind, packet)
				}
				return
			}
			if packet == nil || packet.Type != parser.Event || packet.Namespace != tc.want.Namespace || string(packet.Data) != tc.data {
				t.Errorf("packet = %+v, want an EVENT for %s with data %s", packet, tc.want.Namespace, tc.data)
			}
		})
	}
}

func TestPublishedBinaryOperationKeepsTheAttachments(t *testing.T) {
	t.Parallel()
	nodes := newCluster(t, 2)
	if err := nodes[0].server.Emit("file", []byte{1, 2}, []byte{3}); err != nil {
		t.Fatal(err)
	}
	published := nodes[0].adapter.published()
	if len(published) != 1 || published[0].Packet == nil {
		t.Fatalf("published = %+v, want one emit with a packet", published)
	}
	packet := published[0].Packet
	if packet.Type != parser.BinaryEvent || len(packet.Attachments) != 2 || !slices.Equal(packet.Attachments[0], []byte{1, 2}) || !slices.Equal(packet.Attachments[1], []byte{3}) {
		t.Errorf("packet = %+v, want a BINARY_EVENT with the attachments [1 2] and [3]", packet)
	}
}
