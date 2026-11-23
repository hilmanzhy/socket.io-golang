package socketio_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

const self = "@self"

func resolve(rooms []string, id string) []string {
	out := slices.Clone(rooms)
	for i, room := range out {
		if room == self {
			out[i] = id
		}
	}
	return out
}

func besidesOwn(s *socketio.Socket) []string {
	return slices.DeleteFunc(s.Rooms(), func(room string) bool { return room == s.ID() })
}

func newNodes(t *testing.T, opts ...socketio.Options) []*node {
	t.Helper()
	shared := &bus{}
	nodes := make([]*node, len(opts))
	for i, o := range opts {
		adapter := shared.adapter()
		o.Adapter = adapter
		nodes[i] = &node{fixture: newFixture(t, o), adapter: adapter}
	}
	return nodes
}

func logEntries(t *testing.T, logs *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		out = append(out, entry)
	}
	return out
}

func warnings(t *testing.T, logs *syncBuffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, entry := range logEntries(t, logs) {
		if entry["level"] == slog.LevelWarn.String() {
			out = append(out, entry)
		}
	}
	return out
}

func TestSocketJoinRoomLimit(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		limit int
		first []string
		join  []string
		want  []string
		fails bool
	}{
		{"no limit", 0, nil, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, false},
		{"up to the limit", 3, nil, []string{"a", "b", "c"}, []string{"a", "b", "c"}, false},
		{"one room over the limit", 3, nil, []string{"a", "b", "c", "d"}, nil, true},
		{"over the limit in a later call", 2, []string{"a", "b"}, []string{"c"}, []string{"a", "b"}, true},
		{"a later call that crosses the limit", 2, []string{"a"}, []string{"b", "c"}, []string{"a"}, true},
		{"its own room does not count", 1, nil, []string{self, "a"}, []string{"a"}, false},
		{"its own room is already joined", 1, []string{"a"}, []string{self}, []string{"a"}, false},
		{"a room it is already in", 1, []string{"a"}, []string{"a"}, []string{"a"}, false},
		{"every room already joined at the limit", 2, []string{"a", "b"}, []string{"b", "a"}, []string{"a", "b"}, false},
		{"a joined room next to a new one over the limit", 2, []string{"a", "b"}, []string{"a", "c"}, []string{"a", "b"}, true},
		{"the same room twice in one call", 1, nil, []string{"a", "a"}, []string{"a"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{MaxRooms: tc.limit})
			_, s := f.join(t)
			if err := s.Join(resolve(tc.first, s.ID())...); err != nil {
				t.Fatalf("setup Join(%q) = %v", tc.first, err)
			}
			err := s.Join(resolve(tc.join, s.ID())...)
			if got := errors.Is(err, socketio.ErrRoomLimit); got != tc.fails {
				t.Errorf("Join(%q) = %v, want ErrRoomLimit %v", tc.join, err, tc.fails)
			}
			if !tc.fails && err != nil {
				t.Errorf("Join(%q) = %v, want nil", tc.join, err)
			}
			if got := besidesOwn(s); !slices.Equal(got, tc.want) {
				t.Errorf("rooms besides its own = %q, want %q", got, tc.want)
			}
			if !slices.Contains(s.Rooms(), s.ID()) {
				t.Errorf("rooms = %q, the socket left its own room", s.Rooms())
			}
		})
	}
}

func TestRefusedSocketJoinDoesNotSubscribeTheSocket(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{MaxRooms: 1})
	c, s := f.join(t)
	if err := s.Join("a"); err != nil {
		t.Fatalf("Join(a) = %v", err)
	}
	if err := s.Join("b", "c"); !errors.Is(err, socketio.ErrRoomLimit) {
		t.Fatalf("Join(b, c) = %v, want ErrRoomLimit", err)
	}
	if err := f.server.To("b", "c").Emit("news"); err != nil {
		t.Fatal(err)
	}
	c.delivered(s)
	if err := f.server.To("a").Emit("news"); err != nil {
		t.Fatal(err)
	}
	c.delivered(s, text(`42["news"]`))
}

func TestRoomLimitAppliesToEveryNamespace(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{MaxRooms: 1})
	chat := watch(f.server.Of("/chat"))
	c, main := f.join(t)
	c.connect("/chat", "")
	other := chat.socket(t)
	for _, s := range []*socketio.Socket{main, other} {
		if err := s.Join("a"); err != nil {
			t.Fatalf("Join(a) on %s = %v", s.Namespace().Name(), err)
		}
	}
	for _, s := range []*socketio.Socket{main, other} {
		if err := s.Join("b"); !errors.Is(err, socketio.ErrRoomLimit) {
			t.Errorf("Join(b) on %s = %v, want ErrRoomLimit", s.Namespace().Name(), err)
		}
		if got := besidesOwn(s); !slices.Equal(got, []string{"a"}) {
			t.Errorf("rooms of %s besides its own = %q, want [a]", s.Namespace().Name(), got)
		}
	}
}

func TestBroadcastJoinReportsTheRoomLimit(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		operator func(*socketio.Server) socketio.BroadcastOperator
		joined   []string
		refused  bool
	}{
		{"Server.To", func(s *socketio.Server) socketio.BroadcastOperator { return s.To("x") }, []string{"free1", "free2"}, true},
		{"Namespace.To", func(s *socketio.Server) socketio.BroadcastOperator { return s.Of("/").To("x") }, []string{"free1", "free2"}, true},
		{"Namespace.Local.To", func(s *socketio.Server) socketio.BroadcastOperator { return s.Of("/").Local().To("x") }, []string{"free1", "free2"}, true},
		{"To with a room nobody joined", func(s *socketio.Server) socketio.BroadcastOperator { return s.To("x", "nobody") }, []string{"free1", "free2"}, true},
		{"To everybody", func(s *socketio.Server) socketio.BroadcastOperator { return s.To() }, []string{"free1", "free2", "outside"}, true},
		{"Local everybody", func(s *socketio.Server) socketio.BroadcastOperator { return s.Local() }, []string{"free1", "free2", "outside"}, true},
		{"Except a room of the full socket", func(s *socketio.Server) socketio.BroadcastOperator { return s.Except("f1") }, []string{"free1", "free2", "outside"}, false},
		{"To and Except skipping the full socket", func(s *socketio.Server) socketio.BroadcastOperator { return s.To("x").Except("f1") }, []string{"free1", "free2"}, false},
		{"To a room the full socket is not in", func(s *socketio.Server) socketio.BroadcastOperator { return s.To("y") }, []string{"outside"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, socketio.Options{MaxRooms: 2})
			guests := []guest{
				f.seat(t, "free1", "x"),
				f.seat(t, "full", "x", "f1"),
				f.seat(t, "free2", "x"),
				f.seat(t, "outside", "y"),
			}
			err := tc.operator(f.server).Join("new")
			if got := errors.Is(err, socketio.ErrRoomLimit); got != tc.refused {
				t.Errorf("Join = %v, want ErrRoomLimit %v", err, tc.refused)
			}
			if !tc.refused && err != nil {
				t.Errorf("Join = %v, want nil", err)
			}
			for _, g := range guests {
				if got, want := slices.Contains(g.s.Rooms(), "new"), slices.Contains(tc.joined, g.name); got != want {
					t.Errorf("%s in room new = %v, want %v", g.name, got, want)
				}
			}
			if got := besidesOwn(named(guests, "full").s); !slices.Equal(got, []string{"f1", "x"}) {
				t.Errorf("rooms of the full socket = %q, want [f1 x]", got)
			}
		})
	}
}

func TestBroadcastJoinIsAllOrNothingPerSocket(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{MaxRooms: 3})
	guests := []guest{
		f.seat(t, "light", "x"),
		f.seat(t, "middle", "x", "m1"),
		f.seat(t, "heavy", "x", "h1", "h2"),
	}
	err := f.server.To("x").Join("n1", "n2")
	if !errors.Is(err, socketio.ErrRoomLimit) {
		t.Errorf("Join = %v, want ErrRoomLimit", err)
	}
	for name, want := range map[string][]string{
		"light":  {"n1", "n2", "x"},
		"middle": {"m1", "x"},
		"heavy":  {"h1", "h2", "x"},
	} {
		if got := besidesOwn(named(guests, name).s); !slices.Equal(got, want) {
			t.Errorf("rooms of %s besides its own = %q, want %q", name, got, want)
		}
	}
}

func TestBroadcastJoinWithoutAnySocketAtTheLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{MaxRooms: 4})
	guests := []guest{f.seat(t, "one", "x"), f.seat(t, "two", "x", "y"), f.seat(t, "three")}
	if err := f.server.To("x").Join("a", "b"); err != nil {
		t.Errorf("Join = %v, want nil", err)
	}
	for name, want := range map[string][]string{
		"one":   {"a", "b", "x"},
		"two":   {"a", "b", "x", "y"},
		"three": {},
	} {
		if got := besidesOwn(named(guests, name).s); !slices.Equal(got, want) {
			t.Errorf("rooms of %s besides its own = %q, want %q", name, got, want)
		}
	}
}

func TestBroadcastLeaveIsNotAffectedByTheRoomLimit(t *testing.T) {
	t.Parallel()
	f := newFixture(t, socketio.Options{MaxRooms: 1})
	g := f.seat(t, "full", "x")
	if err := f.server.To("x").Leave("x"); err != nil {
		t.Errorf("Leave = %v, want nil", err)
	}
	if got := besidesOwn(g.s); len(got) != 0 {
		t.Errorf("rooms besides its own = %q, want none", got)
	}
}

func TestBroadcastJoinStillReachesTheClusterWhenLocalSocketsRefuse(t *testing.T) {
	t.Parallel()
	nodes := newNodes(t, socketio.Options{MaxRooms: 1}, socketio.Options{MaxRooms: 1})
	full0 := nodes[0].seat(t, "full0", "a")
	free1 := nodes[1].seat(t, "free1")
	full1 := nodes[1].seat(t, "full1", "a")
	err := nodes[0].server.To().Join("n")
	if !errors.Is(err, socketio.ErrRoomLimit) {
		t.Errorf("Join = %v, want ErrRoomLimit", err)
	}
	nodes[0].expectPublished(t, 1)
	if got := besidesOwn(full0.s); !slices.Equal(got, []string{"a"}) {
		t.Errorf("rooms of full0 besides its own = %q, want [a]", got)
	}
	if got := besidesOwn(free1.s); !slices.Equal(got, []string{"n"}) {
		t.Errorf("rooms of free1 besides its own = %q, want [n]", got)
	}
	if got := besidesOwn(full1.s); !slices.Equal(got, []string{"a"}) {
		t.Errorf("rooms of full1 besides its own = %q, want [a]", got)
	}
}

func TestBroadcastJoinReportsBothTheRoomLimitAndAPublishFailure(t *testing.T) {
	t.Parallel()
	nodes := newNodes(t, socketio.Options{MaxRooms: 1}, socketio.Options{MaxRooms: 1})
	nodes[0].seat(t, "full", "a")
	down := errors.New("broker down")
	nodes[0].adapter.failWith(down)
	err := nodes[0].server.To().Join("n")
	if !errors.Is(err, socketio.ErrRoomLimit) {
		t.Errorf("Join = %v, want it to match ErrRoomLimit", err)
	}
	if !errors.Is(err, down) {
		t.Errorf("Join = %v, want it to match the publish error", err)
	}
}

func TestBroadcastJoinReportsOnlyAPublishFailureWhenNoSocketRefuses(t *testing.T) {
	t.Parallel()
	nodes := newNodes(t, socketio.Options{MaxRooms: 2}, socketio.Options{MaxRooms: 2})
	nodes[0].seat(t, "free", "a")
	down := errors.New("broker down")
	nodes[0].adapter.failWith(down)
	err := nodes[0].server.To().Join("n")
	if !errors.Is(err, down) || errors.Is(err, socketio.ErrRoomLimit) {
		t.Errorf("Join = %v, want only the publish error", err)
	}
}

func TestRemoteJoinOverTheRoomLimitIsLogged(t *testing.T) {
	t.Parallel()
	join := socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Targets: []string{"n"}}
	cases := []struct {
		name     string
		op       socketio.Operation
		full     int
		free     int
		warnings int
		joins    bool
	}{
		{"one socket over the limit", join, 1, 1, 1, true},
		{"every socket over the limit", join, 3, 0, 1, false},
		{"no socket over the limit", join, 0, 2, 0, true},
		{"a room nobody is in", socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"nobody"}, Targets: []string{"n"}}, 1, 1, 0, false},
		{"rooms every socket has", socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Targets: []string{"a"}}, 2, 1, 0, false},
		{"a leave", socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Targets: []string{"a"}}, 1, 1, 0, false},
		{"a namespace that does not exist", socketio.Operation{Kind: socketio.OpJoin, Namespace: "/missing", Targets: []string{"n"}}, 1, 1, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logs := &syncBuffer{}
			adapter := (&bus{}).adapter()
			f := newFixture(t, socketio.Options{Adapter: adapter, MaxRooms: 1, Logger: slog.New(slog.NewJSONHandler(logs, nil))})
			var free []guest
			for range tc.full {
				f.seat(t, "full", "a")
			}
			for range tc.free {
				free = append(free, f.seat(t, "free"))
			}
			adapter.inject(tc.op)
			got := warnings(t, logs)
			if len(got) != tc.warnings {
				t.Fatalf("%d warnings logged, want %d: %s", len(got), tc.warnings, logs.String())
			}
			for _, entry := range got {
				if entry["kind"] != "join" || entry["namespace"] != "/" || entry["error"] != socketio.ErrRoomLimit.Error() {
					t.Errorf("warning = %v, want kind join, namespace / and error %q", entry, socketio.ErrRoomLimit.Error())
				}
			}
			for _, g := range free {
				if in := slices.Contains(g.s.Rooms(), "n"); in != tc.joins {
					t.Errorf("a socket under the limit is in room n = %v, want %v", in, tc.joins)
				}
			}
		})
	}
}

func TestRemoteJoinLimitWarningGoesToTheLoggerOfTheServerThatHitIt(t *testing.T) {
	t.Parallel()
	limited, open := &syncBuffer{}, &syncBuffer{}
	nodes := newNodes(t,
		socketio.Options{Logger: slog.New(slog.NewJSONHandler(open, nil))},
		socketio.Options{MaxRooms: 1, Logger: slog.New(slog.NewJSONHandler(limited, nil))},
	)
	nodes[0].seat(t, "a", "x", "y", "z")
	nodes[1].seat(t, "b", "x")
	if err := nodes[0].server.To().Join("n"); err != nil {
		t.Errorf("Join = %v, want nil: the limited socket is on the other server", err)
	}
	if got := warnings(t, limited); len(got) != 1 {
		t.Errorf("%d warnings on the limited server, want 1: %s", len(got), limited.String())
	}
	if got := warnings(t, open); len(got) != 0 {
		t.Errorf("%d warnings on the server without a limit, want 0: %s", len(got), open.String())
	}
}
