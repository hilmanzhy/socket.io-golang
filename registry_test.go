package socketio

import (
	"fmt"
	"slices"
	"sync"
	"testing"
)

func newSocketIn(r *registry, id string, rooms ...string) *Socket {
	s := &Socket{id: id}
	r.add(s)
	r.join(id, rooms)
	return s
}

func socketIDs(sockets []*Socket) []string {
	out := make([]string, len(sockets))
	for i, s := range sockets {
		out[i] = s.id
	}
	slices.Sort(out)
	return out
}

func isEmpty(r *registry) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.sockets) == 0 && len(r.members) == 0 && len(r.joined) == 0
}

func roomCount(r *registry) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

func TestRegistryAddPutsTheSocketInItsOwnRoom(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	s := &Socket{id: "s1"}
	if r.has("s1") {
		t.Fatal("an empty registry has a socket")
	}
	r.add(s)
	if !r.has("s1") {
		t.Error("the socket is not connected after add")
	}
	if got := r.of("s1"); !slices.Equal(got, []string{"s1"}) {
		t.Errorf("rooms = %q, want only its own id", got)
	}
	if got := r.match([]string{"s1"}, nil); len(got) != 1 || got[0] != s {
		t.Errorf("match by its own id = %v, want the socket", socketIDs(got))
	}
}

func TestRegistryRemove(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		remove func(r *registry, s *Socket) bool
		want   bool
		gone   bool
	}{
		{"the connected socket", func(r *registry, s *Socket) bool { return r.remove(s) }, true, true},
		{"twice", func(r *registry, s *Socket) bool { r.remove(s); return r.remove(s) }, false, true},
		{"a socket that never connected", func(r *registry, _ *Socket) bool { return r.remove(&Socket{id: "ghost"}) }, false, false},
		{"another socket with the same id", func(r *registry, s *Socket) bool { return r.remove(&Socket{id: s.id}) }, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry()
			s := newSocketIn(r, "s1", "room")
			if got := tc.remove(r, s); got != tc.want {
				t.Errorf("remove = %v, want %v", got, tc.want)
			}
			if r.has("s1") == tc.gone {
				t.Errorf("has = %v after the removal, want %v", r.has("s1"), !tc.gone)
			}
			if !tc.gone {
				if got := r.of("s1"); !slices.Equal(got, []string{"room", "s1"}) {
					t.Errorf("rooms = %q, the failed removal changed them", got)
				}
			}
		})
	}
}

func TestRegistryRemoveLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	a := newSocketIn(r, "a", "shared", "only-a")
	b := newSocketIn(r, "b", "shared", "only-b")
	r.remove(a)
	if got := roomCount(r); got != 3 {
		t.Errorf("%d rooms after removing a, want 3 (b, shared, only-b)", got)
	}
	if got := r.match([]string{"only-a"}, nil); len(got) != 0 {
		t.Errorf("a room of the removed socket still has %v", socketIDs(got))
	}
	if got := socketIDs(r.match([]string{"shared"}, nil)); !slices.Equal(got, []string{"b"}) {
		t.Errorf("shared = %q, want [b]", got)
	}
	r.remove(b)
	if !isEmpty(r) {
		t.Errorf("registry not empty after every socket left: %d sockets, %d rooms, %d joined", len(r.sockets), len(r.members), len(r.joined))
	}
}

func TestRegistryJoin(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		id    string
		rooms []string
		want  []string
		count int
	}{
		{"one room", "s1", []string{"a"}, []string{"a", "s1"}, 2},
		{"several rooms", "s1", []string{"b", "a"}, []string{"a", "b", "s1"}, 3},
		{"the same room twice in one call", "s1", []string{"a", "a"}, []string{"a", "s1"}, 2},
		{"its own room again", "s1", []string{"s1"}, []string{"s1"}, 1},
		{"no rooms", "s1", nil, []string{"s1"}, 1},
		{"an unknown socket", "ghost", []string{"a"}, nil, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry()
			r.add(&Socket{id: "s1"})
			r.join(tc.id, tc.rooms)
			if got := r.of(tc.id); !slices.Equal(got, tc.want) {
				t.Errorf("rooms = %q, want %q", got, tc.want)
			}
			if got := roomCount(r); got != tc.count {
				t.Errorf("%d rooms exist, want %d", got, tc.count)
			}
			if r.has("ghost") {
				t.Error("join connected a socket")
			}
		})
	}
}

func TestRegistryJoinIsIdempotent(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	newSocketIn(r, "s1", "a")
	r.join("s1", []string{"a"})
	r.join("s1", []string{"a"})
	if got := r.match([]string{"a"}, nil); len(got) != 1 {
		t.Errorf("room a has %d members after joining three times, want 1", len(got))
	}
	r.leave("s1", []string{"a"})
	if got := r.match([]string{"a"}, nil); len(got) != 0 {
		t.Errorf("one leave after three joins left %d members", len(got))
	}
}

func TestRegistryLeave(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		id    string
		rooms []string
		want  []string
	}{
		{"a joined room", "s1", []string{"a"}, []string{"b", "s1"}},
		{"several rooms", "s1", []string{"a", "b"}, []string{"s1"}},
		{"a room it never joined", "s1", []string{"c"}, []string{"a", "b", "s1"}},
		{"a room that does not exist", "s1", []string{"nowhere"}, []string{"a", "b", "s1"}},
		{"a room somebody else joined", "s2", []string{"a"}, []string{"c", "s2"}},
		{"its own room", "s1", []string{"s1"}, []string{"a", "b"}},
		{"an unknown socket", "ghost", []string{"a"}, nil},
		{"no rooms", "s1", nil, []string{"a", "b", "s1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry()
			newSocketIn(r, "s1", "a", "b")
			newSocketIn(r, "s2", "c")
			r.leave(tc.id, tc.rooms)
			if got := r.of(tc.id); !slices.Equal(got, tc.want) {
				t.Errorf("rooms of %s = %q, want %q", tc.id, got, tc.want)
			}
			if tc.id != "ghost" && !r.has(tc.id) {
				t.Errorf("leave disconnected %s", tc.id)
			}
		})
	}
}

func TestRegistryLeaveDeletesEmptyRoomsOnly(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	newSocketIn(r, "s1", "shared", "alone")
	newSocketIn(r, "s2", "shared")
	before := roomCount(r)
	r.leave("s1", []string{"shared", "alone"})
	if got, want := roomCount(r), before-1; got != want {
		t.Errorf("%d rooms after leaving, want %d (only alone disappears)", got, want)
	}
	if got := socketIDs(r.match([]string{"shared"}, nil)); !slices.Equal(got, []string{"s2"}) {
		t.Errorf("shared = %q, want [s2]", got)
	}
}

func TestRegistryOf(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	newSocketIn(r, "s1", "zeta", "alpha", "mid", "Alpha")
	if got, want := r.of("s1"), []string{"Alpha", "alpha", "mid", "s1", "zeta"}; !slices.Equal(got, want) {
		t.Errorf("rooms = %q, want %q", got, want)
	}
	if got := r.of("ghost"); len(got) != 0 {
		t.Errorf("an unknown socket has rooms %q", got)
	}
	rooms := r.of("s1")
	rooms[0] = "changed"
	if got := r.of("s1"); got[0] != "Alpha" {
		t.Error("the returned slice shares memory with the registry")
	}
}

func TestRegistryMatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		rooms  []string
		except []string
		want   []string
	}{
		{"nothing given selects everybody", nil, nil, []string{"a", "b", "c", "d"}},
		{"empty slices select everybody", []string{}, []string{}, []string{"a", "b", "c", "d"}},
		{"one room", []string{"red"}, nil, []string{"a", "b"}},
		{"another room", []string{"blue"}, nil, []string{"b", "c"}},
		{"two rooms without duplicates", []string{"red", "blue"}, nil, []string{"a", "b", "c"}},
		{"the same room twice", []string{"red", "red"}, nil, []string{"a", "b"}},
		{"a room nobody joined", []string{"green"}, nil, nil},
		{"an unknown room next to a known one", []string{"green", "blue"}, nil, []string{"b", "c"}},
		{"a socket id", []string{"d"}, nil, []string{"d"}},
		{"a socket id and a room", []string{"d", "red"}, nil, []string{"a", "b", "d"}},
		{"everybody except a room", nil, []string{"red"}, []string{"c", "d"}},
		{"everybody except two rooms", nil, []string{"red", "blue"}, []string{"d"}},
		{"everybody except an unknown room", nil, []string{"green"}, []string{"a", "b", "c", "d"}},
		{"everybody except a socket id", nil, []string{"a"}, []string{"b", "c", "d"}},
		{"a room except another", []string{"red"}, []string{"blue"}, []string{"a"}},
		{"two rooms except one", []string{"red", "blue"}, []string{"red"}, []string{"c"}},
		{"a room except itself", []string{"red"}, []string{"red"}, nil},
		{"two rooms except both", []string{"red", "blue"}, []string{"red", "blue"}, nil},
		{"a room except a socket id inside it", []string{"red"}, []string{"b"}, []string{"a"}},
		{"a room except a room nobody joined", []string{"red"}, []string{"green"}, []string{"a", "b"}},
		{"an unknown room except a known one", []string{"green"}, []string{"red"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry()
			newSocketIn(r, "a", "red")
			newSocketIn(r, "b", "red", "blue")
			newSocketIn(r, "c", "blue")
			newSocketIn(r, "d")
			if got := socketIDs(r.match(tc.rooms, tc.except)); !slices.Equal(got, tc.want) {
				t.Errorf("match(%q, %q) = %q, want %q", tc.rooms, tc.except, got, tc.want)
			}
		})
	}
}

func TestRegistryMatchOnAnEmptyRegistry(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	for _, rooms := range [][]string{nil, {"a"}, {"a", "b"}} {
		if got := r.match(rooms, []string{"c"}); len(got) != 0 {
			t.Errorf("match(%q) = %v on an empty registry", rooms, socketIDs(got))
		}
	}
}

func TestRegistryMatchDoesNotChangeTheRegistry(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	newSocketIn(r, "a", "red")
	newSocketIn(r, "b", "red", "blue")
	for _, call := range []struct{ rooms, except []string }{
		{nil, nil},
		{[]string{"red"}, nil},
		{[]string{"red", "blue"}, []string{"blue"}},
		{nil, []string{"red"}},
	} {
		got := r.match(call.rooms, call.except)
		for i := range got {
			got[i] = nil
		}
	}
	if got := socketIDs(r.match([]string{"red"}, nil)); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("red = %q after matching, want [a b]", got)
	}
	if got := socketIDs(r.match(nil, nil)); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("everybody = %q after matching, want [a b]", got)
	}
}

func TestRegistryConcurrentUse(t *testing.T) {
	t.Parallel()
	const workers, rounds = 8, 60
	r := newRegistry()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			own := fmt.Sprintf("worker-%d", w)
			var kept []*Socket
			for round := range rounds {
				s := &Socket{id: fmt.Sprintf("%s-%d", own, round)}
				r.add(s)
				r.join(s.id, []string{"all", own, fmt.Sprintf("room-%d", round%3)})
				if !r.has(s.id) {
					t.Errorf("%s is not connected right after add", s.id)
				}
				r.match([]string{"all"}, []string{own})
				r.match(nil, []string{"room-1", "room-2"})
				r.of(s.id)
				r.leave(s.id, []string{"all"})
				if round%2 == 1 {
					kept = append(kept, s)
				} else if !r.remove(s) {
					t.Errorf("%s could not be removed", s.id)
				}
			}
			for _, s := range kept {
				if !r.remove(s) {
					t.Errorf("%s could not be removed", s.id)
				}
			}
		}()
	}
	wg.Wait()
	if !isEmpty(r) {
		t.Errorf("registry not empty after every socket left: %d sockets, %d rooms, %d joined", len(r.sockets), len(r.members), len(r.joined))
	}
}
