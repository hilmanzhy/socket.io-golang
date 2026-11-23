package socketio

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

func TestRegistryJoinLimit(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		limit int
		start []string
		join  []string
		want  []string
		fails bool
	}{
		{"no limit takes any number of rooms", 0, nil, []string{"a", "b", "c", "d", "e", "f"}, []string{"a", "b", "c", "d", "e", "f", "s1"}, false},
		{"no limit keeps taking rooms one by one", 0, []string{"a", "b", "c", "d"}, []string{"e"}, []string{"a", "b", "c", "d", "e", "s1"}, false},
		{"one room under the limit", 2, nil, []string{"a"}, []string{"a", "s1"}, false},
		{"exactly the limit in one call", 2, nil, []string{"a", "b"}, []string{"a", "b", "s1"}, false},
		{"exactly the limit over two calls", 2, []string{"a"}, []string{"b"}, []string{"a", "b", "s1"}, false},
		{"one room over the limit in one call", 2, nil, []string{"a", "b", "c"}, []string{"s1"}, true},
		{"many rooms over the limit in one call", 2, nil, []string{"a", "b", "c", "d", "e"}, []string{"s1"}, true},
		{"one room over the limit in a later call", 2, []string{"a", "b"}, []string{"c"}, []string{"a", "b", "s1"}, true},
		{"a later call that reaches past the limit", 2, []string{"a"}, []string{"b", "c"}, []string{"a", "s1"}, true},
		{"a limit of one takes one room", 1, nil, []string{"a"}, []string{"a", "s1"}, false},
		{"a limit of one refuses two rooms", 1, nil, []string{"a", "b"}, []string{"s1"}, true},
		{"a limit of one refuses a second room", 1, []string{"a"}, []string{"b"}, []string{"a", "s1"}, true},
		{"the limit reached by the last room of a call", 4, []string{"a"}, []string{"b", "c", "d"}, []string{"a", "b", "c", "d", "s1"}, false},
		{"the limit passed by the last room of a call", 3, []string{"a"}, []string{"b", "c", "d"}, []string{"a", "s1"}, true},
		{"the limit passed in the middle of a call", 3, []string{"a"}, []string{"b", "c", "d", "e", "f"}, []string{"a", "s1"}, true},
		{"its own room does not count", 1, nil, []string{"s1", "a"}, []string{"a", "s1"}, false},
		{"its own room first and the limit reached", 2, []string{"a"}, []string{"s1", "b"}, []string{"a", "b", "s1"}, false},
		{"its own room alone at a full socket", 1, []string{"a"}, []string{"s1"}, []string{"a", "s1"}, false},
		{"a room it is already in does not count twice", 1, []string{"a"}, []string{"a"}, []string{"a", "s1"}, false},
		{"every room already joined at the limit", 2, []string{"a", "b"}, []string{"a", "b"}, []string{"a", "b", "s1"}, false},
		{"joined rooms repeated in one call", 2, []string{"a", "b"}, []string{"b", "a", "b"}, []string{"a", "b", "s1"}, false},
		{"a joined room next to a new one within the limit", 2, []string{"a"}, []string{"a", "b"}, []string{"a", "b", "s1"}, false},
		{"a joined room next to a new one over the limit", 2, []string{"a", "b"}, []string{"a", "c"}, []string{"a", "b", "s1"}, true},
		{"a new room before a joined one over the limit", 2, []string{"a", "b"}, []string{"c", "a"}, []string{"a", "b", "s1"}, true},
		{"the same new room twice counts once", 1, nil, []string{"a", "a"}, []string{"a", "s1"}, false},
		{"the same new room twice and one more", 1, nil, []string{"a", "a", "b"}, []string{"s1"}, true},
		{"no rooms at a full socket", 1, []string{"a"}, nil, []string{"a", "s1"}, false},
		{"an empty room name counts as a room", 1, []string{"a"}, []string{""}, []string{"a", "s1"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry(tc.limit)
			s := newSocketIn(r, "s1")
			if err := r.join(s.id, tc.start); err != nil {
				t.Fatalf("setup join(%q) = %v", tc.start, err)
			}
			rooms := roomCount(r)
			err := r.join(s.id, tc.join)
			if tc.fails != errors.Is(err, ErrRoomLimit) {
				t.Errorf("join(%q) = %v, want ErrRoomLimit %v", tc.join, err, tc.fails)
			}
			if !tc.fails && err != nil {
				t.Errorf("join(%q) = %v, want nil", tc.join, err)
			}
			if got := r.of(s.id); !slices.Equal(got, tc.want) {
				t.Errorf("rooms = %q, want %q", got, tc.want)
			}
			if tc.fails {
				if got := roomCount(r); got != rooms {
					t.Errorf("%d rooms exist after the refused call, want the %d from before it", got, rooms)
				}
			}
			for _, room := range tc.want {
				if got := socketIDs(r.match([]string{room}, nil)); !slices.Equal(got, []string{"s1"}) {
					t.Errorf("members of %q = %q, want [s1]", room, got)
				}
			}
		})
	}
}

func TestRegistryJoinRefusedLeavesNoTrace(t *testing.T) {
	t.Parallel()
	r := newRegistry(2)
	newSocketIn(r, "s1", "kept")
	newSocketIn(r, "s2", "shared")
	before := roomCount(r)
	if err := r.join("s1", []string{"shared", "x", "y", "z"}); !errors.Is(err, ErrRoomLimit) {
		t.Fatalf("join = %v, want ErrRoomLimit", err)
	}
	if got := r.of("s1"); !slices.Equal(got, []string{"kept", "s1"}) {
		t.Errorf("rooms = %q, want [kept s1]", got)
	}
	if got := roomCount(r); got != before {
		t.Errorf("%d rooms exist, want %d: the refused call left rooms behind", got, before)
	}
	for _, room := range []string{"x", "y", "z"} {
		if got := r.match([]string{room}, nil); len(got) != 0 {
			t.Errorf("room %q has members %v", room, socketIDs(got))
		}
	}
	if got := socketIDs(r.match([]string{"shared"}, nil)); !slices.Equal(got, []string{"s2"}) {
		t.Errorf("shared = %q, want [s2]: the refused call changed a room it entered", got)
	}
}

func TestRegistryJoinRefusedKeepsRoomsJoinedBefore(t *testing.T) {
	t.Parallel()
	r := newRegistry(2)
	newSocketIn(r, "s1", "a", "b")
	for range 3 {
		if err := r.join("s1", []string{"c", "a"}); !errors.Is(err, ErrRoomLimit) {
			t.Fatalf("join = %v, want ErrRoomLimit", err)
		}
	}
	if got := r.of("s1"); !slices.Equal(got, []string{"a", "b", "s1"}) {
		t.Errorf("rooms = %q, want [a b s1]", got)
	}
	if got := socketIDs(r.match([]string{"a", "b"}, nil)); !slices.Equal(got, []string{"s1"}) {
		t.Errorf("a and b = %q, want [s1]", got)
	}
}

func TestRegistryJoinFreedRoomsCanBeUsedAgain(t *testing.T) {
	t.Parallel()
	r := newRegistry(2)
	newSocketIn(r, "s1", "a", "b")
	if err := r.join("s1", []string{"c"}); !errors.Is(err, ErrRoomLimit) {
		t.Fatalf("join over the limit = %v, want ErrRoomLimit", err)
	}
	r.leave("s1", []string{"a"})
	if err := r.join("s1", []string{"c"}); err != nil {
		t.Errorf("join after leaving a room = %v, want nil", err)
	}
	if got := r.of("s1"); !slices.Equal(got, []string{"b", "c", "s1"}) {
		t.Errorf("rooms = %q, want [b c s1]", got)
	}
	r.leave("s1", []string{"b", "c"})
	if err := r.join("s1", []string{"x", "y"}); err != nil {
		t.Errorf("join of the full limit after leaving everything = %v, want nil", err)
	}
}

func TestRegistryJoinLimitIsPerSocket(t *testing.T) {
	t.Parallel()
	r := newRegistry(1)
	newSocketIn(r, "s1", "a")
	if err := r.join("s2", []string{"a"}); err != nil {
		t.Errorf("join of an unknown socket = %v, want nil", err)
	}
	r.add(&Socket{id: "s2"})
	if err := r.join("s2", []string{"a"}); err != nil {
		t.Errorf("join of a second socket = %v, want nil: the first one filled its own limit", err)
	}
	if err := r.join("s1", []string{"b"}); !errors.Is(err, ErrRoomLimit) {
		t.Errorf("join of the full socket = %v, want ErrRoomLimit", err)
	}
	if got := socketIDs(r.match([]string{"a"}, nil)); !slices.Equal(got, []string{"s1", "s2"}) {
		t.Errorf("a = %q, want [s1 s2]", got)
	}
}

func TestRegistryJoinLimitIsNotUsedUpByRemovedSockets(t *testing.T) {
	t.Parallel()
	r := newRegistry(1)
	first := newSocketIn(r, "s1", "a")
	r.remove(first)
	again := &Socket{id: "s1"}
	r.add(again)
	if err := r.join("s1", []string{"b"}); err != nil {
		t.Errorf("join after a socket with the same id was removed = %v, want nil", err)
	}
	if got := r.of("s1"); !slices.Equal(got, []string{"b", "s1"}) {
		t.Errorf("rooms = %q, want [b s1]", got)
	}
}

func TestRegistryJoinLimitHoldsUnderConcurrency(t *testing.T) {
	t.Parallel()
	const limit, workers = 5, 32
	r := newRegistry(limit)
	newSocketIn(r, "s1")
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, refused := 0, 0
	start := make(chan struct{})
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := r.join("s1", []string{fmt.Sprintf("room-%d", w)})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrRoomLimit):
				refused++
			default:
				t.Errorf("join = %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if accepted != limit || refused != workers-limit {
		t.Errorf("%d joins accepted and %d refused, want %d and %d", accepted, refused, limit, workers-limit)
	}
	if got := len(r.of("s1")); got != limit+1 {
		t.Errorf("the socket is in %d rooms, want %d (the limit and its own)", got, limit+1)
	}
	if got := roomCount(r); got != limit+1 {
		t.Errorf("%d rooms exist, want %d", got, limit+1)
	}
}

func TestRegistryLimitStillHoldsAfterLeavingTheOwnRoom(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		calls [][]string
		want  []string
	}{
		{"all rooms in one call", [][]string{{"a", "b", "c"}}, nil},
		{"one room per call", [][]string{{"a"}, {"b"}, {"c"}}, []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry(2)
			s := newSocketIn(r, "s1")
			r.leave(s.id, []string{s.id})
			var refused bool
			for _, rooms := range tc.calls {
				if err := r.join(s.id, rooms); errors.Is(err, ErrRoomLimit) {
					refused = true
				}
			}
			if !refused {
				t.Errorf("no join was refused: with a limit of 2 the socket joined %q besides its own room", r.of(s.id))
			}
			if got := r.of(s.id); !slices.Equal(got, tc.want) {
				t.Errorf("rooms = %q, want %q", got, tc.want)
			}
		})
	}
}
