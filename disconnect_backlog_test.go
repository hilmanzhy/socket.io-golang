package socketio_test

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
)

type backlogCase struct {
	name    string
	sent    int
	blocked bool
	want    []string
}

func backlogCases() []backlogCase {
	return []backlogCase{
		{"no event waiting", 0, false, []string{"event 0", "disconnect"}},
		{"one event waiting", 1, false, []string{"event 0", "event 1", "disconnect"}},
		{"a full backlog", 2, false, []string{"event 0", "event 1", "event 2", "disconnect"}},
		{"one event over a full backlog", 3, true, []string{"event 0", "event 1", "event 2", "disconnect"}},
		{"many events over a full backlog", 10, true, []string{"event 0", "event 1", "event 2", "disconnect"}},
	}
}

type backlogRig struct {
	c       *client
	s       *socketio.Socket
	ran     chan string
	reasons chan socketio.Reason
	open    func()
}

func newBacklogRig(t *testing.T, tc backlogCase) *backlogRig {
	t.Helper()
	rig := &backlogRig{ran: make(chan string, 64), reasons: make(chan socketio.Reason, 2)}
	gate := make(chan struct{})
	var once sync.Once
	rig.open = func() { once.Do(func() { close(gate) }) }
	started := make(chan struct{}, 1)
	server, ts := newServer(t, socketio.Options{EventBacklog: 2})
	t.Cleanup(rig.open)
	main := watch(server.Namespace, func(s *socketio.Socket) {
		s.On("n", func(e *socketio.Event) {
			var n int
			e.Bind(&n)
			if n == 0 {
				started <- struct{}{}
				<-gate
			}
			rig.ran <- fmt.Sprintf("event %d", n)
		})
		s.OnDisconnect(func(r socketio.Reason) {
			rig.ran <- "disconnect"
			rig.reasons <- r
		})
	})
	other := watch(server.Of("/other"))
	rig.c = dial(t, ts)
	rig.c.connect("/", "")
	rig.c.connect("/other", "")
	rig.s, _ = main.socket(t), other.socket(t)

	rig.c.send(`42["n",0]`)
	receive(t, started)
	for i := 1; i <= tc.sent; i++ {
		rig.c.send(fmt.Sprintf(`42["n",%d]`, i))
	}
	return rig
}

func TestDisconnectRunsNoEventHandlerAfterOnDisconnect(t *testing.T) {
	t.Parallel()
	for _, tc := range backlogCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := newBacklogRig(t, tc)
			answer := pkt('3', "/other", `9999[]`)
			rig.c.send(pkt('2', "/other", `9999["ping"]`))
			if tc.blocked {
				rig.c.expectSilence()
			} else {
				rig.c.expect(answer)
			}

			rig.s.Disconnect(false)
			if tc.blocked {
				rig.c.expectSet(pkt('1', "/", ""), answer)
			} else {
				rig.c.expect(pkt('1', "/", ""))
			}
			nothing(t, rig.ran)
			nothing(t, rig.reasons)

			rig.c.send(`42["n",100]`)
			rig.c.ping("/other")
			nothing(t, rig.ran)

			rig.open()
			var got []string
			for range tc.want {
				got = append(got, receive(t, rig.ran))
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("handlers ran in order %q, want %q", got, tc.want)
			}
			if reason := receive(t, rig.reasons); reason != socketio.ReasonServerDisconnect {
				t.Errorf("reason = %q, want %q", reason, socketio.ReasonServerDisconnect)
			}

			rig.c.send(`42["n",101]`)
			rig.c.send(`42["n",102]`)
			rig.c.ping("/other")
			nothing(t, rig.ran)
			nothing(t, rig.reasons)
			rig.c.expectOpen()
		})
	}
}

func TestDisconnectClosingTheConnectionRunsNoEventHandlerAfterOnDisconnect(t *testing.T) {
	t.Parallel()
	for _, tc := range backlogCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := newBacklogRig(t, tc)
			answer := pkt('3', "/other", `9999[]`)
			rig.c.send(pkt('2', "/other", `9999["ping"]`))
			if tc.blocked {
				rig.c.expectSilence()
			} else {
				rig.c.expect(answer)
			}

			returned := make(chan struct{})
			go func() {
				rig.s.Disconnect(true)
				close(returned)
			}()
			receive(t, returned)
			rig.c.expectClosed()
			var farewells []string
			for _, text := range rig.c.buffered() {
				if text != answer {
					farewells = append(farewells, text)
				}
			}
			if want := sorted(pkt('1', "/", ""), pkt('1', "/other", "")); !slices.Equal(sorted(farewells...), want) {
				t.Errorf("frames after Disconnect(true) = %q, want %q", farewells, want)
			}
			nothing(t, rig.ran)
			nothing(t, rig.reasons)

			rig.open()
			var got []string
			for range tc.want {
				got = append(got, receive(t, rig.ran))
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("handlers ran in order %q, want %q", got, tc.want)
			}
			if reason := receive(t, rig.reasons); reason != socketio.ReasonServerDisconnect {
				t.Errorf("reason = %q, want %q", reason, socketio.ReasonServerDisconnect)
			}
			nothing(t, rig.ran)
			nothing(t, rig.reasons)
		})
	}
}
