package engineio_test

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/nuriontech/socket.io-golang/engineio"
)

// collect reads server messages until it holds n of them and returns their
// text without the packet type.
func (c *client) collect(n int) []string {
	c.h.t.Helper()
	var got []string
	add := func(packet string) {
		if packet == "" || packet[0] != '4' {
			c.h.t.Fatalf("unexpected packet %q", packet)
		}
		got = append(got, packet[1:])
	}
	for len(got) < n {
		if c.ws != nil {
			add(c.ws.text())
			continue
		}
		r := c.h.poll(c.sid)
		if r.status != http.StatusOK {
			c.h.t.Fatalf("poll = %d %q", r.status, r.body)
		}
		for _, packet := range split(r.body) {
			add(packet)
		}
	}
	return got
}

func TestSendKeepsGroupsTogether(t *testing.T) {
	t.Parallel()
	const senders, rounds, size = 8, 40, 3
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			c := tr.connect(h)
			var wg sync.WaitGroup
			for g := range senders {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for k := range rounds {
						group := make([]engineio.Message, size)
						for j := range group {
							group[j] = msg(fmt.Sprintf("%d.%d.%d", g, k, j))
						}
						if err := c.rec.session.Send(group...); err != nil {
							t.Errorf("sender %d round %d: %v", g, k, err)
							return
						}
					}
				}()
			}
			defer wg.Wait()
			got := c.collect(senders * rounds * size)
			next := make([]int, senders)
			for i := 0; i < len(got); i += size {
				var g, k, j int
				if _, err := fmt.Sscanf(got[i], "%d.%d.%d", &g, &k, &j); err != nil || j != 0 {
					t.Fatalf("message %d = %q does not start a group", i, got[i])
				}
				for m := 1; m < size; m++ {
					var g2, k2, j2 int
					if _, err := fmt.Sscanf(got[i+m], "%d.%d.%d", &g2, &k2, &j2); err != nil || g2 != g || k2 != k || j2 != m {
						t.Fatalf("group %d.%d was interleaved: message %d = %q", g, k, i+m, got[i+m])
					}
				}
				if k != next[g] {
					t.Fatalf("sender %d: round %d arrived where round %d was expected", g, k, next[g])
				}
				next[g]++
			}
			for g, n := range next {
				if n != rounds {
					t.Errorf("sender %d: %d rounds delivered, want %d", g, n, rounds)
				}
			}
		})
	}
}

func TestSendKeepsOrder(t *testing.T) {
	t.Parallel()
	const total = 300
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			c := tr.connect(h)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range total {
					if err := c.rec.session.Send(msg(strconv.Itoa(i))); err != nil {
						t.Errorf("Send %d: %v", i, err)
						return
					}
				}
			}()
			defer wg.Wait()
			for i, got := range c.collect(total) {
				if got != strconv.Itoa(i) {
					t.Fatalf("message %d = %q", i, got)
				}
			}
		})
	}
}

func TestSendAfterEnd(t *testing.T) {
	t.Parallel()
	enders := []struct {
		name string
		end  func(*harness, *client)
	}{
		{"Session.Close", func(_ *harness, c *client) { c.rec.session.Close(engineio.ReasonForcedClose) }},
		{"Server.Close", func(h *harness, _ *client) { h.server.Close() }},
		{"client close", func(_ *harness, c *client) {
			c.hangUp()
			c.rec.ended(c.h.t)
		}},
	}
	for _, tr := range transportCases {
		for _, e := range enders {
			t.Run(tr.name+" "+e.name, func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, engineio.Options{})
				c := tr.connect(h)
				e.end(h, c)
				if err := c.rec.session.Send(msg("x")); !errors.Is(err, engineio.ErrClosed) {
					t.Errorf("Send = %v, want ErrClosed", err)
				}
				if err := c.rec.session.Send(msg("x"), blob(1)); !errors.Is(err, engineio.ErrClosed) {
					t.Errorf("Send of two messages = %v, want ErrClosed", err)
				}
			})
		}
	}
}

func TestMaxBufferPolling(t *testing.T) {
	t.Parallel()
	const limit = 1000
	filler := func(n int) engineio.Message { return engineio.Message{Data: bytes.Repeat([]byte{'x'}, n)} }
	newClient := func(t *testing.T) (*harness, string, *recorder) {
		h := newHarness(t, engineio.Options{MaxBuffer: limit})
		sid, rec := h.open()
		return h, sid, rec
	}
	t.Run("over the limit ends the session", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := newClient(t)
		if err := rec.session.Send(filler(600)); err != nil {
			t.Fatal(err)
		}
		if err := rec.session.Send(filler(600)); !errors.Is(err, engineio.ErrClosed) {
			t.Fatalf("Send over the limit = %v, want ErrClosed", err)
		}
		if got := rec.reason(t); got != engineio.ReasonTransportError {
			t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportError)
		}
		rec.ended(t)
		if r := h.poll(sid); r.status != http.StatusBadRequest {
			t.Errorf("poll after the overflow = %d, want 400", r.status)
		}
		if err := rec.session.Send(msg("x")); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("Send after the overflow = %v, want ErrClosed", err)
		}
		rec.closedOnce(t)
	})
	t.Run("exactly the limit fits", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := newClient(t)
		for range 2 {
			if err := rec.session.Send(filler(500)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rec.session.Send(filler(1)); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("one byte past the limit = %v, want ErrClosed", err)
		}
		if r := h.poll(sid); r.status != http.StatusBadRequest {
			t.Errorf("poll after the overflow = %d, want 400", r.status)
		}
	})
	t.Run("several sends within the limit", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := newClient(t)
		for range 3 {
			if err := rec.session.Send(filler(300)); err != nil {
				t.Fatal(err)
			}
		}
		r := h.poll(sid)
		if r.status != http.StatusOK || len(split(r.body)) != 3 {
			t.Errorf("poll = %d with %d packets, want 200 with 3", r.status, len(split(r.body)))
		}
	})
	t.Run("a poll empties the buffer", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := newClient(t)
		for round := range 3 {
			if err := rec.session.Send(filler(600)); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
			if r := h.poll(sid); r.status != http.StatusOK || len(r.body) != 601 {
				t.Fatalf("round %d: poll = %d with %d bytes, want 200 with 601", round, r.status, len(r.body))
			}
		}
	})
	t.Run("binary messages count by their bytes", func(t *testing.T) {
		t.Parallel()
		_, _, rec := newClient(t)
		big := engineio.Message{Binary: true, Data: bytes.Repeat([]byte{1}, 600)}
		if err := rec.session.Send(big); err != nil {
			t.Fatal(err)
		}
		if err := rec.session.Send(big); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("second binary Send = %v, want ErrClosed", err)
		}
	})
}

func TestMaxBufferWebSocket(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{MaxBuffer: 2 << 20})
	c := transportCases[1].connect(h)
	chunk := engineio.Message{Data: bytes.Repeat([]byte{'x'}, 1<<20)}
	var err error
	for i := 0; i < 4096 && err == nil; i++ {
		err = c.rec.session.Send(chunk)
	}
	if !errors.Is(err, engineio.ErrClosed) {
		t.Fatalf("Send never failed although the client reads nothing: %v", err)
	}
	if got := c.rec.reason(t); got != engineio.ReasonTransportError {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportError)
	}
	c.rec.ended(t)
	if err := c.ws.readError(); err == nil {
		t.Error("connection stayed open")
	}
	c.rec.closedOnce(t)
}
