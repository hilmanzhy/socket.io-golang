package engineio_test

import (
	"errors"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hilmanzhy/socket.io-golang/engineio"
)

func TestReceiveOrder(t *testing.T) {
	t.Parallel()
	const total, batch = 400, 10
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{}, withReceive(func(*recorder, engineio.Message) error {
				runtime.Gosched()
				return nil
			}))
			c := tr.connect(h)
			for i := 0; i < total; i += batch {
				packets := make([]string, 0, batch)
				for j := i; j < i+batch; j++ {
					packets = append(packets, "4"+strconv.Itoa(j))
				}
				c.write(packets...)
			}
			for i := range total {
				if got := c.rec.text(t); got != strconv.Itoa(i) {
					t.Fatalf("message %d = %q", i, got)
				}
			}
			if c.rec.overlapped.Load() {
				t.Error("Receive ran concurrently")
			}
		})
	}
}

func TestClosedWaitsForReceive(t *testing.T) {
	t.Parallel()
	enders := []struct {
		name string
		end  func(*harness, *recorder)
		want engineio.CloseReason
	}{
		{"Session.Close", func(_ *harness, r *recorder) { r.session.Close(engineio.ReasonForcedClose) }, engineio.ReasonForcedClose},
		{"Server.Close", func(h *harness, _ *recorder) { h.server.Close() }, engineio.ReasonServerShutdown},
	}
	for _, tr := range transportCases {
		for _, e := range enders {
			t.Run(tr.name+" "+e.name, func(t *testing.T) {
				t.Parallel()
				release := make(chan struct{})
				var once sync.Once
				h := newHarness(t, engineio.Options{}, withReceive(func(*recorder, engineio.Message) error {
					<-release
					return nil
				}))
				t.Cleanup(func() { once.Do(func() { close(release) }) })
				c := tr.connect(h)
				c.writeAsync("4block")
				c.rec.message(t)
				e.end(h, c.rec)
				if !quiet(c.rec.closed, settle) {
					t.Fatal("Closed ran while Receive was still in flight")
				}
				once.Do(func() { close(release) })
				if got := c.rec.reason(t); got != e.want {
					t.Errorf("reason = %q, want %q", got, e.want)
				}
				c.rec.closedOnce(t)
			})
		}
	}
}

func TestClosedOnceUnderConcurrentEnds(t *testing.T) {
	t.Parallel()
	const sessions = 20
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			clients := make([]*client, sessions)
			for i := range clients {
				clients[i] = tr.connect(h)
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			launch := func(fn func()) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					fn()
				}()
			}
			for _, c := range clients {
				launch(func() { c.rec.session.Close(engineio.ReasonForcedClose) })
				launch(func() { c.rec.session.Close(engineio.ReasonTransportError) })
				launch(func() { c.rec.session.Send(msg("late")) })
				launch(c.hangUp)
			}
			launch(h.server.Close)
			close(start)
			wg.Wait()
			for _, c := range clients {
				c.rec.reason(t)
				c.rec.ended(t)
			}
			<-time.After(settle)
			for i, c := range clients {
				if n := c.rec.closeCalls.Load(); n != 1 {
					t.Errorf("session %d: Closed called %d times, want 1", i, n)
				}
			}
		})
	}
}

func TestCloseReportsGivenReason(t *testing.T) {
	t.Parallel()
	reasons := []engineio.CloseReason{
		engineio.ReasonForcedClose,
		engineio.ReasonTransportError,
		engineio.ReasonParseError,
		engineio.ReasonPingTimeout,
		"application chose this",
	}
	for _, tr := range transportCases {
		for _, reason := range reasons {
			t.Run(tr.name+" "+string(reason), func(t *testing.T) {
				t.Parallel()
				h := newHarness(t, engineio.Options{})
				c := tr.connect(h)
				c.rec.session.Close(reason)
				if got := c.rec.reason(t); got != reason {
					t.Errorf("reason = %q, want %q", got, reason)
				}
				c.rec.closedOnce(t)
			})
		}
	}
}

func TestDoneClosesWhenSessionEnds(t *testing.T) {
	t.Parallel()
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			c := tr.connect(h)
			if !c.rec.open() {
				t.Fatal("Done is closed for a live session")
			}
			c.hangUp()
			c.rec.ended(t)
			c.rec.ended(t)
			if c.rec.open() {
				t.Error("Done reopened")
			}
		})
	}
}

func TestReceiveErrorEndsWebSocketSession(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withReceive(func(_ *recorder, m engineio.Message) error {
		if string(m.Data) == "bad" {
			return errors.New("refused")
		}
		return nil
	}))
	c := transportCases[1].connect(h)
	c.write("4good", "4bad", "4after")
	if got := c.rec.text(t); got != "good" {
		t.Errorf("first message = %q", got)
	}
	if got := c.rec.text(t); got != "bad" {
		t.Errorf("second message = %q", got)
	}
	if got := c.rec.reason(t); got != engineio.ReasonParseError {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonParseError)
	}
	if err := c.ws.readError(); err == nil {
		t.Error("connection stayed open")
	}
	c.rec.ended(t)
	if n := len(c.rec.messages); n != 0 {
		t.Errorf("%d messages were delivered after Receive failed", n)
	}
	c.rec.closedOnce(t)
}

func TestCloseInsideReceive(t *testing.T) {
	t.Parallel()
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{}, withReceive(func(r *recorder, _ engineio.Message) error {
				r.session.Close(engineio.ReasonForcedClose)
				return nil
			}))
			c := tr.connect(h)
			c.write("4bye")
			if got := c.rec.text(t); got != "bye" {
				t.Errorf("message = %q", got)
			}
			if got := c.rec.reason(t); got != engineio.ReasonForcedClose {
				t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
			}
			if err := c.rec.session.Send(msg("x")); !errors.Is(err, engineio.ErrClosed) {
				t.Errorf("Send after Close = %v, want ErrClosed", err)
			}
			c.rec.closedOnce(t)
		})
	}
}

func TestCloseInsideAccept(t *testing.T) {
	t.Parallel()
	closeNow := withAccept(func(s *engineio.Session) { s.Close(engineio.ReasonForcedClose) })
	t.Run("polling", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{}, closeNow)
		c := transportCases[0].connect(h)
		if got := c.rec.reason(t); got != engineio.ReasonForcedClose {
			t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
		}
		if r := h.poll(c.sid); r.status != http.StatusOK || r.body != "1" {
			t.Errorf("poll = %d %q, want 200 \"1\"", r.status, r.body)
		}
		c.rec.ended(t)
		c.rec.closedOnce(t)
	})
	t.Run("websocket", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{}, closeNow)
		c := transportCases[1].connect(h)
		if got := c.rec.reason(t); got != engineio.ReasonForcedClose {
			t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
		}
		c.ws.SetReadDeadline(time.Now().Add(wait))
		if _, _, err := c.ws.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			t.Errorf("read = %v, want a normal closure", err)
		}
		c.rec.ended(t)
		c.rec.closedOnce(t)
	})
}

func TestGracefulCloseGivesClientPingTimeout(t *testing.T) {
	t.Parallel()
	const timeout = time.Second
	h := newHarness(t, engineio.Options{PingTimeout: timeout})
	sid, rec := h.open()
	start := time.Now()
	if err := rec.session.Send(msg("last")); err != nil {
		t.Fatal(err)
	}
	rec.session.Close(engineio.ReasonForcedClose)
	if got := rec.reason(t); got != engineio.ReasonForcedClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
	}
	if !rec.open() {
		t.Fatal("the session ended before the client had PingTimeout to collect its packets")
	}
	rec.ended(t)
	if elapsed := time.Since(start); elapsed < timeout {
		t.Errorf("session ended after %v, want at least PingTimeout %v", elapsed, timeout)
	}
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after the timeout = %d, want 400", r.status)
	}
	rec.closedOnce(t)
}

func TestMessagesAfterCloseAreNotDelivered(t *testing.T) {
	t.Parallel()
	for _, tr := range transportCases {
		t.Run(tr.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			c := tr.connect(h)
			c.rec.session.Close(engineio.ReasonForcedClose)
			c.rec.reason(t)
			if c.ws != nil {
				c.ws.WriteMessage(websocket.TextMessage, []byte("4ignored"))
			} else {
				h.post(c.sid, "4ignored")
			}
			if !quiet(c.rec.messages, settle) {
				t.Error("a message was delivered after the session closed")
			}
			if c.rec.lateReceive.Load() {
				t.Error("Receive ran after Closed")
			}
		})
	}
}
