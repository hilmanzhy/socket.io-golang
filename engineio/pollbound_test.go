package engineio_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
)

// pollUntil polls until it holds at least n packets and returns the body of
// every response.
func (h *harness) pollUntil(sid string, n int) []string {
	h.t.Helper()
	var bodies []string
	for got := 0; got < n; {
		r := h.poll(sid)
		if r.status != http.StatusOK {
			h.t.Fatalf("poll = %d %q", r.status, r.body)
		}
		bodies = append(bodies, r.body)
		got += len(split(r.body))
	}
	return bodies
}

func numbered(prefix string, n int) []engineio.Message {
	messages := make([]engineio.Message, n)
	for i := range messages {
		messages[i] = msg(fmt.Sprintf("%s%d", prefix, i))
	}
	return messages
}

func (c *wsConn) expectSilence(d time.Duration) {
	c.t.Helper()
	c.SetReadDeadline(time.Now().Add(d))
	if kind, data, err := c.ReadMessage(); err == nil {
		c.t.Errorf("unexpected frame %d %q", kind, data)
	}
}

func TestPollingGetBoundedByMaxPayload(t *testing.T) {
	t.Parallel()
	const total = 30
	tests := []struct {
		name    string
		limit   int64
		perPoll int
	}{
		{"one byte short of eight records", 38, 7},
		{"exactly eight records", 39, 8},
		{"one byte over eight records", 40, 8},
		{"exactly nine records", 44, 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxPayload: tt.limit})
			sid, rec := h.open()
			var want []string
			for i := range total {
				text := fmt.Sprintf("m%02d", i)
				want = append(want, "4"+text)
				if err := rec.session.Send(msg(text)); err != nil {
					t.Fatal(err)
				}
			}
			bodies := h.pollUntil(sid, total)
			var got []string
			for i, body := range bodies {
				if int64(len(body)) > tt.limit {
					t.Errorf("poll %d answered %d bytes, over the limit %d", i, len(body), tt.limit)
				}
				packets := split(body)
				if last := i == len(bodies)-1; !last && len(packets) != tt.perPoll || last && len(packets) > tt.perPoll {
					t.Errorf("poll %d carried %d packets, want %d", i, len(packets), tt.perPoll)
				}
				got = append(got, packets...)
			}
			if wantPolls := (total + tt.perPoll - 1) / tt.perPoll; len(bodies) != wantPolls {
				t.Errorf("%d polls, want %d", len(bodies), wantPolls)
			}
			if !slices.Equal(got, want) {
				t.Errorf("packets = %q, want %q", got, want)
			}
		})
	}
}

func TestPollingGetBoundsBinaryByBase64Size(t *testing.T) {
	t.Parallel()
	const limit, count, size = 40, 5, 9
	h := newHarness(t, engineio.Options{MaxPayload: limit})
	sid, rec := h.open()
	for i := range count {
		data := bytes.Repeat([]byte{byte(i + 1)}, size)
		if err := rec.session.Send(engineio.Message{Binary: true, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	bodies := h.pollUntil(sid, count)
	var sizes []int
	next := 1
	for i, body := range bodies {
		if len(body) > limit {
			t.Errorf("poll %d answered %d bytes, over the limit %d", i, len(body), limit)
		}
		for _, packet := range split(body) {
			data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(packet, "b"))
			if err != nil || !bytes.Equal(data, bytes.Repeat([]byte{byte(next)}, size)) {
				t.Fatalf("packet %q is not message %d: %v", packet, next, err)
			}
			next++
		}
		sizes = append(sizes, len(split(body)))
	}
	if want := []int{2, 2, 1}; !slices.Equal(sizes, want) {
		t.Errorf("packets per poll = %v, want %v: each record takes 13 bytes on the wire", sizes, want)
	}
}

func TestPollingGetSendsOversizedPacketAlone(t *testing.T) {
	t.Parallel()
	const limit = 20
	big := strings.Repeat("x", 50)
	blobData := bytes.Repeat([]byte{7}, 30)
	bigBlob := engineio.Message{Binary: true, Data: blobData}
	blobRecord := "b" + base64.StdEncoding.EncodeToString(blobData)
	tests := []struct {
		name string
		send []engineio.Message
		want []string
	}{
		{"first", []engineio.Message{msg(big), msg("a"), msg("b")}, []string{"4" + big, "4a\x1e4b"}},
		{"middle", []engineio.Message{msg("a"), msg(big), msg("b")}, []string{"4a", "4" + big, "4b"}},
		{"last", []engineio.Message{msg("a"), msg("b"), msg(big)}, []string{"4a\x1e4b", "4" + big}},
		{"binary", []engineio.Message{msg("a"), bigBlob, msg("b")}, []string{"4a", blobRecord, "4b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxPayload: limit})
			sid, rec := h.open()
			if err := rec.session.Send(tt.send...); err != nil {
				t.Fatal(err)
			}
			for i, want := range tt.want {
				if r := h.poll(sid); r.status != http.StatusOK || r.body != want {
					t.Fatalf("poll %d = %d %q, want %q", i, r.status, r.body, want)
				}
			}
			rec.session.Send(msg("end"))
			if r := h.poll(sid); r.body != "4end" {
				t.Errorf("poll after the last packet = %q, want only 4end", r.body)
			}
		})
	}
}

func TestPollingGetReturnsLeftoversWithoutWaiting(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{MaxPayload: 11})
	sid, rec := h.open()
	if err := rec.session.Send(msg("aa"), msg("bb"), msg("cc"), msg("dd"), msg("ee"), msg("ff")); err != nil {
		t.Fatal(err)
	}
	if r := h.poll(sid); r.body != "4aa\x1e4bb\x1e4cc" {
		t.Fatalf("first poll = %q", r.body)
	}
	if r := h.startPoll(sid).wait(t); r.status != http.StatusOK || r.body != "4dd\x1e4ee\x1e4ff" {
		t.Fatalf("second poll = %d %q, want the leftovers", r.status, r.body)
	}
	later := h.startPoll(sid)
	if !later.running(settle) {
		t.Fatalf("poll answered with nothing to send: %+v", later.reply)
	}
	rec.session.Send(msg("gg"))
	if r := later.wait(t); r.body != "4gg" {
		t.Errorf("third poll = %q, want 4gg", r.body)
	}
}

func TestPollingPartialDrainKeepsBufferAccounting(t *testing.T) {
	t.Parallel()
	const maxBuffer, maxPayload, size = 100, 30, 10
	filler := func(n int, binary bool) engineio.Message {
		return engineio.Message{Binary: binary, Data: bytes.Repeat([]byte{'x'}, n)}
	}
	fill := func(t *testing.T) (*harness, string, *recorder) {
		h := newHarness(t, engineio.Options{MaxBuffer: maxBuffer, MaxPayload: maxPayload})
		sid, rec := h.open()
		for i := range 8 {
			if err := rec.session.Send(filler(size, i%2 == 1)); err != nil {
				t.Fatal(err)
			}
		}
		return h, sid, rec
	}
	t.Run("a poll frees what it took and no more", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := fill(t)
		if r := h.poll(sid); len(split(r.body)) != 2 {
			t.Fatalf("poll carried %d packets, want 2: %q", len(split(r.body)), r.body)
		}
		if err := rec.session.Send(filler(40, false)); err != nil {
			t.Errorf("Send of the 2 messages' worth of space that was freed = %v", err)
		}
		if err := rec.session.Send(filler(1, false)); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("Send past the limit = %v, want ErrClosed", err)
		}
	})
	t.Run("a poll does not free more than it took", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := fill(t)
		h.poll(sid)
		if err := rec.session.Send(filler(41, false)); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("Send of one byte more than was freed = %v, want ErrClosed", err)
		}
	})
	t.Run("draining everything empties the buffer", func(t *testing.T) {
		t.Parallel()
		h, sid, rec := fill(t)
		h.pollUntil(sid, 8)
		if err := rec.session.Send(filler(maxBuffer, false)); err != nil {
			t.Fatalf("Send of the whole buffer after a full drain = %v", err)
		}
		if err := rec.session.Send(filler(1, false)); !errors.Is(err, engineio.ErrClosed) {
			t.Errorf("Send past the limit = %v, want ErrClosed", err)
		}
	})
}

func TestPollingBoundedUnderConcurrentSend(t *testing.T) {
	t.Parallel()
	const senders, perSender, limit = 4, 60, 64
	h := newHarness(t, engineio.Options{MaxPayload: limit})
	sid, rec := h.open()
	var wg sync.WaitGroup
	for g := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perSender {
				if err := rec.session.Send(msg(fmt.Sprintf("%d.%d", g, i))); err != nil {
					t.Errorf("sender %d message %d: %v", g, i, err)
					return
				}
			}
		}()
	}
	defer wg.Wait()
	next := make([]int, senders)
	for got := 0; got < senders*perSender; {
		r := h.poll(sid)
		if len(r.body) > limit {
			t.Errorf("poll answered %d bytes, over the limit %d", len(r.body), limit)
		}
		for _, packet := range split(r.body) {
			var g, i int
			if _, err := fmt.Sscanf(packet[1:], "%d.%d", &g, &i); err != nil || i != next[g] {
				t.Fatalf("packet %q arrived where message %d of its sender was expected: %v", packet, next[g], err)
			}
			next[g]++
			got++
		}
	}
}

func TestUpgradeDeliversLeftoversOfBoundedPoll(t *testing.T) {
	t.Parallel()
	const limit = 19
	t.Run("leftovers arrive over WebSocket", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{MaxPayload: limit})
		sid, rec := h.open()
		pending := h.startPoll(sid)
		if err := rec.session.Send(numbered("m", 10)...); err != nil {
			t.Fatal(err)
		}
		if r := pending.wait(t); r.body != "4m0\x1e4m1\x1e4m2\x1e4m3\x1e4m4" {
			t.Fatalf("poll = %q, want the first five messages", r.body)
		}
		ws := h.connectUpgrade(sid)
		ws.write("2probe")
		if got := ws.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		ws.write("5")
		for i := 5; i < 10; i++ {
			if got := ws.text(); got != fmt.Sprintf("4m%d", i) {
				t.Fatalf("frame = %q, want 4m%d", got, i)
			}
		}
		rec.session.Send(msg("after"))
		if got := ws.text(); got != "4after" {
			t.Errorf("frame = %q, want 4after", got)
		}
		ws.expectSilence(settle)
	})
	t.Run("polls during the probe take leftovers before the flush noop", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{MaxPayload: limit})
		sid, rec := h.open()
		pending := h.startPoll(sid)
		if err := rec.session.Send(numbered("m", 15)...); err != nil {
			t.Fatal(err)
		}
		if r := pending.wait(t); r.body != "4m0\x1e4m1\x1e4m2\x1e4m3\x1e4m4" {
			t.Fatalf("poll = %q, want the first five messages", r.body)
		}
		ws := h.connectUpgrade(sid)
		ws.write("2probe")
		if got := ws.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		for i, want := range []string{
			"4m5\x1e4m6\x1e4m7\x1e4m8\x1e4m9",
			"4m10\x1e4m11\x1e4m12\x1e4m13",
			"4m14",
			"6",
		} {
			if r := h.poll(sid); r.status != http.StatusOK || r.body != want {
				t.Fatalf("poll %d during the probe = %d %q, want %q", i, r.status, r.body, want)
			}
		}
		ws.write("5")
		rec.session.Send(msg("after"))
		if got := ws.text(); got != "4after" {
			t.Errorf("frame = %q, want 4after", got)
		}
		ws.expectSilence(settle)
	})
	t.Run("leftovers stay available to polling after a failed upgrade", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{MaxPayload: limit})
		sid, rec := h.open()
		pending := h.startPoll(sid)
		if err := rec.session.Send(numbered("m", 10)...); err != nil {
			t.Fatal(err)
		}
		pending.wait(t)
		ws := h.connectUpgrade(sid)
		ws.write("2probe")
		if got := ws.text(); got != "3probe" {
			t.Fatalf("probe answer = %q", got)
		}
		ws.Close()
		if r := h.poll(sid); r.body != "4m5\x1e4m6\x1e4m7\x1e4m8\x1e4m9" {
			t.Errorf("poll = %q, want the leftovers", r.body)
		}
	})
}

func TestPollingGracefulCloseWithLeftovers(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{MaxPayload: 19})
	sid, rec := h.open()
	if err := rec.session.Send(numbered("m", 10)...); err != nil {
		t.Fatal(err)
	}
	rec.session.Close(engineio.ReasonForcedClose)
	for i, want := range []string{"4m0\x1e4m1\x1e4m2\x1e4m3\x1e4m4", "4m5\x1e4m6\x1e4m7\x1e4m8\x1e4m9", "1"} {
		if r := h.poll(sid); r.status != http.StatusOK || r.body != want {
			t.Fatalf("poll %d = %d %q, want %q", i, r.status, r.body, want)
		}
	}
	rec.ended(t)
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after close = %d, want 400", r.status)
	}
	if got := rec.reason(t); got != engineio.ReasonForcedClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
	}
	rec.closedOnce(t)
}

func TestWebSocketWritesIgnoreMaxPayload(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{MaxPayload: 20})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	big := strings.Repeat("x", 50)
	if err := rec.session.Send(msg("a"), msg(big), msg("b")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"4a", "4" + big, "4b"} {
		if got := ws.text(); got != want {
			t.Errorf("frame = %q, want %q", got, want)
		}
	}
	ws.expectSilence(settle)
	if !rec.open() {
		t.Error("session ended while writing oversized messages over WebSocket")
	}
}
