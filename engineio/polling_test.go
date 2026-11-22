package engineio_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/nuriontech/socket.io-golang/engineio"
)

func TestPollingEcho(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	if r := h.post(sid, "4hello"); r.status != http.StatusOK || r.body != "ok" {
		t.Fatalf("POST = %d %q, want 200 \"ok\"", r.status, r.body)
	}
	if got := rec.text(t); got != "hello" {
		t.Errorf("received %q, want hello", got)
	}
	if r := h.poll(sid); r.status != http.StatusOK || r.body != "4hello" {
		t.Errorf("GET = %d %q, want 200 \"4hello\"", r.status, r.body)
	}
}

func TestPollingPostSeveralPackets(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	if r := h.post(sid, "4one\x1e4two\x1e4three"); r.body != "ok" {
		t.Fatalf("POST body = %q, want ok", r.body)
	}
	for _, want := range []string{"one", "two", "three"} {
		if got := rec.text(t); got != want {
			t.Errorf("received %q, want %q", got, want)
		}
	}
}

func TestPollingPostBinary(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	h.post(sid, "bAQIDBA==\x1e4text")
	first := rec.message(t)
	if !first.Binary || !bytes.Equal(first.Data, []byte{1, 2, 3, 4}) {
		t.Errorf("first message = %+v, want binary 1,2,3,4", first)
	}
	if second := rec.message(t); second.Binary || string(second.Data) != "text" {
		t.Errorf("second message = %+v, want text", second)
	}
}

func TestPollingGetJoinsQueuedPackets(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	if err := rec.session.Send(msg("a"), msg("b")); err != nil {
		t.Fatal(err)
	}
	if err := rec.session.Send(blob(1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	}
	r := h.poll(sid)
	if r.status != http.StatusOK || r.body != "4a\x1e4b\x1ebAQIDBA==" {
		t.Errorf("GET = %d %q, want the three packets joined by the record separator", r.status, r.body)
	}
}

func TestPollingGetWaitsForMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	c := h.startPoll(sid)
	if !c.running(settle) {
		t.Fatalf("poll answered with nothing to send: %+v", c.reply)
	}
	if err := rec.session.Send(msg("late")); err != nil {
		t.Fatal(err)
	}
	if r := c.wait(t); r.status != http.StatusOK || r.body != "4late" {
		t.Errorf("GET = %d %q, want 200 \"4late\"", r.status, r.body)
	}
}

func TestPollingMessageSentInAccept(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withAccept(func(s *engineio.Session) {
		s.Send(msg("welcome"))
	}))
	r := h.poll("")
	packets := split(r.body)
	sid := parseOpen(t, packets[0]).SID
	if len(packets) < 2 {
		packets = append(packets, split(h.poll(sid).body)...)
	}
	if len(packets) != 2 || packets[1] != "4welcome" {
		t.Errorf("packets = %q, want the open packet followed by 4welcome", packets)
	}
}

func TestPollingClientClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	var last reply
	for range 5 {
		sid, rec := h.open()
		pending := h.startPoll(sid)
		if r := h.post(sid, "1"); r.status != http.StatusOK || r.body != "ok" {
			t.Fatalf("POST 1 = %d %q, want 200 \"ok\"", r.status, r.body)
		}
		last = pending.wait(t)
		if last.status == http.StatusBadRequest {
			continue
		}
		if last.status != http.StatusOK || last.body != "6" {
			t.Fatalf("pending poll = %d %q, want 200 \"6\"", last.status, last.body)
		}
		if got := rec.reason(t); got != engineio.ReasonTransportClose {
			t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportClose)
		}
		rec.ended(t)
		if r := h.poll(sid); r.status != http.StatusBadRequest {
			t.Errorf("poll after close = %d, want 400", r.status)
		}
		return
	}
	t.Fatalf("the poll never reached the server before the close; last reply %+v", last)
}

func TestPollingOverlappingGet(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	first := h.startPoll(sid)
	second := h.poll(sid)
	replies := []reply{first.wait(t), second}
	slices.SortFunc(replies, func(a, b reply) int { return a.status - b.status })
	served, rejected := replies[0], replies[1]
	if rejected.status != http.StatusBadRequest {
		t.Fatalf("overlapping poll = %d %q, want 400", rejected.status, rejected.body)
	}
	if served.status != http.StatusOK || served.body != "1" {
		t.Errorf("first poll = %d %q, want 200 \"1\"", served.status, served.body)
	}
	if got := rec.reason(t); got != engineio.ReasonTransportError {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportError)
	}
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after overlap = %d, want 400", r.status)
	}
}

func TestPollingOverlappingPost(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() })
	req, err := http.NewRequest(http.MethodPost, h.pollingURL(sid), pr)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		if resp, err := h.client.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	if _, err := pw.Write([]byte("4a")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the overlapping POST to be rejected", func() bool {
		return h.post(sid, "3").status == http.StatusBadRequest
	})
	if got := rec.reason(t); got != engineio.ReasonTransportError {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportError)
	}
	pw.Close()
	receive(t, finished, "the first POST to finish")
}

func TestPollingMalformedPost(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, body string }{
		{"text", "abc"},
		{"unknown packet type", "7"},
		{"empty body", ""},
		{"trailing separator", "4a\x1e"},
		{"bad base64", "b!!"},
		{"valid packet then garbage", "4a\x1eabc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			sid, rec := h.open()
			r := h.post(sid, tt.body)
			if r.status != http.StatusBadRequest {
				t.Fatalf("POST = %d %q, want 400", r.status, r.body)
			}
			parseFailure(t, r)
			if got := rec.reason(t); got != engineio.ReasonParseError {
				t.Errorf("reason = %q, want %q", got, engineio.ReasonParseError)
			}
			rec.ended(t)
			if r := h.poll(sid); r.status != http.StatusBadRequest {
				t.Errorf("poll after parse error = %d, want 400", r.status)
			}
		})
	}
}

func TestPollingPayloadLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		size   int
		status int
	}{
		{"at the limit", 100, http.StatusOK},
		{"one byte over", 101, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxPayload: 100})
			sid, open := h.handshake()
			if open.MaxPayload != 100 {
				t.Fatalf("maxPayload = %d, want 100", open.MaxPayload)
			}
			r := h.post(sid, "4"+strings.Repeat("a", tt.size-1))
			if r.status != tt.status {
				t.Errorf("POST of %d bytes = %d, want %d", tt.size, r.status, tt.status)
			}
			if tt.status == http.StatusOK {
				if got := h.recorder(sid).text(t); len(got) != tt.size-1 {
					t.Errorf("received %d bytes, want %d", len(got), tt.size-1)
				}
			}
		})
	}
}

func TestPollingAbortedPoll(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	conn, err := net.Dial("tcp", h.http.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET /engine.io/?%s&sid=%s HTTP/1.1\r\nHost: %s\r\n\r\n", pollingQuery, sid, h.http.Listener.Addr())
	conn.Close()
	if got := rec.reason(t); got != engineio.ReasonTransportError {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportError)
	}
}

func TestPollingGracefulClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	rec.session.Send(msg("a"), msg("b"))
	rec.session.Close(engineio.ReasonForcedClose)
	if got := rec.reason(t); got != engineio.ReasonForcedClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
	}
	if err := rec.session.Send(msg("c")); !errors.Is(err, engineio.ErrClosed) {
		t.Errorf("Send after Close = %v, want ErrClosed", err)
	}
	if r := h.poll(sid); r.status != http.StatusOK || r.body != "4a\x1e4b" {
		t.Errorf("first poll = %d %q, want only the queued packets", r.status, r.body)
	}
	if r := h.poll(sid); r.status != http.StatusOK || r.body != "1" {
		t.Errorf("second poll = %d %q, want the close packet alone", r.status, r.body)
	}
	rec.ended(t)
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after close = %d, want 400", r.status)
	}
	rec.closedOnce(t)
}

func TestPollingCloseWithNothingQueued(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	rec.session.Close(engineio.ReasonForcedClose)
	if r := h.poll(sid); r.status != http.StatusOK || r.body != "1" {
		t.Errorf("poll = %d %q, want 200 \"1\"", r.status, r.body)
	}
	rec.ended(t)
}

func TestPollingCloseWakesPendingPoll(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	pending := h.startPoll(sid)
	rec.session.Close(engineio.ReasonForcedClose)
	if r := pending.wait(t); r.status != http.StatusOK || r.body != "1" {
		t.Errorf("pending poll = %d %q, want 200 \"1\"", r.status, r.body)
	}
	rec.ended(t)
}

func TestPollingCloseKeepsQueuedPacketsApart(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	pending := h.startPoll(sid)
	rec.session.Send(msg("last"))
	rec.session.Close(engineio.ReasonForcedClose)
	if first := pending.wait(t); first.status != http.StatusOK || first.body != "4last" {
		t.Errorf("first poll = %d %q, want only the message", first.status, first.body)
	}
	if second := h.poll(sid); second.status != http.StatusOK || second.body != "1" {
		t.Errorf("second poll = %d %q, want the close packet alone", second.status, second.body)
	}
}

func TestPollingReceiveError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withReceive(func(_ *recorder, m engineio.Message) error {
		if string(m.Data) == "bad" {
			return errors.New("rejected")
		}
		return nil
	}))
	sid, rec := h.open()
	h.post(sid, "4bad")
	if got := rec.reason(t); got != engineio.ReasonParseError {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonParseError)
	}
	rec.ended(t)
	if r := h.poll(sid); r.status != http.StatusBadRequest {
		t.Errorf("poll after rejected message = %d, want 400", r.status)
	}
}

func TestPollingLargeMessage(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, _ := h.open()
	payload := strings.Repeat("héllo ✓ ", 60000)
	h.post(sid, "4"+payload)
	if r := h.poll(sid); r.body != "4"+payload {
		t.Errorf("echoed %d bytes, want %d", len(r.body), len(payload)+1)
	}
}

func TestPollingUnicode(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{}, withEcho())
	sid, rec := h.open()
	h.post(sid, "4héllo ✓ 日本語")
	if got := rec.text(t); got != "héllo ✓ 日本語" {
		t.Errorf("received %q", got)
	}
	if r := h.poll(sid); r.body != "4héllo ✓ 日本語" {
		t.Errorf("echo = %q", r.body)
	}
}

func TestPollingServerBinary(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	sid, rec := h.open()
	rec.session.Send(blob(0, 255, 16), blob())
	if r := h.poll(sid); r.body != "bAP8Q\x1eb" {
		t.Errorf("poll = %q, want %q", r.body, "bAP8Q\x1eb")
	}
}

func TestPollingUnsupportedMethod(t *testing.T) {
	t.Parallel()
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{}, withEcho())
			sid, _ := h.open()
			if r := h.request(method, h.pollingURL(sid), "", nil); r.status < http.StatusBadRequest {
				t.Errorf("%s with a sid = %d, want an error status", method, r.status)
			}
			h.post(sid, "4after")
			if r := h.poll(sid); r.status != http.StatusOK || r.body != "4after" {
				t.Errorf("poll after the refused %s = %d %q, want the echo", method, r.status, r.body)
			}
		})
	}
}
