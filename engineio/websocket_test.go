package engineio_test

import (
	"bytes"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hilmanzhy/socket.io-golang/engineio"
)

func TestWebSocketEcho(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		kind int
		data string
	}{
		{"text", websocket.TextMessage, "4hello"},
		{"unicode", websocket.TextMessage, "4héllo ✓ 日本語"},
		{"empty text message", websocket.TextMessage, "4"},
		{"record separator stays inside one frame", websocket.TextMessage, "4a\x1e4b"},
		{"binary", websocket.BinaryMessage, "\x00\x01\x02\xff"},
		{"empty binary", websocket.BinaryMessage, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{}, withEcho())
			ws, _ := h.openWS(nil)
			ws.SetWriteDeadline(time.Now().Add(wait))
			if err := ws.WriteMessage(tt.kind, []byte(tt.data)); err != nil {
				t.Fatal(err)
			}
			kind, data := ws.frame()
			if kind != tt.kind || string(data) != tt.data {
				t.Errorf("echo = %d %q, want %d %q", kind, data, tt.kind, tt.data)
			}
		})
	}
}

func TestWebSocketReceive(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	ws.write("4hi")
	ws.writeBinary([]byte{1, 2, 3, 4})
	ws.write("4a\x1e4b")
	if m := rec.message(t); m.Binary || string(m.Data) != "hi" {
		t.Errorf("first = %+v, want text hi", m)
	}
	if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3, 4}) {
		t.Errorf("second = %+v, want binary 1,2,3,4", m)
	}
	if m := rec.message(t); m.Binary || string(m.Data) != "a\x1e4b" {
		t.Errorf("third = %+v, want one message \"a\\x1e4b\"", m)
	}
}

func TestWebSocketSendFrames(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	if err := rec.session.Send(msg("a"), blob(9, 8), msg("c")); err != nil {
		t.Fatal(err)
	}
	if got := ws.text(); got != "4a" {
		t.Errorf("frame 1 = %q, want 4a", got)
	}
	if got := ws.binary(); !bytes.Equal(got, []byte{9, 8}) {
		t.Errorf("frame 2 = %v, want 9,8", got)
	}
	if got := ws.text(); got != "4c" {
		t.Errorf("frame 3 = %q, want 4c", got)
	}
}

func TestWebSocketClientPackets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		frame  string
		reason engineio.CloseReason
	}{
		{"close packet", "1", engineio.ReasonTransportClose},
		{"text", "abc", engineio.ReasonParseError},
		{"base64 record that does not decode", "b!!", engineio.ReasonParseError},
		{"unknown packet type", "7", engineio.ReasonParseError},
		{"empty frame", "", engineio.ReasonParseError},
		{"ping from the client", "2", engineio.ReasonTransportError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{})
			ws, _ := h.openWS(nil)
			rec := h.sole()
			ws.write(tt.frame)
			if err := ws.readError(); err == nil {
				t.Fatal("connection stayed open")
			}
			if got := rec.reason(t); got != tt.reason {
				t.Errorf("reason = %q, want %q", got, tt.reason)
			}
			rec.ended(t)
			rec.closedOnce(t)
		})
	}
}

func TestBase64Record(t *testing.T) {
	t.Parallel()
	expectBinary := func(t *testing.T, ws *wsConn, rec *recorder) {
		t.Helper()
		ws.write("bAQID")
		if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
			t.Fatalf("text frame bAQID = %+v, want binary 1,2,3", m)
		}
		ws.write("4still open")
		if got := rec.text(t); got != "still open" {
			t.Errorf("text after the record = %q, want still open", got)
		}
	}
	t.Run("a WebSocket text frame of a client that forces base64", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		ws, _ := h.openWS(nil)
		rec := h.sole()
		ws.writeBinary([]byte{1, 2, 3})
		if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
			t.Fatalf("binary frame = %+v, want binary 1,2,3", m)
		}
		expectBinary(t, ws, rec)
	})
	t.Run("a WebSocket text frame after an upgrade", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		sid, rec := h.open()
		expectBinary(t, h.upgrade(sid), rec)
	})
	t.Run("a polling payload", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{})
		sid, rec := h.open()
		if r := h.post(sid, "bAQID"); r.status != http.StatusOK || r.body != "ok" {
			t.Fatalf("POST = %d %q, want 200 ok", r.status, r.body)
		}
		if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
			t.Errorf("message = %+v, want binary 1,2,3", m)
		}
		if r := h.post(sid, "4a\x1ebAQID"); r.status != http.StatusOK {
			t.Fatalf("POST = %d %q, want 200", r.status, r.body)
		}
		if got := rec.text(t); got != "a" {
			t.Errorf("text = %q, want a", got)
		}
		if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
			t.Errorf("message = %+v, want binary 1,2,3", m)
		}
	})
}

func TestWebSocketCloseFrame(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	frame := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	if err := ws.WriteControl(websocket.CloseMessage, frame, time.Now().Add(wait)); err != nil {
		t.Fatal(err)
	}
	if got := rec.reason(t); got != engineio.ReasonTransportClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonTransportClose)
	}
	rec.ended(t)
}

func TestWebSocketAbruptDisconnect(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	ws.UnderlyingConn().Close()
	rec.reason(t)
	rec.ended(t)
	rec.closedOnce(t)
}

func TestWebSocketPayloadLimit(t *testing.T) {
	t.Parallel()
	t.Run("at the limit", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{MaxPayload: 64}, withEcho())
		ws, _ := h.openWS(nil)
		text := "4" + string(bytes.Repeat([]byte{'a'}, 63))
		ws.write(text)
		if got := ws.text(); got != text {
			t.Errorf("echo of %d bytes = %d bytes", len(text), len(got))
		}
		raw := bytes.Repeat([]byte{7}, 64)
		ws.writeBinary(raw)
		if got := ws.binary(); !bytes.Equal(got, raw) {
			t.Errorf("binary echo of 64 bytes = %d bytes", len(got))
		}
	})
	for _, tt := range []struct {
		name string
		send func(*wsConn)
	}{
		{"text over the limit", func(ws *wsConn) { ws.write("4" + string(bytes.Repeat([]byte{'a'}, 64))) }},
		{"binary over the limit", func(ws *wsConn) { ws.writeBinary(bytes.Repeat([]byte{7}, 65)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{MaxPayload: 64})
			ws, _ := h.openWS(nil)
			rec := h.sole()
			tt.send(ws)
			if err := ws.readError(); err == nil {
				t.Fatal("connection stayed open")
			}
			rec.reason(t)
			rec.ended(t)
			if n := len(rec.messages); n != 0 {
				t.Errorf("%d oversized messages were delivered", n)
			}
		})
	}
}

func TestWebSocketGracefulClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	rec.session.Send(msg("a"), msg("b"))
	rec.session.Close(engineio.ReasonForcedClose)
	if got := rec.reason(t); got != engineio.ReasonForcedClose {
		t.Errorf("reason = %q, want %q", got, engineio.ReasonForcedClose)
	}
	if got := ws.text(); got != "4a" {
		t.Errorf("frame 1 = %q, want 4a", got)
	}
	if got := ws.text(); got != "4b" {
		t.Errorf("frame 2 = %q, want 4b", got)
	}
	ws.SetReadDeadline(time.Now().Add(wait))
	_, _, err := ws.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Errorf("read after the queue = %v, want a normal closure", err)
	}
	rec.ended(t)
	if err := rec.session.Send(msg("c")); !errors.Is(err, engineio.ErrClosed) {
		t.Errorf("Send after Close = %v, want ErrClosed", err)
	}
	rec.closedOnce(t)
}

func TestWebSocketSendDoesNotBlock(t *testing.T) {
	t.Parallel()
	const count, size = 8, 1 << 20
	h := newHarness(t, engineio.Options{MaxBuffer: 64 << 20})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	sent := make(chan error, 1)
	go func() {
		for i := range count {
			data := bytes.Repeat([]byte{byte('a' + i)}, size)
			if err := rec.session.Send(engineio.Message{Data: data}); err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	if err := receive(t, sent, "Send to return while the client is not reading"); err != nil {
		t.Fatal(err)
	}
	for i := range count {
		got := ws.text()
		if len(got) != size+1 || got[1] != byte('a'+i) {
			t.Fatalf("frame %d has %d bytes, want %d", i, len(got), size+1)
		}
	}
}

func TestWebSocketRequestUsableAfterClose(t *testing.T) {
	t.Parallel()
	h := newHarness(t, engineio.Options{})
	ws, _ := h.openWS(nil)
	rec := h.sole()
	ws.Close()
	rec.reason(t)
	rec.ended(t)
	if err := rec.session.Request().Context().Err(); err != nil {
		t.Errorf("request context is done after the handler returned: %v", err)
	}
}

// expectBlob reads the frame that carries the binary message 1,2,3: a text
// frame "bAQID" when text is set, a binary frame otherwise.
func expectBlob(t *testing.T, ws *wsConn, text bool) {
	t.Helper()
	want, wantKind := []byte{1, 2, 3}, websocket.BinaryMessage
	if text {
		want, wantKind = []byte("bAQID"), websocket.TextMessage
	}
	if kind, data := ws.frame(); kind != wantKind || !bytes.Equal(data, want) {
		t.Errorf("frame = %d %q, want %d %q", kind, data, wantKind, want)
	}
}

// upgradeWithQuery runs the upgrade of sid like upgrade does, with extra
// appended to the query of the WebSocket request.
func (h *harness) upgradeWithQuery(sid, extra string) *wsConn {
	h.t.Helper()
	pending := h.async(http.MethodGet, h.pollingURL(sid), "")
	ws, _, err := h.connect(socketQuery+"&sid="+sid+extra, nil)
	if err != nil {
		h.t.Fatalf("dial upgrade: %v", err)
	}
	ws.write("2probe")
	if got := ws.text(); got != "3probe" {
		h.t.Fatalf("probe answer = %q, want 3probe", got)
	}
	if r := pending.wait(h.t); r.status != http.StatusOK || r.body != "6" {
		h.t.Fatalf("pending poll = %d %q, want 200 \"6\"", r.status, r.body)
	}
	ws.write("5")
	return ws
}

// TestWebSocketB64Query checks that the b64 query parameter turns the binary
// packets of the WebSocket the request opens into text frames "b<base64>", as
// in the reference: any non-empty value counts, and the last one wins.
func TestWebSocketB64Query(t *testing.T) {
	t.Parallel()
	queries := []struct {
		name  string
		query string
		text  bool
	}{
		{"absent", "", false},
		{"1", "&b64=1", true},
		{"0 is a value like any other", "&b64=0", true},
		{"empty", "&b64=", false},
		{"without a value", "&b64", false},
		{"an empty value then 1", "&b64=&b64=1", true},
		{"1 then an empty value", "&b64=1&b64=", false},
	}
	exchange := func(t *testing.T, ws *wsConn, rec *recorder, text bool) {
		t.Helper()
		if err := rec.session.Send(blob(1, 2, 3), msg("a")); err != nil {
			t.Fatal(err)
		}
		expectBlob(t, ws, text)
		if got := ws.text(); got != "4a" {
			t.Errorf("text frame = %q, want 4a", got)
		}
		ws.write("bAQID")
		if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
			t.Fatalf("text frame bAQID = %+v, want binary 1,2,3", m)
		}
		expectBlob(t, ws, text)
		ws.writeBinary([]byte{1, 2, 3})
		if m := rec.message(t); !m.Binary || !bytes.Equal(m.Data, []byte{1, 2, 3}) {
			t.Fatalf("binary frame = %+v, want binary 1,2,3", m)
		}
		expectBlob(t, ws, text)
	}
	for _, q := range queries {
		t.Run("handshake "+q.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{}, withEcho())
			ws, _, err := h.connect(socketQuery+q.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			parseOpen(t, ws.text())
			exchange(t, ws, h.sole(), q.text)
		})
		t.Run("upgrade "+q.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, engineio.Options{}, withEcho())
			sid, rec := h.open()
			exchange(t, h.upgradeWithQuery(sid, q.query), rec, q.text)
		})
	}
	t.Run("the upgrade request decides, not the handshake", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, engineio.Options{}, withEcho())
		r := h.request(http.MethodGet, h.url(pollingQuery+"&b64=1"), "", nil)
		rec := h.recorder(parseOpen(t, split(r.body)[0]).SID)
		exchange(t, h.upgradeWithQuery(rec.session.ID(), ""), rec, false)
	})
}
