package engineio_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nuriontech/socket.io-golang/engineio"
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
