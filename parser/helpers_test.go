package parser_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

const (
	ph0 = `{"_placeholder":true,"num":0}`
	ph1 = `{"_placeholder":true,"num":1}`
	ph2 = `{"_placeholder":true,"num":2}`
)

type vector struct {
	name   string
	wire   string
	packet parser.Packet
}

var vectors = []vector{
	{"connect root", `0`, parser.Packet{Type: parser.Connect, Namespace: "/"}},
	{"connect with auth", `0{"token":"123"}`, parser.Packet{Type: parser.Connect, Namespace: "/", Data: []byte(`{"token":"123"}`)}},
	{"connect admin with payload", `0/admin,{"sid":"oSO0OpakMV_3jnilAAAA"}`, parser.Packet{Type: parser.Connect, Namespace: "/admin", Data: []byte(`{"sid":"oSO0OpakMV_3jnilAAAA"}`)}},
	{"disconnect root", `1`, parser.Packet{Type: parser.Disconnect, Namespace: "/"}},
	{"disconnect admin", `1/admin,`, parser.Packet{Type: parser.Disconnect, Namespace: "/admin"}},
	{"event", `2["foo"]`, parser.Packet{Type: parser.Event, Namespace: "/", Data: []byte(`["foo"]`)}},
	{"event admin", `2/admin,["foo"]`, parser.Packet{Type: parser.Event, Namespace: "/admin", Data: []byte(`["foo"]`)}},
	{"event with ack id", `212["foo"]`, parser.Packet{Type: parser.Event, Namespace: "/", ID: 12, HasID: true, Data: []byte(`["foo"]`)}},
	{"event admin with ack id", `2/admin,13["foo","bar"]`, parser.Packet{Type: parser.Event, Namespace: "/admin", ID: 13, HasID: true, Data: []byte(`["foo","bar"]`)}},
	{"ack", `312["bar"]`, parser.Packet{Type: parser.Ack, Namespace: "/", ID: 12, HasID: true, Data: []byte(`["bar"]`)}},
	{"ack admin", `3/admin,13["bar"]`, parser.Packet{Type: parser.Ack, Namespace: "/admin", ID: 13, HasID: true, Data: []byte(`["bar"]`)}},
	{"connect error", `4{"message":"Not authorized"}`, parser.Packet{Type: parser.ConnectError, Namespace: "/", Data: []byte(`{"message":"Not authorized"}`)}},
	{"connect error admin", `4/admin,{"message":"Not authorized"}`, parser.Packet{Type: parser.ConnectError, Namespace: "/admin", Data: []byte(`{"message":"Not authorized"}`)}},
	{"binary event", `51-["baz",` + ph0 + `]`, parser.Packet{Type: parser.BinaryEvent, Namespace: "/", Data: []byte(`["baz",` + ph0 + `]`), Attachments: [][]byte{{1, 2, 3}}}},
	{"binary event admin with two attachments", `52-/admin,["baz",` + ph0 + `,` + ph1 + `]`, parser.Packet{Type: parser.BinaryEvent, Namespace: "/admin", Data: []byte(`["baz",` + ph0 + `,` + ph1 + `]`), Attachments: [][]byte{{1}, {2, 3}}}},
	{"binary ack", `61-15["bar",` + ph0 + `]`, parser.Packet{Type: parser.BinaryAck, Namespace: "/", ID: 15, HasID: true, Data: []byte(`["bar",` + ph0 + `]`), Attachments: [][]byte{{4, 5}}}},
	{"binary ack admin", `61-/admin,1[` + ph0 + `]`, parser.Packet{Type: parser.BinaryAck, Namespace: "/admin", ID: 1, HasID: true, Data: []byte(`[` + ph0 + `]`), Attachments: [][]byte{{6}}}},
}

func show(p *parser.Packet) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{%v ns=%q id=%d hasID=%t data=%q attachments=%q}", p.Type, p.Namespace, p.ID, p.HasID, p.Data, p.Attachments)
}

func equalPacket(a, b *parser.Packet) bool {
	return a.Type == b.Type && a.Namespace == b.Namespace && a.ID == b.ID && a.HasID == b.HasID &&
		bytes.Equal(a.Data, b.Data) && slices.EqualFunc(a.Attachments, b.Attachments, bytes.Equal)
}

func assertPacket(t *testing.T, got, want *parser.Packet) {
	t.Helper()
	if got == nil || !equalPacket(got, want) {
		t.Fatalf("packet = %s, want %s", show(got), show(want))
	}
}

func decodeAll(d *parser.Decoder, wire string, attachments [][]byte) (*parser.Packet, error) {
	p, err := d.Text([]byte(wire))
	for _, blob := range attachments {
		if err != nil {
			return nil, err
		}
		p, err = d.Binary(blob)
	}
	return p, err
}

func mustDecode(t *testing.T, wire string, attachments ...[]byte) *parser.Packet {
	t.Helper()
	var d parser.Decoder
	p, err := decodeAll(&d, wire, attachments)
	if err != nil || p == nil {
		t.Fatalf("decode %q: packet %s, error %v", wire, show(p), err)
	}
	return p
}

func mustEvent(t *testing.T, wire string, attachments ...[]byte) (string, parser.Args) {
	t.Helper()
	name, args, err := parser.ParseEvent(mustDecode(t, wire, attachments...))
	if err != nil {
		t.Fatalf("ParseEvent %q: %v", wire, err)
	}
	return name, args
}

func mustAck(t *testing.T, wire string, attachments ...[]byte) parser.Args {
	t.Helper()
	args, err := parser.ParseAck(mustDecode(t, wire, attachments...))
	if err != nil {
		t.Fatalf("ParseAck %q: %v", wire, err)
	}
	return args
}

func parsePayload(p *parser.Packet) error {
	switch p.Type {
	case parser.Event, parser.BinaryEvent:
		_, _, err := parser.ParseEvent(p)
		return err
	case parser.Ack, parser.BinaryAck:
		_, err := parser.ParseAck(p)
		return err
	}
	return nil
}

func requireMalformed(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, parser.ErrMalformed) {
		t.Fatalf("error = %v, want one wrapping ErrMalformed", err)
	}
}

func blobs(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte{byte(i), byte(i) + 1}
	}
	return out
}

func binaryEventWire(n int) string {
	var b strings.Builder
	b.WriteString("5" + strconv.Itoa(n) + `-["ev"`)
	for i := range n {
		fmt.Fprintf(&b, `,{"_placeholder":true,"num":%d}`, i)
	}
	b.WriteByte(']')
	return b.String()
}
