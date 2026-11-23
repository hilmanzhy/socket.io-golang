package parser_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

func TestTypeValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		typ  parser.Type
		want int
		name string
	}{
		{parser.Connect, 0, "CONNECT"},
		{parser.Disconnect, 1, "DISCONNECT"},
		{parser.Event, 2, "EVENT"},
		{parser.Ack, 3, "ACK"},
		{parser.ConnectError, 4, "CONNECT_ERROR"},
		{parser.BinaryEvent, 5, "BINARY_EVENT"},
		{parser.BinaryAck, 6, "BINARY_ACK"},
		{parser.Type(7), 7, "UNKNOWN"},
		{parser.Type(255), 255, "UNKNOWN"},
	}
	for _, tc := range tests {
		t.Run(tc.name+" "+fmt.Sprint(tc.want), func(t *testing.T) {
			t.Parallel()
			if int(tc.typ) != tc.want {
				t.Errorf("type value = %d, want %d", tc.typ, tc.want)
			}
			if got := tc.typ.String(); got != tc.name {
				t.Errorf("String() = %q, want %q", got, tc.name)
			}
		})
	}
}

func TestDefaultNamespace(t *testing.T) {
	t.Parallel()
	if parser.DefaultNamespace != "/" {
		t.Fatalf("DefaultNamespace = %q, want %q", parser.DefaultNamespace, "/")
	}
}

func TestEncodeVectors(t *testing.T) {
	t.Parallel()
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			p := v.packet
			if got := string(p.Encode()); got != v.wire {
				t.Fatalf("Encode() = %q, want %q", got, v.wire)
			}
		})
	}
}

func TestEncodeOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		packet parser.Packet
		want   string
	}{
		{"empty namespace is omitted", parser.Packet{Type: parser.Event, Data: []byte(`["foo"]`)}, `2["foo"]`},
		{"default namespace is omitted", parser.Packet{Type: parser.Event, Namespace: "/", Data: []byte(`["foo"]`)}, `2["foo"]`},
		{"connect on empty namespace", parser.Packet{Type: parser.Connect}, `0`},
		{"disconnect on named namespace", parser.Packet{Type: parser.Disconnect, Namespace: "/chat"}, `1/chat,`},
		{"ack id zero is written", parser.Packet{Type: parser.Event, ID: 0, HasID: true, Data: []byte(`["foo"]`)}, `20["foo"]`},
		{"ack id is ignored without HasID", parser.Packet{Type: parser.Event, ID: 7, Data: []byte(`["foo"]`)}, `2["foo"]`},
		{"largest ack id", parser.Packet{Type: parser.Event, ID: math.MaxUint64, HasID: true, Data: []byte(`["foo"]`)}, `218446744073709551615["foo"]`},
		{"namespace and ack id", parser.Packet{Type: parser.Ack, Namespace: "/a", ID: 3, HasID: true, Data: []byte(`[]`)}, `3/a,3[]`},
		{"binary event announces zero attachments", parser.Packet{Type: parser.BinaryEvent, Data: []byte(`["foo"]`)}, `50-["foo"]`},
		{"binary event announces ten attachments", parser.Packet{Type: parser.BinaryEvent, Data: []byte(`["foo"]`), Attachments: make([][]byte, 10)}, `510-["foo"]`},
		{"binary ack with namespace and id zero", parser.Packet{Type: parser.BinaryAck, Namespace: "/x", HasID: true, Data: []byte(`[]`), Attachments: make([][]byte, 2)}, `62-/x,0[]`},
		{"text type does not announce attachments", parser.Packet{Type: parser.Event, Data: []byte(`["foo"]`), Attachments: make([][]byte, 2)}, `2["foo"]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := string(tc.packet.Encode()); got != tc.want {
				t.Fatalf("Encode() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEncodeDoesNotModifyPacket(t *testing.T) {
	t.Parallel()
	p := vectors[len(vectors)-1].packet
	before := show(&p)
	p.Encode()
	p.Encode()
	if after := show(&p); after != before {
		t.Fatalf("packet changed: %s, was %s", after, before)
	}
}

var roundTripExtras = []parser.Packet{
	{Type: parser.Connect, Namespace: "/", Data: []byte(`{}`)},
	{Type: parser.Connect, Namespace: "/chat/room-1", Data: []byte(`{"a":{"b":[1,2,{"c":null}]}}`)},
	{Type: parser.Disconnect, Namespace: "/ünï"},
	{Type: parser.Event, Namespace: "/", ID: 0, HasID: true, Data: []byte(`["foo"]`)},
	{Type: parser.Event, Namespace: "/chat", ID: math.MaxUint64, HasID: true, Data: []byte(`["foo",1,"two",[3],{"four":4},null]`)},
	{Type: parser.Event, Namespace: "/room1", ID: 7, HasID: true, Data: []byte(`["foo"]`)},
	{Type: parser.Ack, Namespace: "/", ID: 0, HasID: true, Data: []byte(`[]`)},
	{Type: parser.Ack, Namespace: "/chat", ID: 99, HasID: true, Data: []byte(`[null,"x"]`)},
	{Type: parser.ConnectError, Namespace: "/", Data: []byte(`{}`)},
	{Type: parser.ConnectError, Namespace: "/chat", Data: []byte(`{"message":"no","data":{"code":1}}`)},
	{Type: parser.BinaryEvent, Namespace: "/", Data: []byte(`["x",` + ph0 + `,` + ph1 + `,` + ph2 + `]`), Attachments: [][]byte{{}, {0, 255}, {1}}},
	{Type: parser.BinaryEvent, Namespace: "/a-b", ID: 5, HasID: true, Data: []byte(`["x",{"k":` + ph0 + `}]`), Attachments: [][]byte{{9, 9}}},
	{Type: parser.BinaryAck, Namespace: "/", ID: 0, HasID: true, Data: []byte(`[` + ph0 + `]`), Attachments: [][]byte{{1, 2, 3}}},
}

func roundTrips() []parser.Packet {
	out := make([]parser.Packet, 0, len(vectors)+len(roundTripExtras))
	for _, v := range vectors {
		out = append(out, v.packet)
	}
	return append(out, roundTripExtras...)
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	for i, p := range roundTrips() {
		t.Run(fmt.Sprintf("%02d %v", i, p.Type), func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			got, err := decodeAll(&d, string(p.Encode()), p.Attachments)
			if err != nil {
				t.Fatalf("decode %q: %v", p.Encode(), err)
			}
			assertPacket(t, got, &p)
		})
	}
}

func TestRoundTripCoversEveryType(t *testing.T) {
	t.Parallel()
	seen := map[parser.Type]bool{}
	for _, p := range roundTrips() {
		seen[p.Type] = true
	}
	for typ := parser.Connect; typ <= parser.BinaryAck; typ++ {
		if !seen[typ] {
			t.Errorf("no round trip case for %v", typ)
		}
	}
}

func TestRoundTripOnOneDecoder(t *testing.T) {
	t.Parallel()
	var d parser.Decoder
	for i, p := range roundTrips() {
		got, err := decodeAll(&d, string(p.Encode()), p.Attachments)
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		assertPacket(t, got, &p)
	}
}
