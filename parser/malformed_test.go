package parser_test

import (
	"fmt"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

var reservedNames = []string{"connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener"}

func TestTextMalformed(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, wire string }{
		{"empty frame", ""},
		{"type 7", `7`},
		{"type 8", `8`},
		{"type 9", `9`},
		{"type 9 with payload", `9["foo"]`},
		{"lowercase letter", `a`},
		{"uppercase letter", `A`},
		{"slash", `/`},
		{"leading space", ` 2["foo"]`},
		{"NUL", "\x00"},
		{"high byte", "\xff"},
		{"non ASCII digit", "٣[\"foo\"]"},

		{"connect error garbage", `4abc`},
		{"connect error empty", `4`},
		{"connect error truncated object", `4{"message":"x"`},
		{"connect error namespace only", `4/admin,`},
		{"connect error trailing garbage", `4{"message":"x"}y`},

		{"event empty", `2`},
		{"event object payload", `2{}`},
		{"event not JSON", `2abc`},
		{"event string payload", `2"foo"`},
		{"event number payload", `2123`},
		{"event ack id and no payload", `212`},
		{"event namespace and no payload", `2/admin,`},
		{"event namespace without comma or payload", `2/admin`},
		{"event leading space in payload", `2 ["foo"]`},

		{"ack empty", `3`},
		{"ack object payload", `3{}`},
		{"ack id and no payload", `312`},
		{"ack namespace and no payload", `3/admin,13`},

		{"connect array payload", `0[]`},
		{"connect string payload", `0"token"`},
		{"connect null payload", `0null`},
		{"connect boolean payload", `0true`},
		{"connect truncated object", `0{"token":`},
		{"connect unquoted key", `0{token:1}`},
		{"connect trailing garbage", `0{}x`},
		{"connect namespace array payload", `0/admin,[]`},
		{"connect plain text payload", `0abc`},

		{"disconnect object payload", `1{}`},
		{"disconnect array payload", `1["foo"]`},
		{"disconnect namespace object payload", `1/admin,{}`},
		{"disconnect text payload", `1x`},

		{"ack id overflows uint64 on event", `218446744073709551616["foo"]`},
		{"ack id overflows uint64 with namespace", `2/admin,18446744073709551616["foo"]`},
		{"ack id overflows uint64 on ack", `318446744073709551616["foo"]`},
		{"ack id far above uint64", `299999999999999999999999999999["foo"]`},

		{"binary event without count", `5-["x"]`},
		{"binary event without dash", `51["x"]`},
		{"binary event without count or dash", `5["x"]`},
		{"binary event count not a number", `5x-["x"]`},
		{"binary event negative count", `5-1-["x"]`},
		{"binary event signed count", `5+1-["x"]`},
		{"binary event count above int31", `52147483648-["x"]`},
		{"binary event count above uint64", `518446744073709551616-["x"]`},
		{"binary event empty after type", `5`},
		{"binary event count only", `51-`},
		{"binary event object payload", `51-{}`},
		{"binary event namespace only", `51-/admin,`},
		{"binary ack without count", `6-[1]`},
		{"binary ack without dash", `61[1]`},
		{"binary ack id only", `61-15`},
		{"binary ack object payload", `61-15{}`},

		{"namespace without slash on event", `2admin,["foo"]`},
		{"namespace without slash on connect", `0admin,`},
		{"namespace without slash on disconnect", `1admin,`},
		{"namespace without slash on connect error", `4admin,{"message":"x"}`},
		{"namespace without slash on ack", `3admin,1["x"]`},
		{"namespace without slash on binary event", `51-admin,["x"]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			p, err := d.Text([]byte(tc.wire))
			if p != nil {
				t.Fatalf("packet = %s, want nil", show(p))
			}
			requireMalformed(t, err)
		})
	}
}

func TestPayloadMalformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wire        string
		attachments [][]byte
	}{
		{"event empty array", `2[]`, nil},
		{"event with ack id and empty array", `212[]`, nil},
		{"event namespace and empty array", `2/admin,[]`, nil},
		{"event truncated array", `2["foo"`, nil},
		{"event invalid JSON element", `2[abc]`, nil},
		{"event invalid JSON in argument", `2["foo",{bad}]`, nil},
		{"event trailing garbage", `2["foo"]x`, nil},
		{"event trailing array", `2["foo"][1]`, nil},
		{"event dangling comma", `2["foo",]`, nil},
		{"event empty first element", `2[,"foo"]`, nil},
		{"event first element number", `2[1,"x"]`, nil},
		{"event first element null", `2[null]`, nil},
		{"event first element null with arguments", `2[null,"x"]`, nil},
		{"event first element true", `2[true]`, nil},
		{"event first element object", `2[{}]`, nil},
		{"event first element named object", `2[{"event":"foo"}]`, nil},
		{"event first element array", `2[["foo"]]`, nil},

		{"ack truncated array", `3["bar"`, nil},
		{"ack invalid JSON element", `3[abc]`, nil},
		{"ack trailing garbage", `312["bar"]x`, nil},

		{"binary event empty array", `51-[]`, blobs(1)},
		{"binary event first element is binary", `51-[` + ph0 + `]`, blobs(1)},
		{"binary event first element number", `51-[1,` + ph0 + `]`, blobs(1)},
		{"binary event truncated array", `51-["x",` + ph0, blobs(1)},
		{"binary event placeholder out of range", `51-["x",` + ph1 + `]`, blobs(1)},
		{"binary event placeholder far out of range", `51-["x",{"_placeholder":true,"num":99}]`, blobs(1)},
		{"binary event negative placeholder", `51-["x",{"_placeholder":true,"num":-1}]`, blobs(1)},
		{"binary event placeholder without num", `51-["x",{"_placeholder":true}]`, blobs(1)},
		{"binary event placeholder with null num", `51-["x",{"_placeholder":true,"num":null}]`, blobs(1)},
		{"binary event placeholder with string num", `51-["x",{"_placeholder":true,"num":"0"}]`, blobs(1)},
		{"binary event placeholder with fractional num", `51-["x",{"_placeholder":true,"num":0.5}]`, blobs(1)},
		{"binary event placeholder with huge num", `51-["x",{"_placeholder":true,"num":99999999999999999999}]`, blobs(1)},
		{"binary event second placeholder out of range", `52-["x",` + ph0 + `,` + ph2 + `]`, blobs(2)},
		{"binary event nested placeholder out of range", `51-["x",{"a":{"_placeholder":true,"num":3}}]`, blobs(1)},
		{"binary event nested placeholder without num", `51-["x",{"a":[{"_placeholder":true}]}]`, blobs(1)},
		{"binary event nested negative placeholder", `51-["x",[{"_placeholder":true,"num":-2}]]`, blobs(1)},
		{"binary event nested placeholder with string num", `51-["x",{"a":{"_placeholder":true,"num":"0"}}]`, blobs(1)},
		{"binary event placeholder whose last num is a string", `51-["x",{"num":0,"_placeholder":true,"num":"0"}]`, blobs(1)},
		{"binary event placeholder whose last num is an array", `51-["x",{"_placeholder":true,"num":0,"num":[0]}]`, blobs(1)},
		{"binary event placeholder whose last num is an object", `51-["x",{"_placeholder":true,"num":0,"num":{"num":0}}]`, blobs(1)},
		{"binary event placeholder with an exponent", `51-["x",{"_placeholder":true,"num":0e0}]`, blobs(1)},
		{"binary event placeholder with a zero fraction", `51-["x",{"_placeholder":true,"num":0.0}]`, blobs(1)},
		{"binary event nested placeholder with members in another order", `51-["x",{"a":[{"num":1,"_placeholder":true}]}]`, blobs(1)},
		{"binary event nested placeholder repeated in another order", `51-["x",{"a":{"num":0,"_placeholder":true},"b":[` + ph0 + `]}]`, blobs(1)},
		{"binary event placeholder repeated", `51-["x",` + ph0 + `,` + ph0 + `]`, blobs(1)},
		{"binary event placeholder repeated among others", `52-["x",` + ph0 + `,` + ph1 + `,` + ph0 + `]`, blobs(2)},
		{"binary event nested placeholder repeated in an object", `51-["x",{"a":` + ph0 + `,"b":` + ph0 + `}]`, blobs(1)},
		{"binary event nested placeholder repeated in an array", `51-["x",[` + ph0 + `,` + ph0 + `]]`, blobs(1)},
		{"binary event placeholder repeated across arguments", `51-["x",` + ph0 + `,{"a":` + ph0 + `}]`, blobs(1)},
		{"binary event nested placeholder repeated across arguments", `51-["x",{"a":` + ph0 + `},[` + ph0 + `]]`, blobs(1)},

		{"binary ack placeholder out of range", `61-1[{"_placeholder":true,"num":2}]`, blobs(1)},
		{"binary ack placeholder without num", `61-1[{"_placeholder":true}]`, blobs(1)},
		{"binary ack nested placeholder out of range", `61-1[{"a":` + ph1 + `}]`, blobs(1)},
		{"binary ack placeholder repeated", `61-1[` + ph0 + `,` + ph0 + `]`, blobs(1)},
		{"binary ack nested placeholder repeated", `61-1[{"a":[` + ph0 + `,` + ph0 + `]}]`, blobs(1)},
		{"binary ack truncated array", `61-1[{`, blobs(1)},
		{"binary ack trailing garbage", `61-1[` + ph0 + `]x`, blobs(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			p, err := decodeAll(&d, tc.wire, tc.attachments)
			if err == nil {
				err = parsePayload(p)
			}
			requireMalformed(t, err)
		})
	}
}

func TestReservedEventNamesAreMalformed(t *testing.T) {
	t.Parallel()
	for _, name := range reservedNames {
		wires := map[string]struct {
			wire        string
			attachments [][]byte
		}{
			"plain":             {`2["` + name + `"]`, nil},
			"with arguments":    {`2["` + name + `","x",1]`, nil},
			"with ack id":       {`212["` + name + `"]`, nil},
			"with namespace":    {`2/admin,13["` + name + `","x"]`, nil},
			"binary":            {`51-["` + name + `",` + ph0 + `]`, blobs(1)},
			"binary namespace":  {`52-/admin,["` + name + `",` + ph0 + `,` + ph1 + `]`, blobs(2)},
			"unicode escape":    {fmt.Sprintf("2[\"\\u%04x%s\"]", name[0], name[1:]), nil},
			"padded whitespace": {`2[ "` + name + `" ]`, nil},
		}
		for kind, tc := range wires {
			t.Run(name+" "+kind, func(t *testing.T) {
				t.Parallel()
				var d parser.Decoder
				p, err := decodeAll(&d, tc.wire, tc.attachments)
				if err == nil {
					err = parsePayload(p)
				}
				requireMalformed(t, err)
			})
		}
	}
}

func TestLookalikeEventNamesAreAccepted(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Connect", "connected", "connect_", "connection", "message", "error", "DISCONNECT", "new_listener", " connect"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, _ := mustEvent(t, `2["`+name+`"]`)
			if got != name {
				t.Fatalf("name = %q, want %q", got, name)
			}
		})
	}
}

func TestBinaryFrameWithoutPendingPacket(t *testing.T) {
	t.Parallel()
	for _, frame := range [][]byte{nil, {}, {0}, []byte("hello")} {
		var d parser.Decoder
		p, err := d.Binary(frame)
		if p != nil {
			t.Fatalf("Binary(%q) = %s, want nil", frame, show(p))
		}
		requireMalformed(t, err)
	}
}

func TestBinaryFrameAfterCompletePacket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wire string
		atts [][]byte
	}{
		{"text packet", `2["foo"]`, nil},
		{"connect", `0`, nil},
		{"binary packet", `51-["x",` + ph0 + `]`, blobs(1)},
		{"binary ack", `61-1[` + ph0 + `]`, blobs(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			if _, err := decodeAll(&d, tc.wire, tc.atts); err != nil {
				t.Fatal(err)
			}
			p, err := d.Binary([]byte{1})
			if p != nil {
				t.Fatalf("Binary = %s, want nil", show(p))
			}
			requireMalformed(t, err)
		})
	}
}

func TestTextFrameWhilePending(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		first    string
		received int
		next     string
	}{
		{"event after binary event", `51-["x",` + ph0 + `]`, 0, `2["foo"]`},
		{"binary event after binary event", `51-["x",` + ph0 + `]`, 0, `51-["y",` + ph0 + `]`},
		{"disconnect after binary event", `51-["x",` + ph0 + `]`, 0, `1`},
		{"invalid frame after binary event", `51-["x",` + ph0 + `]`, 0, `9`},
		{"event after partial attachments", `53-["x",` + ph0 + `,` + ph1 + `,` + ph2 + `]`, 2, `2["foo"]`},
		{"event after binary ack", `61-1[` + ph0 + `]`, 0, `2["foo"]`},
		{"empty frame while pending", `51-["x",` + ph0 + `]`, 0, ``},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			if p, err := d.Text([]byte(tc.first)); p != nil || err != nil {
				t.Fatalf("first Text = %s, %v; want nil, nil", show(p), err)
			}
			for i := range tc.received {
				if p, err := d.Binary([]byte{byte(i)}); p != nil || err != nil {
					t.Fatalf("Binary %d = %s, %v; want nil, nil", i, show(p), err)
				}
			}
			p, err := d.Text([]byte(tc.next))
			if p != nil {
				t.Fatalf("Text = %s, want nil", show(p))
			}
			requireMalformed(t, err)
		})
	}
}
