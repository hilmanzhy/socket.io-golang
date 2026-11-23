package parser_test

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

func TestDecodeVectors(t *testing.T) {
	t.Parallel()
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			got, err := decodeAll(&d, v.wire, v.packet.Attachments)
			if err != nil {
				t.Fatalf("decode %q: %v", v.wire, err)
			}
			assertPacket(t, got, &v.packet)
		})
	}
}

func TestDecoderReusedAcrossVectors(t *testing.T) {
	t.Parallel()
	var d parser.Decoder
	for _, v := range vectors {
		got, err := decodeAll(&d, v.wire, v.packet.Attachments)
		if err != nil {
			t.Fatalf("%s: decode %q: %v", v.name, v.wire, err)
		}
		assertPacket(t, got, &v.packet)
	}
}

func TestTextReturnsNothingWhileAttachmentsPending(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2, 3, parser.DefaultMaxAttachments} {
		t.Run(strconv.Itoa(n)+" attachments", func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			p, err := d.Text([]byte(binaryEventWire(n)))
			if p != nil || err != nil {
				t.Fatalf("Text = %s, %v; want nil, nil", show(p), err)
			}
			frames := blobs(n)
			for i, frame := range frames {
				p, err = d.Binary(frame)
				if err != nil {
					t.Fatalf("Binary %d: %v", i, err)
				}
				if complete := i == n-1; complete != (p != nil) {
					t.Fatalf("after attachment %d of %d: packet %s", i+1, n, show(p))
				}
			}
			if p.Type != parser.BinaryEvent || p.Namespace != "/" {
				t.Fatalf("packet = %s", show(p))
			}
			if len(p.Attachments) != n {
				t.Fatalf("got %d attachments, want %d", len(p.Attachments), n)
			}
			for i, frame := range frames {
				if !bytes.Equal(p.Attachments[i], frame) {
					t.Errorf("attachment %d = %v, want %v", i, p.Attachments[i], frame)
				}
			}
			next, err := d.Text([]byte(`2["foo"]`))
			if err != nil || next == nil {
				t.Fatalf("next packet = %s, %v", show(next), err)
			}
		})
	}
}

func TestBinaryAckReassembly(t *testing.T) {
	t.Parallel()
	var d parser.Decoder
	p, err := d.Text([]byte(`62-/admin,9[` + ph0 + `,` + ph1 + `]`))
	if p != nil || err != nil {
		t.Fatalf("Text = %s, %v; want nil, nil", show(p), err)
	}
	if p, err = d.Binary([]byte{1}); p != nil || err != nil {
		t.Fatalf("first Binary = %s, %v; want nil, nil", show(p), err)
	}
	p, err = d.Binary([]byte{2})
	if err != nil {
		t.Fatalf("second Binary: %v", err)
	}
	want := parser.Packet{
		Type: parser.BinaryAck, Namespace: "/admin", ID: 9, HasID: true,
		Data:        []byte(`[` + ph0 + `,` + ph1 + `]`),
		Attachments: [][]byte{{1}, {2}},
	}
	assertPacket(t, p, &want)
}

func TestDecoderRetainsBinaryFramesInOrder(t *testing.T) {
	t.Parallel()
	args := mustAck(t, `63-1[`+ph2+`,`+ph0+`,`+ph1+`]`, []byte("zero"), []byte("one"), []byte("two"))
	for i, want := range []string{"two", "zero", "one"} {
		if got := string(args.Bytes(i)); got != want {
			t.Errorf("argument %d = %q, want %q", i, got, want)
		}
	}
}

func TestMaxAttachments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		limit    int
		count    int64
		accepted bool
	}{
		{0, 1, true},
		{0, int64(parser.DefaultMaxAttachments), true},
		{0, int64(parser.DefaultMaxAttachments) + 1, false},
		{1, 1, true},
		{1, 2, false},
		{2, 2, true},
		{2, 3, false},
		{100, 100, true},
		{100, 101, false},
		{-1, 1, true},
		{-1, int64(parser.DefaultMaxAttachments), true},
		{-1, int64(parser.DefaultMaxAttachments) + 1, false},
		{0, math.MaxInt32, false},
		{0, math.MaxInt32 + 1, false},
		{1000, math.MaxInt32, false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("limit %d count %d", tc.limit, tc.count), func(t *testing.T) {
			t.Parallel()
			d := parser.Decoder{MaxAttachments: tc.limit, MaxArguments: looseArguments}
			if !tc.accepted {
				p, err := d.Text([]byte(fmt.Sprintf(`5%d-["ev"]`, tc.count)))
				if p != nil {
					t.Fatalf("packet = %s, want nil", show(p))
				}
				requireMalformed(t, err)
				if next, err := d.Text([]byte(`2["foo"]`)); err != nil || next == nil {
					t.Fatalf("decoder unusable after rejection: %s, %v", show(next), err)
				}
				return
			}
			n := int(tc.count)
			p, err := decodeAll(&d, binaryEventWire(n), blobs(n))
			if err != nil || p == nil || len(p.Attachments) != n {
				t.Fatalf("packet = %s, error %v", show(p), err)
			}
		})
	}
}

func TestNamespaces(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wire        string
		attachments [][]byte
		want        string
	}{
		{"no namespace", `2["foo"]`, nil, "/"},
		{"explicit root", `2/,["foo"]`, nil, "/"},
		{"named", `2/admin,["foo"]`, nil, "/admin"},
		{"nested path", `2/chat/room,["foo"]`, nil, "/chat/room"},
		{"digits in name are not an ack id", `2/room1,["foo"]`, nil, "/room1"},
		{"digits in name followed by ack id", `2/room1,7["foo"]`, nil, "/room1"},
		{"dash in binary namespace", `51-/a-b,["x",` + ph0 + `]`, blobs(1), "/a-b"},
		{"non ASCII", `2/ünï,["foo"]`, nil, "/ünï"},
		{"connect", `0/admin,`, nil, "/admin"},
		{"disconnect", `1/admin,`, nil, "/admin"},
		{"connect error", `4/admin,{"message":"no"}`, nil, "/admin"},
		{"ack", `3/admin,1["x"]`, nil, "/admin"},
		{"query string is kept verbatim", `0/admin?token=abc&x=1,`, nil, "/admin?token=abc&x=1"},
		{"query string on event", `2/admin?token=abc,["foo"]`, nil, "/admin?token=abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := mustDecode(t, tc.wire, tc.attachments...).Namespace; got != tc.want {
				t.Fatalf("Namespace = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAckIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		wire  string
		id    uint64
		hasID bool
		nsp   string
	}{
		{"absent", `2["foo"]`, 0, false, "/"},
		{"zero", `20["foo"]`, 0, true, "/"},
		{"leading zeros", `2007["foo"]`, 7, true, "/"},
		{"event", `2123["foo"]`, 123, true, "/"},
		{"ack", `3123["foo"]`, 123, true, "/"},
		{"with namespace", `2/admin,0["foo"]`, 0, true, "/admin"},
		{"largest", `218446744073709551615["foo"]`, math.MaxUint64, true, "/"},
		{"above 32 bits", `24294967296["foo"]`, 4294967296, true, "/"},
		{"binary ack", `61-0[` + ph0 + `]`, 0, true, "/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var attachments [][]byte
			if tc.wire[0] == '6' {
				attachments = blobs(1)
			}
			p := mustDecode(t, tc.wire, attachments...)
			if p.ID != tc.id || p.HasID != tc.hasID || p.Namespace != tc.nsp {
				t.Fatalf("packet = %s, want id=%d hasID=%t ns=%q", show(p), tc.id, tc.hasID, tc.nsp)
			}
		})
	}
}

func TestDecodeValidPayloads(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, wire string
	}{
		{"connect empty object", `0{}`},
		{"connect nested object", `0{"a":{"b":[1,2,{"c":null}]},"d":"e"}`},
		{"connect namespace empty object", `0/admin,{}`},
		{"connect error empty object", `4{}`},
		{"event with every JSON type", `2["foo",1,-2.5e3,"s",true,false,null,[],{}]`},
		{"ack empty array", `3[]`},
		{"ack id and empty array", `37[]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mustDecode(t, tc.wire)
		})
	}
}

func TestDecoderRecoversFromTextErrors(t *testing.T) {
	t.Parallel()
	var d parser.Decoder
	for _, bad := range []string{``, `9`, `4abc`, `2{}`, `5-["x"]`, `0[]`, `218446744073709551616["foo"]`, `5999-["x"]`} {
		p, err := d.Text([]byte(bad))
		if p != nil {
			t.Fatalf("%q: packet %s, want nil", bad, show(p))
		}
		requireMalformed(t, err)
		got, err := d.Text([]byte(`2["foo"]`))
		if err != nil || got == nil {
			t.Fatalf("after %q: packet %s, error %v", bad, show(got), err)
		}
	}
}
