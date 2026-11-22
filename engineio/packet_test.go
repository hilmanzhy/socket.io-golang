package engineio

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"
)

func samePacket(a, b packet) bool {
	return a.kind == b.kind && a.binary == b.binary && bytes.Equal(a.data, b.data)
}

func samePackets(a, b []packet) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !samePacket(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestPacketKinds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		kind kind
		want byte
	}{
		{"open", kindOpen, '0'},
		{"close", kindClose, '1'},
		{"ping", kindPing, '2'},
		{"pong", kindPong, '3'},
		{"message", kindMessage, '4'},
		{"upgrade", kindUpgrade, '5'},
		{"noop", kindNoop, '6'},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if byte(tt.kind) != tt.want {
				t.Errorf("kind = %q, want %q", byte(tt.kind), tt.want)
			}
			if got := (packet{kind: tt.kind}).prefix(); !bytes.Equal(got, []byte{tt.want}) {
				t.Errorf("prefix = %q, want %q", got, string(tt.want))
			}
		})
	}
}

func TestAppendPayload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dst     string
		packets []packet
		want    string
	}{
		{"no packets", "", nil, ""},
		{"no packets keeps dst", "keep", nil, "keep"},
		{"message", "", []packet{{kind: kindMessage, data: []byte("hello")}}, "4hello"},
		{"empty message", "", []packet{{kind: kindMessage}}, "4"},
		{"ping", "", []packet{{kind: kindPing}}, "2"},
		{"pong", "", []packet{{kind: kindPong}}, "3"},
		{"close", "", []packet{{kind: kindClose}}, "1"},
		{"noop", "", []packet{{kind: kindNoop}}, "6"},
		{"open", "", []packet{{kind: kindOpen, data: []byte(`{"sid":"x"}`)}}, `0{"sid":"x"}`},
		{"appends to dst", "xx", []packet{{kind: kindMessage, data: []byte("a")}}, "xx4a"},
		{"binary", "", []packet{{kind: kindMessage, binary: true, data: []byte{1, 2, 3, 4}}}, "bAQIDBA=="},
		{"empty binary", "", []packet{{kind: kindMessage, binary: true}}, "b"},
		{"binary padding of two bytes", "", []packet{{kind: kindMessage, binary: true, data: []byte{0, 255}}}, "bAP8="},
		{"binary without padding", "", []packet{{kind: kindMessage, binary: true, data: []byte{0, 255, 16}}}, "bAP8Q"},
		{
			"separator between packets",
			"",
			[]packet{
				{kind: kindMessage, data: []byte("a")},
				{kind: kindMessage, binary: true, data: []byte{1, 2, 3, 4}},
				{kind: kindPing},
			},
			"4a\x1ebAQIDBA==\x1e2",
		},
		{
			"no separator after the last packet",
			"",
			[]packet{{kind: kindMessage, data: []byte("a")}, {kind: kindMessage, data: []byte("b")}},
			"4a\x1e4b",
		},
		{"unicode", "", []packet{{kind: kindMessage, data: []byte("héllo ✓")}}, "4héllo ✓"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := appendPayload([]byte(tt.dst), tt.packets)
			if string(got) != tt.want {
				t.Errorf("appendPayload = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecodePacket(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk string
		want  packet
		fail  bool
	}{
		{name: "message", chunk: "4hello", want: packet{kind: kindMessage, data: []byte("hello")}},
		{name: "empty message", chunk: "4", want: packet{kind: kindMessage}},
		{name: "open", chunk: `0{"sid":"x"}`, want: packet{kind: kindOpen, data: []byte(`{"sid":"x"}`)}},
		{name: "close", chunk: "1", want: packet{kind: kindClose}},
		{name: "ping", chunk: "2", want: packet{kind: kindPing}},
		{name: "ping probe", chunk: "2probe", want: packet{kind: kindPing, data: []byte("probe")}},
		{name: "pong", chunk: "3", want: packet{kind: kindPong}},
		{name: "pong probe", chunk: "3probe", want: packet{kind: kindPong, data: []byte("probe")}},
		{name: "upgrade", chunk: "5", want: packet{kind: kindUpgrade}},
		{name: "noop", chunk: "6", want: packet{kind: kindNoop}},
		{name: "message that starts with b", chunk: "4bAQID", want: packet{kind: kindMessage, data: []byte("bAQID")}},
		{name: "unicode", chunk: "4héllo ✓", want: packet{kind: kindMessage, data: []byte("héllo ✓")}},
		{name: "binary", chunk: "bAQIDBA==", want: packet{kind: kindMessage, binary: true, data: []byte{1, 2, 3, 4}}},
		{name: "binary of three bytes", chunk: "bAP8Q", want: packet{kind: kindMessage, binary: true, data: []byte{0, 255, 16}}},
		{name: "binary with padding", chunk: "bAP8=", want: packet{kind: kindMessage, binary: true, data: []byte{0, 255}}},
		{name: "empty binary", chunk: "b", want: packet{kind: kindMessage, binary: true}},
		{name: "empty chunk", chunk: "", fail: true},
		{name: "unknown kind 7", chunk: "7", fail: true},
		{name: "unknown kind 9", chunk: "9x", fail: true},
		{name: "kind below zero", chunk: "/", fail: true},
		{name: "letter", chunk: "a", fail: true},
		{name: "upper case b", chunk: "B", fail: true},
		{name: "high byte", chunk: "\xff", fail: true},
		{name: "separator only", chunk: "\x1e", fail: true},
		{name: "bad base64 characters", chunk: "b!!!!", fail: true},
		{name: "truncated base64", chunk: "bAQ", fail: true},
		{name: "base64 in the wrong alphabet", chunk: "bAQ_-", fail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodePacket([]byte(tt.chunk))
			if tt.fail {
				if !errors.Is(err, errMalformed) {
					t.Fatalf("err = %v, want errMalformed (packet %+v)", err, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !samePacket(got, tt.want) {
				t.Errorf("packet = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDecodePayload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want []packet
		fail bool
	}{
		{name: "single", body: "4a", want: []packet{{kind: kindMessage, data: []byte("a")}}},
		{name: "pong", body: "3", want: []packet{{kind: kindPong}}},
		{
			name: "several",
			body: "4a\x1e4b\x1e3",
			want: []packet{
				{kind: kindMessage, data: []byte("a")},
				{kind: kindMessage, data: []byte("b")},
				{kind: kindPong},
			},
		},
		{
			name: "text and binary",
			body: "4a\x1ebAQID\x1e4b",
			want: []packet{
				{kind: kindMessage, data: []byte("a")},
				{kind: kindMessage, binary: true, data: []byte{1, 2, 3}},
				{kind: kindMessage, data: []byte("b")},
			},
		},
		{
			name: "close after a message",
			body: "4a\x1e1",
			want: []packet{{kind: kindMessage, data: []byte("a")}, {kind: kindClose}},
		},
		{name: "empty body", body: "", fail: true},
		{name: "trailing separator", body: "4a\x1e", fail: true},
		{name: "leading separator", body: "\x1e4a", fail: true},
		{name: "doubled separator", body: "4a\x1e\x1e4b", fail: true},
		{name: "separator only", body: "\x1e", fail: true},
		{name: "second packet unknown", body: "4a\x1e7", fail: true},
		{name: "first packet unknown", body: "7\x1e4a", fail: true},
		{name: "second packet bad base64", body: "4a\x1eb!!", fail: true},
		{name: "plain text", body: "abc", fail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodePayload([]byte(tt.body))
			if tt.fail {
				if !errors.Is(err, errMalformed) {
					t.Fatalf("err = %v, want errMalformed", err)
				}
				if got != nil {
					t.Errorf("packets = %+v alongside an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !samePackets(got, tt.want) {
				t.Errorf("packets = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func randomPacket(rng *rand.Rand) packet {
	data := make([]byte, rng.IntN(40))
	if rng.IntN(3) == 0 {
		for i := range data {
			data[i] = byte(rng.IntN(256))
		}
		return packet{kind: kindMessage, binary: true, data: data}
	}
	for i := range data {
		b := byte(rng.IntN(256))
		if b == separator {
			b = 'x'
		}
		data[i] = b
	}
	return packet{kind: kindOpen + kind(rng.IntN(int(kindNoop-kindOpen)+1)), data: data}
}

func TestPayloadRoundTrip(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 500 {
		packets := make([]packet, 1+rng.IntN(6))
		for j := range packets {
			packets[j] = randomPacket(rng)
		}
		encoded := appendPayload(nil, packets)
		got, err := decodePayload(encoded)
		if err != nil {
			t.Fatalf("round %d: decode %q: %v", i, encoded, err)
		}
		if !samePackets(got, packets) {
			t.Fatalf("round %d: %q decoded to %+v, want %+v", i, encoded, got, packets)
		}
	}
}

func FuzzDecodePayload(f *testing.F) {
	seeds := []string{
		"", "4", "4a", "2", "3", "1", "5", "6", "0{}", "4a\x1e4b", "4a\x1ebAQID\x1e2",
		"bAQIDBA==", "b", "bAP8Q", "b!!", "\x1e", "4a\x1e", "\x1e4a", "4a\x1e\x1e4b",
		"7", "abc", "\xff\xfe", "4héllo ✓", "bAQ\nID",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		packets, err := decodePayload(body)
		if err != nil {
			if !errors.Is(err, errMalformed) {
				t.Fatalf("unexpected error type: %v", err)
			}
			if packets != nil {
				t.Fatalf("packets %+v returned with error %v", packets, err)
			}
			return
		}
		if want := bytes.Count(body, []byte{separator}) + 1; len(packets) != want {
			t.Fatalf("%d packets decoded from %d separators", len(packets), want-1)
		}
		encoded := appendPayload(nil, packets)
		again, err := decodePayload(encoded)
		if err != nil {
			t.Fatalf("re-encoded payload %q does not decode: %v", encoded, err)
		}
		if !samePackets(packets, again) {
			t.Fatalf("decode, encode, decode changed the packets: %+v then %+v", packets, again)
		}
	})
}
