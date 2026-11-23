package parser_test

import (
	"bytes"
	"cmp"
	"encoding/json"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

const frameSeparator = 0xff

var fuzzSeeds = []string{
	"", "9", "a", "4abc", `2{}`, `2[]`, `2[null]`, `2["connect"]`, `2["foo"`, `5-["x"]`, `0[]`, `1{}`,
	`218446744073709551616["foo"]`,
	`51-["x",{"_placeholder":true,"num":1}]`,
	`51-["x",{"_placeholder":true}]`,
	`51-["x",{"a":[{"_placeholder":true,"num":0}]}]`,
	`52147483648-["x"]`,
	`52-/admin,["baz",{"_placeholder":true,"num":0},{"nested":{"_placeholder":true,"num":1}}]`,
	`2["a",1,2,3,[4,5],{"k":"x,y"}]`,
	`3[1,"a,b",[2,3],4]`,
	`51-["x",{"_placeholder":true,"num":0},1,[2,3]]`,
	"2[\"foo\"]\xff3[\"bar\"]",
	"51-[\"x\",{\"_placeholder\":true,\"num\":0}]\xff2[\"y\"]",
}

func FuzzDecoder(f *testing.F) {
	blob := []byte{0x01, 0x02, 0x03}
	for _, v := range vectors {
		f.Add([]byte(v.wire), blob, uint8(0))
	}
	for _, seed := range fuzzSeeds {
		f.Add([]byte(seed), blob, uint8(0))
		f.Add([]byte(seed), blob, uint8(5))
		f.Add([]byte(seed), blob, uint8(8))
		f.Add([]byte(seed), blob, uint8(16))
	}
	f.Fuzz(func(t *testing.T, text, blob []byte, mode uint8) {
		d := parser.Decoder{MaxAttachments: int(mode & 3), MaxArguments: int(mode >> 3 & 3)}
		limit := cmp.Or(d.MaxArguments, parser.DefaultMaxArguments)
		leavePending := mode&4 != 0
		for _, frame := range bytes.Split(text, []byte{frameSeparator}) {
			p, err := d.Text(frame)
			for i := 0; !leavePending && p == nil && err == nil && i < parser.DefaultMaxAttachments+2; i++ {
				p, err = d.Binary(blob[:len(blob)*i/(parser.DefaultMaxAttachments+2)])
			}
			if err != nil {
				requireMalformed(t, err)
				continue
			}
			if p != nil {
				exercise(t, p, limit)
			} else if !leavePending {
				t.Fatalf("frame %q: attachments still pending after %d binary frames", frame, parser.DefaultMaxAttachments+2)
			}
		}
		p, err := d.Binary(blob)
		if err != nil {
			requireMalformed(t, err)
		} else if p != nil {
			exercise(t, p, limit)
		}
	})
}

func exercise(t *testing.T, p *parser.Packet, limit int) {
	t.Helper()
	checkArgumentCap(t, p, limit)
	frame := p.Encode()
	again := parser.Decoder{MaxAttachments: max(len(p.Attachments), 1)}
	got, err := decodeAll(&again, string(frame), p.Attachments)
	if err != nil || got == nil || !equalPacket(got, p) {
		t.Fatalf("re-decoding %q gave %s, %v; want %s", frame, show(got), err, show(p))
	}
	name, args, err := parser.ParseEvent(p)
	if err != nil {
		requireMalformed(t, err)
	} else {
		if parser.IsReserved(name) {
			t.Fatalf("ParseEvent accepted reserved name %q", name)
		}
		checkArgs(t, args)
	}
	args, err = parser.ParseAck(p)
	if err != nil {
		requireMalformed(t, err)
	} else {
		checkArgs(t, args)
	}
}

func checkArgs(t *testing.T, a parser.Args) {
	t.Helper()
	for i := -2; i < a.Len()+2; i++ {
		inRange := i >= 0 && i < a.Len()
		binary := a.IsBinary(i)
		if !inRange && (binary || a.Bytes(i) != nil || a.Raw(i) != nil) {
			t.Fatalf("argument %d is out of range but returned data", i)
		}
		if inRange && binary == (a.Raw(i) != nil) {
			t.Fatalf("argument %d: IsBinary=%t but Raw=%q", i, binary, a.Raw(i))
		}
		if !binary && a.Bytes(i) != nil {
			t.Fatalf("argument %d: Bytes returned data for a JSON argument", i)
		}
	}
	var (
		generic any
		blob    []byte
		text    string
		number  int
	)
	_ = a.Bind(&generic, &blob)
	_ = a.Bind(&text, &number)
	_ = a.Bind()
}

func checkArgumentCap(t *testing.T, p *parser.Packet, limit int) {
	t.Helper()
	switch p.Type {
	case parser.Event, parser.BinaryEvent, parser.Ack, parser.BinaryAck:
	default:
		return
	}
	var elements []json.RawMessage
	if json.Unmarshal(p.Data, &elements) == nil && len(elements) > limit {
		t.Fatalf("decoder accepted %d payload elements with a limit of %d: %q", len(elements), limit, p.Data)
	}
}
