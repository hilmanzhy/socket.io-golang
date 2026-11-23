package parser_test

import (
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

func BenchmarkEncodeEvent(b *testing.B) {
	p := &parser.Packet{Type: parser.Event, Namespace: "/admin", ID: 13, HasID: true, Data: []byte(`["foo","bar"]`)}
	b.ReportAllocs()
	for b.Loop() {
		_ = p.Encode()
	}
}

func BenchmarkDecodeEvent(b *testing.B) {
	frame := []byte(`2/admin,13["foo","bar",{"n":1}]`)
	var d parser.Decoder
	b.ReportAllocs()
	for b.Loop() {
		if _, err := d.Text(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewEvent(b *testing.B) {
	args := []any{"hello", counter{N: 1}}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := parser.NewEvent("/", "chat", args); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseEventBind(b *testing.B) {
	var d parser.Decoder
	p, err := d.Text([]byte(`2["chat","hello",{"n":1}]`))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_, args, err := parser.ParseEvent(p)
		if err != nil {
			b.Fatal(err)
		}
		var (
			text string
			n    counter
		)
		if err := args.Bind(&text, &n); err != nil {
			b.Fatal(err)
		}
	}
}
