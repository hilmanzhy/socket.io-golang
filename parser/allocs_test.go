package parser_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

const (
	hugeElementCount = 200_000
	rawMessageSize   = 24
)

func bytesPerRun(runs int, f func()) float64 {
	f()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		f()
	}
	runtime.ReadMemStats(&after)
	return float64(after.TotalAlloc-before.TotalAlloc) / float64(runs)
}

// TestHugePayloadIsRejectedWithoutAllocatingForIt is not parallel on purpose:
// testing.AllocsPerRun and bytesPerRun measure the whole process, so a test
// running beside them would skew the figures.
func TestHugePayloadIsRejectedWithoutAllocatingForIt(t *testing.T) {
	const (
		maxAllocs     = 16
		unboundedCost = hugeElementCount * rawMessageSize
		maxBytes      = unboundedCost / 1000
	)
	for _, kind := range argumentPackets {
		t.Run(kind.name, func(t *testing.T) {
			frame := []byte(argumentsWire(kind.header, hugeElementCount, kind.attachments))
			var d parser.Decoder
			reject := func() {
				if p, err := d.Text(frame); p != nil || err == nil {
					t.Fatalf("Text = %s, %v; want nil and an error", show(p), err)
				}
			}
			if allocs := testing.AllocsPerRun(20, reject); allocs > maxAllocs {
				t.Errorf("rejecting %d elements made %v allocations, want at most %d", hugeElementCount, allocs, maxAllocs)
			}
			if got := bytesPerRun(20, reject); got > maxBytes {
				t.Errorf("rejecting %d elements allocated %.0f bytes, want at most %d (decoding them would take %d)", hugeElementCount, got, maxBytes, unboundedCost)
			}
		})
	}
}

// TestParseWithoutTopLevelPlaceholderSkipsTheBlobTable is not parallel for
// the same reason as TestHugePayloadIsRejectedWithoutAllocatingForIt.
func TestParseWithoutTopLevelPlaceholderSkipsTheBlobTable(t *testing.T) {
	const (
		arguments     = parser.DefaultMaxArguments
		blobTableSize = arguments * rawMessageSize
		maxExtraBytes = blobTableSize / 6
	)
	data := []byte("[0" + strings.Repeat(",0", arguments-1) + "]")
	plain := &parser.Packet{Type: parser.Ack, Namespace: "/", Data: data}
	withAttachment := &parser.Packet{Type: parser.BinaryAck, Namespace: "/", Data: data, Attachments: [][]byte{{1}}}
	parse := func(p *parser.Packet) func() {
		return func() {
			if args, err := parser.ParseAck(p); err != nil || args.Len() != arguments {
				t.Fatalf("ParseAck = %d arguments, %v; want %d", args.Len(), err, arguments)
			}
		}
	}
	extra := bytesPerRun(50, parse(withAttachment)) - bytesPerRun(50, parse(plain))
	if extra > maxExtraBytes {
		t.Fatalf("attachments cost %.0f extra bytes, want at most %d (a table of %d slots would take %d)", extra, maxExtraBytes, arguments, blobTableSize)
	}
}

// TestNestedPlaceholderIsInlinedWithoutDecodingTheArgument is not parallel
// for the same reason as TestHugePayloadIsRejectedWithoutAllocatingForIt.
func TestNestedPlaceholderIsInlinedWithoutDecodingTheArgument(t *testing.T) {
	const values = 100_000
	argument := "[" + strings.Repeat("{},", values) + ph0 + "]"
	p := &parser.Packet{Type: parser.BinaryAck, Namespace: "/", Data: []byte("[" + argument + "]"), Attachments: [][]byte{{1, 2, 3}}}
	parse := func() {
		if args, err := parser.ParseAck(p); err != nil || args.Len() != 1 || args.IsBinary(0) {
			t.Fatalf("ParseAck = %d arguments, %v; want one JSON argument", args.Len(), err)
		}
	}
	if got, limit := bytesPerRun(10, parse), float64(3*len(argument)); got > limit {
		t.Errorf("inlining one placeholder among %d values allocated %.0f bytes, want at most %.0f", values, got, limit)
	}
}
