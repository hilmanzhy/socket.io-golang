package parser_test

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

const looseArguments = 1000

var argumentPackets = []struct {
	name        string
	header      string
	attachments int
}{
	{"event", "2", 0},
	{"event with namespace and ack id", "2/admin,7", 0},
	{"ack", "37", 0},
	{"ack with namespace", "3/admin,7", 0},
	{"binary event", "51-", 1},
	{"binary event with namespace", "51-/admin,", 1},
	{"binary ack", "61-7", 1},
}

func argumentsWire(header string, n, attachments int) string {
	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('[')
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		switch {
		case i == 0:
			b.WriteString(`"e"`)
		case i <= attachments:
			fmt.Fprintf(&b, `{"_placeholder":true,"num":%d}`, i-1)
		default:
			b.WriteByte('0')
		}
	}
	b.WriteByte(']')
	return b.String()
}

func elementCount(t *testing.T, p *parser.Packet) int {
	t.Helper()
	switch p.Type {
	case parser.Event, parser.BinaryEvent:
		_, args, err := parser.ParseEvent(p)
		if err != nil {
			t.Fatalf("ParseEvent: %v", err)
		}
		return args.Len() + 1
	}
	args, err := parser.ParseAck(p)
	if err != nil {
		t.Fatalf("ParseAck: %v", err)
	}
	return args.Len()
}

func requireDecoderUsable(t *testing.T, d *parser.Decoder) {
	t.Helper()
	p, err := d.Binary([]byte{1})
	if p != nil {
		t.Fatalf("Binary = %s, want nil: a rejected packet must not stay pending", show(p))
	}
	requireMalformed(t, err)
	next, err := d.Text([]byte(`2["foo"]`))
	if err != nil || next == nil {
		t.Fatalf("decoder unusable after rejection: %s, %v", show(next), err)
	}
}

func TestMaxArguments(t *testing.T) {
	t.Parallel()
	limits := []struct {
		name      string
		max       int
		effective int
	}{
		{"zero uses the default", 0, parser.DefaultMaxArguments},
		{"negative uses the default", -1, parser.DefaultMaxArguments},
		{"most negative uses the default", math.MinInt, parser.DefaultMaxArguments},
		{"custom one", 1, 1},
		{"custom two", 2, 2},
		{"custom five", 5, 5},
		{"above the default", 200, 200},
	}
	for _, kind := range argumentPackets {
		for _, limit := range limits {
			t.Run(kind.name+"/"+limit.name, func(t *testing.T) {
				t.Parallel()
				d := parser.Decoder{MaxArguments: limit.max}
				p, err := decodeAll(&d, argumentsWire(kind.header, limit.effective, kind.attachments), blobs(kind.attachments))
				if err != nil || p == nil {
					t.Fatalf("%d elements: packet %s, error %v", limit.effective, show(p), err)
				}
				if got := elementCount(t, p); got != limit.effective {
					t.Fatalf("accepted packet has %d elements, want %d", got, limit.effective)
				}
				p, err = decodeAll(&d, argumentsWire(kind.header, limit.effective+1, kind.attachments), blobs(kind.attachments))
				if p != nil {
					t.Fatalf("%d elements: packet %s, want nil", limit.effective+1, show(p))
				}
				requireMalformed(t, err)
				if !strings.Contains(err.Error(), "too many arguments") {
					t.Fatalf("error = %q, want it to mention too many arguments", err)
				}
				requireDecoderUsable(t, &d)
			})
		}
	}
}

func TestMaxArgumentsIsCheckedBeforeAttachmentsArrive(t *testing.T) {
	t.Parallel()
	d := parser.Decoder{MaxArguments: 2}
	p, err := d.Text([]byte(argumentsWire("52-", 3, 2)))
	if p != nil {
		t.Fatalf("Text = %s, want nil", show(p))
	}
	requireMalformed(t, err)
	requireDecoderUsable(t, &d)
}

func TestMaxArgumentsCountsOneElementPerValue(t *testing.T) {
	t.Parallel()
	values := []struct{ name, json string }{
		{"number", `1`},
		{"negative exponent number", `-1.5e+3`},
		{"true", `true`},
		{"null", `null`},
		{"plain string", `"a"`},
		{"empty array", `[]`},
		{"empty object", `{}`},
		{"nested empty containers", `[[],{},[{}],{"a":[]}]`},
		{"array of numbers", `[1,2,3,4,5]`},
		{"array of arrays", `[[1,2],[3,[4,5]],[]]`},
		{"object with several keys", `{"a":1,"b":2,"c":[3,4],"d":{"e":5,"f":6}}`},
		{"deeply nested array", `[[[[[[[[[[1,2],3],4],5],6],7],8],9],10],11]`},
		{"string with commas", `"a,b,c,d"`},
		{"string with square brackets", `"[,]"`},
		{"string with closing then opening bracket", `"],["`},
		{"string with braces", `"{,}"`},
		{"string with unbalanced braces", `"}}},{{{"`},
		{"string with escaped quote", `"say \"hi\", then leave"`},
		{"string with escaped quote and bracket", `"\"],[\""`},
		{"string ending in an escaped backslash", `"path\\"`},
		{"string with escaped backslash before a comma", `"a\\,b\\"`},
		{"string with backslash and escaped quote", `"\\\",\\\""`},
		{"string with unicode escapes", "\"\x5cu002c\x5cu005d\x5cu0022\""},
		{"string with multibyte text", `"日本,語,😀"`},
		{"empty string", `""`},
		{"object with special keys and values", `{"k,]":"v,}","[":"]","\"":"\\"}`},
		{"array of tricky strings", `["a,b","]","\"",[","]]`},
	}
	layouts := []struct{ name, open, separator, closing string }{
		{"compact", "[", ",", "]"},
		{"spaced", "[ ", " , ", " ]"},
		{"multiline", "[\n\t", "\n\t,\r\n ", "\r\n]"},
	}
	for _, kind := range argumentPackets {
		if kind.attachments > 0 {
			continue
		}
		for _, value := range values {
			for _, layout := range layouts {
				t.Run(kind.name+"/"+value.name+"/"+layout.name, func(t *testing.T) {
					t.Parallel()
					wire := kind.header + layout.open + strings.Join([]string{`"e"`, value.json, value.json}, layout.separator) + layout.closing
					exact := parser.Decoder{MaxArguments: 3}
					p, err := exact.Text([]byte(wire))
					if err != nil || p == nil {
						t.Fatalf("limit 3: packet %s, error %v", show(p), err)
					}
					if got := elementCount(t, p); got != 3 {
						t.Fatalf("parsed %d elements, want 3", got)
					}
					tight := parser.Decoder{MaxArguments: 2}
					p, err = tight.Text([]byte(wire))
					if p != nil {
						t.Fatalf("limit 2: packet %s, want nil", show(p))
					}
					requireMalformed(t, err)
				})
			}
		}
	}
}

func TestMaxArgumentsAllowsEmptyArray(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wire        string
		attachments [][]byte
	}{
		{"ack", `3[]`, nil},
		{"ack with id", `37[]`, nil},
		{"ack with whitespace", "3[ \n]", nil},
		{"ack with namespace", `3/admin,7[]`, nil},
		{"binary ack", `61-7[]`, blobs(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := parser.Decoder{MaxArguments: 1}
			p, err := decodeAll(&d, tc.wire, tc.attachments)
			if err != nil || p == nil {
				t.Fatalf("packet %s, error %v", show(p), err)
			}
			if got := elementCount(t, p); got != 0 {
				t.Fatalf("parsed %d elements, want 0", got)
			}
		})
	}
}

func TestMaxArgumentsOnlyCapsThePayloadArray(t *testing.T) {
	t.Parallel()
	crowd := strings.Repeat(",0", 3*parser.DefaultMaxArguments)
	tests := []struct {
		name string
		wire string
	}{
		{"connect object holding a long array", `0{"a":[0` + crowd + `]}`},
		{"connect error holding a long array", `4[0` + crowd + `]`},
		{"long array nested in an event argument", `2["e",[0` + crowd + `]]`},
		{"long array nested in an ack argument", `37[[0` + crowd + `]]`},
		{"long object nested in an event argument", `2["e",{"k":[0` + crowd + `]}]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			if p, err := d.Text([]byte(tc.wire)); err != nil || p == nil {
				t.Fatalf("packet %s, error %v", show(p), err)
			}
		})
	}
}

func TestMaxArgumentsLeavesMalformedPayloadsToTheParse(t *testing.T) {
	t.Parallel()
	crowd := strings.Repeat(",0", 3*parser.DefaultMaxArguments)
	tests := []struct {
		name    string
		wire    string
		decodes bool
	}{
		{"bytes after the array are not elements", `2["e"]` + crowd, true},
		{"commas of an unterminated string are not elements", `2["e","unterminated` + crowd, true},
		{"an unterminated array still counts its elements", `2["e"` + crowd, false},
		{"a closed array with a trailing comma still counts its commas", `2["e"` + crowd + `,]`, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var d parser.Decoder
			p, err := d.Text([]byte(tc.wire))
			if !tc.decodes {
				if p != nil {
					t.Fatalf("Text = %s, want nil", show(p))
				}
				requireMalformed(t, err)
				return
			}
			if err != nil || p == nil {
				t.Fatalf("Text: packet %s, error %v", show(p), err)
			}
			requireMalformed(t, parsePayload(p))
		})
	}
}
