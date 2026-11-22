package parser_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/nuriontech/socket.io-golang/parser"
)

type counter struct {
	N int `json:"n"`
}

type holder struct {
	Data []byte `json:"data"`
}

func TestIsReserved(t *testing.T) {
	t.Parallel()
	for _, name := range reservedNames {
		if !parser.IsReserved(name) {
			t.Errorf("IsReserved(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"", "Connect", "connected", "connection", "message", "error", "connect ", "NEWLISTENER", "remove_listener"} {
		if parser.IsReserved(name) {
			t.Errorf("IsReserved(%q) = true, want false", name)
		}
	}
}

func TestNewEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		namespace string
		event     string
		args      []any
		wantType  parser.Type
		wantWire  string
		wantBlobs [][]byte
	}{
		{"nil arguments", "/", "foo", nil, parser.Event, `2["foo"]`, nil},
		{"empty arguments", "/", "foo", []any{}, parser.Event, `2["foo"]`, nil},
		{"named namespace", "/admin", "foo", []any{"bar"}, parser.Event, `2/admin,["foo","bar"]`, nil},
		{"empty namespace", "", "foo", nil, parser.Event, `2["foo"]`, nil},
		{
			"json arguments", "/", "foo",
			[]any{"text", 42, 1.5, true, nil, []int{1, 2}, map[string]any{"a": 1}, counter{3}},
			parser.Event, `2["foo","text",42,1.5,true,null,[1,2],{"a":1},{"n":3}]`, nil,
		},
		{"raw json passes through", "/", "foo", []any{json.RawMessage(`{"a":[1,2]}`)}, parser.Event, `2["foo",{"a":[1,2]}]`, nil},
		{"nested bytes stay json", "/", "foo", []any{holder{[]byte{1, 2}}}, parser.Event, `2["foo",{"data":"AQI="}]`, nil},
		{"slice of byte slices stays json", "/", "foo", []any{[][]byte{{1}}}, parser.Event, `2["foo",["AQ=="]]`, nil},
		{"one binary argument", "/", "foo", []any{[]byte{1, 2, 3}}, parser.BinaryEvent, `51-["foo",` + ph0 + `]`, [][]byte{{1, 2, 3}}},
		{"binary after json", "/", "foo", []any{"a", []byte{1}}, parser.BinaryEvent, `51-["foo","a",` + ph0 + `]`, [][]byte{{1}}},
		{"binary before json", "/", "foo", []any{[]byte{1}, "a"}, parser.BinaryEvent, `51-["foo",` + ph0 + `,"a"]`, [][]byte{{1}}},
		{
			"binary arguments are numbered in order", "/", "foo",
			[]any{[]byte{1}, "mid", []byte{2, 3}, 5, []byte{4}},
			parser.BinaryEvent, `53-["foo",` + ph0 + `,"mid",` + ph1 + `,5,` + ph2 + `]`, [][]byte{{1}, {2, 3}, {4}},
		},
		{
			"binary arguments with namespace", "/admin", "baz", []any{[]byte{1}, []byte{2, 3}},
			parser.BinaryEvent, `52-/admin,["baz",` + ph0 + `,` + ph1 + `]`, [][]byte{{1}, {2, 3}},
		},
		{"empty binary argument", "/", "foo", []any{[]byte{}}, parser.BinaryEvent, `51-["foo",` + ph0 + `]`, [][]byte{{}}},
		{"nil binary argument", "/", "foo", []any{[]byte(nil)}, parser.BinaryEvent, `51-["foo",` + ph0 + `]`, [][]byte{nil}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := parser.NewEvent(tc.namespace, tc.event, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if p.Type != tc.wantType || p.Namespace != tc.namespace || p.HasID || p.ID != 0 {
				t.Fatalf("packet = %s, want type %v namespace %q without id", show(p), tc.wantType, tc.namespace)
			}
			if got := string(p.Encode()); got != tc.wantWire {
				t.Fatalf("Encode() = %q, want %q", got, tc.wantWire)
			}
			if !slices.EqualFunc(p.Attachments, tc.wantBlobs, bytes.Equal) {
				t.Fatalf("attachments = %q, want %q", p.Attachments, tc.wantBlobs)
			}
		})
	}
}

func TestNewEventRejectsReservedNames(t *testing.T) {
	t.Parallel()
	for _, name := range reservedNames {
		for _, args := range [][]any{nil, {"x"}, {[]byte{1}}} {
			p, err := parser.NewEvent("/", name, args)
			if p != nil {
				t.Errorf("NewEvent(%q) = %s, want nil", name, show(p))
			}
			if !errors.Is(err, parser.ErrReservedEvent) {
				t.Errorf("NewEvent(%q) error = %v, want ErrReservedEvent", name, err)
			}
		}
	}
}

func TestNewPacketRejectsUnmarshalableArguments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []any
	}{
		{"channel", []any{make(chan int)}},
		{"function", []any{func() {}}},
		{"complex number", []any{complex(1, 2)}},
		{"NaN", []any{math.NaN()}},
		{"infinity", []any{math.Inf(1)}},
		{"channel after json", []any{"ok", 1, make(chan int)}},
		{"channel after binary", []any{[]byte{1}, make(chan int)}},
		{"channel before binary", []any{make(chan int), []byte{1}}},
		{"channel inside map", []any{map[string]any{"c": make(chan int)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			event, err := parser.NewEvent("/", "foo", tc.args)
			if event != nil || err == nil {
				t.Errorf("NewEvent = %s, %v; want nil and an error", show(event), err)
			}
			if errors.Is(err, parser.ErrReservedEvent) {
				t.Errorf("NewEvent error %v matches ErrReservedEvent", err)
			}
			ack, err := parser.NewAck("/", 1, tc.args)
			if ack != nil || err == nil {
				t.Errorf("NewAck = %s, %v; want nil and an error", show(ack), err)
			}
		})
	}
}

func TestNewAck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		namespace string
		id        uint64
		args      []any
		wantType  parser.Type
		wantWire  string
		wantBlobs [][]byte
	}{
		{"nil arguments", "/", 12, nil, parser.Ack, `312[]`, nil},
		{"empty arguments", "/", 12, []any{}, parser.Ack, `312[]`, nil},
		{"one argument", "/", 12, []any{"bar"}, parser.Ack, `312["bar"]`, nil},
		{"several arguments", "/", 1, []any{"a", 2, nil, counter{4}}, parser.Ack, `31["a",2,null,{"n":4}]`, nil},
		{"named namespace", "/admin", 13, []any{"bar"}, parser.Ack, `3/admin,13["bar"]`, nil},
		{"id zero", "/", 0, []any{"x"}, parser.Ack, `30["x"]`, nil},
		{"largest id", "/", math.MaxUint64, nil, parser.Ack, `318446744073709551615[]`, nil},
		{"raw json passes through", "/", 2, []any{json.RawMessage(`[1]`)}, parser.Ack, `32[[1]]`, nil},
		{"binary after json", "/", 15, []any{"bar", []byte{4, 5}}, parser.BinaryAck, `61-15["bar",` + ph0 + `]`, [][]byte{{4, 5}}},
		{"only binary", "/admin", 1, []any{[]byte{6}}, parser.BinaryAck, `61-/admin,1[` + ph0 + `]`, [][]byte{{6}}},
		{
			"binary arguments are numbered in order", "/", 4, []any{[]byte{1}, []byte{2}, "x", []byte{3}},
			parser.BinaryAck, `63-4[` + ph0 + `,` + ph1 + `,"x",` + ph2 + `]`, [][]byte{{1}, {2}, {3}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, err := parser.NewAck(tc.namespace, tc.id, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if p.Type != tc.wantType || p.Namespace != tc.namespace || p.ID != tc.id || !p.HasID {
				t.Fatalf("packet = %s, want type %v namespace %q id %d", show(p), tc.wantType, tc.namespace, tc.id)
			}
			if got := string(p.Encode()); got != tc.wantWire {
				t.Fatalf("Encode() = %q, want %q", got, tc.wantWire)
			}
			if !slices.EqualFunc(p.Attachments, tc.wantBlobs, bytes.Equal) {
				t.Fatalf("attachments = %q, want %q", p.Attachments, tc.wantBlobs)
			}
		})
	}
}

func TestNewEventNameSurvivesTheWire(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"a", "chat message", `say "hi"`, `back\slash`, "ünï", "<tag>&", "line\nbreak", "日本語", "emoji😀", "tab\there", "comma,name", "dash-1"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, err := parser.NewEvent("/", name, []any{1})
			if err != nil {
				t.Fatal(err)
			}
			got, args, err := parser.ParseEvent(mustDecode(t, string(p.Encode())))
			if err != nil {
				t.Fatal(err)
			}
			if got != name || args.Len() != 1 {
				t.Fatalf("name = %q with %d arguments, want %q with 1", got, args.Len(), name)
			}
		})
	}
}

func TestNewEventRoundTrip(t *testing.T) {
	t.Parallel()
	type profile struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	want := profile{"ada", []string{"x", "y"}}
	p, err := parser.NewEvent("/admin", "upload", []any{"a.png", []byte{9, 8, 7}, want, 3.5})
	if err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]*parser.Packet{
		"constructed packet": p,
		"decoded wire":       mustDecode(t, string(p.Encode()), p.Attachments...),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			event, args, err := parser.ParseEvent(source)
			if err != nil || event != "upload" || args.Len() != 4 {
				t.Fatalf("ParseEvent = %q, %d arguments, %v", event, args.Len(), err)
			}
			var (
				file  string
				blob  []byte
				got   profile
				ratio float64
			)
			if err := args.Bind(&file, &blob, &got, &ratio); err != nil {
				t.Fatal(err)
			}
			if file != "a.png" || !bytes.Equal(blob, []byte{9, 8, 7}) || !reflect.DeepEqual(got, want) || ratio != 3.5 {
				t.Fatalf("bound %q %v %+v %v", file, blob, got, ratio)
			}
		})
	}
}

func TestNewAckRoundTrip(t *testing.T) {
	t.Parallel()
	p, err := parser.NewAck("/chat", 77, []any{nil, "ok", []byte{1, 2}, counter{5}})
	if err != nil {
		t.Fatal(err)
	}
	decoded := mustDecode(t, string(p.Encode()), p.Attachments...)
	assertPacket(t, decoded, p)
	args, err := parser.ParseAck(decoded)
	if err != nil || args.Len() != 4 {
		t.Fatalf("ParseAck = %d arguments, %v", args.Len(), err)
	}
	var (
		first any = "unset"
		text  string
		blob  []byte
		n     counter
	)
	if err := args.Bind(&first, &text, &blob, &n); err != nil {
		t.Fatal(err)
	}
	if first != nil || text != "ok" || !bytes.Equal(blob, []byte{1, 2}) || n.N != 5 {
		t.Fatalf("bound %v %q %v %+v", first, text, blob, n)
	}
}

func TestParseEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wire        string
		attachments [][]byte
		wantName    string
		wantArgs    int
	}{
		{"no arguments", `2["foo"]`, nil, "foo", 0},
		{"two arguments", `2["foo","bar",1]`, nil, "foo", 2},
		{"namespace and ack id", `2/admin,13["foo","bar"]`, nil, "foo", 1},
		{"escaped name", "2[\"f\\u006fo\"]", nil, "foo", 0},
		{"whitespace around elements", `2[ "foo" , 1 ]`, nil, "foo", 1},
		{"null argument", `2["foo",null]`, nil, "foo", 1},
		{"binary event", `51-["baz",` + ph0 + `]`, blobs(1), "baz", 1},
		{"binary event with two attachments", `52-/admin,["baz",` + ph0 + `,` + ph1 + `]`, blobs(2), "baz", 2},
		{"binary event with no argument", `51-["baz"]`, blobs(1), "baz", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			name, args := mustEvent(t, tc.wire, tc.attachments...)
			if name != tc.wantName || args.Len() != tc.wantArgs {
				t.Fatalf("ParseEvent = %q with %d arguments, want %q with %d", name, args.Len(), tc.wantName, tc.wantArgs)
			}
		})
	}
}

func TestParseAck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		wire        string
		attachments [][]byte
		want        int
	}{
		{"no arguments", `3[]`, nil, 0},
		{"one argument", `312["bar"]`, nil, 1},
		{"null argument", `3[null]`, nil, 1},
		{"namespace", `3/admin,13["bar","baz"]`, nil, 2},
		{"binary ack", `61-15["bar",` + ph0 + `]`, blobs(1), 2},
		{"binary ack with only binary", `61-/admin,1[` + ph0 + `]`, blobs(1), 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := mustAck(t, tc.wire, tc.attachments...).Len(); got != tc.want {
				t.Fatalf("Len() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestParseRejectsPacketsWithoutAnArrayPayload(t *testing.T) {
	t.Parallel()
	for name, data := range map[string]json.RawMessage{
		"nil":          nil,
		"empty":        {},
		"object":       json.RawMessage(`{}`),
		"string":       json.RawMessage(`"foo"`),
		"number":       json.RawMessage(`1`),
		"truncated":    json.RawMessage(`["foo"`),
		"text":         json.RawMessage(`foo`),
		"object event": json.RawMessage(`{"event":"foo"}`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, _, err := parser.ParseEvent(&parser.Packet{Type: parser.Event, Namespace: "/", Data: data})
			requireMalformed(t, err)
			_, err = parser.ParseAck(&parser.Packet{Type: parser.Ack, Namespace: "/", Data: data})
			requireMalformed(t, err)
		})
	}
}

func TestParseEventReturnsEmptyResultOnError(t *testing.T) {
	t.Parallel()
	name, args, err := parser.ParseEvent(&parser.Packet{Type: parser.Event, Data: []byte(`["connect","x"]`)})
	requireMalformed(t, err)
	if name != "" || args.Len() != 0 {
		t.Fatalf("name = %q with %d arguments, want empty", name, args.Len())
	}
	args, err = parser.ParseAck(&parser.Packet{Type: parser.Ack, Data: []byte(`[abc]`)})
	requireMalformed(t, err)
	if args.Len() != 0 {
		t.Fatalf("%d arguments, want none", args.Len())
	}
}
