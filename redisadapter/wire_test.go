package redisadapter

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	socketio "github.com/hilmanzhy/socket.io-golang"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

// The hexadecimal literals below are MessagePack values written by notepack.io
// 3.0.1, the parser of @socket.io/redis-adapter 8.3.0, and the requests are the
// JSON that JSON.stringify writes. The node* values are the broadcasts a
// Node.js server publishes, the keys of each object in its insertion order. The
// emit* values are what notepack.io writes for the broadcasts this adapter must
// publish: the keys of each object are in the order of the JSON text of the
// data, not sorted, and the envelope is [uid, {type, data, nsp}, {rooms,
// except, flags}] in that order.

const (
	defaultKey = "socket.io"
	ownUID     = "go-a"
	peerUID    = "go-b"
	thirdUID   = "go-c"
	nodeUID    = "node-1"
)

func describe(op socketio.Operation) string {
	text := fmt.Sprintf("%+v", op)
	if op.Packet != nil {
		text += fmt.Sprintf(" packet={%+v data=%s}", *op.Packet, op.Packet.Data)
	}
	return text
}

// requirePacket compares two packets, the attachments and the offsets of their
// placeholders by length and content, so that a nil list and an empty one are
// the same.
func requirePacket(t testing.TB, got, want *parser.Packet) {
	t.Helper()
	if got.Type != want.Type || got.Namespace != want.Namespace || got.ID != want.ID || got.HasID != want.HasID {
		t.Errorf("packet = {type %v, namespace %q, id %d, has id %t}, want {type %v, namespace %q, id %d, has id %t}",
			got.Type, got.Namespace, got.ID, got.HasID, want.Type, want.Namespace, want.ID, want.HasID)
	}
	requireText(t, got.Data, string(want.Data))
	requireAttachments(t, got.Attachments, want.Attachments)
	if !slices.Equal(got.Placeholders, want.Placeholders) {
		t.Errorf("placeholder offsets = %v, want %v", got.Placeholders, want.Placeholders)
	}
}

// requireSlots checks that the offsets of a packet lead to the placeholders of
// its attachments, one for each and in their order.
func requireSlots(t testing.TB, p *parser.Packet) {
	t.Helper()
	if len(p.Placeholders) != len(p.Attachments) {
		t.Fatalf("%d placeholder offsets for %d attachments", len(p.Placeholders), len(p.Attachments))
	}
	for number, slot := range p.Placeholders {
		if slot < 0 || slot >= len(p.Data) || placeholderLength(p.Data[slot:], number) == 0 {
			t.Fatalf("offset %d of the attachment %d does not lead to its placeholder in %s", slot, number, excerptText(p.Data))
		}
	}
}

func requireOperation(t testing.TB, got, want socketio.Operation) {
	t.Helper()
	gotPacket, wantPacket := got.Packet, want.Packet
	got.Packet, want.Packet = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("operation:\n got: %s\nwant: %s", describe(got), describe(want))
	}
	switch {
	case gotPacket == nil && wantPacket == nil:
	case gotPacket == nil || wantPacket == nil:
		t.Errorf("packet = %+v, want %+v", gotPacket, wantPacket)
	default:
		requirePacket(t, gotPacket, wantPacket)
	}
}

func requireDecoded(t testing.TB, w wire, channel string, payload []byte, want socketio.Operation) {
	t.Helper()
	got, relevant, err := w.decode(channel, payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !relevant {
		t.Fatal("decode reported a message it should apply as not relevant")
	}
	requireOperation(t, got, want)
}

func requireIgnored(t testing.TB, w wire, channel string, payload []byte) {
	t.Helper()
	got, relevant, err := w.decode(channel, payload)
	if err != nil || relevant {
		t.Fatalf("decode = %s, %v, %v; want the zero operation, false, nil", describe(got), relevant, err)
	}
	requireOperation(t, got, socketio.Operation{})
}

// requireFailure checks that decoding fails with an error that wraps want, and
// reports neither an operation nor a relevant message.
func requireFailure(t testing.TB, w wire, channel string, payload []byte, want error) {
	t.Helper()
	got, relevant, err := w.decode(channel, payload)
	if !errors.Is(err, want) {
		t.Fatalf("decode = %s, %v, %v; want an error wrapping %v", describe(got), relevant, err, want)
	}
	if relevant {
		t.Error("decode reported a message it failed on as relevant")
	}
	requireOperation(t, got, socketio.Operation{})
}

func requireMalformed(t testing.TB, w wire, channel string, payload []byte) {
	t.Helper()
	requireFailure(t, w, channel, payload, errMalformed)
}

// emitOperation is the emit of a packet that holds data and attachments, with
// the placeholder offsets that slotsOf finds in data.
func emitOperation(namespace string, rooms, except []string, data string, attachments [][]byte) socketio.Operation {
	packet := &parser.Packet{Type: parser.Event, Namespace: namespace, Data: json.RawMessage(data)}
	if len(attachments) > 0 {
		packet.Type = parser.BinaryEvent
	}
	packet.Attachments, packet.Placeholders = attachments, slotsOf(data, len(attachments))
	return socketio.Operation{Kind: socketio.OpEmit, Namespace: namespace, Rooms: rooms, Except: except, Packet: packet}
}

// withSlots returns op with the placeholder offsets of its packet replaced.
func withSlots(op socketio.Operation, slots ...int) socketio.Operation {
	packet := *op.Packet
	packet.Placeholders = slots
	op.Packet = &packet
	return op
}

// blueRed is the emit of the room blue, but not of the room red, in the root
// namespace: what the hand-built envelopes below stand for.
func blueRed(data string, attachments [][]byte) socketio.Operation {
	return emitOperation("/", []string{"blue"}, []string{"red"}, data, attachments)
}

func placeholderText(n int) string {
	return fmt.Sprintf(`{"_placeholder":true,"num":%d}`, n)
}

// The functions and values below write the pieces of a broadcast by hand, from
// the specification alone, so that a test can send a message no encoder of this
// package would.

// mpWideText is a string whose header is the one of the given width in bits,
// whatever its length.
func mpWideText(s string, width int) []byte {
	switch width {
	case 8:
		return append([]byte{0xd9, byte(len(s))}, s...)
	case 16:
		return append(binary.BigEndian.AppendUint16([]byte{0xda}, uint16(len(s))), s...)
	}
	return append(binary.BigEndian.AppendUint32([]byte{0xdb}, uint32(len(s))), s...)
}

func mpTexts(values []string) []byte {
	items := make([][]byte, len(values))
	for i, value := range values {
		items[i] = mpText(value)
	}
	return mpArray(items...)
}

var (
	mpNode  = mpText(nodeUID)
	mpEvent = mpMember("type", []byte{2})
	mpData  = mpMember("data", mpArray(mpText("e")))
	mpRoot  = mpMember("nsp", mpText("/"))
	mpPlain = mpMap(mpEvent, mpData, mpRoot)
	mpOpts  = mpMap(
		mpMember("rooms", mpArray(mpText("blue"))),
		mpMember("except", mpArray(mpText("red"))),
		mpMember("flags", mpMap()),
	)
)

// mpPacket is the packet of an emit that holds data.
func mpPacket(data []byte) []byte {
	return mpMap(mpEvent, mpMember("data", data), mpRoot)
}

// mpBroadcast is the whole message that notepack.io writes for an emit of data
// by the server sender.
func mpBroadcast(sender string, data []byte, namespace string, rooms, except []string) []byte {
	return mpArray(
		mpText(sender),
		mpMap(mpEvent, mpMember("data", data), mpMember("nsp", mpText(namespace))),
		mpMap(mpMember("rooms", mpTexts(rooms)), mpMember("except", mpTexts(except)), mpMember("flags", mpMap())),
	)
}

// nestedData is the data of an event that holds the given number of containers
// in all, the outer array included, as MessagePack and as JSON text.
func nestedData(containers int, objects bool) ([]byte, string) {
	if containers == 1 {
		return mpArray(mpText("e")), `["e"]`
	}
	return mpArray(mpText("e"), nestedBytes(containers-1, objects)), `["e",` + nestedText(containers-1, objects) + `]`
}

// nesting is the deepest nesting of arrays and objects in the JSON text.
func nesting(source []byte) int {
	deepest, depth, inString := 0, 0, false
	for i := 0; i < len(source); i++ {
		switch c := source[i]; {
		case inString && c == '\\':
			i++
		case c == '"':
			inString = !inString
		case inString:
		case c == '[' || c == '{':
			depth++
			deepest = max(deepest, depth)
		case c == ']' || c == '}':
			depth--
		}
	}
	return deepest
}

const nodeBinaryEmitHex = "93a66e6f64652d3183a47479706502a46461746193a46e65777381a16101c403010203a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeTextEmitHex = "93a66e6f64652d3183a47479706502a46461746193a46e65777381a16101a178a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeAdminEmitHex = "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a62f61646d696e83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeOwnEmitHex = "93a4676f2d6183a47479706502a46461746193a46e65777381a16101a178a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeNoNamespaceEmitHex = "93a66e6f64652d3182a47479706502a46461746191a46e65777383a5726f6f6d7390a665786365707490a5666c61677380"
const nodeNoRoomsEmitHex = "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f81a5666c61677380"
const nodeNullRoomsEmitHex = "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d73c0a6657863657074c0a5666c61677380"
const nodeHTMLEmitHex = "93a66e6f64652d3183a47479706502a46461746192a46e657773b23c623e26616d703b3c2f623ee280a8e280a9a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeTwoBuffersEmitHex = "93a66e6f64652d3183a47479706502a46461746194a466696c65c4010181a5696e6e657291c4020203c400a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeNumbersEmitHex = "93a66e6f64652d3183a47479706502a46461746199a16ecb3ff8000000000000ce00011170cf0000000100000000fdc0c3a2c3a9d7ff1d5353006553f100a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeFlagsEmitHex = "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707490a5666c61677384a8636f6d7072657373c3a8766f6c6174696c65c3a774696d656f7574cd05dcb465787065637453696e676c65526573706f6e7365c2"
const nodeExtraFieldsEmitHex = "95a66e6f64652d3185a47479706502a46461746191a46e657773a36e7370a12fa2696407ab6174746163686d656e74730084a5726f6f6d7390a665786365707490a5666c61677380a56f74686572930181a16102c40104a5657874726193010203"
const nodeUnorderedEmitHex = "93a66e6f64652d3183a47479706502a46461746192a46e65777383a47a65746101a5616c70686182a17902a162920382a17104a17005a36d6964a16da36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"

const emitWithBinaryHex = "93a4676f2d6183a47479706502a46461746196a46e65777382a16101a66e657374656482a46465657093c3c0a178a166cb3ff8000000000000ce00011170fdcf0000000100000000c403010203a36e7370a12f83a5726f6f6d7392a4626c7565a5677265656ea665786365707491a3726564a5666c61677380"
const emitPlainAdminHex = "93a4676f2d6183a47479706502a46461746191a470696e67a36e7370a62f61646d696e83a5726f6f6d7390a665786365707490a5666c61677380"
const emitSingleRoomHex = "93a4676f2d6183a47479706502a46461746192a470696e67a2c3a9a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707490a5666c61677380"
const emitUnorderedHex = "93a4676f2d6183a47479706502a46461746196a46e65777383a47a65746101a5616c70686182a17901a162920282a17103a17004a36d6964a16dcd03e800ce00011170cb3ff8000000000000a36e7370a12f83a5726f6f6d7390a665786365707490a5666c61677380"
const emitNumbersHex = "93a4676f2d6183a47479706502a46461746198a16e01cd03e80000cf8ac7230489e80000cb3ff8000000000000fea36e7370a12f83a5726f6f6d7390a665786365707490a5666c61677380"
const emitStringsHex = "93a4676f2d6183a47479706502a46461746195a173a5c3a90a225ca4f09f9880a6e280a83c3e26a101a36e7370a32fc3a983a5726f6f6d7391a7c3a90a726f6f6da665786365707491a3e280a8a5666c61677380"
const emitManyBinaryHex = "93a4676f2d6183a47479706502a46461746194a166c4010181a16b92c400c4020203c5012c090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909090909a36e7370a12f83a5726f6f6d7390a665786365707490a5666c61677380"

var malformedBroadcastGoldens = []struct{ name, hex string }{
	{"not an array", "81a47479706502"},
	{"a scalar", "05"},
	{"null", "c0"},
	{"a string", "a46e657773"},
	{"empty array", "90"},
	{"one element", "91a66e6f64652d31"},
	{"two elements", "92a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f"},
	{"own uid and two elements", "92a4676f2d6183a47479706502a46461746191a46e657773a36e7370a12f"},
	{"packet type ack", "93a66e6f64652d3183a47479706503a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type binary event", "93a66e6f64652d3183a47479706505a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type connect", "93a66e6f64652d3183a47479706500a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type as text", "93a66e6f64652d3183a474797065a132a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type a float", "93a66e6f64652d3183a474797065cb4004000000000000a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type null", "93a66e6f64652d3183a474797065c0a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type true", "93a66e6f64652d3183a474797065c3a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type a buffer", "93a66e6f64652d3183a474797065c40102a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet type missing", "93a66e6f64652d3182a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet empty", "93a66e6f64652d318083a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet not a map", "93a66e6f64652d31a46e65777383a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet a list", "93a66e6f64652d31930291a46e657773a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"packet null", "93a66e6f64652d31c083a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"nsp a number", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e73700583a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"nsp a list", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e737091a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"nsp null", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370c083a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"nsp a buffer", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370c4012f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"nsp a map", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e737081a1610183a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data missing", "93a66e6f64652d3182a47479706502a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data empty", "93a66e6f64652d3183a47479706502a46461746190a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data null", "93a66e6f64652d3183a47479706502a464617461c0a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data a string", "93a66e6f64652d3183a47479706502a464617461a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data a number", "93a66e6f64652d3183a47479706502a46461746107a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data a map", "93a66e6f64652d3183a47479706502a46461746181a130a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"data a buffer", "93a66e6f64652d3183a47479706502a464617461c4046e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"first element a number", "93a66e6f64652d3183a47479706502a4646174619205a178a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"first element null", "93a66e6f64652d3183a47479706502a46461746191c0a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"first element a map", "93a66e6f64652d3183a47479706502a4646174619181a16101a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"first element a list", "93a66e6f64652d3183a47479706502a4646174619191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"first element a buffer", "93a66e6f64652d3183a47479706502a46461746191c40101a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"first element a date", "93a66e6f64652d3183a47479706502a46461746191d6ff00000000a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"reserved connect", "93a66e6f64652d3183a47479706502a46461746191a7636f6e6e656374a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"reserved connect_error", "93a66e6f64652d3183a47479706502a46461746191ad636f6e6e6563745f6572726f72a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"reserved disconnect", "93a66e6f64652d3183a47479706502a46461746192aa646973636f6e6e656374a178a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"reserved disconnecting", "93a66e6f64652d3183a47479706502a46461746191ad646973636f6e6e656374696e67a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"reserved newListener", "93a66e6f64652d3183a47479706502a46461746191ab6e65774c697374656e6572a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"reserved removeListener", "93a66e6f64652d3183a47479706502a46461746191ae72656d6f76654c697374656e6572a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"},
	{"opts a string", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12fa4626c7565"},
	{"opts a number", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f05"},
	{"opts null", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12fc0"},
	{"opts a list", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f90"},
	{"opts a list of rooms", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f91a4626c7565"},
	{"opts a buffer", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12fc40101"},
	{"rooms with a number", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7392a4626c756505a665786365707490a5666c61677380"},
	{"rooms with null", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7391c0a665786365707490a5666c61677380"},
	{"rooms with a buffer", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7391c40101a665786365707490a5666c61677380"},
	{"rooms with a map", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d739181a16101a665786365707490a5666c61677380"},
	{"rooms a string", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d73a4626c7565a665786365707490a5666c61677380"},
	{"rooms a map", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7381a16101a665786365707490a5666c61677380"},
	{"rooms a number", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7303a665786365707490a5666c61677380"},
	{"except with a number", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707492a372656405a5666c61677380"},
	{"except with null", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7390a665786365707491c0a5666c61677380"},
	{"except a string", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7390a6657863657074a3726564a5666c61677380"},
	{"except a boolean", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7390a6657863657074c3a5666c61677380"},
	{"first room fine second a number", "93a66e6f64652d3183a47479706502a46461746191a46e657773a36e7370a12f83a5726f6f6d7393a4626c7565a5677265656e07a665786365707490a5666c61677380"},
}

var nodeRequestGoldens = []struct{ name, json string }{
	{"join", `{"uid":"node-1","type":2,"opts":{"rooms":["blue"],"except":["red"]},"rooms":["r1","r2"]}`},
	{"leave", `{"uid":"node-1","type":3,"opts":{"rooms":[],"except":[]},"rooms":["r1"]}`},
	{"disconnectWithoutClose", `{"uid":"node-1","type":4,"opts":{"rooms":["blue"],"except":[]},"close":false}`},
	{"disconnectWithClose", `{"uid":"node-1","type":4,"opts":{"rooms":[],"except":["red"]},"close":true}`},
}

const nodeSampleHex = "93a66e6f64652d3183a47479706502a4646174619ba46e65777382a16101a66e657374656481a46c6973749c01fecd012cce00011170cf0000000100000000d1ff38d2ffff63c0d3ffffffff7fffffffcb3ff8000000000000c0c3c2c403010203d7ff1d5353006553f100d6ff6553f100c70cff3b8b87c0ffffffffffffffffd928c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9c3a9da012c787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878c5012c0707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707079080a36e7370a12f83a5726f6f6d7391a4626c7565a665786365707491a3726564a5666c61677380"
const nodeSampleData = "[\"news\",{\"a\":1,\"nested\":{\"list\":[1,-2,300,70000,4294967296,-200,-40000,-2147483649,1.5,null,true,false]}},{\"_placeholder\":true,\"num\":0},\"2023-11-14T22:13:20.123Z\",\"2023-11-14T22:13:20.000Z\",\"1969-12-31T23:59:59.999Z\",\"éééééééééééééééééééé\",\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\",{\"_placeholder\":true,\"num\":1},[],{}]"

var nodeSampleAttachments = [][]byte{{1, 2, 3}, {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}}

func TestWireChannels(t *testing.T) {
	t.Parallel()
	emitTo := func(namespace string, rooms ...string) socketio.Operation {
		return emitOperation(namespace, rooms, nil, `["ping"]`, nil)
	}
	requestFor := func(kind socketio.OperationKind, namespace string, rooms ...string) socketio.Operation {
		return socketio.Operation{Kind: kind, Namespace: namespace, Rooms: rooms, Targets: []string{"r"}}
	}
	cases := []struct {
		name string
		key  string
		op   socketio.Operation
		want string
	}{
		{"emit in the root namespace", defaultKey, emitTo("/"), "socket.io#/#"},
		{"emit to one room", defaultKey, emitTo("/", "blue"), "socket.io#/#blue#"},
		{"emit to two rooms", defaultKey, emitTo("/", "blue", "green"), "socket.io#/#"},
		{"emit to three rooms", defaultKey, emitTo("/", "a", "b", "c"), "socket.io#/#"},
		{"emit to an empty list of rooms", defaultKey, emitOperation("/", []string{}, nil, `["ping"]`, nil), "socket.io#/#"},
		{"emit to everyone but one room", defaultKey, emitOperation("/", nil, []string{"red"}, `["ping"]`, nil), "socket.io#/#"},
		{"emit to one room but another", defaultKey, emitOperation("/", []string{"blue"}, []string{"red"}, `["ping"]`, nil), "socket.io#/#blue#"},
		{"emit in a named namespace", defaultKey, emitTo("/admin"), "socket.io#/admin#"},
		{"emit to one room of a named namespace", defaultKey, emitTo("/admin", "blue"), "socket.io#/admin#blue#"},
		{"emit to a room named with a separator", defaultKey, emitTo("/", "a#b"), "socket.io#/#a#b#"},
		{"binary emit to one room", defaultKey, emitOperation("/", []string{"blue"}, nil, `["f",`+placeholderText(0)+`]`, [][]byte{{1}}), "socket.io#/#blue#"},
		{"join", defaultKey, requestFor(socketio.OpJoin, "/"), "socket.io-request#/#"},
		{"leave", defaultKey, requestFor(socketio.OpLeave, "/"), "socket.io-request#/#"},
		{"disconnect", defaultKey, requestFor(socketio.OpDisconnect, "/"), "socket.io-request#/#"},
		{"join in a named namespace", defaultKey, requestFor(socketio.OpJoin, "/admin"), "socket.io-request#/admin#"},
		{"leave in a named namespace", defaultKey, requestFor(socketio.OpLeave, "/admin"), "socket.io-request#/admin#"},
		{"disconnect in a named namespace", defaultKey, requestFor(socketio.OpDisconnect, "/admin"), "socket.io-request#/admin#"},
		{"join selecting one room", defaultKey, requestFor(socketio.OpJoin, "/", "blue"), "socket.io-request#/#"},
		{"emit with a custom key", "app", emitTo("/"), "app#/#"},
		{"emit to one room with a custom key", "app", emitTo("/admin", "blue"), "app#/admin#blue#"},
		{"join with a custom key", "app", requestFor(socketio.OpJoin, "/"), "app-request#/#"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := newWire(tc.key, ownUID).encode(tc.op)
			if err != nil {
				t.Fatal(err)
			}
			if msg.channel != tc.want {
				t.Errorf("channel = %q, want %q", msg.channel, tc.want)
			}
		})
	}
}

func TestWirePatterns(t *testing.T) {
	t.Parallel()
	cases := []struct {
		key  string
		want []string
	}{
		{defaultKey, []string{"socket.io#*", "socket.io-request#*"}},
		{"app", []string{"app#*", "app-request#*"}},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			requireEqual(t, "patterns", newWire(tc.key, ownUID).patterns(), tc.want)
		})
	}
}

func TestWireRequestJSON(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		op   socketio.Operation
		want string
	}{
		{
			"join",
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"blue"}, Except: []string{"red"}, Targets: []string{"r1", "r2"}},
			`{"uid":"go-a","type":2,"opts":{"rooms":["blue"],"except":["red"]},"rooms":["r1","r2"]}`,
		},
		{
			"join without a selection",
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Targets: []string{"r1"}},
			`{"uid":"go-a","type":2,"opts":{"rooms":[],"except":[]},"rooms":["r1"]}`,
		},
		{
			"join without rooms to join",
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/"},
			`{"uid":"go-a","type":2,"opts":{"rooms":[],"except":[]},"rooms":[]}`,
		},
		{
			"leave",
			socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Rooms: []string{"blue", "green"}, Except: []string{"red"}, Targets: []string{"r1"}},
			`{"uid":"go-a","type":3,"opts":{"rooms":["blue","green"],"except":["red"]},"rooms":["r1"]}`,
		},
		{
			"leave without a selection",
			socketio.Operation{Kind: socketio.OpLeave, Namespace: "/admin", Targets: []string{"r1", "r2"}},
			`{"uid":"go-a","type":3,"opts":{"rooms":[],"except":[]},"rooms":["r1","r2"]}`,
		},
		{
			"disconnect",
			socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{"blue"}},
			`{"uid":"go-a","type":4,"opts":{"rooms":["blue"],"except":[]},"close":false}`,
		},
		{
			"disconnect and close",
			socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Except: []string{"red"}, Close: true},
			`{"uid":"go-a","type":4,"opts":{"rooms":[],"except":["red"]},"close":true}`,
		},
		{
			"disconnect everyone",
			socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/admin"},
			`{"uid":"go-a","type":4,"opts":{"rooms":[],"except":[]},"close":false}`,
		},
		{
			"rooms with the characters that HTML gives a meaning",
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"<b>&</b>"}, Except: []string{"a>b"}, Targets: []string{"x&y"}},
			`{"uid":"go-a","type":2,"opts":{"rooms":["<b>&</b>"],"except":["a>b"]},"rooms":["x&y"]}`,
		},
		{
			"rooms with a quote, a backslash and a newline",
			socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Rooms: []string{"a\"b\\c\nd"}, Targets: []string{"r"}},
			`{"uid":"go-a","type":3,"opts":{"rooms":["a\"b\\c\nd"],"except":[]},"rooms":["r"]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := newWire(defaultKey, ownUID).encode(tc.op)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(msg.payload); got != tc.want {
				t.Errorf("payload:\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

var emitGoldens = []struct {
	name    string
	op      socketio.Operation
	channel string
	hex     string
	want    socketio.Operation
}{
	{
		"binary event to two rooms",
		emitOperation("/", []string{"blue", "green"}, []string{"red"},
			`["news",{"a":1,"nested":{"deep":[true,null,"x"],"f":1.5}},70000,-3,4294967296,`+placeholderText(0)+`]`,
			[][]byte{{1, 2, 3}}),
		"socket.io#/#",
		emitWithBinaryHex,
		emitOperation("/", []string{"blue", "green"}, []string{"red"},
			`["news",{"a":1,"nested":{"deep":[true,null,"x"],"f":1.5}},70000,-3,4294967296,`+placeholderText(0)+`]`,
			[][]byte{{1, 2, 3}}),
	},
	{
		"plain event without rooms in a named namespace",
		emitOperation("/admin", nil, nil, `["ping"]`, nil),
		"socket.io#/admin#",
		emitPlainAdminHex,
		emitOperation("/admin", []string{}, []string{}, `["ping"]`, nil),
	},
	{
		"plain event to one room",
		emitOperation("/", []string{"blue"}, nil, `["ping","é"]`, nil),
		"socket.io#/#blue#",
		emitSingleRoomHex,
		emitOperation("/", []string{"blue"}, []string{}, `["ping","é"]`, nil),
	},
	{
		"members that are not in alphabetical order",
		emitOperation("/", nil, nil, `["news",{"zeta":1,"alpha":{"y":1,"b":[2,{"q":3,"p":4}]},"mid":"m"},1000,0,70000,1.5]`, nil),
		"socket.io#/#",
		emitUnorderedHex,
		emitOperation("/", []string{}, []string{}, `["news",{"zeta":1,"alpha":{"y":1,"b":[2,{"q":3,"p":4}]},"mid":"m"},1000,0,70000,1.5]`, nil),
	},
	{
		"numbers written in other ways",
		emitOperation("/", nil, nil, `["n",1.0,1e3,-0,-0.0,10000000000000000000,1.5,-2]`, nil),
		"socket.io#/#",
		emitNumbersHex,
		emitOperation("/", []string{}, []string{}, `["n",1,1000,0,0,10000000000000000000,1.5,-2]`, nil),
	},
	{
		"escapes, emoji and separators in the data, the namespace and the rooms",
		emitOperation("/é", []string{"é\nroom"}, []string{"\u2028"}, `["s","é\n\"\\","😀","\u2028<>&","\u0001"]`, nil),
		"socket.io#/é#é\nroom#",
		emitStringsHex,
		emitOperation("/é", []string{"é\nroom"}, []string{"\u2028"}, "[\"s\",\"é\\n\\\"\\\\\",\"😀\",\"\u2028<>&\",\"\\u0001\"]", nil),
	},
	{
		"several buffers, one of them empty and one of 300 bytes",
		emitOperation("/", nil, nil, `["f",`+placeholderText(0)+`,{"k":[`+placeholderText(1)+`,`+placeholderText(2)+`]},`+placeholderText(3)+`]`,
			[][]byte{{1}, {}, {2, 3}, bytes.Repeat([]byte{9}, 300)}),
		"socket.io#/#",
		emitManyBinaryHex,
		emitOperation("/", []string{}, []string{}, `["f",`+placeholderText(0)+`,{"k":[`+placeholderText(1)+`,`+placeholderText(2)+`]},`+placeholderText(3)+`]`,
			[][]byte{{1}, {}, {2, 3}, bytes.Repeat([]byte{9}, 300)}),
	},
}

func TestWireEmitBytes(t *testing.T) {
	t.Parallel()
	for _, tc := range emitGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := newWire(defaultKey, ownUID).encode(tc.op)
			if err != nil {
				t.Fatal(err)
			}
			if msg.channel != tc.channel {
				t.Errorf("channel = %q, want %q", msg.channel, tc.channel)
			}
			requireBytes(t, msg.payload, unhex(t, tc.hex))
		})
	}
}

func TestWireDecodesWhatItEmits(t *testing.T) {
	t.Parallel()
	for _, tc := range emitGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireDecoded(t, newWire(defaultKey, peerUID), tc.channel, unhex(t, tc.hex), tc.want)
		})
	}
}

func TestWireEncodesTheEnvelopeInNodeOrder(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("n", 300)
	many := make([]string, 70_000)
	for i := range many {
		many[i] = fmt.Sprint(i % 10)
	}
	cases := []struct {
		name    string
		op      socketio.Operation
		channel string
		want    []byte
	}{
		{
			"no rooms",
			emitOperation("/", nil, nil, `["e"]`, nil),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e")), "/", nil, nil),
		},
		{
			"rooms and except",
			emitOperation("/", []string{"blue", "green"}, []string{"red"}, `["e"]`, nil),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e")), "/", []string{"blue", "green"}, []string{"red"}),
		},
		{
			"a namespace of 300 bytes",
			emitOperation(long, nil, nil, `["e"]`, nil),
			"socket.io#" + long + "#",
			mpBroadcast(ownUID, mpArray(mpText("e")), long, nil, nil),
		},
		{
			"one room of 300 bytes",
			emitOperation("/", []string{long}, nil, `["e"]`, nil),
			"socket.io#/#" + long + "#",
			mpBroadcast(ownUID, mpArray(mpText("e")), "/", []string{long}, nil),
		},
		{
			"16 rooms",
			emitOperation("/", slices.Repeat([]string{"r"}, 16), nil, `["e"]`, nil),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e")), "/", slices.Repeat([]string{"r"}, 16), nil),
		},
		{
			"70,000 rooms",
			emitOperation("/", many, many, `["e"]`, nil),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e")), "/", many, many),
		},
		{
			"an empty namespace",
			emitOperation("", nil, nil, `["e"]`, nil),
			"socket.io##",
			mpBroadcast(ownUID, mpArray(mpText("e")), "", nil, nil),
		},
		{
			"a buffer",
			emitOperation("/", nil, nil, `["e",`+placeholderText(0)+`]`, [][]byte{{1, 2, 3}}),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e"), mpBin([]byte{1, 2, 3})), "/", nil, nil),
		},
		{
			"a buffer used twice",
			emitOperation("/", nil, nil, `["e",`+placeholderText(0)+`,`+placeholderText(0)+`]`, [][]byte{{1}}),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e"), mpBin([]byte{1}), mpMap(mpMember("_placeholder", []byte{0xc3}), mpMember("num", []byte{0}))), "/", nil, nil),
		},
		{
			"a buffer of 70,000 bytes",
			emitOperation("/", nil, nil, `["e",`+placeholderText(0)+`]`, [][]byte{bytes.Repeat([]byte{5}, 70_000)}),
			"socket.io#/#",
			mpBroadcast(ownUID, mpArray(mpText("e"), mpBin(bytes.Repeat([]byte{5}, 70_000))), "/", nil, nil),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := newWire(defaultKey, ownUID).encode(tc.op)
			if err != nil {
				t.Fatal(err)
			}
			if msg.channel != tc.channel {
				t.Errorf("channel = %q, want %q", msg.channel[:min(len(msg.channel), 80)], tc.channel[:min(len(tc.channel), 80)])
			}
			requireBytes(t, msg.payload, tc.want)
		})
	}
}

func TestWireDecodesWhatNodeSends(t *testing.T) {
	t.Parallel()
	const channel = "socket.io#/#"
	cases := []struct {
		name    string
		hex     string
		channel string
		want    socketio.Operation
	}{
		{
			"binary event",
			nodeBinaryEmitHex,
			"socket.io#/#blue#",
			blueRed(`["news",{"a":1},`+placeholderText(0)+`]`, [][]byte{{1, 2, 3}}),
		},
		{
			"text event",
			nodeTextEmitHex,
			"socket.io#/#blue#",
			blueRed(`["news",{"a":1},"x"]`, nil),
		},
		{
			"named namespace",
			nodeAdminEmitHex,
			"socket.io#/admin#blue#",
			emitOperation("/admin", []string{"blue"}, []string{"red"}, `["news"]`, nil),
		},
		{
			"namespace left out",
			nodeNoNamespaceEmitHex,
			channel,
			emitOperation("/", []string{}, []string{}, `["news"]`, nil),
		},
		{
			"rooms and except left out",
			nodeNoRoomsEmitHex,
			channel,
			emitOperation("/", nil, nil, `["news"]`, nil),
		},
		{
			"rooms and except null",
			nodeNullRoomsEmitHex,
			channel,
			emitOperation("/", nil, nil, `["news"]`, nil),
		},
		{
			"markup and separators in text",
			nodeHTMLEmitHex,
			"socket.io#/#blue#",
			blueRed("[\"news\",\"<b>&amp;</b>\u2028\u2029\"]", nil),
		},
		{
			"several buffers",
			nodeTwoBuffersEmitHex,
			"socket.io#/#blue#",
			blueRed(`["file",`+placeholderText(0)+`,{"inner":[`+placeholderText(1)+`]},`+placeholderText(2)+`]`, [][]byte{{1}, {2, 3}, {}}),
		},
		{
			"numbers, null and a date",
			nodeNumbersEmitHex,
			"socket.io#/#blue#",
			blueRed(`["n",1.5,70000,4294967296,-3,null,true,"é","2023-11-14T22:13:20.123Z"]`, nil),
		},
		{
			"flags that this adapter does not read",
			nodeFlagsEmitHex,
			"socket.io#/#blue#",
			emitOperation("/", []string{"blue"}, []string{}, `["news"]`, nil),
		},
		{
			"members and elements that this adapter does not read",
			nodeExtraFieldsEmitHex,
			channel,
			emitOperation("/", []string{}, []string{}, `["news"]`, nil),
		},
		{
			"members that are not in alphabetical order",
			nodeUnorderedEmitHex,
			channel,
			blueRed(`["news",{"zeta":1,"alpha":{"y":2,"b":[3,{"q":4,"p":5}]},"mid":"m"}]`, nil),
		},
		{
			"every kind of value",
			nodeSampleHex,
			"socket.io#/#blue#",
			blueRed(nodeSampleData, nodeSampleAttachments),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireDecoded(t, newWire(defaultKey, ownUID), tc.channel, unhex(t, tc.hex), tc.want)
		})
	}
	t.Run("a broadcast of a server that has the uid of the sender", func(t *testing.T) {
		t.Parallel()
		requireDecoded(t, newWire(defaultKey, peerUID), channel, unhex(t, nodeOwnEmitHex),
			blueRed(`["news",{"a":1},"x"]`, nil))
	})
}

func TestWireDecodedEmitPacketShape(t *testing.T) {
	t.Parallel()
	t.Run("binary event has the binary packet type", func(t *testing.T) {
		t.Parallel()
		op, relevant, err := newWire(defaultKey, ownUID).decode("socket.io#/#", unhex(t, nodeBinaryEmitHex))
		if err != nil || !relevant {
			t.Fatalf("decode = %v, %v", relevant, err)
		}
		if op.Kind != socketio.OpEmit || op.Packet == nil {
			t.Fatalf("operation = %s", describe(op))
		}
		if op.Packet.Type != parser.BinaryEvent || len(op.Packet.Attachments) != 1 {
			t.Errorf("packet type = %v with %d attachments, want %v with 1", op.Packet.Type, len(op.Packet.Attachments), parser.BinaryEvent)
		}
	})
	t.Run("event without binary has the event packet type", func(t *testing.T) {
		t.Parallel()
		op, relevant, err := newWire(defaultKey, ownUID).decode("socket.io#/#", unhex(t, nodeTextEmitHex))
		if err != nil || !relevant {
			t.Fatalf("decode = %v, %v", relevant, err)
		}
		if op.Packet == nil || op.Packet.Type != parser.Event || len(op.Packet.Attachments) != 0 {
			t.Errorf("operation = %s, want the event packet type and no attachments", describe(op))
		}
	})
	t.Run("data is valid JSON that holds what was sent", func(t *testing.T) {
		t.Parallel()
		op, _, err := newWire(defaultKey, ownUID).decode("socket.io#/#", unhex(t, nodeHTMLEmitHex))
		if err != nil {
			t.Fatal(err)
		}
		var data []string
		if err := json.Unmarshal(op.Packet.Data, &data); err != nil {
			t.Fatalf("data %s: %v", op.Packet.Data, err)
		}
		requireEqual(t, "data", data, []string{"news", "<b>&amp;</b>\u2028\u2029"})
	})
	t.Run("a map that looks like a placeholder is data, not an attachment", func(t *testing.T) {
		t.Parallel()
		lookalike := mpMap(mpMember("_placeholder", []byte{0xc3}), mpMember("num", []byte{0}))
		payload := mpBroadcast(nodeUID, mpArray(mpText("e"), lookalike), "/", nil, nil)
		requireDecoded(t, newWire(defaultKey, ownUID), "socket.io#/#", payload,
			emitOperation("/", []string{}, []string{}, `["e",`+placeholderText(0)+`]`, nil))
	})
	t.Run("the data of two emits do not share a buffer", func(t *testing.T) {
		t.Parallel()
		w := newWire(defaultKey, ownUID)
		first, _, err := w.decode("socket.io#/#", unhex(t, nodeTextEmitHex))
		if err != nil {
			t.Fatal(err)
		}
		before := string(first.Packet.Data)
		if _, _, err := w.decode("socket.io#/#", unhex(t, nodeNumbersEmitHex)); err != nil {
			t.Fatal(err)
		}
		requireText(t, first.Packet.Data, before)
	})
}

func TestWireDecodesWhatNodeSendsAsRequests(t *testing.T) {
	t.Parallel()
	wants := map[string]socketio.Operation{
		"join": {
			Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"blue"}, Except: []string{"red"}, Targets: []string{"r1", "r2"},
		},
		"leave": {
			Kind: socketio.OpLeave, Namespace: "/", Rooms: []string{}, Except: []string{}, Targets: []string{"r1"},
		},
		"disconnectWithoutClose": {
			Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{"blue"}, Except: []string{},
		},
		"disconnectWithClose": {
			Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{}, Except: []string{"red"}, Close: true,
		},
	}
	for _, tc := range nodeRequestGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want, found := wants[tc.name]
			if !found {
				t.Fatalf("no expectation for the request %q", tc.name)
			}
			requireDecoded(t, newWire(defaultKey, ownUID), "socket.io-request#/#", []byte(tc.json), want)
		})
	}
	extra := []struct {
		name    string
		peer    wire
		channel string
		payload string
		want    socketio.Operation
	}{
		{
			"named namespace",
			newWire(defaultKey, ownUID),
			"socket.io-request#/admin#",
			`{"uid":"node-1","type":3,"opts":{"rooms":[],"except":[]},"rooms":["r1"]}`,
			socketio.Operation{Kind: socketio.OpLeave, Namespace: "/admin", Rooms: []string{}, Except: []string{}, Targets: []string{"r1"}},
		},
		{
			"without uid",
			newWire(defaultKey, ownUID),
			"socket.io-request#/#",
			`{"type":2,"opts":{"rooms":[],"except":[]},"rooms":["r"]}`,
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{}, Except: []string{}, Targets: []string{"r"}},
		},
		{
			"fields it does not know",
			newWire(defaultKey, ownUID),
			"socket.io-request#/#",
			`{"uid":"node-1","requestId":"x","type":3,"opts":{"rooms":["blue"],"except":[],"flags":{}},"rooms":["r"],"data":[1]}`,
			socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Rooms: []string{"blue"}, Except: []string{}, Targets: []string{"r"}},
		},
		{
			"custom key",
			newWire("app", ownUID),
			"app-request#/#",
			`{"uid":"node-1","type":4,"opts":{"rooms":[],"except":[]},"close":true}`,
			socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{}, Except: []string{}, Close: true},
		},
		{
			"null in place of every list of rooms",
			newWire(defaultKey, ownUID),
			"socket.io-request#/#",
			`{"uid":"node-1","type":2,"opts":{"rooms":null,"except":null},"rooms":null}`,
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/"},
		},
		{
			"the room without a name",
			newWire(defaultKey, ownUID),
			"socket.io-request#/#",
			`{"uid":"node-1","type":2,"opts":{"rooms":[""],"except":[""]},"rooms":[""]}`,
			socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{""}, Except: []string{""}, Targets: []string{""}},
		},
	}
	for _, tc := range extra {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireDecoded(t, tc.peer, tc.channel, []byte(tc.payload), tc.want)
		})
	}
}

var roundTripCases = []struct {
	name string
	op   socketio.Operation
	want socketio.Operation
}{
	{
		name: "emit to everyone",
		op:   emitOperation("/", nil, nil, `["news",{"a":1}]`, nil),
		want: emitOperation("/", []string{}, []string{}, `["news",{"a":1}]`, nil),
	},
	{
		name: "emit to rooms except a room",
		op:   emitOperation("/", []string{"blue", "green"}, []string{"red"}, `["news","x"]`, nil),
	},
	{
		name: "emit to one room",
		op:   emitOperation("/", []string{"blue"}, nil, `["news","x"]`, nil),
		want: emitOperation("/", []string{"blue"}, []string{}, `["news","x"]`, nil),
	},
	{
		name: "emit in a named namespace",
		op:   emitOperation("/admin", []string{"blue"}, []string{"red"}, `["news"]`, nil),
	},
	{
		name: "emit numbers, null, text and markup",
		op:   emitOperation("/", []string{"blue"}, []string{"red"}, `["n",1.5,70000,4294967296,-3,null,true,"é","<b>&</b>"]`, nil),
	},
	{
		name: "emit with binary",
		op: emitOperation("/", []string{"blue"}, []string{"red"},
			`["news",`+placeholderText(0)+`,{"k":`+placeholderText(1)+`}]`,
			[][]byte{{1, 2, 3}, {4}}),
	},
	{
		name: "emit with an empty binary",
		op:   emitOperation("/", []string{"blue"}, []string{"red"}, `["f",`+placeholderText(0)+`]`, [][]byte{{}}),
	},
	{
		name: "join",
		op:   socketio.Operation{Kind: socketio.OpJoin, Namespace: "/admin", Rooms: []string{"blue"}, Except: []string{"red"}, Targets: []string{"r1", "r2"}},
	},
	{
		name: "join without a selection",
		op:   socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Targets: []string{"r1"}},
		want: socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{}, Except: []string{}, Targets: []string{"r1"}},
	},
	{
		name: "leave",
		op:   socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Rooms: []string{"blue"}, Except: []string{"red"}, Targets: []string{"r1"}},
	},
	{
		name: "disconnect",
		op:   socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{"blue"}},
		want: socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{"blue"}, Except: []string{}},
	},
	{
		name: "disconnect and close",
		op:   socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/admin", Except: []string{"red"}, Close: true},
		want: socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/admin", Rooms: []string{}, Except: []string{"red"}, Close: true},
	},
}

func TestWireRoundTripBetweenTwoNodes(t *testing.T) {
	t.Parallel()
	sender, receiver := newWire(defaultKey, ownUID), newWire(defaultKey, peerUID)
	for _, tc := range roundTripCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := sender.encode(tc.op)
			if err != nil {
				t.Fatal(err)
			}
			want := tc.want
			if want.Kind == "" {
				want = tc.op
			}
			requireDecoded(t, receiver, msg.channel, msg.payload, want)
		})
	}
}

func TestWireIgnoresItsOwnMessages(t *testing.T) {
	t.Parallel()
	sender := newWire(defaultKey, ownUID)
	for _, tc := range roundTripCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := sender.encode(tc.op)
			if err != nil {
				t.Fatal(err)
			}
			requireIgnored(t, sender, msg.channel, msg.payload)
		})
	}
	t.Run("broadcast of a Node.js server with the same uid", func(t *testing.T) {
		t.Parallel()
		requireIgnored(t, newWire(defaultKey, ownUID), "socket.io#/#", unhex(t, nodeOwnEmitHex))
	})
	for _, tc := range nodeRequestGoldens {
		t.Run("request "+tc.name, func(t *testing.T) {
			t.Parallel()
			requireIgnored(t, newWire(defaultKey, nodeUID), "socket.io-request#/#", []byte(tc.json))
		})
	}
	t.Run("own malformed broadcast", func(t *testing.T) {
		t.Parallel()
		payload := unhex(t, "93a66e6f64652d3181a47479706502a5636f6e746f")
		requireIgnored(t, newWire(defaultKey, nodeUID), "socket.io#/#", payload)
	})
}

func TestWireIgnoresRequestsItDoesNotTakePartIn(t *testing.T) {
	t.Parallel()
	const opts = `"opts":{"rooms":[],"except":[]}`
	cases := []struct{ name, payload string }{
		{"without opts", `{"uid":"node-1","type":2,"rooms":["r"]}`},
		{"with null opts", `{"uid":"node-1","type":2,"opts":null,"rooms":["r"]}`},
		{"empty object", `{}`},
		{"without a type", `{"uid":"node-1",` + opts + `,"rooms":["r"]}`},
		{"empty payload", ``},
		{"array", `[]`},
		{"null", `null`},
		{"text", `join`},
		{"number", `5`},
		{"leading space", ` {"uid":"node-1","type":2,` + opts + `,"rooms":["r"]}`},
		{"leading newline", "\n{\"uid\":\"node-1\",\"type\":2," + opts + `,"rooms":["r"]}`},
		{"MessagePack", string(unhex(t, nodeBinaryEmitHex))},
	}
	for _, code := range []int{-1, 0, 1, 5, 6, 7, 8, 9, 10, 11, 99} {
		cases = append(cases, struct{ name, payload string }{
			fmt.Sprintf("type %d", code),
			fmt.Sprintf(`{"uid":"node-1","type":%d,%s,"rooms":["r"]}`, code, opts),
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireIgnored(t, newWire(defaultKey, ownUID), "socket.io-request#/#", []byte(tc.payload))
		})
	}
}

func TestWireRejectsMalformedRequests(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, payload string }{
		{"brace only", `{`},
		{"cut after a key", `{"uid":`},
		{"cut inside a string", `{"uid":"node`},
		{"text after the object", `{"uid":"node-1","type":2,"opts":{}} x`},
		{"second object after the first", `{"uid":"node-1","type":2,"opts":{}}{}`},
		{"uid a number", `{"uid":5,"type":2,"opts":{"rooms":[],"except":[]}}`},
		{"type a string", `{"uid":"node-1","type":"2","opts":{"rooms":[],"except":[]}}`},
		{"type a fraction", `{"uid":"node-1","type":2.5,"opts":{"rooms":[],"except":[]}}`},
		{"opts a string", `{"uid":"node-1","type":2,"opts":"blue"}`},
		{"opts rooms a string", `{"uid":"node-1","type":2,"opts":{"rooms":"blue","except":[]}}`},
		{"opts rooms holding a number", `{"uid":"node-1","type":2,"opts":{"rooms":[1],"except":[]}}`},
		{"rooms a string", `{"uid":"node-1","type":2,"opts":{"rooms":[],"except":[]},"rooms":"r"}`},
		{"rooms holding a number", `{"uid":"node-1","type":2,"opts":{"rooms":[],"except":[]},"rooms":[1]}`},
		{"close a string", `{"uid":"node-1","type":4,"opts":{"rooms":[],"except":[]},"close":"yes"}`},
		{"opts rooms holding null", `{"uid":"node-1","type":2,"opts":{"rooms":[null],"except":[]},"rooms":["r"]}`},
		{"opts except holding null", `{"uid":"node-1","type":2,"opts":{"rooms":[],"except":["red",null]},"rooms":["r"]}`},
		{"rooms holding null", `{"uid":"node-1","type":2,"opts":{"rooms":[],"except":[]},"rooms":["r",null]}`},
		{"opts rooms holding null in a disconnect", `{"uid":"node-1","type":4,"opts":{"rooms":[null],"except":[]},"close":false}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireMalformed(t, newWire(defaultKey, ownUID), "socket.io-request#/#", []byte(tc.payload))
		})
	}
}

func TestWireRejectsMalformedBroadcasts(t *testing.T) {
	t.Parallel()
	for _, tc := range malformedBroadcastGoldens {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireMalformed(t, newWire(defaultKey, ownUID), "socket.io#/#", unhex(t, tc.hex))
		})
	}
}

func TestWireRejectsBroadcastsThatAreNotMessagePack(t *testing.T) {
	t.Parallel()
	valid := unhex(t, nodeBinaryEmitHex)
	cases := []struct {
		name    string
		payload []byte
		want    error
	}{
		{"empty payload", nil, errTruncated},
		{"JSON object, whose first byte is a number", []byte(`{"uid":"node-1","type":2}`), errMalformed},
		{"text, whose first byte is a number", []byte("hello"), errMalformed},
		{"a number alone", []byte{0x05}, errMalformed},
		{"a number and one byte more", []byte{0x05, 0x00}, errMalformed},
		{"one byte too many", append(slices.Clone(valid), 0x00), errTrailing},
		{"unused type byte", []byte{0xc1}, errUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireFailure(t, newWire(defaultKey, ownUID), "socket.io#/#", tc.payload, tc.want)
		})
	}
	t.Run("every strict prefix of a valid broadcast", func(t *testing.T) {
		t.Parallel()
		for length := range len(valid) {
			op, relevant, err := newWire(defaultKey, ownUID).decode("socket.io#/#", valid[:length])
			if !errors.Is(err, errTruncated) || relevant {
				t.Fatalf("decode of the first %d of %d bytes = %s, %v, %v; want an error wrapping errTruncated", length, len(valid), describe(op), relevant, err)
			}
		}
	})
}

func TestWireIgnoresOtherChannels(t *testing.T) {
	t.Parallel()
	channels := []string{
		"",
		"socket.io",
		"socket.io-request",
		"socket.io-response#/#go-b#",
		"socket.io-response#/#",
		"socketio#/#",
		"xsocket.io#/#",
		"Socket.io#/#",
		"other#/#",
		"app#/#",
		"app-request#/#",
		"socket.io#",
		"socket.io##",
		"socket.io#admin#",
		"socket.io#admin#blue#",
		"socket.io#blue",
	}
	payloads := []struct {
		name string
		data []byte
	}{
		{"broadcast", unhex(t, nodeBinaryEmitHex)},
		{"request", []byte(nodeRequestGoldens[0].json)},
		{"garbage", []byte{0xff, 0xc1}},
		{"empty", nil},
	}
	for _, channel := range channels {
		for _, payload := range payloads {
			t.Run(channel+" with "+payload.name, func(t *testing.T) {
				t.Parallel()
				requireIgnored(t, newWire(defaultKey, ownUID), channel, payload.data)
			})
		}
	}
	t.Run("default key channels on a wire with a custom key", func(t *testing.T) {
		t.Parallel()
		requireIgnored(t, newWire("app", ownUID), "socket.io#/#", unhex(t, nodeBinaryEmitHex))
		requireIgnored(t, newWire("app", ownUID), "socket.io-request#/#", []byte(nodeRequestGoldens[0].json))
	})
}

func TestWireCountsAnEmitOnlyOnTheChannelItIsPublishedOn(t *testing.T) {
	t.Parallel()
	root := mpBroadcast(nodeUID, mpArray(mpText("e")), "/", []string{"blue"}, []string{"red"})
	admin := mpBroadcast(nodeUID, mpArray(mpText("e")), "/admin", []string{"blue"}, []string{"red"})
	unnamed := envelope(mpNode, mpMap(mpEvent, mpData), mpOpts)
	inRoot := blueRed(`["e"]`, nil)
	inAdmin := emitOperation("/admin", []string{"blue"}, []string{"red"}, `["e"]`, nil)
	slashless := mpBroadcast(nodeUID, mpArray(mpText("e")), "admin", nil, nil)
	nameless := envelope(mpNode, mpMap(mpEvent, mpData, mpMember("nsp", mpText(""))), mpOpts)
	toRooms := func(rooms ...string) []byte {
		return mpBroadcast(nodeUID, mpArray(mpText("e")), "/", rooms, nil)
	}
	inRooms := func(rooms ...string) socketio.Operation {
		return emitOperation("/", append([]string{}, rooms...), []string{}, `["e"]`, nil)
	}
	accepted := []struct {
		name    string
		key     string
		channel string
		payload []byte
		want    socketio.Operation
	}{
		{"the channel of the namespace", "socket.io", "socket.io#/#", root, inRoot},
		{"the channel of the room", "socket.io", "socket.io#/#blue#", root, inRoot},
		{"the channel of a named namespace", "socket.io", "socket.io#/admin#", admin, inAdmin},
		{"the channel of a room of a named namespace", "socket.io", "socket.io#/admin#blue#", admin, inAdmin},
		{"a namespace left out, on the channel of the main one", "socket.io", "socket.io#/#", unnamed, inRoot},
		{"a key that holds the separator", "a#b", "a#b#/#", root, inRoot},
		{"the channel of the namespace, for several rooms", "socket.io", "socket.io#/#", toRooms("blue", "green"), inRooms("blue", "green")},
		{"the channel of the namespace, for no room", "socket.io", "socket.io#/#", toRooms(), inRooms()},
		{"the channel of a room that holds the separator", "socket.io", "socket.io#/#a#b#", toRooms("a#b"), inRooms("a#b")},
		{"the channel of the room without a name", "socket.io", "socket.io#/##", toRooms(""), inRooms("")},
	}
	for _, tc := range accepted {
		t.Run("applies "+tc.name, func(t *testing.T) {
			t.Parallel()
			requireDecoded(t, newWire(tc.key, ownUID), tc.channel, tc.payload, tc.want)
		})
	}
	ignored := []struct {
		name    string
		key     string
		channel string
		payload []byte
	}{
		{"the channel of a key that starts with this one", "a", "a#b#/#", root},
		{"the channel of a room of such a key", "a", "a#b#/#blue#", root},
		{"the channel of another namespace", "socket.io", "socket.io#/admin#", root},
		{"the channel of a room of another namespace", "socket.io", "socket.io#/admin#blue#", root},
		{"the channel of the main namespace, for a named one", "socket.io", "socket.io#/#", admin},
		{"the channel of a namespace that starts with its own", "socket.io", "socket.io#/administrators#", admin},
		{"the channel of a namespace that its own starts with", "socket.io", "socket.io#/adm#", admin},
		{"a namespace left out, on the channel of a named one", "socket.io", "socket.io#/admin#", unnamed},
		{"a channel without the closing separator", "socket.io", "socket.io#/", root},
		{"the prefix alone", "socket.io", "socket.io#", root},
		{"the channel of another room", "socket.io", "socket.io#/#green#", root},
		{"the channel of a room that its own starts with", "socket.io", "socket.io#/#blu#", root},
		{"the channel of a room that starts with its own", "socket.io", "socket.io#/#blues#", root},
		{"the channel of its room with more after it", "socket.io", "socket.io#/#blue#x#", root},
		{"the channel of its room without the closing separator", "socket.io", "socket.io#/#blue", root},
		{"the channel of one of its rooms, for several rooms", "socket.io", "socket.io#/#blue#", toRooms("blue", "green")},
		{"the channel of a room, for no room", "socket.io", "socket.io#/#blue#", toRooms()},
		{"the channel of the room without a name, for no room", "socket.io", "socket.io#/##", toRooms()},
		{"a channel whose namespace does not start with a slash", "socket.io", "socket.io#admin#", slashless},
		{"the channel of an empty namespace", "socket.io", "socket.io##", nameless},
		{"the channel of a room of an empty namespace", "socket.io", "socket.io##blue#", nameless},
	}
	for _, tc := range ignored {
		t.Run("ignores "+tc.name, func(t *testing.T) {
			t.Parallel()
			requireIgnored(t, newWire(tc.key, ownUID), tc.channel, tc.payload)
		})
	}
}

func TestWireEncodeErrors(t *testing.T) {
	t.Parallel()
	deep := func(containers int) string {
		return strings.Repeat("[", containers) + strings.Repeat("]", containers)
	}
	cases := []struct {
		name string
		op   socketio.Operation
		want error
	}{
		{"unknown kind", socketio.Operation{Kind: "bogus", Namespace: "/"}, nil},
		{"empty kind", socketio.Operation{Namespace: "/"}, nil},
		{"emit without a packet", socketio.Operation{Kind: socketio.OpEmit, Namespace: "/"}, nil},
		{"emit with empty data", emitOperation("/", nil, nil, ``, nil), errJSON},
		{"emit with unfinished data", emitOperation("/", nil, nil, `["news"`, nil), errJSON},
		{"emit with data that is not JSON", emitOperation("/", nil, nil, `news`, nil), errJSON},
		{"emit with a stray closing bracket", emitOperation("/", nil, nil, `["news"]]`, nil), errJSON},
		{"emit with a stray comma", emitOperation("/", nil, nil, `,["news"]`, nil), errJSON},
		{"emit with a trailing comma", emitOperation("/", nil, nil, `["news",]`, nil), errJSON},
		{"emit with a single quoted string", emitOperation("/", nil, nil, `['news']`, nil), errJSON},
		{
			"emit with a placeholder past the attachments",
			emitOperation("/", nil, nil, `["news",{"_placeholder":true,"num":1}]`, [][]byte{{1}}),
			errAttachment,
		},
		{
			"emit with a placeholder number that does not fit an int",
			emitOperation("/", nil, nil, `["news",{"_placeholder":true,"num":99999999999999999999}]`, [][]byte{{1}}),
			errAttachment,
		},
		{
			"emit with an attachment that no placeholder names",
			emitOperation("/", nil, nil, `["news",`+placeholderText(1)+`]`, [][]byte{{1}, {2}}),
			errAttachment,
		},
		{
			"emit with attachments and no placeholder",
			emitOperation("/", nil, nil, `["news"]`, [][]byte{{1}}),
			errAttachment,
		},
		{
			"emit with placeholders out of order",
			emitOperation("/", nil, nil, `["news",`+placeholderText(1)+`,`+placeholderText(0)+`]`, [][]byte{{1}, {2}}),
			errAttachment,
		},
		{
			"emit with attachments and no offsets",
			withSlots(emitOperation("/", nil, nil, `["news",`+placeholderText(0)+`]`, [][]byte{{1}})),
			errAttachment,
		},
		{
			"emit with an offset and no attachment",
			withSlots(emitOperation("/", nil, nil, `["news",`+placeholderText(0)+`]`, nil), 8),
			errAttachment,
		},
		{
			"emit with an offset that leads to another object",
			withSlots(emitOperation("/", nil, nil, `["news",{"a":1},`+placeholderText(0)+`]`, [][]byte{{1}}), 8),
			errAttachment,
		},
		{
			"emit with an offset that leads into a string",
			withSlots(emitOperation("/", nil, nil, `["{news",`+placeholderText(0)+`]`, [][]byte{{1}}), 2),
			errAttachment,
		},
		{
			"emit with an offset past the data",
			withSlots(emitOperation("/", nil, nil, `["news",`+placeholderText(0)+`]`, [][]byte{{1}}), 1000),
			errAttachment,
		},
		{
			"emit with the offsets the other way round",
			withSlots(emitOperation("/", nil, nil, `["news",`+placeholderText(0)+`,`+placeholderText(1)+`]`, [][]byte{{1}, {2}}), 9+len(placeholderText(0)), 8),
			errAttachment,
		},
		{
			"emit with data nested too deeply",
			emitOperation("/", nil, nil, deep(600), nil),
			errTooDeep,
		},
		{
			"emit with data one container beyond the limit",
			emitOperation("/", nil, nil, deep(maxDepth-envelopeDepth+1), nil),
			errTooDeep,
		},
		{
			"emit with a very deep object",
			emitOperation("/", nil, nil, nestedText(100_000, true), nil),
			errTooDeep,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := newWire(defaultKey, ownUID).encode(tc.op)
			if err == nil {
				t.Fatal("encode succeeded, want an error")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want one wrapping %v", err, tc.want)
			}
			if msg.channel != "" || msg.payload != nil {
				t.Errorf("message = %+v, want the zero message", msg)
			}
		})
	}
	t.Run("unknown kind is named in the error", func(t *testing.T) {
		t.Parallel()
		_, err := newWire(defaultKey, ownUID).encode(socketio.Operation{Kind: "bogus", Namespace: "/"})
		if err == nil || !strings.Contains(err.Error(), "bogus") {
			t.Errorf("error = %v, want one that names the kind", err)
		}
	})
}

func TestWireEncodesAPlaceholderOnlyWhenItIsOne(t *testing.T) {
	t.Parallel()
	one := [][]byte{{9}}
	flag, zero := mpMember("_placeholder", []byte{0xc3}), mpMember("num", []byte{0})
	encoded := func(t *testing.T, data string, attachments [][]byte) []byte {
		t.Helper()
		msg, err := newWire(defaultKey, ownUID).encode(emitOperation("/", nil, nil, data, attachments))
		if err != nil {
			t.Fatal(err)
		}
		return msg.payload
	}
	t.Run("exactly as the parser writes it", func(t *testing.T) {
		t.Parallel()
		want := mpArray(mpText("e"), mpBin([]byte{9}))
		requireBytes(t, encoded(t, `["e",`+placeholderText(0)+`]`, one), mpBroadcast(ownUID, want, "/", nil, nil))
	})
	t.Run("without attachments", func(t *testing.T) {
		t.Parallel()
		want := mpArray(mpText("e"), mpMap(flag, zero))
		requireBytes(t, encoded(t, `["e",`+placeholderText(0)+`]`, nil), mpBroadcast(ownUID, want, "/", nil, nil))
	})
	cases := []struct {
		name  string
		value string
		want  []byte
	}{
		{"keys the other way round", `{"num":0,"_placeholder":true}`, mpMap(zero, flag)},
		{"with spaces", `{"_placeholder": true, "num": 0}`, mpMap(flag, zero)},
		{"with another member", `{"_placeholder":true,"num":0,"x":1}`, mpMap(flag, zero, mpMember("x", []byte{1}))},
		{"with a negative number", `{"_placeholder":true,"num":-1}`, mpMap(flag, mpMember("num", []byte{0xff}))},
		{"with a fraction", `{"_placeholder":true,"num":1.0}`, mpMap(flag, mpMember("num", []byte{1}))},
		{"with an exponent", `{"_placeholder":true,"num":0e0}`, mpMap(flag, zero)},
		{"with a false flag", `{"_placeholder":false,"num":0}`, mpMap(mpMember("_placeholder", []byte{0xc2}), zero)},
		{"with a quoted number", `{"_placeholder":true,"num":"0"}`, mpMap(flag, mpMember("num", mpText("0")))},
		{"with the number of a later attachment", placeholderText(1), mpMap(flag, mpMember("num", []byte{1}))},
		{"inside a string", `"{\"_placeholder\":true,\"num\":0}"`, mpText(`{"_placeholder":true,"num":0}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name+", ahead of the real one", func(t *testing.T) {
			t.Parallel()
			want := mpArray(mpText("e"), tc.want, mpBin([]byte{9}))
			requireBytes(t, encoded(t, `["e",`+tc.value+`,`+placeholderText(0)+`]`, one), mpBroadcast(ownUID, want, "/", nil, nil))
		})
	}
	twice := `["e",` + placeholderText(0) + `,` + placeholderText(0) + `]`
	t.Run("the same bytes, ahead of the real one", func(t *testing.T) {
		t.Parallel()
		op := withSlots(emitOperation("/", nil, nil, twice, one), strings.LastIndex(twice, placeholderText(0)))
		msg, err := newWire(defaultKey, ownUID).encode(op)
		if err != nil {
			t.Fatal(err)
		}
		want := mpArray(mpText("e"), mpMap(flag, zero), mpBin([]byte{9}))
		requireBytes(t, msg.payload, mpBroadcast(ownUID, want, "/", nil, nil))
	})
	t.Run("the same bytes, after the real one", func(t *testing.T) {
		t.Parallel()
		want := mpArray(mpText("e"), mpBin([]byte{9}), mpMap(flag, zero))
		requireBytes(t, encoded(t, twice, one), mpBroadcast(ownUID, want, "/", nil, nil))
	})
}

func TestWireKeepsAnArgumentThatLooksLikeAPlaceholder(t *testing.T) {
	t.Parallel()
	look := map[string]any{"_placeholder": true, "num": 0}
	packet, err := parser.NewEvent("/", "e", []any{look, []byte{9}, look})
	if err != nil {
		t.Fatal(err)
	}
	sent := socketio.Operation{Kind: socketio.OpEmit, Namespace: "/", Rooms: []string{}, Except: []string{}, Packet: packet}
	msg, err := newWire(defaultKey, ownUID).encode(sent)
	if err != nil {
		t.Fatal(err)
	}
	flag, zero := mpMember("_placeholder", []byte{0xc3}), mpMember("num", []byte{0})
	want := mpArray(mpText("e"), mpMap(flag, zero), mpBin([]byte{9}), mpMap(flag, zero))
	requireBytes(t, msg.payload, mpBroadcast(ownUID, want, "/", nil, nil))
	got, relevant, err := newWire(defaultKey, peerUID).decode(msg.channel, msg.payload)
	if err != nil || !relevant {
		t.Fatalf("decode = %v, %v", relevant, err)
	}
	requireOperation(t, got, sent)
	requireSlots(t, got.Packet)
	again, err := newWire(defaultKey, ownUID).encode(got)
	if err != nil {
		t.Fatal(err)
	}
	requireBytes(t, again.payload, msg.payload)
}

func TestWireRefusesARequestForARoomThatIsNotUTF8(t *testing.T) {
	t.Parallel()
	const broken = "caf\xe9"
	cases := []struct {
		name string
		op   socketio.Operation
	}{
		{"join of the sockets of the room", socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Rooms: []string{"blue", broken}, Targets: []string{"r"}}},
		{"join except the sockets of the room", socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Except: []string{broken}, Targets: []string{"r"}}},
		{"join of the room", socketio.Operation{Kind: socketio.OpJoin, Namespace: "/", Targets: []string{"r", broken}}},
		{"leave of the room", socketio.Operation{Kind: socketio.OpLeave, Namespace: "/", Targets: []string{broken}}},
		{"disconnect of the sockets of the room", socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Rooms: []string{broken}}},
		{"disconnect except the sockets of the room", socketio.Operation{Kind: socketio.OpDisconnect, Namespace: "/", Except: []string{broken}, Close: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg, err := newWire(defaultKey, ownUID).encode(tc.op)
			if !errors.Is(err, errRoomName) {
				t.Fatalf("error = %v, want errRoomName", err)
			}
			if msg.channel != "" || msg.payload != nil {
				t.Errorf("message = %+v, want the zero message", msg)
			}
		})
	}
	t.Run("an emit carries the name as it is", func(t *testing.T) {
		t.Parallel()
		msg, err := newWire(defaultKey, ownUID).encode(emitOperation("/", []string{broken}, []string{broken}, `["e"]`, nil))
		if err != nil {
			t.Fatal(err)
		}
		requireDecoded(t, newWire(defaultKey, peerUID), msg.channel, msg.payload, emitOperation("/", []string{broken}, []string{broken}, `["e"]`, nil))
	})
}

func TestTextSize(t *testing.T) {
	t.Parallel()
	each := len(placeholder) + placeholderTail
	cases := []struct {
		name          string
		values, blobs int
		want          int
		err           error
	}{
		{"nothing", 0, 0, 0, nil},
		{"values alone", 10, 0, 20, nil},
		{"bin values alone", 0, 3, 3 * each, nil},
		{"values and bin values", 10, 3, 20 + 3*each, nil},
		{"the most values that fit", math.MaxInt / 2, 0, math.MaxInt - 1, nil},
		{"values that double past an int", math.MaxInt/2 + 1, 0, 0, errTooLarge},
		{"bin values whose placeholders reach past an int", 0, math.MaxInt/each + 1, 0, errTooLarge},
		{"values and bin values that reach past an int together", math.MaxInt / 2, 1, 0, errTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := textSize(tc.values, tc.blobs)
			if got != tc.want || !errors.Is(err, tc.err) {
				t.Errorf("textSize(%d, %d) = %d, %v, want %d, %v", tc.values, tc.blobs, got, err, tc.want, tc.err)
			}
		})
	}
}

func TestWireWritesAnAttachmentOnce(t *testing.T) {
	t.Parallel()
	const lookAlikes = 30_000
	blob := bytes.Repeat([]byte{0xab}, 2048)
	data := `["e",` + placeholderText(0) + strings.Repeat(","+placeholderText(0), lookAlikes) + `]`
	sent := emitOperation("/", []string{}, []string{}, data, [][]byte{blob})
	msg, err := newWire(defaultKey, ownUID).encode(sent)
	if err != nil {
		t.Fatal(err)
	}
	if written := bytes.Count(msg.payload, blob); written != 1 {
		t.Errorf("the attachment is written %d times, want once", written)
	}
	if limit := len(data) + len(blob) + len(mpBroadcast(ownUID, nil, "/", nil, nil)); len(msg.payload) > limit {
		t.Errorf("payload = %d bytes, want at most %d, the text and the attachment", len(msg.payload), limit)
	}
	requireDecoded(t, newWire(defaultKey, peerUID), msg.channel, msg.payload, sent)
}

type payloadRow struct {
	name    string
	payload []byte
}

type acceptedRow struct {
	name    string
	payload []byte
	want    socketio.Operation
}

type failureRow struct {
	name    string
	payload []byte
	err     error
}

// envelope is a whole broadcast: the sender, the packet, the opts and any
// elements after them.
func envelope(sender, packet, opts []byte, extra ...[]byte) []byte {
	return mpArray(append([][]byte{sender, packet, opts}, extra...)...)
}

// optsWith is the opts of an emit to the room blue except the room red, with
// more members after those two.
func optsWith(members ...[]byte) []byte {
	return mpMap(append([][]byte{
		mpMember("rooms", mpTexts([]string{"blue"})),
		mpMember("except", mpTexts([]string{"red"})),
	}, members...)...)
}

// wideMember is a map entry whose key has the string header of the given
// width in bits.
func wideMember(key string, width int, value []byte) []byte {
	return append(mpWideText(key, width), value...)
}

var (
	mpNil          = []byte{0xc0}
	mpTimestamp    = []byte{0xd6, 0xff, 0, 0, 0, 0}
	mpUnknownExt   = []byte{0xd4, 0x01, 0x00}
	mpNumericMap   = slices.Concat(mpMapHead(1), []byte{1}, []byte{2})
	mpGoodEnvelope = envelope(mpNode, mpPlain, mpOpts)
)

// acceptedEnvelopes are broadcasts that no encoder of this package writes and
// that the decoder must still read, with the operation each one stands for.
func acceptedEnvelopes() []acceptedRow {
	plain := blueRed(`["e"]`, nil)
	var rows []acceptedRow
	add := func(name string, payload []byte, want socketio.Operation) {
		rows = append(rows, acceptedRow{name, payload, want})
	}
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"a positive fixint", []byte{0x02}},
		{"uint8", []byte{0xcc, 0x02}},
		{"uint16", []byte{0xcd, 0x00, 0x02}},
		{"uint32", []byte{0xce, 0x00, 0x00, 0x00, 0x02}},
		{"uint64", []byte{0xcf, 0, 0, 0, 0, 0, 0, 0, 0x02}},
		{"int8", []byte{0xd0, 0x02}},
		{"int16", []byte{0xd1, 0x00, 0x02}},
		{"int32", []byte{0xd2, 0x00, 0x00, 0x00, 0x02}},
		{"int64", []byte{0xd3, 0, 0, 0, 0, 0, 0, 0, 0x02}},
	} {
		add("type written as "+tc.name, envelope(mpNode, mpMap(mpMember("type", tc.value), mpData, mpRoot), mpOpts), plain)
	}
	add("packet members in another order", envelope(mpNode, mpMap(mpRoot, mpData, mpEvent), mpOpts), plain)
	add("data before type and no namespace", envelope(mpNode, mpMap(mpData, mpEvent), mpOpts), plain)
	add("namespace left out", envelope(mpNode, mpMap(mpEvent, mpData), mpOpts), plain)
	add("named namespace", envelope(mpNode, mpMap(mpEvent, mpData, mpMember("nsp", mpText("/admin"))), mpOpts),
		emitOperation("/admin", []string{"blue"}, []string{"red"}, `["e"]`, nil))
	add("namespace twice, the last wins", envelope(mpNode, mpMap(mpEvent, mpData, mpMember("nsp", mpText("/other")), mpRoot), mpOpts), plain)
	add("type twice, both 2", envelope(mpNode, mpMap(mpEvent, mpEvent, mpData, mpRoot), mpOpts), plain)
	binFirst := mpMember("data", mpArray(mpText("e"), mpBin([]byte{1})))
	binSecond := mpMember("data", mpArray(mpText("e"), mpBin([]byte{2, 3})))
	add("data twice, the last wins", envelope(mpNode, mpMap(mpEvent, mpMember("data", mpArray(mpText("first"))), mpData, mpRoot), mpOpts), plain)
	add("data twice, a binary value in the first only", envelope(mpNode, mpMap(mpEvent, binFirst, mpData, mpRoot), mpOpts), plain)
	add("data twice, a binary value in the second only", envelope(mpNode, mpMap(mpEvent, mpData, binSecond, mpRoot), mpOpts),
		blueRed(`["e",`+placeholderText(0)+`]`, [][]byte{{2, 3}}))
	add("data twice, a binary value in both", envelope(mpNode, mpMap(mpEvent, binFirst, binSecond, mpRoot), mpOpts),
		blueRed(`["e",`+placeholderText(0)+`]`, [][]byte{{2, 3}}))
	for _, tc := range []struct {
		name   string
		member []byte
	}{
		{"id", mpMember("id", []byte{7})},
		{"attachments", mpMember("attachments", []byte{0})},
		{"a nested binary value", mpMember("junk", mpArray(mpBin([]byte{1}), mpMap(mpMember("k", mpBin([]byte{2})))))},
		{"an extension", mpMember("x", mpUnknownExt)},
		{"a timestamp", mpMember("t", mpTimestamp)},
		{"a map with numeric keys", mpMember("m", mpNumericMap)},
		{"a string that is not UTF-8", mpMember("s", append(mpSized(2, 0xa0, 32, 0xd9, 0xda), 0xff, 0xfe))},
		{"nil", mpMember("n", mpNil)},
	} {
		add("packet member that is not read: "+tc.name, envelope(mpNode, mpMap(mpEvent, tc.member, mpData, mpRoot), mpOpts), plain)
	}
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"nil", mpNil},
		{"an integer", []byte{0x05}},
		{"a binary value", mpBin([]byte{1, 2})},
		{"text", mpText("x")},
		{"an array of binary values", mpArray(mpBin([]byte{1}), mpBin([]byte{2}))},
		{"a map", mpMap(mpMember("compress", []byte{0xc3}), mpMember("timeout", []byte{0xcd, 0x05, 0xdc}))},
		{"a deeply nested array", nestedBytes(1000, false)},
	} {
		add("flags holding "+tc.name, envelope(mpNode, mpPlain, optsWith(mpMember("flags", tc.value))), plain)
	}
	add("opts without flags", envelope(mpNode, mpPlain, optsWith()), plain)
	add("opts with flags first", envelope(mpNode, mpPlain, mpMap(mpMember("flags", mpMap()), mpMember("except", mpTexts([]string{"red"})), mpMember("rooms", mpTexts([]string{"blue"})))), plain)
	add("opts member that is not read, holding a binary value", envelope(mpNode, mpPlain, optsWith(mpMember("other", mpArray(mpBin([]byte{4}))))), plain)
	add("empty opts", envelope(mpNode, mpPlain, mpMap()), emitOperation("/", nil, nil, `["e"]`, nil))
	add("opts with flags only", envelope(mpNode, mpPlain, mpMap(mpMember("flags", mpMap()))), emitOperation("/", nil, nil, `["e"]`, nil))
	add("rooms twice, the last wins", envelope(mpNode, mpPlain, mpMap(
		mpMember("rooms", mpTexts([]string{"green"})), mpMember("rooms", mpTexts([]string{"blue"})), mpMember("except", mpTexts([]string{"red"})))), plain)
	add("rooms nil after rooms", envelope(mpNode, mpPlain, mpMap(
		mpMember("rooms", mpTexts([]string{"blue"})), mpMember("rooms", mpNil), mpMember("except", mpTexts([]string{"red"})))),
		emitOperation("/", nil, []string{"red"}, `["e"]`, nil))
	add("rooms and except as empty arrays", envelope(mpNode, mpPlain, mpMap(mpMember("rooms", mpArray()), mpMember("except", mpArray()))),
		emitOperation("/", []string{}, []string{}, `["e"]`, nil))
	add("rooms and except nil", envelope(mpNode, mpPlain, mpMap(mpMember("rooms", mpNil), mpMember("except", mpNil))),
		emitOperation("/", nil, nil, `["e"]`, nil))
	add("rooms only", envelope(mpNode, mpPlain, mpMap(mpMember("rooms", mpTexts([]string{"blue"})))),
		emitOperation("/", []string{"blue"}, nil, `["e"]`, nil))
	add("except only", envelope(mpNode, mpPlain, mpMap(mpMember("except", mpTexts([]string{"red"})))),
		emitOperation("/", nil, []string{"red"}, `["e"]`, nil))
	twenty := make([]string, 20)
	for i := range twenty {
		twenty[i] = fmt.Sprintf("room-%d", i)
	}
	add("twenty rooms", envelope(mpNode, mpPlain, mpMap(mpMember("rooms", mpTexts(twenty)))),
		emitOperation("/", twenty, nil, `["e"]`, nil))
	add("rooms with empty names", envelope(mpNode, mpPlain, mpMap(mpMember("rooms", mpTexts([]string{"", ""})))),
		emitOperation("/", []string{"", ""}, nil, `["e"]`, nil))
	add("an extra text element", envelope(mpNode, mpPlain, mpOpts, mpText("x")), plain)
	add("an extra binary element", envelope(mpNode, mpPlain, mpOpts, mpBin([]byte{1})), plain)
	add("an extra map holding a binary value", envelope(mpNode, mpPlain, mpOpts, mpMap(mpMember("k", mpBin([]byte{1})))), plain)
	add("three extra elements", envelope(mpNode, mpPlain, mpOpts, mpNil, []byte{1}, mpArray(mpBin([]byte{1}))), plain)
	for _, tc := range []struct {
		name   string
		sender []byte
	}{
		{"nil", mpNil},
		{"an integer", []byte{0x05}},
		{"a negative integer", []byte{0xff}},
		{"a float", []byte{0xcb, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0}},
		{"a boolean", []byte{0xc3}},
		{"a binary value holding another uid", mpBin([]byte(nodeUID))},
		{"a binary value holding the uid of this server", mpBin([]byte(ownUID))},
		{"an array holding the uid of this server", mpArray(mpText(ownUID))},
		{"a map keyed by the uid of this server", mpMap(mpMember(ownUID, []byte{1}))},
		{"the uid of this server in capitals", mpText("GO-A")},
		{"the uid of this server and a space", mpText(ownUID + " ")},
		{"a prefix of the uid of this server", mpText("go-")},
		{"the uid of this server and a suffix", mpText(ownUID + "b")},
		{"an empty string", mpText("")},
		{"an extension", mpUnknownExt},
		{"a deeply nested array", nestedBytes(100_000, false)},
	} {
		add("sender being "+tc.name, envelope(tc.sender, mpPlain, mpOpts), plain)
	}
	add("array with a 16 bit header", slices.Concat([]byte{0xdc, 0, 3}, mpNode, mpPlain, mpOpts), plain)
	add("array with a 32 bit header", slices.Concat([]byte{0xdd, 0, 0, 0, 3}, mpNode, mpPlain, mpOpts), plain)
	add("array with a 16 bit header and an extra element", slices.Concat([]byte{0xdc, 0, 4}, mpNode, mpPlain, mpOpts, mpText("x")), plain)
	for _, width := range []int{8, 16, 32} {
		packet := mpMap(
			wideMember("type", width, []byte{2}),
			wideMember("data", width, mpArray(mpWideText("e", width))),
			wideMember("nsp", width, mpWideText("/", width)),
		)
		opts := mpMap(
			wideMember("rooms", width, mpArray(mpWideText("blue", width))),
			wideMember("except", width, mpArray(mpWideText("red", width))),
			wideMember("flags", width, mpMap()),
		)
		add(fmt.Sprintf("strings with a %d bit header", width), envelope(mpWideText(nodeUID, width), packet, opts), plain)
	}
	add("binary values in the order they appear",
		envelope(mpNode, mpPacket(mpArray(mpText("e"), mpBin([]byte{1}), mpArray(mpBin([]byte{2})), mpMap(mpMember("k", mpBin([]byte{3}))))), mpOpts),
		blueRed(`["e",`+placeholderText(0)+`,[`+placeholderText(1)+`],{"k":`+placeholderText(2)+`}]`, [][]byte{{1}, {2}, {3}}))
	add("a map that looks like a placeholder next to a binary value",
		envelope(mpNode, mpPacket(mpArray(mpText("e"), mpBin([]byte{9}), mpMap(mpMember("_placeholder", []byte{0xc3}), mpMember("num", []byte{0})))), mpOpts),
		blueRed(`["e",`+placeholderText(0)+`,`+placeholderText(0)+`]`, [][]byte{{9}}))
	add("an event name made of 31 and 32 bytes",
		envelope(mpNode, mpPacket(mpArray(mpText(strings.Repeat("n", 32)), mpText(strings.Repeat("m", 31)))), mpOpts),
		blueRed(`["`+strings.Repeat("n", 32)+`","`+strings.Repeat("m", 31)+`"]`, nil))
	return rows
}

// ignoredEnvelopes are broadcasts whose first element is the uid of this
// server. The decoder must report them as not relevant without reading, or
// checking, anything after that element.
func ignoredEnvelopes() []payloadRow {
	var rows []payloadRow
	senders := []struct {
		name  string
		bytes []byte
	}{
		{"fixed string", mpText(ownUID)},
		{"string with an 8 bit header", mpWideText(ownUID, 8)},
		{"string with a 16 bit header", mpWideText(ownUID, 16)},
		{"string with a 32 bit header", mpWideText(ownUID, 32)},
	}
	heads := []struct {
		name  string
		bytes []byte
	}{
		{"array of three", mpArrayHead(3)},
		{"array of three with a 16 bit header", []byte{0xdc, 0, 3}},
		{"array of three with a 32 bit header", []byte{0xdd, 0, 0, 0, 3}},
	}
	tails := []struct {
		name  string
		bytes []byte
	}{
		{"nothing", nil},
		{"an unused type byte", []byte{0xc1}},
		{"two bytes of 0xff", []byte{0xff, 0xff}},
		{"a packet cut short", mpPlain[:5]},
		{"a packet and opts", slices.Concat(mpPlain, mpOpts)},
		{"a packet of another type", slices.Concat(mpMap(mpMember("type", []byte{3}), mpData, mpRoot), mpOpts)},
		{"a packet that is not a map", slices.Concat(mpArray(), mpArray())},
		{"bytes after the message", slices.Concat(mpPlain, mpOpts, []byte{0xc1, 0xc1})},
	}
	for _, head := range heads {
		for _, sender := range senders {
			for _, tail := range tails {
				rows = append(rows, payloadRow{
					head.name + " and a " + sender.name + " then " + tail.name,
					slices.Concat(head.bytes, sender.bytes, tail.bytes),
				})
			}
		}
	}
	rows = append(rows, payloadRow{"a whole broadcast of this server", mpBroadcast(ownUID, mpArray(mpText("e")), "/", []string{"blue"}, []string{"red"})})
	return rows
}

// rejectedEnvelopes are broadcasts that must fail, with the error that each
// one must wrap.
func rejectedEnvelopes() []failureRow {
	var rows []failureRow
	add := func(name string, payload []byte, err error) {
		rows = append(rows, failureRow{name, payload, err})
	}
	typed := func(value []byte) []byte {
		return envelope(mpNode, mpMap(mpMember("type", value), mpData, mpRoot), mpOpts)
	}
	named := func(packet []byte) []byte { return envelope(mpNode, packet, mpOpts) }
	selecting := func(members ...[]byte) []byte { return envelope(mpNode, mpPlain, mpMap(members...)) }
	add("a sender cut short", []byte{0x93, 0xa4, 'g', 'o'}, errTruncated)
	add("a sender with an 8 bit length beyond the input", []byte{0x93, 0xd9, 0xff, 'a'}, errTruncated)
	add("nothing after the sender", slices.Concat(mpArrayHead(3), mpNode), errTruncated)
	add("a packet cut short", slices.Concat(mpArrayHead(3), mpNode, mpPlain[:len(mpPlain)-1]), errTruncated)
	add("opts cut short", mpGoodEnvelope[:len(mpGoodEnvelope)-1], errTruncated)
	add("an extra element missing", slices.Concat(mpArrayHead(4), mpNode, mpPlain, mpOpts), errTruncated)
	add("an array head beyond the input", []byte{0xdd, 0xff, 0xff, 0xff, 0xff}, errTruncated)
	add("a map", mpMap(), errMalformed)
	add("text", mpText("hello"), errMalformed)
	add("nil", mpNil, errMalformed)
	add("an empty array", mpArray(), errMalformed)
	add("an array of two", mpArray(mpNode, mpPlain), errMalformed)
	add("an array of two that starts with the uid of this server", mpArray(mpText(ownUID), mpPlain), errMalformed)
	add("a packet that is an array", named(mpArray()), errMalformed)
	add("a packet that is nil", named(mpNil), errMalformed)
	add("opts that is an array", envelope(mpNode, mpPlain, mpArray()), errMalformed)
	add("opts that is nil", envelope(mpNode, mpPlain, mpNil), errMalformed)
	add("opts that is text", envelope(mpNode, mpPlain, mpText("opts")), errMalformed)
	add("type 2 and then 3", named(mpMap(mpEvent, mpMember("type", []byte{3}), mpData, mpRoot)), errMalformed)
	add("type 3 and then 2", named(mpMap(mpMember("type", []byte{3}), mpEvent, mpData, mpRoot)), errMalformed)
	add("type left out", named(mpMap(mpData, mpRoot)), errMalformed)
	add("type 5, a binary event", typed([]byte{0x05}), errMalformed)
	add("type 0", typed([]byte{0x00}), errMalformed)
	add("type a float64 2", typed([]byte{0xcb, 0x40, 0, 0, 0, 0, 0, 0, 0}), errMalformed)
	add("type a float32 2", typed([]byte{0xca, 0x40, 0, 0, 0}), errMalformed)
	add("type int8 -2", typed([]byte{0xd0, 0xfe}), errMalformed)
	add("type uint64 beyond 2^63 that ends in 2", typed([]byte{0xcf, 0x80, 0, 0, 0, 0, 0, 0, 0x02}), errMalformed)
	add("type an array holding 2", typed(mpArray([]byte{2})), errMalformed)
	add("type text", typed(mpText("2")), errMalformed)
	add("type nil", typed(mpNil), errMalformed)
	add("type true", typed([]byte{0xc3}), errMalformed)
	add("data left out", named(mpMap(mpEvent, mpRoot)), errMalformed)
	add("data nil", named(mpMap(mpEvent, mpMember("data", mpNil), mpRoot)), errMalformed)
	add("data text", named(mpPacket(mpText("e"))), errMalformed)
	add("data a map", named(mpPacket(mpMap(mpMember("e", []byte{1})))), errMalformed)
	add("data an empty array", named(mpPacket(mpArray())), errMalformed)
	add("data starting with an integer", named(mpPacket(mpArray([]byte{1}))), errMalformed)
	add("data starting with nil", named(mpPacket(mpArray(mpNil))), errMalformed)
	add("data starting with a binary value", named(mpPacket(mpArray(mpBin([]byte("e"))))), errMalformed)
	add("data starting with an array", named(mpPacket(mpArray(mpArray(mpText("e"))))), errMalformed)
	for _, name := range []string{"connect", "connect_error", "disconnect", "disconnecting", "newListener", "removeListener"} {
		add("data starting with the reserved name "+name, named(mpPacket(mpArray(mpText(name)))), errMalformed)
	}
	add("data twice, the first empty", named(mpMap(mpEvent, mpMember("data", mpArray()), mpData, mpRoot)), errMalformed)
	add("data twice, the first reserved", named(mpMap(mpEvent, mpMember("data", mpArray(mpText("connect"))), mpData, mpRoot)), errMalformed)
	add("data twice, the last empty", named(mpMap(mpEvent, mpData, mpMember("data", mpArray()), mpRoot)), errMalformed)
	add("data twice, the last reserved", named(mpMap(mpEvent, mpData, mpMember("data", mpArray(mpText("disconnect"))), mpRoot)), errMalformed)
	add("data twice, an extension that is not a timestamp in the first",
		named(mpMap(mpEvent, mpMember("data", mpArray(mpText("e"), mpUnknownExt)), mpData, mpRoot)), errUnsupported)
	add("namespace nil", named(mpMap(mpEvent, mpData, mpMember("nsp", mpNil))), errMalformed)
	add("namespace an integer", named(mpMap(mpEvent, mpData, mpMember("nsp", []byte{1}))), errMalformed)
	add("namespace a binary value", named(mpMap(mpEvent, mpData, mpMember("nsp", mpBin([]byte("/"))))), errMalformed)
	add("namespace an array", named(mpMap(mpEvent, mpData, mpMember("nsp", mpArray(mpText("/"))))), errMalformed)
	add("namespace twice, the last nil", named(mpMap(mpEvent, mpData, mpRoot, mpMember("nsp", mpNil))), errMalformed)
	add("a packet key that is a binary value", named(slices.Concat(mpMapHead(1), mpBin([]byte("type")), []byte{2})), errMalformed)
	add("a packet key that is a number", named(slices.Concat(mpMapHead(1), []byte{1}, []byte{2})), errMalformed)
	add("a packet key that is nil", named(slices.Concat(mpMapHead(1), mpNil, []byte{2})), errMalformed)
	add("an opts key that is a binary value", envelope(mpNode, mpPlain, slices.Concat(mpMapHead(1), mpBin([]byte("rooms")), mpArray())), errMalformed)
	add("an opts key that is a number", envelope(mpNode, mpPlain, slices.Concat(mpMapHead(1), []byte{1}, mpArray())), errMalformed)
	add("rooms text", selecting(mpMember("rooms", mpText("blue"))), errMalformed)
	add("rooms an integer", selecting(mpMember("rooms", []byte{1})), errMalformed)
	add("rooms a map", selecting(mpMember("rooms", mpMap(mpMember("blue", []byte{1})))), errMalformed)
	add("rooms holding an integer", selecting(mpMember("rooms", mpArray(mpText("blue"), []byte{1}))), errMalformed)
	add("rooms holding nil", selecting(mpMember("rooms", mpArray(mpNil))), errMalformed)
	add("rooms holding a binary value", selecting(mpMember("rooms", mpArray(mpBin([]byte("blue"))))), errMalformed)
	add("rooms holding an array", selecting(mpMember("rooms", mpArray(mpArray(mpText("blue"))))), errMalformed)
	add("rooms twice, the last holding an integer", selecting(mpMember("rooms", mpTexts([]string{"blue"})), mpMember("rooms", mpArray([]byte{1}))), errMalformed)
	add("except text", selecting(mpMember("except", mpText("red"))), errMalformed)
	add("except holding an integer", selecting(mpMember("except", mpArray([]byte{1}))), errMalformed)
	add("except holding a binary value", selecting(mpMember("except", mpArray(mpBin([]byte("red"))))), errMalformed)
	add("rooms a deeply nested array", selecting(mpMember("rooms", nestedBytes(100_000, false))), errMalformed)
	add("data holding a map keyed by numbers", named(mpPacket(mpArray(mpText("e"), mpNumericMap))), errUnsupported)
	add("data holding an extension that is not a timestamp", named(mpPacket(mpArray(mpText("e"), mpUnknownExt))), errUnsupported)
	add("data holding a timestamp of the wrong size", named(mpPacket(mpArray(mpText("e"), []byte{0xd5, 0xff, 0, 0}))), errUnsupported)
	add("data holding the unused type byte", named(mpPacket(mpArray(mpText("e"), []byte{0xc1}))), errUnsupported)
	add("an unused type byte as the sender", envelope([]byte{0xc1}, mpPlain, mpOpts), errUnsupported)
	add("an unused type byte in a packet member", named(mpMap(mpEvent, mpMember("junk", []byte{0xc1}), mpData, mpRoot)), errUnsupported)
	add("an unused type byte in flags", envelope(mpNode, mpPlain, optsWith(mpMember("flags", []byte{0xc1}))), errUnsupported)
	add("an unused type byte as an extra element", envelope(mpNode, mpPlain, mpOpts, []byte{0xc1}), errUnsupported)
	add("a byte after the message", append(slices.Clone(mpGoodEnvelope), 0x00), errTrailing)
	add("a second broadcast after the first", slices.Concat(mpGoodEnvelope, mpGoodEnvelope), errTrailing)
	add("a byte after an extra element", append(envelope(mpNode, mpPlain, mpOpts, mpText("x")), 0x00), errTrailing)
	add("data nested one container too deep", named(mpPacket(mpNested(maxDepth-envelopeDepth+1))), errTooDeep)
	add("data nested 100,000 containers deep", named(mpPacket(mpNested(100_000))), errTooDeep)
	return rows
}

// channelOf is the channel that a server of the default key publishes the
// emit op on.
func channelOf(op socketio.Operation) string {
	channel := "socket.io#" + op.Namespace + "#"
	if len(op.Rooms) == 1 {
		channel += op.Rooms[0] + "#"
	}
	return channel
}

func TestWireDecodesHandBuiltEnvelopes(t *testing.T) {
	t.Parallel()
	for _, tc := range acceptedEnvelopes() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireDecoded(t, newWire(defaultKey, ownUID), channelOf(tc.want), tc.payload, tc.want)
		})
	}
}

func TestWireIgnoresTheEmitOfItsOwnUIDWithoutReadingFurther(t *testing.T) {
	t.Parallel()
	for _, tc := range ignoredEnvelopes() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireIgnored(t, newWire(defaultKey, ownUID), "socket.io#/#", tc.payload)
		})
	}
}

func TestWireRejectsHandBuiltEnvelopes(t *testing.T) {
	t.Parallel()
	for _, tc := range rejectedEnvelopes() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireFailure(t, newWire(defaultKey, ownUID), "socket.io#/#", tc.payload, tc.err)
		})
	}
}

func TestWireDecodesOnlyDataIntoAttachments(t *testing.T) {
	t.Parallel()
	blob := mpBin([]byte{1, 2, 3})
	cases := []struct {
		name    string
		payload []byte
		want    error
	}{
		{"a binary value as the namespace", envelope(mpNode, mpMap(mpEvent, mpData, mpMember("nsp", blob)), mpOpts), errMalformed},
		{"a binary value as the first room", envelope(mpNode, mpPlain, mpMap(mpMember("rooms", mpArray(blob)))), errMalformed},
		{"a binary value as the first element of data", envelope(mpNode, mpPacket(mpArray(blob)), mpOpts), errMalformed},
		{"a binary value as the type", envelope(mpNode, mpMap(mpMember("type", blob), mpData, mpRoot), mpOpts), errMalformed},
		{"a binary value as a map key of the packet", envelope(mpNode, slices.Concat(mpMapHead(1), blob, []byte{2}), mpOpts), errMalformed},
		{"a binary value as the packet", envelope(mpNode, blob, mpOpts), errMalformed},
		{"a binary value as the opts", envelope(mpNode, mpPlain, blob), errMalformed},
		{"a binary value as the whole message", blob, errMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireFailure(t, newWire(defaultKey, ownUID), "socket.io#/#", tc.payload, tc.want)
		})
	}
}

func TestWireAttachmentsAliasThePayload(t *testing.T) {
	t.Parallel()
	payload := unhex(t, nodeBinaryEmitHex)
	op, relevant, err := newWire(defaultKey, ownUID).decode("socket.io#/#", payload)
	if err != nil || !relevant {
		t.Fatalf("decode = %v, %v", relevant, err)
	}
	at := bytes.Index(payload, []byte{0xc4, 0x03, 1, 2, 3})
	if at < 0 {
		t.Fatal("the binary value is not in the payload")
	}
	payload[at+2] = 9
	if got := op.Packet.Attachments[0]; !bytes.Equal(got, []byte{9, 2, 3}) {
		t.Errorf("attachment = %v after the payload changed, want [9 2 3]: it should alias the payload", got)
	}
}

func TestWireAttachmentsDoNotGrowIntoThePayload(t *testing.T) {
	t.Parallel()
	payload := unhex(t, nodeTwoBuffersEmitHex)
	op, relevant, err := newWire(defaultKey, ownUID).decode("socket.io#/#", payload)
	if err != nil || !relevant {
		t.Fatalf("decode = %v, %v", relevant, err)
	}
	before := slices.Clone(payload)
	for _, attachment := range op.Packet.Attachments {
		grown := append(attachment, 0xee, 0xee, 0xee)
		_ = grown
	}
	if !bytes.Equal(payload, before) {
		t.Errorf("appending to an attachment changed the payload:\n got: %x\nwant: %x", payload, before)
	}
}

func TestWireDepthLimit(t *testing.T) {
	t.Parallel()
	const limit = maxDepth - envelopeDepth
	sender, receiver := newWire(defaultKey, ownUID), newWire(defaultKey, peerUID)
	for _, shape := range []struct {
		name    string
		objects bool
	}{{"arrays", false}, {"objects", true}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			t.Run("the limit is written and read", func(t *testing.T) {
				t.Parallel()
				packed, text := nestedData(limit, shape.objects)
				msg, err := sender.encode(emitOperation("/", nil, nil, text, nil))
				if err != nil {
					t.Fatal(err)
				}
				requireBytes(t, msg.payload, mpBroadcast(ownUID, packed, "/", nil, nil))
				requireDecoded(t, receiver, msg.channel, mpBroadcast(nodeUID, packed, "/", nil, nil),
					emitOperation("/", []string{}, []string{}, text, nil))
			})
			for _, containers := range []int{limit + 1, limit + 2, 1000, 100_000} {
				t.Run(fmt.Sprintf("%d containers are refused", containers), func(t *testing.T) {
					t.Parallel()
					packed, text := nestedData(containers, shape.objects)
					msg, err := sender.encode(emitOperation("/", nil, nil, text, nil))
					if !errors.Is(err, errTooDeep) || msg.channel != "" || msg.payload != nil {
						t.Errorf("encode = %+v, %v; want the zero message and an error wrapping errTooDeep", msg, err)
					}
					requireFailure(t, receiver, "socket.io#/#", mpBroadcast(nodeUID, packed, "/", nil, nil), errTooDeep)
				})
			}
			t.Run("the limit is read with a binary value at the bottom", func(t *testing.T) {
				t.Parallel()
				packed := mpArray(mpText("e"), slices.Concat(bytes.Repeat([]byte{0x91}, limit-2), mpArray(mpBin([]byte{1}))))
				text := `["e",` + strings.Repeat("[", limit-2) + "[" + placeholderText(0) + "]" + strings.Repeat("]", limit-2) + `]`
				requireDecoded(t, receiver, "socket.io#/#", mpBroadcast(nodeUID, packed, "/", nil, nil),
					emitOperation("/", []string{}, []string{}, text, [][]byte{{1}}))
			})
		})
	}
}

func TestWireSkipsDeepValues(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name    string
		objects bool
	}{{"arrays", false}, {"objects", true}} {
		deep := nestedBytes(100_000, shape.objects)
		cases := []struct {
			name    string
			payload []byte
		}{
			{"the sender", envelope(deep, mpPlain, mpOpts)},
			{"a packet member that is not read", envelope(mpNode, mpMap(mpEvent, mpMember("junk", deep), mpData, mpRoot), mpOpts)},
			{"flags", envelope(mpNode, mpPlain, optsWith(mpMember("flags", deep)))},
			{"an opts member that is not read", envelope(mpNode, mpPlain, optsWith(mpMember("other", deep)))},
			{"an extra element", envelope(mpNode, mpPlain, mpOpts, deep)},
			{"every skipped position at once", envelope(deep, mpMap(mpEvent, mpMember("junk", deep), mpData, mpRoot), optsWith(mpMember("flags", deep), mpMember("other", deep)), deep, deep)},
		}
		for _, tc := range cases {
			t.Run(shape.name+" in "+tc.name, func(t *testing.T) {
				t.Parallel()
				requireDecoded(t, newWire(defaultKey, ownUID), "socket.io#/#", tc.payload, blueRed(`["e"]`, nil))
			})
		}
	}
}

// The tests below count allocations, which testing.AllocsPerRun cannot do in a
// parallel test, so none of them is parallel.

// The ceilings leave room for the compiler: Go 1.24 allocates the short list
// of open containers that measure keeps, once per level of nesting, where
// later versions keep it on the stack.
const (
	decodeAllocationCeiling = 16
	encodeAllocationCeiling = 12
)

// decodedBytes is the room that the decoder of a broadcast reserves for the
// data of an event before it writes it: two bytes of text for every byte of
// the array that holds it, its header included, except for the bytes that bin
// values hold, and room for a placeholder in place of each of those values.
// The array holds the name e, then count values written as elements.
func decodedBytes(count int, elements []byte, blobs, held int) int {
	return decodedText(2, count, elements, blobs, held)
}

// decodedLimit is the most text that data can turn into: six bytes for every
// byte, which is what false and an escaped control character take, where
// decodedBytes counts two.
func decodedLimit(count int, elements []byte, blobs, held int) int {
	return decodedText(6, count, elements, blobs, held)
}

func decodedText(perByte, count int, elements []byte, blobs, held int) int {
	return perByte*(len(mpArrayHead(count+1))+len(mpText("e"))+len(elements)-held) + blobs*(len(placeholder)+placeholderTail)
}

// decodePayload is the broadcast of an emit whose data is an event name
// followed by count values, written as elements.
func decodePayload(count int, elements []byte) []byte {
	data := slices.Concat(mpArrayHead(count+1), mpText("e"), elements)
	return envelope(mpNode, mpPacket(data), mpOpts)
}

func decodeOnce(t testing.TB, payload []byte) socketio.Operation {
	t.Helper()
	op, relevant, err := newWire(defaultKey, ownUID).decode("socket.io#/#", payload)
	if err != nil || !relevant {
		t.Fatalf("decode of %d bytes = %v, %v; want an operation", len(payload), relevant, err)
	}
	return op
}

// decodeAllocations is the number of allocations of a decode, and zero under
// the race detector, which changes the count.
func decodeAllocations(t testing.TB, payload []byte) float64 {
	t.Helper()
	if raceEnabled {
		return 0
	}
	w := newWire(defaultKey, ownUID)
	return leastAllocations(func() {
		if _, relevant, err := w.decode("socket.io#/#", payload); err != nil || !relevant {
			t.Fatalf("decode of %d bytes = %v, %v; want an operation", len(payload), relevant, err)
		}
	})
}

// requireConstantAllocations checks that the allocations measured at two sizes
// are the same and stay under the ceiling.
func requireConstantAllocations(t testing.TB, small, large float64, ceiling float64) {
	t.Helper()
	if raceEnabled {
		return
	}
	if small != large {
		t.Errorf("allocations grow with the size: %v at the small size, %v at the large one", small, large)
	}
	if large > ceiling {
		t.Errorf("allocations = %v, want at most %v", large, ceiling)
	}
}

// regrowths is the number of reallocations allowed to a buffer that starts
// from an estimate and outgrows it. The text of a value is at most three times
// the room reserved for it, and a reallocation adds a quarter: five of them
// reach that, and the size classes of the allocator move the count by a few.
const regrowths = 8

// requireSlowGrowth checks the allocations of a decode or an encode whose
// buffer outgrows its estimate: at both sizes they stay within regrowths of
// the ceiling, and the large size costs at most regrowths more than the small
// one, where an allocation per value would cost thousands.
func requireSlowGrowth(t testing.TB, small, large float64, ceiling float64) {
	t.Helper()
	if raceEnabled {
		return
	}
	if large > small+regrowths {
		t.Errorf("allocations grow with the size: %v at the small size, %v at the large one", small, large)
	}
	if max(small, large) > ceiling+regrowths {
		t.Errorf("allocations = %v and %v, want at most %v", small, large, ceiling+regrowths)
	}
}

// memoryRow is a kind of data to decode. regrows tells that its text is more
// than twice its size, so that it outgrows the room reserved for it.
type memoryRow struct {
	name    string
	build   func(n int) (elements []byte, count, blobs, held int)
	regrows bool
}

// outgrowing marks a row whose text is more than twice its size.
func outgrowing(row memoryRow) memoryRow {
	row.regrows = true
	return row
}

// repeatedValues is a row of n repetitions of a pattern that holds values
// values, and in them blobs bin values of held bytes.
func repeatedValues(name string, pattern []byte, values, blobs, held int) memoryRow {
	return memoryRow{name: name, build: func(n int) ([]byte, int, int, int) {
		return bytes.Repeat(pattern, n), n * values, n * blobs, n * held
	}}
}

// oneString is a row of one string of n bytes, all the same.
func oneString(name string, unit byte) memoryRow {
	return memoryRow{name: name, build: func(n int) ([]byte, int, int, int) {
		return slices.Concat(mpSized(n, 0xa0, 32, 0xd9, 0xda), bytes.Repeat([]byte{unit}, n)), 1, 0, 0
	}}
}

// tenth and fifth are variables, not constants, so that their sum is the float
// 0.30000000000000004 and not the constant 0.3.
var tenth, fifth = 0.1, 0.2

func memoryRows() []memoryRow {
	rows := []memoryRow{
		repeatedValues("digits", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 10, 0, 0),
		outgrowing(repeatedValues("positive fixint of three digits", []byte{0x7f}, 1, 0, 0)),
		outgrowing(repeatedValues("negative fixint", []byte{0xe0}, 1, 0, 0)),
		repeatedValues("uint8", []byte{0xcc, 0xff}, 1, 0, 0),
		repeatedValues("uint16", []byte{0xcd, 0xff, 0xff}, 1, 0, 0),
		outgrowing(repeatedValues("uint32", []byte{0xce, 0xff, 0xff, 0xff, 0xff}, 1, 0, 0)),
		outgrowing(repeatedValues("uint64", slices.Concat([]byte{0xcf}, bytes.Repeat([]byte{0xff}, 8)), 1, 0, 0)),
		outgrowing(repeatedValues("int8", []byte{0xd0, 0x80}, 1, 0, 0)),
		outgrowing(repeatedValues("int16", []byte{0xd1, 0x80, 0x00}, 1, 0, 0)),
		outgrowing(repeatedValues("int32", []byte{0xd2, 0x80, 0, 0, 0}, 1, 0, 0)),
		outgrowing(repeatedValues("int64", []byte{0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0}, 1, 0, 0)),
		outgrowing(repeatedValues("nil", []byte{0xc0}, 1, 0, 0)),
		outgrowing(repeatedValues("true", []byte{0xc3}, 1, 0, 0)),
		outgrowing(repeatedValues("false", []byte{0xc2}, 1, 0, 0)),
		outgrowing(repeatedValues("empty string", []byte{0xa0}, 1, 0, 0)),
		repeatedValues("string of one byte", []byte{0xa1, 'a'}, 1, 0, 0),
		repeatedValues("float 1.5", []byte{0xcb, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0}, 1, 0, 0),
		outgrowing(repeatedValues("float of 17 digits", binary.BigEndian.AppendUint64([]byte{0xcb}, math.Float64bits(tenth+fifth)), 1, 0, 0)),
		outgrowing(repeatedValues("timestamp", []byte{0xd6, 0xff, 0, 0, 0, 0}, 1, 0, 0)),
		outgrowing(repeatedValues("empty array", []byte{0x90}, 1, 0, 0)),
		outgrowing(repeatedValues("empty map", []byte{0x80}, 1, 0, 0)),
		outgrowing(oneString("one string of control characters", 0x01)),
		outgrowing(oneString("one string of bytes that are not UTF-8", 0xff)),
		oneString("one string of line feeds", '\n'),
		oneString("one string of letters", 'a'),
		repeatedValues("empty binary values", []byte{0xc4, 0x00}, 1, 1, 0),
		repeatedValues("binary values of one byte", []byte{0xc4, 0x01, 0xab}, 1, 1, 1),
		repeatedValues("binary values of 40 bytes", slices.Concat([]byte{0xc4, 40}, bytes.Repeat([]byte{0xab}, 40)), 1, 1, 40),
		repeatedValues("a binary value in an array in an array", []byte{0x91, 0x91, 0xc4, 0x01, 0xab}, 1, 1, 1),
	}
	return rows
}

func TestWireDecodeMemory(t *testing.T) {
	sizes := [2]int{1 << 11, 1 << 16}
	for _, tc := range memoryRows() {
		t.Run(tc.name, func(t *testing.T) {
			var allocations [2]float64
			for i, n := range sizes {
				elements, count, blobs, held := tc.build(n)
				payload := decodePayload(count, elements)
				op := decodeOnce(t, payload)
				reserved, limit := decodedBytes(count, elements, blobs, held), decodedLimit(count, elements, blobs, held)
				data, attachments := op.Packet.Data, op.Packet.Attachments
				if len(data) > limit {
					t.Errorf("n=%d: the data holds %d bytes, more than the %d that a message of that size can turn into", n, len(data), limit)
				}
				if regrew := len(data) > reserved; regrew != tc.regrows {
					t.Errorf("n=%d: the data holds %d bytes and %d were reserved: outgrown = %v, want %v", n, len(data), reserved, regrew, tc.regrows)
				}
				room := reserved + reserved/8
				if tc.regrows {
					room = len(data) + len(data)/2
				}
				if cap(data) > room+8192 {
					t.Errorf("n=%d: the data has a capacity of %d bytes, far more than the %d it needs", n, cap(data), room)
				}
				if len(attachments) != blobs {
					t.Errorf("n=%d: %d attachments, want %d", n, len(attachments), blobs)
				}
				if cap(attachments) < blobs || cap(attachments) > blobs+blobs/8+8 {
					t.Errorf("n=%d: the attachments have a capacity of %d, want about %d", n, cap(attachments), blobs)
				}
				if blobs >= 2048 && blobs&(blobs-1) == 0 && cap(attachments) != blobs {
					t.Errorf("n=%d: the attachments have a capacity of %d, want exactly %d", n, cap(attachments), blobs)
				}
				allocations[i] = decodeAllocations(t, payload)
			}
			if tc.regrows {
				requireSlowGrowth(t, allocations[0], allocations[1], decodeAllocationCeiling)
				return
			}
			requireConstantAllocations(t, allocations[0], allocations[1], decodeAllocationCeiling)
		})
	}
}

func TestWireDecodeMemoryOfRooms(t *testing.T) {
	for _, member := range []string{"rooms", "except"} {
		t.Run(member, func(t *testing.T) {
			var allocations [2]float64
			for i, n := range [2]int{1 << 10, 1 << 16} {
				list := slices.Concat(mpArrayHead(n), bytes.Repeat([]byte{0xa0}, n))
				payload := envelope(mpNode, mpPlain, mpMap(mpMember(member, list)))
				op := decodeOnce(t, payload)
				got := op.Rooms
				if member == "except" {
					got = op.Except
				}
				if len(got) != n || cap(got) != n {
					t.Errorf("n=%d: the list has a length of %d and a capacity of %d, want both %d", n, len(got), cap(got), n)
				}
				allocations[i] = decodeAllocations(t, payload)
			}
			requireConstantAllocations(t, allocations[0], allocations[1], decodeAllocationCeiling)
		})
	}
}

func TestWireDecodeMemoryOfRepeatedData(t *testing.T) {
	cases := []struct {
		name        string
		data        []byte
		wantText    string
		attachments [][]byte
	}{
		{"without binary values", mpArray(mpText("e"), []byte{7}), `["e",7]`, nil},
		{"with a binary value", mpArray(mpText("e"), mpBin([]byte{1, 2, 3})), `["e",` + placeholderText(0) + `]`, [][]byte{{1, 2, 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var allocations [2]float64
			for i, repeats := range [2]int{100, 10_000} {
				members := slices.Repeat([][]byte{mpMember("data", tc.data)}, repeats)
				payload := envelope(mpNode, mpMap(slices.Concat([][]byte{mpEvent}, members, [][]byte{mpRoot})...), mpOpts)
				op := decodeOnce(t, payload)
				requireText(t, op.Packet.Data, tc.wantText)
				requireAttachments(t, op.Packet.Attachments, tc.attachments)
				allocations[i] = decodeAllocations(t, payload)
			}
			requireConstantAllocations(t, allocations[0], allocations[1], decodeAllocationCeiling)
		})
	}
}

func TestWireDecodeMemoryOfSkippedValues(t *testing.T) {
	junk := []struct {
		name  string
		build func(n int) []byte
	}{
		{"an array of nil", func(n int) []byte { return slices.Concat(mpArrayHead(n), bytes.Repeat(mpNil, n)) }},
		{"a map", func(n int) []byte { return slices.Concat(mpMapHead(n), bytes.Repeat([]byte{0xa1, 'k', 0xc0}, n)) }},
		{"a binary value", func(n int) []byte { return mpBin(bytes.Repeat([]byte{1}, n)) }},
		{"a string", func(n int) []byte { return mpText(strings.Repeat("x", n)) }},
		{"an array of empty binary values", func(n int) []byte { return slices.Concat(mpArrayHead(n), bytes.Repeat([]byte{0xc4, 0x00}, n)) }},
		{"nested arrays", func(n int) []byte { return nestedBytes(n, false) }},
		{"nested maps", func(n int) []byte { return nestedBytes(n, true) }},
	}
	positions := []struct {
		name  string
		build func(value []byte) []byte
	}{
		{"an extra element", func(value []byte) []byte { return envelope(mpNode, mpPlain, mpOpts, value) }},
		{"the sender", func(value []byte) []byte { return envelope(value, mpPlain, mpOpts) }},
		{"flags", func(value []byte) []byte { return envelope(mpNode, mpPlain, optsWith(mpMember("flags", value))) }},
		{"an opts member that is not read", func(value []byte) []byte { return envelope(mpNode, mpPlain, optsWith(mpMember("other", value))) }},
		{"a packet member that is not read", func(value []byte) []byte {
			return envelope(mpNode, mpMap(mpEvent, mpMember("junk", value), mpData, mpRoot), mpOpts)
		}},
	}
	for _, position := range positions {
		for _, value := range junk {
			t.Run(value.name+" as "+position.name, func(t *testing.T) {
				var allocations [2]float64
				for i, n := range [2]int{1 << 10, 1 << 16} {
					payload := position.build(value.build(n))
					requireOperation(t, decodeOnce(t, payload), blueRed(`["e"]`, nil))
					allocations[i] = decodeAllocations(t, payload)
				}
				requireConstantAllocations(t, allocations[0], allocations[1], decodeAllocationCeiling)
			})
		}
	}
}

func eventOf(element string, n int) string {
	return `["e",` + strings.TrimSuffix(strings.Repeat(element+",", n), ",") + "]"
}

func TestWireEncodeMemory(t *testing.T) {
	skipUnderRace(t)
	cases := []struct {
		name        string
		element     string
		attachments [][]byte
	}{
		{"small integers", "1", nil},
		{"integers of every width", "70000", nil},
		{"negative integers", "-300", nil},
		{"strings", `"abc"`, nil},
		{"strings with escapes", `"a\nb\u00e9"`, nil},
		{"objects", `{"a":1,"b":[true,null]}`, nil},
		{"placeholders", placeholderText(0), [][]byte{{1, 2, 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWire(defaultKey, ownUID)
			var allocations [2]float64
			for i, n := range [2]int{1 << 10, 1 << 16} {
				op := emitOperation("/", []string{"blue"}, []string{"red"}, eventOf(tc.element, n), tc.attachments)
				allocations[i] = leastAllocations(func() {
					if _, err := w.encode(op); err != nil {
						t.Fatal(err)
					}
				})
			}
			requireConstantAllocations(t, allocations[0], allocations[1], encodeAllocationCeiling)
		})
	}
}

var knownDecodeErrors = []error{errMalformed, errTruncated, errTrailing, errTooDeep, errUnsupported}

func FuzzBroadcastRoundTrip(f *testing.F) {
	seeds := []string{
		`["e"]`,
		`["e",1]`,
		`["e",{"a":[1,2,{"b":null}]}]`,
		`["e","é\n\u2028<>&\u0001"]`,
		`["e",1e999,-1e999]`,
		"[\"e\",\"\u2028\u2029\ufeff\"]",
		`["e",-0,-0.0,1.0,1e3]`,
		`["e",18446744073709551615,18446744073709551616,-9223372036854775808,-9223372036854775809]`,
		`["e",1.5,0.30000000000000004,5e-324,1.7976931348623157e308]`,
		`["e",{"a":1,"a":2}]`,
		`["e",{"_placeholder":true,"num":0}]`,
		`["connect"]`,
		`[]`,
		`{}`,
		`"e"`,
		`["e","𐀀\ud800"]`,
		`["e","\ud800\udc00\ud800"]`,
		textNested(maxDepth - envelopeDepth),
		textNested(maxDepth - envelopeDepth + 1),
	}
	for _, seed := range seeds {
		f.Add([]byte(seed), "/", "blue")
	}
	f.Add([]byte(`["e"]`), "", "")
	f.Add([]byte(`["e"]`), "/é", "é\nroom")
	f.Add([]byte(`["e"]`), strings.Repeat("n", 300), strings.Repeat("r", 300))
	for _, group := range encodeGroups {
		for _, tc := range group.rows {
			if len(tc.json) > 4096 {
				continue
			}
			f.Add([]byte(`["e",`+tc.json+`]`), "/", "blue")
			f.Add([]byte(tc.json), "/", "blue")
		}
	}
	f.Fuzz(func(t *testing.T, source []byte, namespace, room string) {
		if !json.Valid(source) {
			t.Skip()
		}
		want, err := canonicalJSON(source)
		if err != nil {
			t.Skip()
		}
		list, isList := want.([]any)
		if !isList || len(list) == 0 {
			t.Skip()
		}
		if name, isName := list[0].(string); !isName || parser.IsReserved(name) {
			t.Skip()
		}
		msg, err := newWire(defaultKey, ownUID).encode(emitOperation(namespace, []string{room}, nil, string(source), nil))
		if nesting(source) > maxDepth-envelopeDepth {
			if !errors.Is(err, errTooDeep) {
				t.Fatalf("encode of %s nested %d deep = %v, want an error wrapping errTooDeep", excerptText(source), nesting(source), err)
			}
			return
		}
		if err != nil {
			t.Fatalf("encode of %s: %v", excerptText(source), err)
		}
		if wantChannel := "socket.io#" + namespace + "#" + room + "#"; msg.channel != wantChannel {
			t.Fatalf("channel = %q, want %q", msg.channel, wantChannel)
		}
		op, relevant, err := newWire(defaultKey, peerUID).decode(msg.channel, msg.payload)
		if !strings.HasPrefix(namespace, "/") {
			if relevant || err != nil {
				t.Fatalf("decode on the channel %q, which names no namespace = %v, %v, want it ignored", msg.channel, relevant, err)
			}
			return
		}
		if err != nil || !relevant {
			t.Fatalf("decode of the encoding of %s = %v, %v", excerptText(source), relevant, err)
		}
		if !json.Valid(op.Packet.Data) {
			t.Fatalf("decoded data %q is not JSON", op.Packet.Data)
		}
		got, err := canonicalJSON(op.Packet.Data)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("decoded data %s, want %s: %v", excerptText(op.Packet.Data), excerptText(source), err)
		}
		if len(op.Packet.Attachments) != 0 || len(op.Packet.Placeholders) != 0 || op.Packet.Type != parser.Event {
			t.Fatalf("decoded packet type %v with %d attachments and %d placeholder offsets, want the event type and none",
				op.Packet.Type, len(op.Packet.Attachments), len(op.Packet.Placeholders))
		}
		if op.Namespace != namespace || op.Packet.Namespace != namespace {
			t.Fatalf("namespace = %q and %q, want %q", op.Namespace, op.Packet.Namespace, namespace)
		}
		if len(op.Rooms) != 1 || op.Rooms[0] != room || len(op.Except) != 0 {
			t.Fatalf("rooms %q and except %q, want [%q] and none", op.Rooms, op.Except, room)
		}
	})
}

// excerptText shortens a text to the 120 bytes that a failure message shows.
func excerptText(text []byte) string {
	if len(text) > 120 {
		return fmt.Sprintf("%q...", text[:120])
	}
	return fmt.Sprintf("%q", text)
}

func FuzzDecodeBroadcast(f *testing.F) {
	const seedLimit = 64 << 10
	for _, seed := range []string{
		nodeBinaryEmitHex, nodeTextEmitHex, nodeAdminEmitHex, nodeOwnEmitHex, nodeNoNamespaceEmitHex,
		nodeNoRoomsEmitHex, nodeNullRoomsEmitHex, nodeHTMLEmitHex, nodeTwoBuffersEmitHex, nodeNumbersEmitHex,
		nodeFlagsEmitHex, nodeExtraFieldsEmitHex, nodeUnorderedEmitHex, nodeSampleHex,
		emitWithBinaryHex, emitPlainAdminHex, emitSingleRoomHex, emitUnorderedHex, emitNumbersHex,
		emitStringsHex, emitManyBinaryHex,
	} {
		f.Add(unhex(f, seed))
	}
	for _, tc := range malformedBroadcastGoldens {
		f.Add(unhex(f, tc.hex))
	}
	for _, tc := range acceptedEnvelopes() {
		if len(tc.payload) <= seedLimit {
			f.Add(tc.payload)
		}
	}
	for _, tc := range ignoredEnvelopes() {
		if len(tc.payload) <= seedLimit {
			f.Add(tc.payload)
		}
	}
	for _, tc := range rejectedEnvelopes() {
		if len(tc.payload) <= seedLimit {
			f.Add(tc.payload)
		}
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		channel := "socket.io#/#"
		sent, _ := (&reader{src: payload, depth: envelopeDepth}).broadcast(ownUID)
		if sent != nil {
			channel = channelOf(*sent)
		}
		op, relevant, err := newWire(defaultKey, ownUID).decode(channel, payload)
		if sent != nil && !strings.HasPrefix(sent.Namespace, "/") {
			if relevant || err != nil || !reflect.DeepEqual(op, socketio.Operation{}) {
				t.Fatalf("decode of %x on the channel %q, which names no namespace = %s, %t, %v, want it ignored", payload, channel, describe(op), relevant, err)
			}
			return
		}
		if err != nil {
			if !slices.ContainsFunc(knownDecodeErrors, func(known error) bool { return errors.Is(err, known) }) {
				t.Fatalf("decode of %x failed with an error that is none of the known ones: %v", payload, err)
			}
			if relevant || !reflect.DeepEqual(op, socketio.Operation{}) {
				t.Fatalf("decode of %x failed but returned %s and %t", payload, describe(op), relevant)
			}
			return
		}
		if !relevant {
			if !reflect.DeepEqual(op, socketio.Operation{}) || !bytes.Contains(payload, []byte(ownUID)) {
				t.Fatalf("decode of %x ignored a message that is not the emit of this server: %s", payload, describe(op))
			}
			return
		}
		if op.Kind != socketio.OpEmit || op.Packet == nil || op.Namespace != op.Packet.Namespace {
			t.Fatalf("decode of %x = %s, want an emit with a packet of the same namespace", payload, describe(op))
		}
		if _, elsewhere, err := newWire(defaultKey, ownUID).decode("socket.io#"+op.Namespace+"x#", payload); elsewhere || err != nil {
			t.Fatalf("decode of %x on the channel of another namespace = %t, %v, want it ignored", payload, elsewhere, err)
		}
		attachments := op.Packet.Attachments
		if wantType := map[bool]parser.Type{false: parser.Event, true: parser.BinaryEvent}[len(attachments) > 0]; op.Packet.Type != wantType {
			t.Fatalf("decode of %x = packet type %v with %d attachments, want %v", payload, op.Packet.Type, len(attachments), wantType)
		}
		if !json.Valid(op.Packet.Data) {
			t.Fatalf("decode of %x = data %q, which is not JSON", payload, op.Packet.Data)
		}
		value, err := canonicalJSON(op.Packet.Data)
		if err != nil {
			t.Fatalf("decode of %x = data %q: %v", payload, op.Packet.Data, err)
		}
		list, isList := value.([]any)
		if !isList || len(list) == 0 {
			t.Fatalf("decode of %x = data %q, want a non-empty array", payload, op.Packet.Data)
		}
		if name, isName := list[0].(string); !isName || parser.IsReserved(name) {
			t.Fatalf("decode of %x = data %q, want a name that an application may emit", payload, op.Packet.Data)
		}
		if nesting(op.Packet.Data) > maxDepth-envelopeDepth {
			t.Fatalf("decode of %x = data nested %d deep", payload, nesting(op.Packet.Data))
		}
		requireSlots(t, op.Packet)
		msg, err := newWire(defaultKey, thirdUID).encode(op)
		if err != nil {
			t.Fatalf("encode of the decoding of %x: %v", payload, err)
		}
		again, relevant, err := newWire(defaultKey, peerUID).decode(msg.channel, msg.payload)
		if err != nil || !relevant {
			t.Fatalf("decode of the encoding of the decoding of %x = %v, %v", payload, relevant, err)
		}
		other, err := canonicalJSON(again.Packet.Data)
		if err != nil || !reflect.DeepEqual(other, value) {
			t.Fatalf("data after a round trip = %s, want %s: %v", excerptText(again.Packet.Data), excerptText(op.Packet.Data), err)
		}
		requireAttachments(t, again.Packet.Attachments, attachments)
		requireSlots(t, again.Packet)
	})
}
