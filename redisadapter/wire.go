package redisadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	socketio "github.com/hilmanzhy/socket.io-golang"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

// Request types of the Node.js adapter that this adapter speaks.
const (
	remoteJoin       = 2
	remoteLeave      = 3
	remoteDisconnect = 4
)

// envelopeDepth is the number of containers the data of an emit sits in: the
// message and its packet.
const envelopeDepth = 2

// placeholderTail is the room a placeholder takes after its opening: the
// digits of an attachment number, the closing brace and a comma.
const placeholderTail = 12

var (
	errMalformed = errors.New("redisadapter: malformed message")
	errRoomName  = errors.New("redisadapter: room name is not UTF-8")
)

// message is one publication: a payload and the channel it goes to.
type message struct {
	channel string
	payload []byte
}

// request is the JSON form of a join, a leave or a disconnect. Rooms belongs
// to a join or a leave, and Close to a disconnect, which always carries it.
type request struct {
	UID   string     `json:"uid"`
	Type  int        `json:"type"`
	Opts  *selection `json:"opts"`
	Rooms roomList   `json:"rooms,omitzero"`
	Close *bool      `json:"close,omitempty"`
}

// selection is the opts object of a request: the sockets it applies to.
type selection struct {
	Rooms  roomList `json:"rooms"`
	Except roomList `json:"except"`
}

// roomList is a list of rooms in a request.
type roomList []string

// UnmarshalJSON reads a list of rooms, or the null that stands for none. A
// room that is null fails the request: encoding/json would read it as the
// empty name, which is another room.
func (l *roomList) UnmarshalJSON(text []byte) error {
	var rooms []*string
	if err := json.Unmarshal(text, &rooms); err != nil {
		return err
	}
	*l = nil
	if rooms != nil {
		*l = make(roomList, 0, len(rooms))
	}
	for _, room := range rooms {
		if room == nil {
			return errors.New("a room is null")
		}
		*l = append(*l, *room)
	}
	return nil
}

// wire translates the operations of one server to and from the messages of
// the Node.js adapter:
//
//   - an emit is the MessagePack array [uid, packet, opts] on the channel
//     key#namespace#, or key#namespace#room# when it targets a single room;
//   - a join, a leave or a disconnect is a JSON object on the channel
//     key-request#namespace#.
type wire struct {
	uid        string
	broadcasts string
	requests   string
}

func newWire(key, uid string) wire {
	return wire{uid: uid, broadcasts: key + "#", requests: key + "-request#"}
}

// patterns returns the channel patterns to subscribe to.
func (w wire) patterns() []string {
	return []string{w.broadcasts + "*", w.requests + "*"}
}

func (w wire) encode(op socketio.Operation) (message, error) {
	switch op.Kind {
	case socketio.OpEmit:
		return w.encodeBroadcast(op)
	case socketio.OpJoin:
		return w.encodeRequest(op, request{Type: remoteJoin, Rooms: list(op.Targets)})
	case socketio.OpLeave:
		return w.encodeRequest(op, request{Type: remoteLeave, Rooms: list(op.Targets)})
	case socketio.OpDisconnect:
		return w.encodeRequest(op, request{Type: remoteDisconnect, Close: &op.Close})
	}
	return message{}, fmt.Errorf("redisadapter: unknown operation %q", op.Kind)
}

// encodeBroadcast writes an emit the way the Node.js adapter does, member by
// member and in the same order:
//
//	[uid, {type, data, nsp}, {rooms, except, flags}]
func (w wire) encodeBroadcast(op socketio.Operation) (message, error) {
	if op.Packet == nil {
		return message{}, errors.New("redisadapter: emit without a packet")
	}
	payload := appendText(array.header(nil, 3), w.uid)
	payload = dictionary.header(payload, 3)
	payload = append(appendText(payload, "type"), byte(parser.Event))
	payload, err := appendJSON(appendText(payload, "data"), op.Packet.Data, op.Packet.Attachments, op.Packet.Placeholders, envelopeDepth)
	if err != nil {
		return message{}, err
	}
	payload = appendText(appendText(payload, "nsp"), op.Namespace)
	payload = dictionary.header(payload, 3)
	payload = appendTexts(appendText(payload, "rooms"), op.Rooms)
	payload = appendTexts(appendText(payload, "except"), op.Except)
	payload = dictionary.header(appendText(payload, "flags"), 0)
	return message{w.channel(op.Namespace, op.Rooms), payload}, nil
}

// channel returns the channel of an emit: the one of its namespace, or the
// one of its room when it targets exactly one.
func (w wire) channel(namespace string, rooms []string) string {
	channel := w.broadcasts + namespace + "#"
	if len(rooms) == 1 {
		channel += rooms[0] + "#"
	}
	return channel
}

// encodeRequest writes a request the way JSON.stringify does, which leaves <,
// > and & as they are. JSON text cannot hold a room name that is not UTF-8,
// and encoding/json would publish another name in its place, so such a
// request is refused.
func (w wire) encodeRequest(op socketio.Operation, r request) (message, error) {
	r.UID, r.Opts = w.uid, &selection{Rooms: list(op.Rooms), Except: list(op.Except)}
	for _, rooms := range []roomList{r.Opts.Rooms, r.Opts.Except, r.Rooms} {
		if slices.ContainsFunc(rooms, func(room string) bool { return !utf8.ValidString(room) }) {
			return message{}, errRoomName
		}
	}
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(r)
	return message{w.requests + op.Namespace + "#", bytes.TrimSuffix(payload.Bytes(), []byte{'\n'})}, err
}

// decode returns the operation a message asks for. It reports false for the
// messages this server has nothing to do with: its own, and the requests it
// does not take part in.
func (w wire) decode(channel string, payload []byte) (socketio.Operation, bool, error) {
	if namespace, found := strings.CutPrefix(channel, w.requests); found {
		return w.decodeRequest(strings.TrimSuffix(namespace, "#"), payload)
	}
	if strings.HasPrefix(channel, w.broadcasts) {
		return w.decodeBroadcast(channel, payload)
	}
	return socketio.Operation{}, false, nil
}

// decodeBroadcast reads an emit. The pattern of the subscription also matches
// the channels of a key that starts with this one, so the emit counts only
// when it arrived on the channel its sender publishes it on: the one of its
// namespace, or the one of its room when it targets exactly one. Any other
// emit is the traffic of a neighbour, and ignored. The name of a namespace
// starts with a slash, so a channel that names none is ignored unread.
func (w wire) decodeBroadcast(channel string, payload []byte) (socketio.Operation, bool, error) {
	if !strings.HasPrefix(channel, w.broadcasts+"/") {
		return socketio.Operation{}, false, nil
	}
	r := reader{src: payload, depth: envelopeDepth}
	op, err := r.broadcast(w.uid)
	if op == nil || err != nil {
		return socketio.Operation{}, false, err
	}
	if !w.carries(channel, op.Namespace, op.Rooms) {
		return socketio.Operation{}, false, nil
	}
	return *op, true, nil
}

// carries reports whether channel is one that an emit of the namespace to
// rooms is published on: the channel of the namespace, or the one of the room
// when there is exactly one.
func (w wire) carries(channel, namespace string, rooms []string) bool {
	rest, keyed := strings.CutPrefix(channel, w.broadcasts)
	rest, named := strings.CutPrefix(rest, namespace)
	room, closed := strings.CutPrefix(rest, "#")
	if !keyed || !named || !closed {
		return false
	}
	single, ended := strings.CutSuffix(room, "#")
	return room == "" || ended && len(rooms) == 1 && single == rooms[0]
}

func (w wire) decodeRequest(namespace string, payload []byte) (socketio.Operation, bool, error) {
	if !bytes.HasPrefix(payload, []byte("{")) {
		return socketio.Operation{}, false, nil
	}
	var r request
	if err := json.Unmarshal(payload, &r); err != nil {
		return socketio.Operation{}, false, fmt.Errorf("%w: %w", errMalformed, err)
	}
	if r.Opts == nil || r.UID == w.uid {
		return socketio.Operation{}, false, nil
	}
	op := socketio.Operation{Namespace: namespace, Rooms: r.Opts.Rooms, Except: r.Opts.Except}
	switch r.Type {
	case remoteJoin:
		op.Kind, op.Targets = socketio.OpJoin, r.Rooms
	case remoteLeave:
		op.Kind, op.Targets = socketio.OpLeave, r.Rooms
	case remoteDisconnect:
		op.Kind, op.Close = socketio.OpDisconnect, r.Close != nil && *r.Close
	default:
		return socketio.Operation{}, false, nil
	}
	return op, true, nil
}

// broadcast reads the array [uid, packet, opts] of an emit. It returns nil,
// without reading further, for the emit of the server whose uid is own.
//
// Only the data of the packet is turned into JSON text. Everything else is
// read in place or passed over, and the message is never built in memory.
func (r *reader) broadcast(own string) (*socketio.Operation, error) {
	top, err := r.next()
	if err != nil {
		return nil, err
	}
	if top.kind != kindArray || top.size < 3 {
		return nil, errMalformed
	}
	sender, err := r.next()
	if err != nil {
		return nil, err
	}
	if sender.kind == kindText && sender.size <= len(r.src) && string(r.src[:sender.size]) == own {
		return nil, nil
	}
	if _, _, err := r.drop(sender); err != nil {
		return nil, err
	}
	packet, err := r.packet()
	if err != nil {
		return nil, err
	}
	op := &socketio.Operation{Kind: socketio.OpEmit, Namespace: packet.Namespace, Packet: packet}
	err = r.members(func(key []byte) (err error) {
		switch string(key) {
		case "rooms":
			op.Rooms, err = r.names()
		case "except":
			op.Except, err = r.names()
		default:
			err = r.skip()
		}
		return err
	})
	for extra := 3; err == nil && extra < top.size; extra++ {
		err = r.skip()
	}
	if err == nil && len(r.src) > 0 {
		err = errTrailing
	}
	if err != nil {
		return nil, err
	}
	return op, nil
}

// packet reads the packet of an emit: {type, data, nsp}. The type and the
// data are required, and a packet without a namespace belongs to the default
// one. Of two members with the same name the last one wins, as in JavaScript.
func (r *reader) packet() (*parser.Packet, error) {
	p := &parser.Packet{Type: parser.Event, Namespace: parser.DefaultNamespace}
	typed := false
	err := r.members(func(key []byte) error {
		switch string(key) {
		case "type":
			t, err := r.next()
			typed = (t.kind == kindInt || t.kind == kindUint) && t.bits == uint64(parser.Event)
			if err == nil && !typed {
				err = errMalformed
			}
			return err
		case "data":
			return r.event(p)
		case "nsp":
			name, err := r.text()
			p.Namespace = string(name)
			return err
		}
		return r.skip()
	})
	if err == nil && (!typed || p.Data == nil) {
		err = errMalformed
	}
	if len(p.Attachments) > 0 {
		p.Type = parser.BinaryEvent
	}
	return p, err
}

// event reads the data of a packet into p: an array that starts with the name
// of an event a server may emit. Its bin values become the attachments of p.
//
// A first pass counts the bin values and measures the rest, and the second
// one starts with buffers sized from it.
func (r *reader) event(p *parser.Packet) error {
	t, err := r.next()
	if err != nil {
		return err
	}
	if t.kind != kindArray || t.size == 0 {
		return errMalformed
	}
	rest := r.src
	name, err := r.text()
	if err != nil {
		return err
	}
	if parser.IsReserved(string(name)) {
		return errMalformed
	}
	r.src = rest
	blobs, held, err := r.drop(t)
	if err != nil {
		return err
	}
	text, err := textSize(len(rest)-len(r.src)-held, blobs)
	if err != nil {
		return err
	}
	r.src, r.out = rest, slices.Grow(r.out[:0], text)
	r.attachments, r.slots = slices.Grow(r.attachments[:0], blobs), slices.Grow(r.slots[:0], blobs)
	if err := r.array(t.size); err != nil {
		return err
	}
	p.Data, p.Attachments, p.Placeholders = r.out, r.attachments, r.slots
	return nil
}

// textSize returns the room to reserve for the JSON text of data whose
// values, the bin values left out, take the given number of bytes: twice
// those bytes, plus one placeholder per bin value. That is an estimate, not a
// bound. A value of one byte takes up to six bytes of JSON text, false or an
// escaped control character for instance, and data made of such values grows
// the buffer as it is written. A size that does not fit an int, which a
// message can reach only where an int has 32 bits, is refused.
func textSize(values, blobs int) (int, error) {
	size := 2*uint64(values) + uint64(blobs)*uint64(len(placeholder)+placeholderTail)
	if size > math.MaxInt {
		return 0, errTooLarge
	}
	return int(size), nil
}

// members reads the map that comes next. It calls visit with every key, and
// visit reads the value of that key.
func (r *reader) members(visit func(key []byte) error) error {
	t, err := r.next()
	if err != nil {
		return err
	}
	if t.kind != kindMap {
		return errMalformed
	}
	for range t.size {
		key, err := r.text()
		if err != nil {
			return err
		}
		if err := visit(key); err != nil {
			return err
		}
	}
	return nil
}

// names reads a list of rooms, or the nil that stands for none. A room that
// is not a string fails the whole list: dropping it could widen the selection
// to every socket.
func (r *reader) names() ([]string, error) {
	t, err := r.next()
	if err != nil || t.kind == kindNil {
		return nil, err
	}
	if t.kind != kindArray {
		return nil, errMalformed
	}
	rooms := make([]string, 0, t.size)
	for range t.size {
		room, err := r.text()
		if err != nil {
			return nil, err
		}
		rooms = append(rooms, string(room))
	}
	return rooms, nil
}

// text reads the string that comes next. The result aliases the input.
func (r *reader) text() ([]byte, error) {
	t, err := r.next()
	if err != nil {
		return nil, err
	}
	if t.kind != kindText {
		return nil, errMalformed
	}
	return r.take(t.size)
}

// list returns values as a list that JSON encodes as an array, never null.
func list(values []string) roomList {
	return append(roomList{}, values...)
}
