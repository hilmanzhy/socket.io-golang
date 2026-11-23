package parser

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrReservedEvent reports an event name the protocol keeps for itself.
var ErrReservedEvent = errors.New("parser: reserved event name")

var placeholderKey = []byte(`"_placeholder"`)

var reservedEvents = map[string]struct{}{
	"connect":        {},
	"connect_error":  {},
	"disconnect":     {},
	"disconnecting":  {},
	"newListener":    {},
	"removeListener": {},
}

// IsReserved reports whether event is a name applications may not emit.
func IsReserved(event string) bool {
	_, reserved := reservedEvents[event]
	return reserved
}

// NewEvent builds an EVENT packet. Arguments of type []byte become binary
// attachments; every other argument is encoded with encoding/json.
func NewEvent(namespace, event string, args []any) (*Packet, error) {
	if IsReserved(event) {
		return nil, fmt.Errorf("%w: %q", ErrReservedEvent, event)
	}
	name, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("parser: event name: %w", err)
	}
	return newPayloadPacket(Event, namespace, name, args)
}

// NewAck builds the ACK packet that answers the event carrying id.
func NewAck(namespace string, id uint64, args []any) (*Packet, error) {
	packet, err := newPayloadPacket(Ack, namespace, nil, args)
	if err != nil {
		return nil, err
	}
	packet.ID, packet.HasID = id, true
	return packet, nil
}

func newPayloadPacket(kind Type, namespace string, head []byte, args []any) (*Packet, error) {
	data := append([]byte{'['}, head...)
	var attachments [][]byte
	var placeholders []int
	for i, arg := range args {
		if len(data) > 1 {
			data = append(data, ',')
		}
		if blob, ok := arg.([]byte); ok {
			placeholders = append(placeholders, len(data))
			data = fmt.Appendf(data, `{"_placeholder":true,"num":%d}`, len(attachments))
			attachments = append(attachments, blob)
			continue
		}
		encoded, err := json.Marshal(arg)
		if err != nil {
			return nil, fmt.Errorf("parser: argument %d: %w", i, err)
		}
		data = append(data, encoded...)
	}
	if len(attachments) > 0 {
		kind += BinaryEvent - Event
	}
	return &Packet{
		Type:         kind,
		Namespace:    namespace,
		Data:         append(data, ']'),
		Attachments:  attachments,
		Placeholders: placeholders,
	}, nil
}

// ParseEvent splits an EVENT or BINARY_EVENT packet into its name and
// arguments.
func ParseEvent(p *Packet) (string, Args, error) {
	args, err := parseArgs(p)
	if err != nil {
		return "", Args{}, err
	}
	var name *string
	if args.Len() == 0 || args.IsBinary(0) || json.Unmarshal(args.values[0], &name) != nil || name == nil {
		return "", Args{}, malformed("event name must be a string")
	}
	if IsReserved(*name) {
		return "", Args{}, malformed("reserved event name")
	}
	return *name, args.from(1), nil
}

// ParseAck returns the arguments of an ACK or BINARY_ACK packet.
func ParseAck(p *Packet) (Args, error) {
	return parseArgs(p)
}

func parseArgs(p *Packet) (Args, error) {
	var values []json.RawMessage
	if err := json.Unmarshal(p.Data, &values); err != nil {
		return Args{}, malformed("payload must be a JSON array")
	}
	args := Args{values: values}
	if len(p.Attachments) == 0 {
		return args, nil
	}
	set := attachmentSet{blobs: p.Attachments, taken: make([]bool, len(p.Attachments))}
	for i, value := range values {
		if !bytes.Contains(value, placeholderKey) {
			continue
		}
		found := placeholders(value)
		if len(found) == 1 && found[0].end-found[0].start == len(value) {
			blob, err := set.take(found[0].num)
			if err != nil {
				return Args{}, err
			}
			if args.blobs == nil {
				args.blobs = make([][]byte, len(values))
			}
			args.values[i], args.blobs[i] = nil, blob
			continue
		}
		inlined, err := set.inline(value, found)
		if err != nil {
			return Args{}, err
		}
		args.values[i] = inlined
	}
	return args, nil
}

// placeholder locates an object that stands for an attachment inside a JSON
// argument: the object is the bytes from start to end, and num is the text of
// its num member, nil when the object has none or when it is not a number.
type placeholder struct {
	start, end int
	num        []byte
}

// container is an array or an object of a JSON argument that is still open:
// where it starts, how many placeholders were found before it, and, for an
// object, the member being read and what its _placeholder and num members
// hold so far.
type container struct {
	start   int
	found   int
	object  bool
	named   bool
	member  []byte
	flagged bool
	num     []byte
}

// set records value as the member being read when that member is one of the
// two that make an object a placeholder. A later member replaces an earlier
// one of the same name, as it does when the JSON is parsed. A value that is
// an array or an object is given as nil.
func (c *container) set(value []byte) {
	switch string(c.member) {
	case `"_placeholder"`:
		c.flagged = string(value) == "true"
	case `"num"`:
		c.num = nil
		if len(value) > 0 && (value[0] == '-' || value[0] >= '0' && value[0] <= '9') {
			c.num = value
		}
	}
}

// placeholders returns the placeholders of value, which must be valid JSON,
// in order: its objects whose _placeholder member is true. What such an
// object holds is not looked into, so a placeholder never lies inside
// another one. The value is read once and never decoded into a tree: the
// memory used does not grow with the number of values a client packs into
// one argument.
func placeholders(value []byte) []placeholder {
	var found []placeholder
	var open []container
	for i := 0; i < len(value); i++ {
		top := len(open) - 1
		switch c := value[i]; c {
		case ' ', '\t', '\n', '\r', ':':
		case '{', '[':
			if top >= 0 {
				open[top].set(nil)
			}
			open = append(open, container{start: i, found: len(found), object: c == '{'})
		case '}', ']':
			if closed := open[top]; closed.flagged {
				found = append(found[:closed.found], placeholder{closed.start, i + 1, closed.num})
			}
			open = open[:top]
		case ',':
			open[top].named = false
		default:
			token := scalar(value[i:])
			i += len(token) - 1
			switch {
			case top < 0:
			case open[top].object && !open[top].named:
				open[top].member, open[top].named = token, true
			default:
				open[top].set(token)
			}
		}
	}
	return found
}

// scalar returns the string, number or literal that the JSON text src starts
// with.
func scalar(src []byte) []byte {
	switch src[0] {
	case '"':
		return src[:endOfString(src, 0)+1]
	case 't', 'n':
		return src[:len("true")]
	case 'f':
		return src[:len("false")]
	}
	end := 1
	for end < len(src) && strings.IndexByte("+-.eE0123456789", src[end]) >= 0 {
		end++
	}
	return src[:end]
}

// attachmentSet hands the attachments of a packet to the placeholders that
// reference them. An attachment may be referenced only once: a packet that
// repeats one reference would otherwise make the server copy the same
// attachment as many times as the reference appears.
type attachmentSet struct {
	blobs [][]byte
	taken []bool
}

// take returns the attachment that num, the num member of a placeholder,
// refers to.
func (s *attachmentSet) take(num []byte) ([]byte, error) {
	if num == nil {
		return nil, malformed("invalid attachment reference")
	}
	index, err := strconv.ParseInt(string(num), 10, 64)
	if err != nil || index < 0 || index >= int64(len(s.blobs)) {
		return nil, malformed("attachment out of range")
	}
	if s.taken[index] {
		return nil, malformed("attachment referenced twice")
	}
	s.taken[index] = true
	return s.blobs[index], nil
}

// inline returns the argument value with each of the placeholders found in
// it replaced by its attachment, written as the base64 text that
// encoding/json reads into a []byte. The rest of the argument is copied as
// the client wrote it.
func (s *attachmentSet) inline(value []byte, found []placeholder) (json.RawMessage, error) {
	inlined := make([]byte, 0, len(value))
	copied := 0
	for _, p := range found {
		blob, err := s.take(p.num)
		if err != nil {
			return nil, err
		}
		inlined = append(append(inlined, value[copied:p.start]...), '"')
		inlined = append(base64.StdEncoding.AppendEncode(inlined, blob), '"')
		copied = p.end
	}
	return append(inlined, value[copied:]...), nil
}

// Args are the positional arguments of an event or of an acknowledgement.
type Args struct {
	values []json.RawMessage
	blobs  [][]byte
}

// Len returns the number of arguments.
func (a Args) Len() int {
	return len(a.values)
}

// IsBinary reports whether argument i was sent as a binary attachment.
func (a Args) IsBinary(i int) bool {
	return i >= 0 && i < len(a.blobs) && a.values[i] == nil
}

// Bytes returns binary argument i, or nil when it is not binary.
func (a Args) Bytes(i int) []byte {
	if !a.IsBinary(i) {
		return nil
	}
	return a.blobs[i]
}

// Raw returns the JSON of argument i, or nil when it is binary or absent.
func (a Args) Raw(i int) json.RawMessage {
	if i < 0 || i >= len(a.values) {
		return nil
	}
	return a.values[i]
}

// Bind decodes the arguments into targets, in order. A binary argument
// binds to *[]byte or *any; the others follow encoding/json rules, and
// binary values nested inside them decode into []byte fields. Missing
// arguments leave their targets untouched.
func (a Args) Bind(targets ...any) error {
	for i, target := range targets {
		if i >= len(a.values) {
			return nil
		}
		if !a.IsBinary(i) {
			if err := json.Unmarshal(a.values[i], target); err != nil {
				return fmt.Errorf("parser: argument %d: %w", i, err)
			}
			continue
		}
		switch t := target.(type) {
		case *[]byte:
			*t = a.blobs[i]
		case *any:
			*t = a.blobs[i]
		default:
			return fmt.Errorf("parser: argument %d is binary and needs a *[]byte target", i)
		}
	}
	return nil
}

func (a Args) from(start int) Args {
	rest := Args{values: a.values[start:]}
	if start < len(a.blobs) {
		rest.blobs = a.blobs[start:]
	}
	return rest
}
