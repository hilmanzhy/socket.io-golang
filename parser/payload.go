package parser

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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
	for i, arg := range args {
		if len(data) > 1 {
			data = append(data, ',')
		}
		if blob, ok := arg.([]byte); ok {
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
	return &Packet{Type: kind, Namespace: namespace, Data: append(data, ']'), Attachments: attachments}, nil
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
	args.blobs = make([][]byte, len(values))
	for i, value := range values {
		if !bytes.Contains(value, placeholderKey) {
			continue
		}
		if index, ok := placeholderIndex(value); ok {
			blob, err := set.take(index)
			if err != nil {
				return Args{}, err
			}
			args.values[i], args.blobs[i] = nil, blob
			continue
		}
		inlined, err := set.inline(value)
		if err != nil {
			return Args{}, err
		}
		args.values[i] = inlined
	}
	return args, nil
}

func placeholderIndex(value []byte) (int64, bool) {
	var ref struct {
		Placeholder bool   `json:"_placeholder"`
		Num         *int64 `json:"num"`
	}
	if json.Unmarshal(value, &ref) != nil || !ref.Placeholder || ref.Num == nil {
		return 0, false
	}
	return *ref.Num, true
}

// attachmentSet hands the attachments of a packet to the placeholders that
// reference them. An attachment may be referenced only once: a packet that
// repeats one reference would otherwise make the server copy the same
// attachment as many times as the reference appears.
type attachmentSet struct {
	blobs [][]byte
	taken []bool
}

func (s *attachmentSet) take(index int64) ([]byte, error) {
	if index < 0 || index >= int64(len(s.blobs)) {
		return nil, malformed("attachment out of range")
	}
	if s.taken[index] {
		return nil, malformed("attachment referenced twice")
	}
	s.taken[index] = true
	return s.blobs[index], nil
}

// inline replaces the placeholders nested inside a JSON argument with their
// attachments, which encoding/json writes as base64 text.
func (s *attachmentSet) inline(value []byte) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return nil, malformed("invalid JSON argument")
	}
	tree, err := s.substitute(tree)
	if err != nil {
		return nil, err
	}
	return json.Marshal(tree)
}

func (s *attachmentSet) substitute(node any) (any, error) {
	switch n := node.(type) {
	case []any:
		for i, child := range n {
			replaced, err := s.substitute(child)
			if err != nil {
				return nil, err
			}
			n[i] = replaced
		}
	case map[string]any:
		if n["_placeholder"] == true {
			return s.resolve(n["num"])
		}
		for key, child := range n {
			replaced, err := s.substitute(child)
			if err != nil {
				return nil, err
			}
			n[key] = replaced
		}
	}
	return node, nil
}

func (s *attachmentSet) resolve(num any) ([]byte, error) {
	number, ok := num.(json.Number)
	if !ok {
		return nil, malformed("invalid attachment reference")
	}
	index, err := number.Int64()
	if err != nil {
		return nil, malformed("attachment out of range")
	}
	return s.take(index)
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
	if a.blobs != nil {
		rest.blobs = a.blobs[start:]
	}
	return rest
}
