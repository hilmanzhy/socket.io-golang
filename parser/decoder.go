package parser

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// ErrMalformed is wrapped by every error that reports a protocol violation.
var ErrMalformed = errors.New("parser: malformed packet")

// DefaultMaxAttachments is the attachment limit of a zero Decoder.
const DefaultMaxAttachments = 10

// DefaultMaxArguments is the limit of payload elements of a zero Decoder.
const DefaultMaxArguments = 64

func malformed(detail string) error {
	return fmt.Errorf("%w: %s", ErrMalformed, detail)
}

// Decoder turns the frames of one connection back into packets. It keeps the
// packet under reconstruction while its attachments arrive, so a Decoder
// must not be shared between connections or used concurrently. Packets refer
// to the frames they were decoded from instead of copying them: a frame must
// not be modified once it has been handed to the Decoder.
type Decoder struct {
	// MaxAttachments caps the binary attachments of one packet. Zero or a
	// negative value means DefaultMaxAttachments.
	MaxAttachments int

	// MaxArguments caps the elements of the payload array of an EVENT,
	// BINARY_EVENT, ACK or BINARY_ACK packet. The event name of an event
	// counts as one element. Zero or a negative value means
	// DefaultMaxArguments.
	MaxArguments int

	pending *Packet
	missing int
}

// Text consumes a text frame. It returns nil without error when the packet
// still waits for attachments.
func (d *Decoder) Text(frame []byte) (*Packet, error) {
	if d.pending != nil {
		return nil, malformed("text frame while attachments are expected")
	}
	packet, attachments, err := decodeText(frame)
	if err != nil {
		return nil, err
	}
	if err := d.checkLimits(packet, attachments); err != nil {
		return nil, err
	}
	if attachments == 0 {
		return packet, nil
	}
	d.pending, d.missing = packet, attachments
	return nil, nil
}

// Binary consumes a binary frame. It returns the packet once its last
// attachment has arrived.
func (d *Decoder) Binary(frame []byte) (*Packet, error) {
	if d.pending == nil {
		return nil, malformed("binary frame without a packet to attach it to")
	}
	d.pending.Attachments = append(d.pending.Attachments, frame)
	d.missing--
	if d.missing > 0 {
		return nil, nil
	}
	packet := d.pending
	d.pending = nil
	return packet, nil
}

func (d *Decoder) checkLimits(p *Packet, attachments int) error {
	if attachments > positiveOr(d.MaxAttachments, DefaultMaxAttachments) {
		return malformed("too many attachments")
	}
	if p.Type.carriesArguments() && exceedsElements(p.Data, positiveOr(d.MaxArguments, DefaultMaxArguments)) {
		return malformed("too many arguments")
	}
	return nil
}

func positiveOr(n, fallback int) int {
	if n > 0 {
		return n
	}
	return fallback
}

// exceedsElements reports whether the JSON array at the start of data holds
// more than limit elements. It counts the commas that separate the elements
// of the outermost array, so it needs no extra memory and stops at the comma
// that proves the excess. It does not validate the JSON: a malformed array is
// counted as far as its brackets and strings allow, and left to the later
// parse to reject. The limit must be positive.
func exceedsElements(data []byte, limit int) bool {
	depth, commas := 0, 0
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '"':
			i = endOfString(data, i)
		case '[', '{':
			depth++
		case ']', '}':
			depth--
			if depth == 0 {
				return false
			}
		case ',':
			if depth != 1 {
				continue
			}
			if commas++; commas >= limit {
				return true
			}
		}
	}
	return false
}

func endOfString(data []byte, open int) int {
	for i := open + 1; i < len(data); i++ {
		switch data[i] {
		case '\\':
			i++
		case '"':
			return i
		}
	}
	return len(data)
}

func decodeText(frame []byte) (*Packet, int, error) {
	if len(frame) == 0 || frame[0] < '0' || frame[0] > '0'+byte(BinaryAck) {
		return nil, 0, malformed("unknown packet type")
	}
	packet := &Packet{Type: Type(frame[0] - '0'), Namespace: DefaultNamespace}
	rest := frame[1:]

	attachments := 0
	if packet.Type.carriesBinary() {
		count, tail, found := bytes.Cut(rest, []byte{'-'})
		n, err := strconv.ParseUint(string(count), 10, 31)
		if !found || err != nil {
			return nil, 0, malformed("invalid attachment count")
		}
		attachments, rest = int(n), tail
	}

	if len(rest) > 0 && rest[0] == '/' {
		name, tail, _ := bytes.Cut(rest, []byte{','})
		packet.Namespace, rest = string(name), tail
	}

	if digits := leadingDigits(rest); digits > 0 {
		id, err := strconv.ParseUint(string(rest[:digits]), 10, 64)
		if err != nil {
			return nil, 0, malformed("invalid acknowledgement id")
		}
		packet.ID, packet.HasID, rest = id, true, rest[digits:]
	}

	if len(rest) > 0 {
		packet.Data = json.RawMessage(rest)
	}
	if err := checkPayload(packet); err != nil {
		return nil, 0, err
	}
	return packet, attachments, nil
}

func leadingDigits(b []byte) int {
	n := 0
	for n < len(b) && b[n] >= '0' && b[n] <= '9' {
		n++
	}
	return n
}

func checkPayload(p *Packet) error {
	switch p.Type {
	case Connect:
		if len(p.Data) > 0 && (p.Data[0] != '{' || !json.Valid(p.Data)) {
			return malformed("CONNECT payload must be a JSON object")
		}
	case Disconnect:
		if len(p.Data) > 0 {
			return malformed("DISCONNECT carries no payload")
		}
	case ConnectError:
		if !json.Valid(p.Data) {
			return malformed("CONNECT_ERROR payload must be JSON")
		}
	default:
		if len(p.Data) == 0 || p.Data[0] != '[' {
			return malformed(p.Type.String() + " payload must be a JSON array")
		}
	}
	return nil
}
