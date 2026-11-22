// Package parser encodes and decodes Socket.IO protocol v5 packets.
//
// A packet travels as one text frame, followed by one binary frame per
// attachment when it carries binary data:
//
//	<type>[<attachments>-][<namespace>,][<ack id>][<JSON payload>]
package parser

import (
	"encoding/json"
	"strconv"
)

// Type identifies the kind of a packet.
type Type byte

// Packet types defined by Socket.IO protocol v5.
const (
	Connect Type = iota
	Disconnect
	Event
	Ack
	ConnectError
	BinaryEvent
	BinaryAck
)

// DefaultNamespace is the namespace every client joins unless it asks for
// another one.
const DefaultNamespace = "/"

var typeNames = [...]string{"CONNECT", "DISCONNECT", "EVENT", "ACK", "CONNECT_ERROR", "BINARY_EVENT", "BINARY_ACK"}

// String returns the protocol name of t.
func (t Type) String() string {
	if int(t) < len(typeNames) {
		return typeNames[t]
	}
	return "UNKNOWN"
}

func (t Type) carriesBinary() bool {
	return t == BinaryEvent || t == BinaryAck
}

// Packet is one Socket.IO packet. Data holds the JSON payload, in which each
// binary value is replaced by a placeholder that refers to Attachments.
type Packet struct {
	Type        Type            `json:"type"`
	Namespace   string          `json:"nsp"`
	ID          uint64          `json:"id,omitempty"`
	HasID       bool            `json:"hasId,omitempty"`
	Data        json.RawMessage `json:"data,omitempty"`
	Attachments [][]byte        `json:"attachments,omitempty"`
}

// Encode returns the text frame of p. The attachments follow it on the wire
// as binary frames, in order.
func (p *Packet) Encode() []byte {
	frame := make([]byte, 0, 24+len(p.Namespace)+len(p.Data))
	frame = append(frame, '0'+byte(p.Type))
	if p.Type.carriesBinary() {
		frame = strconv.AppendInt(frame, int64(len(p.Attachments)), 10)
		frame = append(frame, '-')
	}
	if p.Namespace != "" && p.Namespace != DefaultNamespace {
		frame = append(frame, p.Namespace...)
		frame = append(frame, ',')
	}
	if p.HasID {
		frame = strconv.AppendUint(frame, p.ID, 10)
	}
	return append(frame, p.Data...)
}
