package socketio

import (
	"sync/atomic"

	"github.com/nuriontech/socket.io-golang/parser"
)

// Args are the positional arguments of an event or of an acknowledgement.
type Args = parser.Args

// Event is one event received from a client.
type Event struct {
	// Socket is the socket that received the event.
	Socket *Socket
	// Name is the event name.
	Name string
	// Args are the arguments that came with the event.
	Args Args

	ackID    uint64
	wantsAck bool
	acked    atomic.Bool
}

// Bind decodes the arguments into targets, in order. See Args.Bind.
func (e *Event) Bind(targets ...any) error {
	return e.Args.Bind(targets...)
}

// WantsAck reports whether the client waits for an acknowledgement.
func (e *Event) WantsAck() bool {
	return e.wantsAck
}

// Ack answers the event. It does nothing when the client did not ask for an
// acknowledgement. Only one answer is sent: the first call whose arguments can
// be encoded claims it, and later calls do nothing.
func (e *Event) Ack(args ...any) error {
	if !e.wantsAck {
		return nil
	}
	packet, err := parser.NewAck(e.Socket.namespace.name, e.ackID, args)
	if err != nil {
		return err
	}
	if !e.acked.CompareAndSwap(false, true) {
		return nil
	}
	return e.Socket.send(packet)
}
