package socketio

import (
	"sync"

	"github.com/hilmanzhy/socket.io-golang/parser"
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
	mu       sync.Mutex
	acked    bool
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
// acknowledgement. Only one answer is sent: the first call that hands it over
// for sending claims it, so a call that fails, because its arguments cannot be
// encoded or because the socket is disconnected, leaves it to the next one, and
// later calls do nothing.
func (e *Event) Ack(args ...any) error {
	if !e.wantsAck {
		return nil
	}
	packet, err := parser.NewAck(e.Socket.namespace.name, e.ackID, args)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.acked {
		return nil
	}
	if err := e.Socket.send(packet); err != nil {
		return err
	}
	e.acked = true
	return nil
}
