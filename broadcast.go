package socketio

import (
	"errors"
	"slices"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

// BroadcastOperator addresses a set of sockets of one namespace: the ones in
// any of its rooms, or all of them, minus the ones in its excluded rooms. It
// is an immutable value; To, Except and Local return a narrowed copy.
//
// Emit, Join, Leave and Disconnect act on the matching sockets of every
// server of the cluster, unless Local was called.
type BroadcastOperator struct {
	namespace *Namespace
	rooms     []string
	except    []string
	local     bool
}

// To adds rooms to the target.
func (b BroadcastOperator) To(rooms ...string) BroadcastOperator {
	b.rooms = append(slices.Clip(b.rooms), rooms...)
	return b
}

// Except excludes the sockets that are in any of rooms.
func (b BroadcastOperator) Except(rooms ...string) BroadcastOperator {
	b.except = append(slices.Clip(b.except), rooms...)
	return b
}

// Local restricts the operation to the sockets of this server.
func (b BroadcastOperator) Local() BroadcastOperator {
	b.local = true
	return b
}

// Emit sends an event to the matching sockets. The packet is encoded once
// and shared by all of them.
func (b BroadcastOperator) Emit(event string, args ...any) error {
	packet, err := parser.NewEvent(b.namespace.name, event, args)
	if err != nil {
		return err
	}
	return b.run(Operation{Kind: OpEmit, Packet: packet})
}

// Join adds the matching sockets to rooms. A socket that would go over
// Options.MaxRooms is skipped: on this server that makes the error wrap
// ErrRoomLimit, on the other servers it is logged.
func (b BroadcastOperator) Join(rooms ...string) error {
	return b.run(Operation{Kind: OpJoin, Targets: rooms})
}

// Leave removes the matching sockets from rooms.
func (b BroadcastOperator) Leave(rooms ...string) error {
	return b.run(Operation{Kind: OpLeave, Targets: rooms})
}

// Disconnect disconnects the matching sockets. See Socket.Disconnect.
func (b BroadcastOperator) Disconnect(closeConnection bool) error {
	return b.run(Operation{Kind: OpDisconnect, Close: closeConnection})
}

// Sockets returns the matching sockets of this server.
func (b BroadcastOperator) Sockets() []*Socket {
	return b.namespace.rooms.match(b.rooms, b.except)
}

// run applies the operation here, then publishes it to the cluster. The
// returned error concerns the publication, or a join refused by the room
// limit.
func (b BroadcastOperator) run(op Operation) error {
	op.Namespace, op.Rooms, op.Except = b.namespace.name, b.rooms, b.except
	failure := b.namespace.apply(op)
	adapter := b.namespace.server.opts.Adapter
	if b.local || adapter == nil {
		return failure
	}
	return errors.Join(failure, adapter.Publish(op))
}
