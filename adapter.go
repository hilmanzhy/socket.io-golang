package socketio

import "github.com/nuriontech/socket.io-golang/parser"

// OperationKind names what a broadcast operation does to its targets.
type OperationKind string

// Kinds of broadcast operation.
const (
	OpEmit       OperationKind = "emit"
	OpJoin       OperationKind = "join"
	OpLeave      OperationKind = "leave"
	OpDisconnect OperationKind = "disconnect"
)

// Operation is one broadcast as it travels between the servers of a cluster.
// Every server applies it to its own sockets.
type Operation struct {
	Kind      OperationKind `json:"kind"`
	Namespace string        `json:"nsp"`
	// Rooms selects the sockets in any of these rooms; empty selects all.
	Rooms []string `json:"rooms,omitempty"`
	// Except removes the sockets in any of these rooms from the selection.
	Except []string `json:"except,omitempty"`
	// Packet is the event to send, for OpEmit.
	Packet *parser.Packet `json:"packet,omitempty"`
	// Targets are the rooms to join or leave, for OpJoin and OpLeave.
	Targets []string `json:"targets,omitempty"`
	// Close also closes the underlying connections, for OpDisconnect.
	Close bool `json:"close,omitempty"`
}

// Adapter carries operations between the servers of a cluster. The server
// that starts an operation applies it locally by itself, so an adapter must
// not hand a server the operations that server published.
//
// Whoever can publish to an adapter can emit to every client: its transport
// must be reachable by the servers only.
type Adapter interface {
	// Publish sends an operation to the other servers. It must not block on
	// the network.
	Publish(Operation) error
	// Subscribe sets the function that applies the operations of the other
	// servers. The Server calls it once, from New.
	Subscribe(apply func(Operation))
	// Close releases the resources of the adapter.
	Close() error
}

// applyRemote runs an operation received from another server. It never
// creates a namespace and only lets event packets through.
func (s *Server) applyRemote(op Operation) {
	namespace := s.lookup(op.Namespace)
	if namespace == nil {
		return
	}
	if op.Kind == OpEmit {
		if op.Packet == nil || (op.Packet.Type != parser.Event && op.Packet.Type != parser.BinaryEvent) {
			return
		}
		op.Packet.Namespace = namespace.name
	}
	namespace.apply(op)
}
