package socketio

import (
	"errors"
	"runtime/debug"
	"sync"
)

var errInternal = errors.New("Internal server error")

// Namespace is a communication channel with its own middleware, connection
// handler and rooms. One client connection can join several namespaces.
type Namespace struct {
	name   string
	server *Server
	rooms  *registry

	mu         sync.RWMutex
	middleware []func(*Socket) error
	onConnect  func(*Socket)
}

// Name returns the namespace name, which starts with "/".
func (n *Namespace) Name() string {
	return n.name
}

// Use adds a middleware that runs for every connecting socket, before it is
// connected: the socket can be inspected but cannot emit or join yet. A
// non-nil error refuses the connection and its message is sent to the
// client; return a *ConnectError to attach data. It runs on the goroutine
// that reads the client's connection, so it should not block for long.
func (n *Namespace) Use(middleware func(*Socket) error) {
	n.mu.Lock()
	n.middleware = append(n.middleware, middleware)
	n.mu.Unlock()
}

// OnConnect sets the handler that runs once for every connected socket,
// replacing the previous one. Register the socket's event handlers in it.
func (n *Namespace) OnConnect(handler func(*Socket)) {
	n.mu.Lock()
	n.onConnect = handler
	n.mu.Unlock()
}

// Emit sends an event to every socket of the namespace, on every server of
// the cluster.
func (n *Namespace) Emit(event string, args ...any) error {
	return n.To().Emit(event, args...)
}

// To starts a broadcast to the sockets that joined at least one of rooms.
// Without rooms it targets every socket of the namespace.
func (n *Namespace) To(rooms ...string) BroadcastOperator {
	return BroadcastOperator{namespace: n}.To(rooms...)
}

// Except starts a broadcast to every socket that joined none of rooms.
func (n *Namespace) Except(rooms ...string) BroadcastOperator {
	return BroadcastOperator{namespace: n}.Except(rooms...)
}

// Local starts a broadcast that stays on this server.
func (n *Namespace) Local() BroadcastOperator {
	return BroadcastOperator{namespace: n, local: true}
}

// Sockets returns the sockets connected to the namespace on this server.
func (n *Namespace) Sockets() []*Socket {
	return n.rooms.match(nil, nil)
}

func (n *Namespace) admit(socket *Socket) (err error) {
	defer func() {
		if value := recover(); value != nil {
			n.server.recovered(value, debug.Stack())
			err = errInternal
		}
	}()
	n.mu.RLock()
	middleware := n.middleware
	n.mu.RUnlock()
	for _, check := range middleware {
		if err := check(socket); err != nil {
			return err
		}
	}
	return nil
}

func (n *Namespace) welcome(socket *Socket) {
	n.mu.RLock()
	handler := n.onConnect
	n.mu.RUnlock()
	if handler != nil {
		handler(socket)
	}
}

// apply runs a broadcast operation against the sockets of this server.
func (n *Namespace) apply(op Operation) {
	targets := n.rooms.match(op.Rooms, op.Except)
	switch op.Kind {
	case OpEmit:
		messages := frames(op.Packet)
		for _, socket := range targets {
			socket.conn.session.Send(messages...)
		}
	case OpJoin:
		for _, socket := range targets {
			socket.Join(op.Targets...)
		}
	case OpLeave:
		for _, socket := range targets {
			socket.Leave(op.Targets...)
		}
	case OpDisconnect:
		for _, socket := range targets {
			socket.Disconnect(op.Close)
		}
	}
}
