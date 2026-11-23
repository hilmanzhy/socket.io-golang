package socketio

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/hilmanzhy/socket.io-golang/parser"
)

// Socket is the server end of one client's connection to one namespace.
type Socket struct {
	id        string
	namespace *Namespace
	conn      *conn
	auth      json.RawMessage
	mailbox   *mailbox

	mu           sync.Mutex
	handlers     map[string]func(*Event)
	middleware   []func(*Event) error
	onDisconnect func(Reason)
	acks         map[uint64]chan Args
	nextAck      uint64
	values       map[string]any
}

func newSocket(namespace *Namespace, c *conn, auth json.RawMessage) *Socket {
	server := namespace.server
	return &Socket{
		id:        rand.Text(),
		namespace: namespace,
		conn:      c,
		auth:      auth,
		mailbox:   newMailbox(server.opts.EventBacklog, c.session.Done(), server.recovered),
		handlers:  make(map[string]func(*Event)),
		acks:      make(map[uint64]chan Args),
		values:    make(map[string]any),
	}
}

// ID returns the identifier of the socket, which is also the name of a room
// that only this socket is in.
func (s *Socket) ID() string {
	return s.id
}

// Namespace returns the namespace the socket belongs to.
func (s *Socket) Namespace() *Namespace {
	return s.namespace
}

// Request returns the HTTP request that opened the connection, without its
// body: headers, cookies, query and remote address. Every socket of the
// connection shares it: do not modify it.
func (s *Socket) Request() *http.Request {
	return s.conn.session.Request()
}

// Auth decodes the payload the client sent when it connected, the auth
// option of the JavaScript client, into target. Without a payload target is
// left untouched.
func (s *Socket) Auth(target any) error {
	if len(s.auth) == 0 {
		return nil
	}
	return json.Unmarshal(s.auth, target)
}

// Connected reports whether the socket is connected to its namespace.
func (s *Socket) Connected() bool {
	return s.namespace.rooms.has(s.id)
}

// On sets the handler of an event, replacing the previous one.
func (s *Socket) On(event string, handler func(*Event)) {
	s.mu.Lock()
	s.handlers[event] = handler
	s.mu.Unlock()
}

// Use adds a middleware that sees every received event before its handler.
// A non-nil error drops the event.
func (s *Socket) Use(middleware func(*Event) error) {
	s.mu.Lock()
	s.middleware = append(s.middleware, middleware)
	s.mu.Unlock()
}

// OnDisconnect sets the handler that runs once when the socket leaves the
// namespace, after the handlers of the events queued before. It replaces the
// previous one. No handler of the socket runs after it: an event that was
// still waiting for room in a full backlog at that moment is dropped.
func (s *Socket) OnDisconnect(handler func(Reason)) {
	s.mu.Lock()
	s.onDisconnect = handler
	s.mu.Unlock()
}

// Emit sends an event to the client. An argument of type []byte travels as a
// binary attachment; every other argument is encoded with encoding/json.
func (s *Socket) Emit(event string, args ...any) error {
	packet, err := parser.NewEvent(s.namespace.name, event, args)
	if err != nil {
		return err
	}
	return s.send(packet)
}

// EmitWithAck sends an event and waits for the client to acknowledge it. It
// returns the arguments of the acknowledgement, ErrDisconnected when the
// socket disconnects first, or the error of ctx.
func (s *Socket) EmitWithAck(ctx context.Context, event string, args ...any) (Args, error) {
	packet, err := parser.NewEvent(s.namespace.name, event, args)
	if err != nil {
		return Args{}, err
	}
	id, reply := s.expectAck()
	defer s.forgetAck(id)
	packet.ID, packet.HasID = id, true
	if err := s.send(packet); err != nil {
		return Args{}, err
	}
	select {
	case answer, ok := <-reply:
		if !ok {
			return Args{}, ErrDisconnected
		}
		return answer, nil
	case <-ctx.Done():
		return Args{}, ctx.Err()
	}
}

// Join adds the socket to rooms. It has no effect on a socket that is not
// connected. With Options.MaxRooms set, a call that would take the socket
// over the limit joins none of the rooms and returns ErrRoomLimit.
func (s *Socket) Join(rooms ...string) error {
	return s.namespace.rooms.join(s.id, rooms)
}

// Leave removes the socket from rooms.
func (s *Socket) Leave(rooms ...string) {
	s.namespace.rooms.leave(s.id, rooms)
}

// Rooms returns the rooms the socket is in, sorted.
func (s *Socket) Rooms() []string {
	return s.namespace.rooms.of(s.id)
}

// To starts a broadcast to rooms that skips this socket.
func (s *Socket) To(rooms ...string) BroadcastOperator {
	return s.Broadcast().To(rooms...)
}

// Broadcast starts a broadcast to every socket of the namespace but this one.
func (s *Socket) Broadcast() BroadcastOperator {
	return s.namespace.Except(s.id)
}

// Disconnect removes the socket from its namespace. With closeConnection it
// also disconnects the other namespaces of the client and closes the
// underlying connection.
func (s *Socket) Disconnect(closeConnection bool) {
	if closeConnection {
		s.conn.close()
		return
	}
	s.disconnect()
}

// Set stores a value on the socket, for example the authenticated user.
func (s *Socket) Set(key string, value any) {
	s.mu.Lock()
	s.values[key] = value
	s.mu.Unlock()
}

// Get returns a value stored with Set.
func (s *Socket) Get(key string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[key]
	return value, ok
}

func (s *Socket) send(p *parser.Packet) error {
	if !s.Connected() || s.conn.send(p) != nil {
		return ErrDisconnected
	}
	return nil
}

func (s *Socket) disconnect() {
	if s.leave(ReasonServerDisconnect) {
		s.conn.send(&parser.Packet{Type: parser.Disconnect, Namespace: s.namespace.name})
	}
}

// leave takes the socket out of its namespace and reports whether this call
// did it: only the first call does.
func (s *Socket) leave(reason Reason) bool {
	if !s.namespace.rooms.remove(s) {
		return false
	}
	s.conn.forget(s)
	s.mu.Lock()
	for id, reply := range s.acks {
		close(reply)
		delete(s.acks, id)
	}
	s.mu.Unlock()
	s.mailbox.finish(func() {
		s.mu.Lock()
		handler := s.onDisconnect
		s.mu.Unlock()
		if handler != nil {
			handler(reason)
		}
	})
	return true
}

func (s *Socket) receiveEvent(p *parser.Packet) error {
	name, args, err := parser.ParseEvent(p)
	if err != nil {
		return err
	}
	event := &Event{Socket: s, Name: name, Args: args, ackID: p.ID, wantsAck: p.HasID}
	s.mailbox.pushWait(func() { s.dispatch(event) })
	return nil
}

func (s *Socket) dispatch(event *Event) {
	s.mu.Lock()
	handler, middleware := s.handlers[event.Name], s.middleware
	s.mu.Unlock()
	for _, check := range middleware {
		if check(event) != nil {
			return
		}
	}
	if handler != nil {
		handler(event)
	}
}

func (s *Socket) receiveAck(p *parser.Packet) error {
	args, err := parser.ParseAck(p)
	if err != nil || !p.HasID {
		return err
	}
	s.mu.Lock()
	if reply, ok := s.acks[p.ID]; ok {
		delete(s.acks, p.ID)
		reply <- args
	}
	s.mu.Unlock()
	return nil
}

func (s *Socket) expectAck() (uint64, chan Args) {
	reply := make(chan Args, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextAck
	s.nextAck++
	s.acks[id] = reply
	return id, reply
}

func (s *Socket) forgetAck(id uint64) {
	s.mu.Lock()
	delete(s.acks, id)
	s.mu.Unlock()
}
