package socketio

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

// conn multiplexes the namespaces of one client over one Engine.IO session.
// Its timer runs exactly while the session is open and has no socket, so a
// connection cannot stay open for longer than ConnectTimeout without a
// namespace, whether it never joined one or left the last one.
type conn struct {
	server  *Server
	session *engineio.Session
	decoder parser.Decoder

	mu      sync.Mutex
	sockets map[string]*Socket
	timer   *time.Timer
	closed  bool
}

func newConn(server *Server, session *engineio.Session) *conn {
	c := &conn{
		server:  server,
		session: session,
		decoder: parser.Decoder{
			MaxAttachments: server.opts.MaxAttachments,
			MaxArguments:   server.opts.MaxArguments,
		},
		sockets: make(map[string]*Socket),
	}
	c.timer = time.AfterFunc(server.opts.ConnectTimeout, c.expire)
	return c
}

// Receive decodes one frame and routes the packet it completes, if any.
func (c *conn) Receive(m engineio.Message) error {
	decode := c.decoder.Text
	if m.Binary {
		decode = c.decoder.Binary
	}
	packet, err := decode(m.Data)
	if err != nil || packet == nil {
		return c.reject(err)
	}
	return c.reject(c.route(packet))
}

// reject logs the error that is about to end the connection, if there is one,
// and returns it.
func (c *conn) reject(err error) error {
	if err != nil {
		c.server.opts.Logger.Warn("socketio: malformed packet", "error", err)
	}
	return err
}

// Closed disconnects every socket once the session has ended.
func (c *conn) Closed(reason engineio.CloseReason) {
	c.mu.Lock()
	c.closed = true
	c.timer.Stop()
	c.mu.Unlock()
	for _, socket := range c.all() {
		socket.leave(Reason(reason))
	}
}

func (c *conn) route(p *parser.Packet) error {
	switch p.Type {
	case parser.Connect:
		return c.connect(p)
	case parser.ConnectError:
		return fmt.Errorf("%w: CONNECT_ERROR sent by a client", parser.ErrMalformed)
	}
	socket := c.socket(p.Namespace)
	if socket == nil {
		return nil
	}
	switch p.Type {
	case parser.Event, parser.BinaryEvent:
		return socket.receiveEvent(p)
	case parser.Ack, parser.BinaryAck:
		return socket.receiveAck(p)
	default:
		socket.leave(ReasonClientDisconnect)
		return nil
	}
}

// connect admits the client to a namespace. The order matters: the CONNECT
// answer goes out before the socket becomes a broadcast target, and the
// connection handler is queued before any event of the socket.
func (c *conn) connect(p *parser.Packet) error {
	if c.socket(p.Namespace) != nil {
		return fmt.Errorf("%w: namespace joined twice", parser.ErrMalformed)
	}
	namespace := c.server.lookup(p.Namespace)
	if namespace == nil {
		return c.refuse(p.Namespace, &ConnectError{Message: "Invalid namespace"})
	}
	socket := newSocket(namespace, c, p.Data)
	if err := namespace.admit(socket); err != nil {
		return c.refuse(p.Namespace, err)
	}

	c.mu.Lock()
	c.sockets[namespace.name] = socket
	c.timer.Stop()
	c.mu.Unlock()

	answer := &parser.Packet{Type: parser.Connect, Namespace: namespace.name, Data: fmt.Appendf(nil, `{"sid":%q}`, socket.id)}
	if err := c.send(answer); err != nil {
		c.forget(socket)
		return nil
	}
	namespace.rooms.add(socket)
	socket.mailbox.push(func() { namespace.welcome(socket) })
	return nil
}

func (c *conn) refuse(namespace string, cause error) error {
	payload := struct {
		Message string `json:"message"`
		Data    any    `json:"data,omitempty"`
	}{Message: cause.Error()}
	var detail *ConnectError
	if errors.As(cause, &detail) {
		payload.Data = detail.Data
	}
	data, err := json.Marshal(payload)
	if err != nil {
		data = fmt.Appendf(nil, `{"message":%q}`, errInternal.Error())
	}
	c.send(&parser.Packet{Type: parser.ConnectError, Namespace: namespace, Data: data})
	return nil
}

func (c *conn) send(p *parser.Packet) error {
	return c.session.Send(frames(p)...)
}

// close disconnects every namespace of the client, then the session.
func (c *conn) close() {
	for _, socket := range c.all() {
		socket.disconnect()
	}
	c.session.Close(engineio.ReasonForcedClose)
}

func (c *conn) expire() {
	c.mu.Lock()
	idle := len(c.sockets) == 0
	c.mu.Unlock()
	if idle {
		c.session.Close(engineio.ReasonForcedClose)
	}
}

func (c *conn) socket(namespace string) *Socket {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sockets[namespace]
}

func (c *conn) all() []*Socket {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Collect(maps.Values(c.sockets))
}

// forget drops a socket and restarts the timer when it was the last one of
// an open session.
func (c *conn) forget(socket *Socket) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sockets[socket.namespace.name] != socket {
		return
	}
	delete(c.sockets, socket.namespace.name)
	if len(c.sockets) == 0 && !c.closed {
		c.timer.Reset(c.server.opts.ConnectTimeout)
	}
}

// frames returns the wire form of a packet: its text frame followed by one
// binary frame per attachment.
func frames(p *parser.Packet) []engineio.Message {
	messages := make([]engineio.Message, 1, 1+len(p.Attachments))
	messages[0] = engineio.Message{Data: p.Encode()}
	for _, attachment := range p.Attachments {
		messages = append(messages, engineio.Message{Binary: true, Data: attachment})
	}
	return messages
}
