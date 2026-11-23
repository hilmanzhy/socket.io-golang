package engineio

import (
	"context"
	"crypto/rand"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type state uint8

const (
	stateOpen state = iota
	stateClosing
	stateClosed
)

// Session is one Engine.IO connection. Its methods are safe for concurrent
// use.
type Session struct {
	id      string
	server  *Server
	request *http.Request
	wake    chan struct{}
	done    chan struct{}

	receiving sync.Mutex
	receiver  Receiver

	mu           sync.Mutex
	state        state
	transport    Transport
	queue        []packet
	buffered     int64
	conn         *websocket.Conn
	farewell     kind
	polling      bool
	posting      bool
	upgrading    bool
	flushing     bool
	awaitingPong bool
	beat         uint64
	timer        *time.Timer
}

func newSession(server *Server, r *http.Request, transport Transport) *Session {
	request := r.Clone(context.WithoutCancel(r.Context()))
	request.Body = http.NoBody
	id := rand.Text()
	return &Session{
		id:        id,
		server:    server,
		request:   request,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		transport: transport,
		queue:     []packet{server.openPacket(id, transport)},
		farewell:  kindClose,
	}
}

// ID returns the session identifier, which the client must keep secret.
func (s *Session) ID() string {
	return s.id
}

// Request returns the handshake request, cloned when the session opened and
// stripped of its body. Every call returns the same value: do not modify it.
func (s *Session) Request() *http.Request {
	return s.request
}

// Transport returns the transport the session currently uses.
func (s *Session) Transport() Transport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transport
}

// Done is closed once the session has ended.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// Send queues messages for the client. They are written in order and stay
// together, so no other Send is interleaved with them.
func (s *Session) Send(messages ...Message) error {
	size := int64(0)
	for _, m := range messages {
		size += int64(len(m.Data))
	}
	s.mu.Lock()
	if s.state != stateOpen {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.buffered > 0 && s.buffered+size > s.server.opts.MaxBuffer {
		s.mu.Unlock()
		s.end(ReasonTransportError)
		return ErrClosed
	}
	s.buffered += size
	for _, m := range messages {
		s.queue = append(s.queue, packet{kind: kindMessage, binary: m.Binary, data: m.Data})
	}
	s.mu.Unlock()
	s.signal()
	return nil
}

// Close ends the session gracefully: the Receiver learns the reason at once,
// while the client gets up to PingTimeout to collect what is still queued.
func (s *Session) Close(reason CloseReason) {
	s.mu.Lock()
	if s.state != stateOpen {
		s.mu.Unlock()
		return
	}
	s.state = stateClosing
	s.arm(s.server.opts.PingTimeout, func() { s.end(reason) })
	s.mu.Unlock()
	go s.notify(reason)
	s.signal()
}

// end closes the session immediately. The reason reaches the Receiver unless
// Close has reported one already.
func (s *Session) end(reason CloseReason) {
	s.mu.Lock()
	if s.state == stateClosed {
		s.mu.Unlock()
		return
	}
	wasOpen := s.state == stateOpen
	s.state, s.queue, s.buffered = stateClosed, nil, 0
	if reason == ReasonTransportClose {
		s.farewell = kindNoop
	}
	if s.timer != nil {
		s.timer.Stop()
	}
	conn := s.conn
	s.mu.Unlock()

	s.server.forget(s)
	close(s.done)
	if conn != nil {
		conn.Close()
	}
	if wasOpen {
		go s.notify(reason)
	}
}

// notify waits for the Receive in flight, then reports the end of the
// session. It runs on its own goroutine because end and Close may be called
// from inside Receive. A session whose accept panicked has no Receiver and
// reports nothing.
func (s *Session) notify(reason CloseReason) {
	s.receiving.Lock()
	receiver := s.receiver
	s.receiving.Unlock()
	if receiver != nil {
		receiver.Closed(reason)
	}
}

func (s *Session) deliver(m Message) {
	s.receiving.Lock()
	defer s.receiving.Unlock()
	if !s.isOpen() {
		return
	}
	if err := s.receiver.Receive(m); err != nil {
		s.end(ReasonParseError)
	}
}

func (s *Session) handle(p packet) {
	switch p.kind {
	case kindMessage:
		s.deliver(Message{Binary: p.binary, Data: p.data})
	case kindPong:
		s.pong()
	case kindClose:
		s.end(ReasonTransportClose)
	case kindPing:
		s.end(ReasonTransportError)
	}
}

func (s *Session) isOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == stateOpen
}

func (s *Session) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// drain hands the queue to the caller and leaves spare in its place. The
// caller holds s.mu.
func (s *Session) drain(spare []packet) []packet {
	batch := s.queue
	s.queue, s.buffered = spare, 0
	return batch
}

// takeFront removes from the queue the packets that fit in one polling payload
// of limit bytes and returns them, leaving the rest queued in order. The
// caller holds s.mu.
func (s *Session) takeFront(limit int64) []packet {
	n := fitting(s.queue, limit)
	if n == len(s.queue) {
		return s.drain(nil)
	}
	batch := slices.Clone(s.queue[:n])
	clear(s.queue[:n])
	s.queue = s.queue[n:]
	s.buffered -= messageBytes(batch)
	return batch
}

// messageBytes totals the data of the messages among packets, which is what
// Session.buffered counts.
func messageBytes(packets []packet) int64 {
	size := int64(0)
	for _, p := range packets {
		if p.kind == kindMessage {
			size += int64(len(p.data))
		}
	}
	return size
}

// arm replaces the session timer; a timer that was replaced never runs its
// task. The caller holds s.mu.
func (s *Session) arm(after time.Duration, task func()) {
	if s.timer != nil {
		s.timer.Stop()
	}
	s.beat++
	beat := s.beat
	s.timer = time.AfterFunc(after, func() {
		s.mu.Lock()
		current := s.beat == beat
		s.mu.Unlock()
		if current {
			task()
		}
	})
}

func (s *Session) startHeartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateOpen {
		s.arm(s.server.opts.PingInterval, s.ping)
	}
}

func (s *Session) ping() {
	s.mu.Lock()
	if s.state != stateOpen {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, packet{kind: kindPing})
	s.awaitingPong = true
	s.arm(s.server.opts.PingTimeout, func() { s.end(ReasonPingTimeout) })
	s.mu.Unlock()
	s.signal()
}

func (s *Session) pong() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateOpen && s.awaitingPong {
		s.awaitingPong = false
		s.arm(s.server.opts.PingInterval, s.ping)
	}
}
