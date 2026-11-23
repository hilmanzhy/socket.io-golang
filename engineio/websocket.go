package engineio

import (
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var errProbe = errors.New("engineio: unexpected packet during upgrade")

// upgrade moves a polling session to WebSocket. Until the client confirms
// with an upgrade packet the session keeps working over polling, so a failed
// upgrade costs nothing but the WebSocket connection.
func (s *Server) upgrade(w http.ResponseWriter, r *http.Request, session *Session) {
	if !session.beginUpgrade() {
		fail(w, http.StatusBadRequest, codeBadRequest)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		session.cancelUpgrade()
		return
	}
	if err := session.probe(conn); err != nil {
		session.cancelUpgrade()
		conn.Close()
		return
	}
	session.serveWebSocket(conn, base64Requested(r))
}

func (s *Session) beginUpgrade() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != stateOpen || s.transport != Polling || s.upgrading {
		return false
	}
	s.upgrading = true
	return true
}

func (s *Session) cancelUpgrade() {
	s.mu.Lock()
	s.upgrading, s.flushing, s.conn = false, false, nil
	s.mu.Unlock()
}

// track makes conn the connection that end closes, so a session that ends
// during the probe does not leave it open until UpgradeTimeout. It reports
// false when the session has ended already.
func (s *Session) track(conn *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateClosed {
		return false
	}
	s.conn = conn
	return true
}

// probe runs the upgrade handshake: the client pings over the new
// connection, the server answers and releases the pending poll, then the
// client confirms.
func (s *Session) probe(conn *websocket.Conn) error {
	if !s.track(conn) {
		return ErrClosed
	}
	conn.SetReadLimit(s.server.opts.MaxPayload)
	deadline := time.Now().Add(s.server.opts.UpgradeTimeout)
	conn.SetReadDeadline(deadline)
	conn.SetWriteDeadline(deadline)
	if err := expectText(conn, "2probe"); err != nil {
		return err
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("3probe")); err != nil {
		return err
	}
	s.mu.Lock()
	s.flushing = true
	s.mu.Unlock()
	s.signal()
	if err := expectText(conn, "5"); err != nil {
		return err
	}
	return conn.SetReadDeadline(time.Time{})
}

func expectText(conn *websocket.Conn, want string) error {
	kind, data, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	if kind != websocket.TextMessage || string(data) != want {
		return errProbe
	}
	return nil
}

// base64Requested reports whether the b64 query parameter asks for binary
// packets as text frames. Like the reference, it takes the last value of the
// parameter and counts any value that is not empty.
func base64Requested(r *http.Request) bool {
	values := r.URL.Query()["b64"]
	return len(values) > 0 && values[len(values)-1] != ""
}

// serveWebSocket makes conn the transport of the session and reads from it
// until the session ends. With binaryAsText the session sends binary packets
// as text frames "b<base64>".
func (s *Session) serveWebSocket(conn *websocket.Conn, binaryAsText bool) {
	conn.SetReadLimit(s.server.opts.MaxPayload)
	if !s.adopt(conn) {
		conn.Close()
		return
	}
	go s.writeLoop(conn, binaryAsText)
	s.readLoop(conn)
}

func (s *Session) adopt(conn *websocket.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateClosed {
		return false
	}
	s.conn, s.transport = conn, WebSocket
	s.upgrading, s.flushing = false, false
	return true
}

func (s *Session) readLoop(conn *websocket.Conn) {
	for {
		kind, data, err := conn.ReadMessage()
		if err != nil {
			s.end(readFailure(err))
			return
		}
		if kind == websocket.BinaryMessage {
			s.deliver(Message{Binary: true, Data: data})
			continue
		}
		p, err := decodePacket(data)
		if err != nil {
			s.end(ReasonParseError)
			return
		}
		s.handle(p)
	}
}

func readFailure(err error) CloseReason {
	var closed *websocket.CloseError
	if errors.As(err, &closed) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return ReasonTransportClose
	}
	return ReasonTransportError
}

// writeLoop is the only writer of data frames. It swaps the queue for an
// empty one under the lock and writes outside of it, so a slow client never
// blocks Send.
func (s *Session) writeLoop(conn *websocket.Conn, binaryAsText bool) {
	var spare []packet
	for {
		s.mu.Lock()
		state := s.state
		batch := s.drain(spare)
		s.mu.Unlock()

		switch {
		case state == stateClosed:
			return
		case len(batch) > 0:
			if err := s.write(conn, batch, binaryAsText); err != nil {
				s.end(ReasonTransportError)
				return
			}
		case state == stateClosing:
			farewell := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
			conn.WriteControl(websocket.CloseMessage, farewell, time.Now().Add(s.server.opts.PingTimeout))
			s.end(ReasonForcedClose)
			return
		default:
			select {
			case <-s.wake:
			case <-s.done:
				return
			}
		}
		clear(batch)
		spare = batch[:0]
	}
}

func (s *Session) write(conn *websocket.Conn, batch []packet, binaryAsText bool) error {
	conn.SetWriteDeadline(time.Now().Add(s.server.opts.PingTimeout))
	for _, p := range batch {
		if p.binary {
			kind, data := websocket.BinaryMessage, p.data
			if binaryAsText {
				kind, data = websocket.TextMessage, appendPayload(nil, []packet{p})
			}
			if err := conn.WriteMessage(kind, data); err != nil {
				return err
			}
			continue
		}
		frame, err := conn.NextWriter(websocket.TextMessage)
		if err != nil {
			return err
		}
		frame.Write(p.prefix())
		frame.Write(p.data)
		if err := frame.Close(); err != nil {
			return err
		}
	}
	return nil
}
