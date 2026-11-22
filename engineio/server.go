package engineio

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Transport names a way of carrying packets between client and server.
type Transport string

// Transports this package implements.
const (
	Polling   Transport = "polling"
	WebSocket Transport = "websocket"
)

// CloseReason tells a Receiver why its session ended.
type CloseReason string

// Reasons a session ends. The strings match the ones Socket.IO reports.
const (
	ReasonTransportClose CloseReason = "transport close"
	ReasonTransportError CloseReason = "transport error"
	ReasonPingTimeout    CloseReason = "ping timeout"
	ReasonParseError     CloseReason = "parse error"
	ReasonForcedClose    CloseReason = "forced close"
	ReasonServerShutdown CloseReason = "server shutting down"
)

// ErrClosed is returned by Session.Send once the session is closing or closed.
var ErrClosed = errors.New("engineio: session closed")

// Receiver consumes what one session receives. Receive is never called
// concurrently for a session, and Closed is called exactly once, after the
// last Receive has returned.
type Receiver interface {
	// Receive handles one message. A non-nil error closes the session with
	// ReasonParseError.
	Receive(Message) error
	// Closed reports that the session ended and why.
	Closed(CloseReason)
}

// Options configures a Server. The zero value is ready to use.
type Options struct {
	// PingInterval is the pause between two heartbeats. Default 25 s.
	PingInterval time.Duration
	// PingTimeout is how long a client may take to answer a ping, to accept a
	// write, or to collect its last packets on close. Default 20 s.
	PingTimeout time.Duration
	// UpgradeTimeout bounds the switch from polling to WebSocket. Default 10 s.
	UpgradeTimeout time.Duration
	// MaxPayload caps one polling request body and one WebSocket message, in
	// bytes. Default 1 MB.
	MaxPayload int64
	// MaxBuffer caps the bytes queued for a client that is not reading. A
	// session that exceeds it is closed. Default 16 MiB.
	MaxBuffer int64
	// Transports lists the enabled transports. Default: polling and WebSocket.
	Transports []Transport
	// CheckOrigin decides whether a cross-origin request is served; allowed
	// origins also receive credentialed CORS headers. Same-origin requests and
	// requests without an Origin header are always served. Nil rejects every
	// cross-origin request.
	CheckOrigin func(*http.Request) bool
	// AllowRequest can refuse a handshake before a session exists, for
	// example to rate limit. A non-nil error answers 403.
	AllowRequest func(*http.Request) error
}

func (o Options) withDefaults() Options {
	o.PingInterval = cmp.Or(o.PingInterval, 25*time.Second)
	o.PingTimeout = cmp.Or(o.PingTimeout, 20*time.Second)
	o.UpgradeTimeout = cmp.Or(o.UpgradeTimeout, 10*time.Second)
	o.MaxPayload = cmp.Or(o.MaxPayload, 1_000_000)
	o.MaxBuffer = cmp.Or(o.MaxBuffer, 16<<20)
	if len(o.Transports) == 0 {
		o.Transports = []Transport{Polling, WebSocket}
	}
	return o
}

// AllowOrigins returns a CheckOrigin function that accepts exactly the given
// origins, written as the browser sends them: "https://app.example.com".
func AllowOrigins(origins ...string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		return slices.Contains(origins, r.Header.Get("Origin"))
	}
}

const protocolVersion = "4"

const (
	codeUnknownTransport = iota
	codeUnknownSession
	codeBadHandshakeMethod
	codeBadRequest
	codeForbidden
	codeUnsupportedVersion
)

var failures = [...]string{
	codeUnknownTransport:   "Transport unknown",
	codeUnknownSession:     "Session ID unknown",
	codeBadHandshakeMethod: "Bad handshake method",
	codeBadRequest:         "Bad request",
	codeForbidden:          "Forbidden",
	codeUnsupportedVersion: "Unsupported protocol version",
}

func fail(w http.ResponseWriter, status, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"code":%d,"message":%q}`, code, failures[code])
}

// Server is an http.Handler that speaks Engine.IO v4.
type Server struct {
	opts     Options
	accept   func(*Session) Receiver
	upgrader websocket.Upgrader

	mu       sync.Mutex
	sessions map[string]*Session
	closed   bool
}

// NewServer returns a Server that calls accept for every new session and
// hands the session's messages to the Receiver that accept returns.
func NewServer(opts Options, accept func(*Session) Receiver) *Server {
	return &Server{
		opts:   opts.withDefaults(),
		accept: accept,
		upgrader: websocket.Upgrader{
			WriteBufferPool: &sync.Pool{},
			CheckOrigin:     func(*http.Request) bool { return true },
		},
		sessions: make(map[string]*Session),
	}
}

// ServeHTTP routes a request to the handshake, to a polling session or to a
// WebSocket upgrade.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrigin(w, r) {
		fail(w, http.StatusForbidden, codeForbidden)
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	query := r.URL.Query()
	if query.Get("EIO") != protocolVersion {
		fail(w, http.StatusBadRequest, codeUnsupportedVersion)
		return
	}
	transport := Transport(query.Get("transport"))
	if !slices.Contains(s.opts.Transports, transport) {
		fail(w, http.StatusBadRequest, codeUnknownTransport)
		return
	}
	sid := query.Get("sid")
	if sid == "" {
		s.handshake(w, r, transport)
		return
	}
	session := s.lookup(sid)
	switch {
	case session == nil:
		fail(w, http.StatusBadRequest, codeUnknownSession)
	case transport == WebSocket:
		s.upgrade(w, r, session)
	default:
		session.servePolling(w, r)
	}
}

// Close closes every session with ReasonServerShutdown and makes the server
// end any session opened afterwards. It does not wait for the clients.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	sessions := slices.Collect(maps.Values(s.sessions))
	s.mu.Unlock()
	for _, session := range sessions {
		session.Close(ReasonServerShutdown)
	}
}

func (s *Server) allowOrigin(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || sameOrigin(origin, r.Host) {
		return true
	}
	if s.opts.CheckOrigin == nil || !s.opts.CheckOrigin(r) {
		return false
	}
	header := w.Header()
	header.Set("Access-Control-Allow-Origin", origin)
	header.Set("Access-Control-Allow-Credentials", "true")
	header.Add("Vary", "Origin")
	if r.Method == http.MethodOptions {
		header.Set("Access-Control-Allow-Methods", "GET, POST")
		header.Set("Access-Control-Max-Age", "86400")
		if requested := r.Header.Get("Access-Control-Request-Headers"); requested != "" {
			header.Set("Access-Control-Allow-Headers", requested)
		}
	}
	return true
}

func sameOrigin(origin, host string) bool {
	parsed, err := url.Parse(origin)
	return err == nil && strings.EqualFold(parsed.Host, host)
}

func (s *Server) handshake(w http.ResponseWriter, r *http.Request, transport Transport) {
	if r.Method != http.MethodGet {
		fail(w, http.StatusBadRequest, codeBadHandshakeMethod)
		return
	}
	if s.opts.AllowRequest != nil && s.opts.AllowRequest(r) != nil {
		fail(w, http.StatusForbidden, codeForbidden)
		return
	}
	if transport == Polling {
		s.open(r, Polling).servePolling(w, r)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.open(r, WebSocket).serveWebSocket(conn)
}

func (s *Server) open(r *http.Request, transport Transport) *Session {
	session := newSession(s, r, transport)
	session.receiving.Lock()
	session.receiver = s.accept(session)
	session.receiving.Unlock()
	session.startHeartbeat()

	s.mu.Lock()
	closed := s.closed
	if !closed {
		s.sessions[session.id] = session
	}
	s.mu.Unlock()
	if closed {
		session.end(ReasonServerShutdown)
	}
	return session
}

func (s *Server) openPacket(id string, transport Transport) packet {
	upgrades := ""
	if transport == Polling && slices.Contains(s.opts.Transports, WebSocket) {
		upgrades = `"websocket"`
	}
	data := fmt.Appendf(nil, `{"sid":%q,"upgrades":[%s],"pingInterval":%d,"pingTimeout":%d,"maxPayload":%d}`,
		id, upgrades, s.opts.PingInterval.Milliseconds(), s.opts.PingTimeout.Milliseconds(), s.opts.MaxPayload)
	return packet{kind: kindOpen, data: data}
}

func (s *Server) lookup(id string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *Server) forget(session *Session) {
	s.mu.Lock()
	delete(s.sessions, session.id)
	s.mu.Unlock()
}
