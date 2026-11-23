package engineio

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"runtime/debug"
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

var errCallbackPanic = errors.New("engineio: callback panic")

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

// Options configures a Server. The zero value is ready to use. A duration or a
// size that is zero or negative means its default.
type Options struct {
	// PingInterval is the pause between two heartbeats. Default 25 s.
	PingInterval time.Duration
	// PingTimeout is how long a client may take to answer a ping, to accept a
	// write, or to collect its last packets on close. Default 20 s.
	PingTimeout time.Duration
	// UpgradeTimeout bounds the switch from polling to WebSocket. Default 10 s.
	UpgradeTimeout time.Duration
	// MaxPayload caps one polling request body and one WebSocket message, in
	// bytes. It also bounds one polling response body: the response carries
	// as many queued packets as fit, and at least one, so a single packet
	// larger than MaxPayload is sent alone. Default 1 MB.
	MaxPayload int64
	// MaxBuffer caps the bytes queued for a client that is not reading. A
	// session that exceeds it is closed. Default 16 MiB.
	MaxBuffer int64
	// MaxSessions caps the sessions open at the same time. A handshake that
	// would exceed it is refused with status 503. Zero or negative means no
	// limit. The cap is global: one client may open sessions until it is
	// reached, so pair it with a limit per client in AllowRequest.
	MaxSessions int
	// Transports lists the enabled transports. Default: polling and WebSocket.
	Transports []Transport
	// CheckOrigin decides whether a cross-origin request is served; allowed
	// origins also receive credentialed CORS headers. Same-origin requests and
	// requests without an Origin header are always served. Nil rejects every
	// cross-origin request. A panic is recovered, logged to Logger, and
	// counts as a refusal.
	CheckOrigin func(*http.Request) bool
	// AllowRequest can refuse a handshake before a session exists, for
	// example to rate limit. A non-nil error answers 403. A panic is
	// recovered, logged to Logger, and refuses the handshake the same way.
	AllowRequest func(*http.Request) error
	// Logger receives the panics of CheckOrigin and AllowRequest. Nil means
	// slog.Default().
	Logger *slog.Logger
}

// withDefaults replaces every duration and size that is zero or negative with
// its default.
func (o Options) withDefaults() Options {
	o.PingInterval = cmp.Or(max(o.PingInterval, 0), 25*time.Second)
	o.PingTimeout = cmp.Or(max(o.PingTimeout, 0), 20*time.Second)
	o.UpgradeTimeout = cmp.Or(max(o.UpgradeTimeout, 0), 10*time.Second)
	o.MaxPayload = cmp.Or(max(o.MaxPayload, 0), 1_000_000)
	o.MaxBuffer = cmp.Or(max(o.MaxBuffer, 0), 16<<20)
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
	slots    int
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
	if s.opts.CheckOrigin == nil || !protect(s, "CheckOrigin", false, func() bool { return s.opts.CheckOrigin(r) }) {
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

// handshake opens a session over transport. A WebSocket whose session is not
// opened, because accept panicked, is closed before the panic goes on: the
// HTTP server does not close a connection it has handed over.
func (s *Server) handshake(w http.ResponseWriter, r *http.Request, transport Transport) {
	if r.Method != http.MethodGet {
		fail(w, http.StatusBadRequest, codeBadHandshakeMethod)
		return
	}
	if s.opts.AllowRequest != nil && protect(s, "AllowRequest", errCallbackPanic, func() error { return s.opts.AllowRequest(r) }) != nil {
		fail(w, http.StatusForbidden, codeForbidden)
		return
	}
	if !s.reserve() {
		fail(w, http.StatusServiceUnavailable, codeForbidden)
		return
	}
	if transport == Polling {
		s.open(r, Polling).servePolling(w, r)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.release()
		return
	}
	opened := false
	defer func() {
		if !opened {
			conn.Close()
		}
	}()
	session := s.open(r, WebSocket)
	opened = true
	session.serveWebSocket(conn, base64Requested(r))
}

// protect calls call and returns its result. A panic in call is recovered and
// logged as the failure of the named callback, and protect returns fallback.
func protect[T any](s *Server, callback string, fallback T, call func() T) (result T) {
	defer func() {
		if value := recover(); value != nil {
			s.logger().Error("engineio: callback panic",
				"callback", callback, "panic", value, "stack", string(debug.Stack()))
			result = fallback
		}
	}()
	return call()
}

func (s *Server) logger() *slog.Logger {
	return cmp.Or(s.opts.Logger, slog.Default())
}

// reserve claims a session slot, or reports that MaxSessions sessions exist
// already. The slot belongs to the session that open creates and is returned
// by forget, or by release when the session is never created.
func (s *Server) reserve() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opts.MaxSessions > 0 && s.slots >= s.opts.MaxSessions {
		return false
	}
	s.slots++
	return true
}

func (s *Server) release() {
	s.mu.Lock()
	s.slots--
	s.mu.Unlock()
}

// open creates the session of a reserved slot and registers it. A panic in
// accept propagates, but ends the session first, which returns the slot. A
// session that ended while accept or the heartbeat set-up ran is not
// registered: the check and the insertion hold the server lock and the session
// lock together, and end takes the server lock only after it marks the session
// closed, so forget always follows a registration it must undo.
func (s *Server) open(r *http.Request, transport Transport) *Session {
	session := newSession(s, r, transport)
	session.receiving.Lock()
	accepted := false
	defer func() {
		if !accepted {
			session.receiving.Unlock()
			session.end(ReasonTransportError)
		}
	}()
	session.receiver = s.accept(session)
	accepted = true
	session.receiving.Unlock()
	session.startHeartbeat()

	s.mu.Lock()
	closed := s.closed
	if !closed {
		session.mu.Lock()
		if session.state != stateClosed {
			s.sessions[session.id] = session
		}
		session.mu.Unlock()
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
	s.slots--
	s.mu.Unlock()
}
