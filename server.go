package socketio

import (
	"cmp"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hilmanzhy/socket.io-golang/engineio"
	"github.com/hilmanzhy/socket.io-golang/parser"
)

// Options configures a Server. The zero value is ready to use.
type Options struct {
	// Engine configures the transport layer: heartbeat, enabled transports,
	// payload limits and the origin check.
	Engine engineio.Options
	// ConnectTimeout is how long a connection may stay open without joining
	// a namespace. Zero or a negative value means the default, 45 s.
	ConnectTimeout time.Duration
	// MaxAttachments caps the binary attachments of one received packet.
	// Zero or a negative value means the default, 10.
	MaxAttachments int
	// MaxArguments caps the arguments of one received event or
	// acknowledgement; the event name counts as one. Zero or a negative value
	// means the default, 64.
	MaxArguments int
	// MaxRooms caps the rooms one socket may be in, not counting the room
	// named after its ID. Zero or a negative value means no limit.
	MaxRooms int
	// EventBacklog is how many received events one socket may have waiting
	// for its handlers before its connection stops being read. Zero or a
	// negative value means the default, 64.
	EventBacklog int
	// Adapter relays broadcasts to the other servers of a cluster. Nil means
	// a single server.
	Adapter Adapter
	// Logger receives the panics of handlers, the malformed packets that end
	// a connection and, unless Engine.Logger is set, the panics of the Engine
	// callbacks. Default slog.Default().
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	o.ConnectTimeout = cmp.Or(max(o.ConnectTimeout, 0), 45*time.Second)
	o.MaxAttachments = cmp.Or(o.MaxAttachments, parser.DefaultMaxAttachments)
	o.MaxArguments = cmp.Or(o.MaxArguments, parser.DefaultMaxArguments)
	o.EventBacklog = cmp.Or(max(o.EventBacklog, 0), 64)
	o.Logger = cmp.Or(o.Logger, slog.Default())
	o.Engine.Logger = cmp.Or(o.Engine.Logger, o.Logger)
	return o
}

// Server is a Socket.IO server and an http.Handler. It embeds its main
// namespace "/", so server.Emit, server.To and server.OnConnect address the
// clients that connected without naming a namespace.
type Server struct {
	*Namespace

	opts     Options
	engine   *engineio.Server
	shutdown func() error

	mu         sync.RWMutex
	namespaces map[string]*Namespace
}

// New returns a Server. Mount it under the path the clients use, which is
// "/socket.io/" unless they are configured otherwise.
func New(opts Options) *Server {
	s := &Server{opts: opts.withDefaults(), namespaces: make(map[string]*Namespace)}
	s.Namespace = s.Of(parser.DefaultNamespace)
	s.engine = engineio.NewServer(s.opts.Engine, s.accept)
	s.shutdown = sync.OnceValue(s.stop)
	if s.opts.Adapter != nil {
		s.opts.Adapter.Subscribe(s.applyRemote)
	}
	return s
}

// ServeHTTP serves the Engine.IO endpoint.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.engine.ServeHTTP(w, r)
}

// Of returns the namespace called name, creating it on first use. Clients
// can only connect to namespaces that exist. A name that contains a comma is
// accepted, but no client can reach it, because the Socket.IO packet format
// ends the namespace at the first comma.
func (s *Server) Of(name string) *Namespace {
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	namespace, ok := s.namespaces[name]
	if !ok {
		namespace = &Namespace{name: name, server: s, rooms: newRegistry(s.opts.MaxRooms)}
		s.namespaces[name] = namespace
	}
	return namespace
}

// Close disconnects every client with ReasonServerShutdown, then closes the
// adapter. It does not stop the HTTP server that serves the handler. Only the
// first call has an effect; the following ones return its result.
func (s *Server) Close() error {
	return s.shutdown()
}

func (s *Server) stop() error {
	s.engine.Close()
	if s.opts.Adapter == nil {
		return nil
	}
	return s.opts.Adapter.Close()
}

func (s *Server) accept(session *engineio.Session) engineio.Receiver {
	return newConn(s, session)
}

func (s *Server) lookup(name string) *Namespace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespaces[name]
}

func (s *Server) recovered(value any, stack []byte) {
	s.opts.Logger.Error("socketio: handler panic", "panic", value, "stack", string(stack))
}
